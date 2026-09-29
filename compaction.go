package kv

// compactCrashHook 仅供测试使用：在回收协议的各阶段被调用。
// 生产代码中恒为 nil（零开销、零分支语义变化）；测试子进程里把它
// 设为 os.Exit 以模拟 SIGKILL，验证重启只能落在"回收前/回收后"
// 两个完整状态之一。阶段：
//
//	snap-wrote         新快照 tmp 已写完并 fsync，尚未 rename
//	snap-installed     快照已 rename 落盘，MANIFEST 尚未提交
//	manifest-committed 新 MANIFEST 已原子替换（回收提交点之后）
//	cleaned            旧文件删除完毕
var compactCrashHook func(stage string)

// Compact 在写锁下执行一次历史数据回收（快照压缩）。
//
// 回收协议（每一步都崩溃安全）：
//  1. 取写锁（与一切读/写互斥；回收期间进行的读会在锁外等待，
//     因此绝不会看到"已失效但尚未被新状态覆盖"的旧数据）。
//  2. 用当前内存索引生成 snap-N.tmp，内含每个键的最新版本；
//     墓碑按 dropBeforeSeq 规则安全丢弃，写完 fsync。
//  3. 滚动新活动段（旧段自此不可变）。
//  4. 把新 MANIFEST（引用新快照 + 新尾巴集合）原子 rename 落盘——
//     这是回收唯一的"提交点"：此前崩溃=回收前完整状态，
//     此后崩溃=回收后完整状态，不存在卡在中间的可能。
//  5. 删除旧快照与被合并的旧段，并 fsync 目录。
//
// 回收后重放集合 = 新快照（状态镜像）+ 新活动段，读取仍然是
// 纯内存 map 查询，耗时不随历史写入量增长。
func (db *DB) Compact() error {
	if err := db.fatalErr(); err != nil {
		return err
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed.Load() {
		return ErrClosed
	}

	dir := db.opts.Dir
	old := db.manifest

	// dropBeforeSeq：当前尾巴中最早的记录序号。
	// 墓碑 seq < 该值时，尾巴里不可能再出现比它更旧的同键记录，
	// 压进快照后无需再保留压制作用。
	var dropBeforeSeq uint64
	for _, t := range old.Tails {
		if t.MinSeq > 0 && (dropBeforeSeq == 0 || t.MinSeq < dropBeforeSeq) {
			dropBeforeSeq = t.MinSeq
		}
	}

	// 步骤 2：新快照先写临时文件并 fsync；此时它尚未被任何清单引用，
	// 崩溃后只是一个孤儿 .tmp，恢复时删除，状态仍是回收前。
	snapID := old.NextSnap
	tmpSnap := snapPathOf(dir, snapID) + tmpSuffix
	finalSnap := snapName(snapID)
	if err := writeSnapshot(tmpSnap, db.lastSeq, db.index, dropBeforeSeq); err != nil {
		return err
	}
	if compactCrashHook != nil {
		compactCrashHook("snap-wrote")
	}

	// 步骤 3：滚动新活动段。若当前活动段已有记录，新段 ID = active+1，
	// 旧活动段作为最后的尾巴一并并入快照；若活动段为空（例如
	// 上次压缩后没有新写入），直接复用它作为压缩后的唯一段。
	newActiveID := old.ActiveID
	var w *walWriter
	if db.active.size > 0 {
		newActiveID = old.ActiveID + 1
		if err := db.active.close(); err != nil {
			return err
		}
		var err error
		w, err = openWALWriter(dir, newActiveID)
		if err != nil {
			return err
		}
	}
	// 把临时快照改名为最终名并 fsync 目录，确保提交清单时文件内容与
	// 目录项都已持久（rename 原子；崩溃只会有/无该文件两种结果）。
	if err := syncRename(dir, tmpSnap, snapPathOf(dir, snapID)); err != nil {
		if w != nil {
			w.close()
		}
		return err
	}
	if compactCrashHook != nil {
		compactCrashHook("snap-installed")
	}

	// 步骤 4：提交新清单。这一行 rename + fsync 之后，
	// 数据库就是"回收后的完整状态"，且内容与回收前逐键一致。
	nm := &manifestData{
		Version:  manifestVersion,
		Snapshot: finalSnap,
		SnapSeq:  db.lastSeq,
		Tails:    []walFileInfo{{ID: newActiveID}},
		ActiveID: newActiveID,
		NextSnap: snapID + 1,
	}
	if err := writeManifest(dir, nm); err != nil {
		// 清单没换成：快照是孤儿，段文件维持原集合；
		// 重新打开旧活动句柄，保证进程内可继续使用。
		if w != nil {
			w.close()
		}
		ow, oerr := openWALWriter(dir, old.ActiveID)
		if oerr == nil {
			db.active = ow
		}
		return err
	}

	if w != nil {
		db.active = w
	}
	db.manifest = nm
	if compactCrashHook != nil {
		compactCrashHook("manifest-committed")
	}

	// 步骤 5：删除已被快照取代的旧段和旧快照（新清单的提交点之后，
	// 删除是否完成不再影响正确性；但为了立刻回收磁盘并让目录状态
	// 可预期，逐项删除并同步目录）。
	keep := db.referencedFiles()
	entries, err := readDirNames(dir)
	if err != nil {
		return err
	}
	for _, name := range entries {
		if keep[name] || name == manifestName {
			continue
		}
		if isWALName(name) || isSnapName(name) {
			if err := removeSynced(dir, name); err != nil {
				return err
			}
		}
	}
	if compactCrashHook != nil {
		compactCrashHook("cleaned")
	}
	return nil
}

// readDirNames 返回目录下所有条目名（含目录与文件，调用方自行过滤）。
func readDirNames(dir string) ([]string, error) {
	entries, err := readDirEntries(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}
