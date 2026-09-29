package kv

import (
	"io"
	"os"
)

// walWriter 是当前活动 WAL 段的追加句柄。
type walWriter struct {
	id     uint64
	path   string
	f      *os.File
	size   int64
	minSeq uint64 // 本文件第一条记录的序号，0 表示仍为空文件
}

// openWALWriter 创建或打开一个用于追加的 WAL 段。
// 调用方负责保证恢复时已经对该文件完成了尾部修复。
func openWALWriter(dir string, id uint64) (*walWriter, error) {
	path := walPathOf(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &walWriter{id: id, path: path, f: f, size: st.Size()}, nil
}

// append 把一整帧原子地写入段文件：单次 Write 调用，配合 O_APPEND。
// fsync=true 时写入后立即同步，返回成功即代表该提交可在强杀后存活。
// 失败时不更新内存索引，文件里至多留下最后一帧的残缺前缀（恢复时丢弃）。
func (w *walWriter) append(rec []byte, fsync bool) error {
	n, err := w.f.Write(rec)
	if err != nil {
		return err
	}
	if n != len(rec) {
		// 普通文件的单次 Write 通常写全部；若发生短写则按 I/O 失败处理，
		// 绝不继续更新内存状态。
		return io.ErrShortWrite
	}
	w.size += int64(n)
	if fsync {
		if err := fsyncFile(w.f); err != nil {
			return err
		}
	}
	return nil
}

// close 同步并关闭段文件。即便是滚动产生的"已封存"段，
// 关闭时也 Sync 一次，让其内容在后续被 MANIFEST 引用前确定落盘。
func (w *walWriter) close() error {
	if w.f == nil {
		return nil
	}
	err := fsyncFile(w.f)
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

// walVisitor 按序号顺序接收重放的记录。
type walVisitor func(rec *record) error

// tornReport 报告重放时对活动段尾部做的修复。
type tornReport struct {
	repaired     bool
	droppedBytes int64
}

// replayWAL 顺序重放一个 WAL 段。
//
// 判定规则（必须与"静默跳过坏数据"严格区分）：
//   - 文件末尾存在凑不齐整帧的残留字节：KindIncompleteTail。
//     仅当 isActive && canRepair 时，把文件物理截断到最后完整帧边界并返回修复报告；
//     非活动段出现此情况同样是损坏（活动段滚动后就应是不可变的）。
//   - 帧长度声称完整但 magic/CRC/字段不一致：*CorruptionError(KindChecksumMismatch)，
//     无论出现在哪里一律失败，绝不跳过。
//   - 序号回退由 visit 回调抛出 KindSequenceRegression。
func replayWAL(path string, isActive, canRepair bool, visit walVisitor) (tornReport, error) {
	var rep tornReport
	data, err := os.ReadFile(path)
	if err != nil {
		return rep, err
	}
	off := 0
	for off < len(data) {
		rec, n, derr := decodeRecord(data[off:])
		if derr == errIncompleteFrame {
			// 只有文件最末尾允许出现不完整帧；decodeRecord 本身保证一旦
			// 帧完整可解析就继续推进，因此能走到这里说明残留字节已到文件尾。
			dropped := int64(len(data) - off)
			if !isActive {
				return rep, newCorrupt(KindIncompleteTail, path,
					"sealed segment has %d torn tail bytes", dropped)
			}
			if !canRepair {
				return rep, newCorrupt(KindIncompleteTail, path,
					"active segment has %d torn tail bytes and repair disabled", dropped)
			}
			if err := truncateFile(path, int64(off)); err != nil {
				return rep, err
			}
			rep.repaired = true
			rep.droppedBytes = dropped
			return rep, nil
		}
		if derr != nil {
			// 帧完整但内容不可信：补上文件名后直接失败。
			if ce, ok := derr.(*CorruptionError); ok {
				ce.File = path
			}
			return rep, derr
		}
		if err := visit(rec); err != nil {
			return rep, err
		}
		off += n
	}
	return rep, nil
}

// truncateFile 把文件裁剪到指定长度并 fsync，使裁剪结果持久化。
func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
