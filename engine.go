package kv

import (
	"errors"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

// ErrNotFound 表示键不存在（或已被删除）。
var ErrNotFound = errors.New("kv: key not found")

// ErrClosed 表示数据库已关闭。
var ErrClosed = errors.New("kv: database closed")

// ErrInvalidKey 表示键为空或非法。
var ErrInvalidKey = errors.New("kv: invalid key")

// RepairReport 描述打开数据库时自动执行的修复动作。
// 注意：自动修复仅限于"活动 WAL 尾部不完整"这一明确类别，
// 修复即丢弃未提交的半截写入，恢复结果与最后一个提交点严格一致。
type RepairReport struct {
	// RepairedTornTail 为 true 时表示截断过活动 WAL 的不完整尾部。
	RepairedTornTail bool
	// RepairedFile 被修复的文件名。
	RepairedFile string
	// KeptSeq 修复后保留下来的最大（最后一条完整）序号。
	KeptSeq uint64
	// DroppedBytes 被丢弃的尾部字节数。
	DroppedBytes int64
}

// DB 是可嵌入的持久化键值存储引擎。
// mu 是一把 RWMutex：Get 持读锁，可与任意多个读并发；
// Put/Delete/Compact 持写锁，与任何读写互斥（单写）。
// compactMu 额外串行化压缩，避免两个压缩并发分配快照编号。
type DB struct {
	mu        sync.RWMutex
	compactMu sync.Mutex
	opts      *Options

	index   memIndex
	lastSeq uint64

	active   *walWriter
	manifest *manifestData

	closed atomic.Bool
	// fatal 在写 I/O 失败且无法回到已知提交点时置位；
	// 置位后除 Close 外的操作一律失败，要求调用方重新 Open。
	fatal atomic.Value // 存储 error

	// Repair 保存 Open 时发生的修复信息（无修复则为零值）。
	Repair RepairReport
}

// Open 打开（或首次创建）数据目录并完成崩溃恢复。
// 返回的错误如果是 *CorruptionError，调用方应按 Kind 区分类别处理；
// 唯一可能被自动修复的是 KindIncompleteTail（需 Options.RepairTornTail）。
func Open(opts *Options) (*DB, error) {
	if opts == nil || opts.Dir == "" {
		return nil, ErrInvalidKey
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, err
	}

	db := &DB{opts: opts, index: memIndex{}}
	m, err := db.loadOrBootstrapManifest()
	if err != nil {
		return nil, err
	}
	db.manifest = m

	if err := db.removeOrphanFiles(); err != nil {
		return nil, err
	}

	if err := db.replayState(); err != nil {
		return nil, err
	}

	active, err := openWALWriter(opts.Dir, m.ActiveID)
	if err != nil {
		return nil, err
	}
	db.active = active
	return db, nil
}

// loadOrBootstrapManifest 读取 MANIFEST；首次创建（或 MANIFEST 丢失）时
// 依据目录内实际存在的 wal-* 段引导出一份新清单并持久化。
func (db *DB) loadOrBootstrapManifest() (*manifestData, error) {
	m, err := readManifest(db.opts.Dir)
	if err == nil {
		return m, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	entries, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if id, ok := parseWALName(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	m = &manifestData{Version: manifestVersion, NextSnap: 1}
	if len(ids) == 0 {
		// 全新数据库：先建段文件，再提交 MANIFEST。
		m.ActiveID = 1
		m.Tails = []walFileInfo{{ID: 1}}
		w, err := openWALWriter(db.opts.Dir, 1)
		if err != nil {
			return nil, err
		}
		if err := w.close(); err != nil {
			return nil, err
		}
	} else {
		// 无 MANIFEST 但存在段：按 ID 顺序全部纳入，最大的为活动段。
		m.ActiveID = ids[len(ids)-1]
		for _, id := range ids {
			m.Tails = append(m.Tails, walFileInfo{ID: id})
		}
	}
	if err := writeManifest(db.opts.Dir, m); err != nil {
		return nil, err
	}
	return m, nil
}

// referencedFiles 汇总当前 MANIFEST 引用的全部数据文件名。
func (db *DB) referencedFiles() map[string]bool {
	ref := map[string]bool{manifestName: true}
	if db.manifest.Snapshot != "" {
		ref[db.manifest.Snapshot] = true
	}
	for _, t := range db.manifest.Tails {
		ref[walName(t.ID)] = true
	}
	return ref
}

// removeOrphanFiles 删除不属于当前清单的数据文件。
// 压缩在"换清单"前后崩溃都会留下孤儿（新快照 tmp，或旧段/旧快照），
// 无论重启落在哪个状态，这些文件都不影响恢复出的完整状态，可安全删除。
func (db *DB) removeOrphanFiles() error {
	entries, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return err
	}
	ref := db.referencedFiles()
	removedAny := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if ref[name] {
			continue
		}
		drop := false
		switch {
		case name == manifestTmp:
			drop = true
		case len(name) > len(tmpSuffix) && name[len(name)-len(tmpSuffix):] == tmpSuffix &&
			(name[:5] == snapPrefix || name[:4] == walPrefix):
			drop = true
		case isWALName(name) || isSnapName(name):
			// 旧清单留下的过期段/快照（压缩已提交新清单）。
			drop = true
		}
		if !drop {
			continue
		}
		if err := os.Remove(db.opts.Dir + "/" + name); err != nil && !os.IsNotExist(err) {
			return err
		}
		removedAny = true
	}
	if removedAny {
		return fsyncDir(db.opts.Dir)
	}
	return nil
}

// replayState 依据 MANIFEST 重建内存状态：
// 先装载快照，再按 ID 顺序重放所有尾巴段（活动段最后）。
func (db *DB) replayState() error {
	m := db.manifest

	// 引用文件必须全部在位，缺失不是"跳过"能解决的问题。
	if m.Snapshot != "" {
		p := db.opts.Dir + "/" + m.Snapshot
		if !fileExists(p) {
			return newCorrupt(KindMissingFile, m.Snapshot,
				"snapshot referenced by manifest is absent")
		}
	}
	for _, t := range m.Tails {
		if !fileExists(walPathOf(db.opts.Dir, t.ID)) {
			return newCorrupt(KindMissingFile, walName(t.ID),
				"wal segment referenced by manifest is absent")
		}
	}

	if m.Snapshot != "" {
		idx, snapSeq, err := readSnapshot(db.opts.Dir + "/" + m.Snapshot)
		if err != nil {
			return err
		}
		if snapSeq != m.SnapSeq {
			return newCorrupt(KindManifestCorrupt, m.Snapshot,
				"snapshot seq %d != manifest snapseq %d", snapSeq, m.SnapSeq)
		}
		db.index = idx
		db.lastSeq = snapSeq
	}

	// 文件内/跨文件共用一个 prevSeq：任何记录都必须严格大于前一条，
	// 否则就是序号回退（段被调换、旧段被拼回等）。
	prevSeq := db.lastSeq

	for i, t := range m.Tails {
		isActive := i == len(m.Tails)-1 && t.ID == m.ActiveID
		fname := walName(t.ID)
		rep, err := replayWAL(walPathOf(db.opts.Dir, t.ID), isActive, db.opts.RepairTornTail,
			func(r *record) error {
				if r.seq <= prevSeq {
					return newCorrupt(KindSequenceRegression, fname,
						"record seq %d is not greater than previous seq %d", r.seq, prevSeq)
				}
				prevSeq = r.seq
				db.applyCommitted(r)
				return nil
			})
		if err != nil {
			return err
		}
		if rep.repaired {
			db.Repair.RepairedTornTail = true
			db.Repair.RepairedFile = walName(t.ID)
			db.Repair.KeptSeq = db.lastSeq
			db.Repair.DroppedBytes = rep.droppedBytes
		}
	}
	return nil
}

// applyCommitted 把一条已提交记录反映到内存索引。
func (db *DB) applyCommitted(r *record) {
	e := indexEntry{seq: r.seq}
	switch r.op {
	case opPut:
		e.value = r.val
	case opDelete:
		e.deleted = true
	}
	db.index[string(r.key)] = e
	db.lastSeq = r.seq
}

// Put 写入或覆盖一个键。写流程严格按"先落盘提交，后改内存"：
// WAL 帧 fsync 成功之前内存索引绝不变化，因此任何读（含崩溃后重启）
// 都看不到这次写入；fsync 失败则回滚文件长度并把引擎置为 fatal。
func (db *DB) Put(key, value []byte) error {
	return db.write(key, value, opPut)
}

// Delete 删除一个键。删除写入一条 opDelete 墓碑记录：
// 键不会从状态中物理抹除，因此重放更旧的段时旧值不会"复活"。
func (db *DB) Delete(key []byte) error {
	return db.write(key, nil, opDelete)
}

func (db *DB) write(key, value []byte, op opType) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if err := db.fatalErr(); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrClosed
	}
	if err := db.rotateIfNeededLocked(); err != nil {
		return db.failLocked(err)
	}

	seq := db.lastSeq + 1
	frame := encodeRecord(&record{
		seq: seq,
		op:  op,
		key: append([]byte(nil), key...),
		val: append([]byte(nil), value...),
	})

	preSize := db.active.size
	if err := db.active.append(frame, db.opts.Fsync); err != nil {
		// 提交失败：尽力把文件裁回写入前的已提交边界。
		// 截断也失败意味着无法保证状态，引擎进入 fatal，必须重开。
		if terr := db.active.f.Truncate(preSize); terr != nil {
			return db.failLocked(terr)
		}
		if terr := fsyncFile(db.active.f); terr != nil {
			return db.failLocked(terr)
		}
		db.active.size = preSize
		return err
	}

	if db.active.minSeq == 0 {
		db.active.minSeq = seq
	}
	if op == opDelete {
		db.index[string(key)] = indexEntry{seq: seq, deleted: true}
	} else {
		db.index[string(key)] = indexEntry{seq: seq, value: append([]byte(nil), value...)}
	}
	db.lastSeq = seq
	return nil
}

// rotateIfNeededLocked 在活动段达到目标大小时滚动新段。
// 顺序：关旧段 -> 建新段文件 -> 提交新 MANIFEST（rename+fsync 目录）。
// 崩溃在任何一点都安全：崩溃在建好新段但清单未提交前，新段是孤儿；
// 清单提交后，活动段已是新段，旧段不可变。
func (db *DB) rotateIfNeededLocked() error {
	if db.active.size < int64(db.opts.segmentSize()) {
		return nil
	}
	oldID := db.active.id
	oldMin := db.active.minSeq
	if err := db.active.close(); err != nil {
		return err
	}
	newID := oldID + 1
	w, err := openWALWriter(db.opts.Dir, newID)
	if err != nil {
		return err
	}
	m := *db.manifest
	m.Tails = append(m.Tails, walFileInfo{ID: newID})
	m.ActiveID = newID
	// 更新旧段在清单中的 minseq（首次滚动时磁盘上可能记的是 0）。
	for i := range m.Tails {
		if m.Tails[i].ID == oldID {
			m.Tails[i].MinSeq = oldMin
		}
	}
	if err := writeManifest(db.opts.Dir, &m); err != nil {
		w.close()
		return err
	}
	db.manifest = &m
	db.active = w
	return nil
}

// fatalErr 返回当前致命错误（无则 nil）。
func (db *DB) fatalErr() error {
	if v := db.fatal.Load(); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
	}
	return nil
}

// failLocked 在无法保证一致性时把引擎置为 fatal。
func (db *DB) failLocked(err error) error {
	db.fatal.Store(err)
	return err
}

// Get 读取一个键。读锁下取索引快照，因此只可能返回提交点上的旧值
// 或新值，绝不会返回写了一半的帧/值。
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.fatalErr(); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return nil, ErrClosed
	}
	e, ok := db.index[string(key)]
	if !ok || e.deleted {
		return nil, ErrNotFound
	}
	return append([]byte(nil), e.value...), nil
}

// Seq 返回已提交的最大序号（单调递增）。
func (db *DB) Seq() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.lastSeq
}

// Close 关闭活动段句柄。关闭后所有操作返回 ErrClosed。
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed.Swap(true) {
		return nil
	}
	if db.active != nil {
		return db.active.close()
	}
	return nil
}

// --- 文件名小工具（与 util.go 的路径构造配套） ---

func parseWALName(name string) (uint64, bool) {
	if !isWALName(name) {
		return 0, false
	}
	var id uint64
	for i := len(walPrefix); i < len(name); i++ {
		c := name[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		id = id*10 + uint64(c-'0')
	}
	return id, id > 0
}

func isWALName(name string) bool {
	if len(name) <= len(walPrefix) || name[:len(walPrefix)] != walPrefix {
		return false
	}
	for i := len(walPrefix); i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	return true
}

func isSnapName(name string) bool {
	if len(name) <= len(snapPrefix) || name[:len(snapPrefix)] != snapPrefix {
		return false
	}
	for i := len(snapPrefix); i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	return true
}
