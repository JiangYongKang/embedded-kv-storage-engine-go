package kv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrClosed 表示对已关闭（或已进入不可恢复错误态）的引擎执行操作。
var ErrClosed = errors.New("kv: store closed")

const logFileName = "wal.log"
const tmpSuffix = ".compaction.tmp"

type indexEntry struct {
	seq     uint64
	deleted bool
	value   []byte
}

// Store 是可嵌入的持久化键值引擎。
//
// 持久化模型：所有变更以带 CRC 的定长头帧追加写入 wal.log，单帧单原子提交
// （write + fsync 成功后才更新内存索引）。重启时顺序重放重建索引，
// 读取为纯内存 O(1)，耗时不随历史写入量增长。
type Store struct {
	mu sync.RWMutex

	dir     string
	logPath string
	f       *os.File

	opts options

	nextSeq uint64 // 下一次提交使用的版本序号，严格单调递增
	offset  int64  // 已提交日志的字节长度（也是追加写位置）
	index   map[string]indexEntry

	closed bool
	broken bool // 提交失败后的不可恢复态，拒绝继续服务以防状态分叉
}

// Open 打开（必要时创建）目录中的引擎。
//
// 恢复策略：任何损坏都不静默跳过——
//   - 尾部不完整（KindIncompleteTail）：最后一次写中途崩溃，可用 Repair 截掉；
//   - 内容校验不一致（KindChecksum）：非尾部数据被破坏，拒绝打开；
//   - 序号回退（KindSequence）：日志顺序被破坏，拒绝打开。
func Open(dir string, opts ...Option) (*Store, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("kv: mkdir: %w", err)
	}
	// 若上次回收在 rename 生效前崩溃，会留下孤儿临时文件：旧日志完整，直接删除 tmp。
	if err := removeStaleTmp(dir); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kv: open log: %w", err)
	}

	s := &Store{
		dir:     dir,
		logPath: logPath,
		f:       f,
		opts:    o,
		index:   make(map[string]indexEntry),
	}
	if err := s.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// replay 顺序重放整个日志重建内存索引。
func (s *Store) replay() error {
	data, err := io.ReadAll(s.f)
	if err != nil {
		return fmt.Errorf("kv: read log: %w", err)
	}
	pos := 0
	var maxSeq uint64
	var checkpointNext uint64
	haveCheckpoint := false
	for pos < len(data) {
		f, n, scanErr := scanFrame(data[pos:])
		if scanErr == io.EOF {
			break
		}
		var cerr *CorruptionError
		if errors.As(scanErr, &cerr) {
			// 尾部不完整时 scanFrame 会把剩余字节数计入 consumed，
			// 异常定位到该坏帧起始偏移；校验类错误 consumed=0，同样定位于此。
			cerr.Offset = int64(pos)
			return cerr
		}
		if scanErr != nil {
			return scanErr
		}
		if f.typ == recCheckpoint {
			// checkpoint 只允许出现在回收产物的首帧。
			if pos != 0 {
				return &CorruptionError{
					Kind:   KindChecksum,
					Offset: int64(pos),
					Detail: "checkpoint frame only allowed at log start",
				}
			}
			haveCheckpoint = true
			checkpointNext = f.seq
			pos += n
			continue
		}
		// 序号必须严格递增：回退意味着日志拼接/顺序被破坏。
		// 允许序号有间隔（回收后序号不连续），但绝不能等于或小于已见最大序号。
		if f.seq <= maxSeq {
			return &CorruptionError{
				Kind:   KindSequence,
				Offset: int64(pos),
				Detail: fmt.Sprintf("seq %d is not greater than previous max %d", f.seq, maxSeq),
			}
		}
		maxSeq = f.seq
		e := indexEntry{seq: f.seq}
		if f.typ == recDelete {
			e.deleted = true
		} else {
			e.value = f.value
		}
		// DELETE 必须保留墓碑：否则旧值（来自更早日志段）会“复活”。
		s.index[string(f.key)] = e
		pos += n
	}
	s.offset = int64(pos)
	switch {
	case haveCheckpoint && checkpointNext >= maxSeq:
		s.nextSeq = checkpointNext
	case haveCheckpoint:
		// checkpoint 不能落后于其后的数据帧。
		return &CorruptionError{
			Kind:   KindSequence,
			Offset: 0,
			Detail: fmt.Sprintf("checkpoint nextSeq %d behind data max %d", checkpointNext, maxSeq),
		}
	default:
		s.nextSeq = maxSeq + 1
	}
	if _, err := s.f.Seek(s.offset, io.SeekStart); err != nil {
		return fmt.Errorf("kv: seek log tail: %w", err)
	}
	return nil
}

// Get 返回键的当前已提交值；键不存在或已被删除时 ok=false。
// 返回的是值的拷贝，调用方修改不会影响引擎内部状态。
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, false, ErrClosed
	}
	e, ok := s.index[string(key)]
	if !ok || e.deleted {
		return nil, false, nil
	}
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true, nil
}

// Put 写入或覆盖一个键。覆盖具备明确版本语义：新版本获得更高序号，
// 成功后旧版本立即不可见；失败时索引与日志均保持旧版本不变。
func (s *Store) Put(key, value []byte) error {
	return s.commit(recPut, key, value)
}

// Delete 删除一个键。删除会写入一条墓碑记录而不是物理抹去，
// 以保证旧数据不会在任何恢复路径中复活。
func (s *Store) Delete(key []byte) error {
	return s.commit(recDelete, key, nil)
}

func (s *Store) commit(typ byte, key, value []byte) error {
	if len(key) == 0 {
		return errors.New("kv: empty key")
	}
	keyCopy := append([]byte(nil), key...)
	var valueCopy []byte
	if typ == recPut {
		valueCopy = append([]byte(nil), value...)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.broken {
		return fmt.Errorf("kv: store is in broken state after a failed commit; reopen required")
	}

	seq := s.nextSeq
	enc, err := encodeFrame(frame{typ: typ, seq: seq, key: keyCopy, value: valueCopy})
	if err != nil {
		return err
	}

	// 提交点：一次完整 Write + 可选 fsync。只有二者全部成功才更新内存索引；
	// 崩溃只可能让这一帧整体缺失（重启时表现为 KindIncompleteTail），
	// 已提交的内容与崩溃前完全一致。
	if h := s.opts.commitHook; h != nil {
		h(seq, "before-write")
	}
	n, werr := s.f.Write(enc)
	var syncErr error
	if s.opts.fsync && werr == nil {
		syncErr = s.f.Sync()
	}
	if h := s.opts.commitHook; h != nil && werr == nil && syncErr == nil && n == len(enc) {
		h(seq, "after-sync")
	}
	if werr != nil || syncErr != nil || n != len(enc) {
		// 失败后尽力把日志截回上一个已提交偏移，绝不留下可被误认为
		// “已提交”的半成品帧；截断再失败则进入 broken 态强制重开。
		if terr := s.f.Truncate(s.offset); terr != nil {
			s.broken = true
		} else if _, serr := s.f.Seek(s.offset, io.SeekStart); serr != nil {
			s.broken = true
		}
		if werr != nil {
			return fmt.Errorf("kv: append write failed: %w", werr)
		}
		if syncErr != nil {
			return fmt.Errorf("kv: append sync failed: %w", syncErr)
		}
		return fmt.Errorf("kv: short write (%d/%d)", n, len(enc))
	}

	// 落盘成功，提交内存状态（同一把写锁，读者此刻无法看到中间态）。
	e := indexEntry{seq: seq}
	if typ == recDelete {
		e.deleted = true
	} else {
		e.value = valueCopy
	}
	s.index[string(keyCopy)] = e
	s.offset += int64(n)
	s.nextSeq = seq + 1

	if s.opts.autoCompactThreshold > 0 && s.offset >= s.opts.autoCompactThreshold {
		if cerr := s.compactLocked(); cerr != nil {
			// 自动回收失败不破坏已提交数据：日志与索引仍然自洽。
			_ = cerr
		}
	}
	return nil
}

// Close 刷盘并关闭日志文件。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.closed = true
	var syncErr error
	if !s.broken {
		syncErr = s.f.Sync()
	}
	if err := s.f.Close(); err != nil && syncErr == nil {
		syncErr = err
	}
	return syncErr
}

// Len 返回当前存活（未删除）键的数量。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.index {
		if !e.deleted {
			n++
		}
	}
	return n
}
