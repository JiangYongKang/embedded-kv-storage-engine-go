package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Compact 将当前所有存活键值重写为一份新的日志，原子替换旧日志。
//
// 崩溃语义（rename 是切换点）：
//   - 在 rename 生效前崩溃：tmp 文件成为孤儿，重开看到的仍是完整的旧日志；
//   - 在 rename 生效后崩溃（含 fsync 目录前掉电）：重开看到的是完整的新日志，
//     因为新日志在 rename 前已经 fsync；不存在“新日志写了一半却已切换”的状态。
//
// 回收期间写锁被持有：开始前已开始的读取先完成，切换期间及之后的新读取只能看到
// 与回收前逐字节一致的存活数据；墓碑与被覆盖的旧版本被丢弃，删除的键不会复活。
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.broken {
		return fmt.Errorf("kv: store is in broken state; reopen required")
	}
	return s.compactLocked()
}

func (s *Store) compactLocked() error {
	if hook := s.opts.compactHook; hook != nil {
		hook("begin")
	}
	tmpPath := s.logPath + tmpSuffix

	// 1) 收集存活键值；稍后按 seq 升序写出（seq 唯一，产物确定可复现）。
	type kv struct {
		k string
		e indexEntry
	}
	live := make([]kv, 0, len(s.index))
	for k, e := range s.index {
		if !e.deleted {
			live = append(live, kv{k, e})
		}
	}
	// 按 seq 升序写出：日志中版本序号始终严格递增，重放规则不变。
	sort.Slice(live, func(i, j int) bool { return live[i].e.seq < live[j].e.seq })

	// 2) 快照式重建内存视图（墓碑丢弃）。
	newIndex := make(map[string]indexEntry, len(live))

	// 3) 写临时文件：checkpoint 记录回收后的 nextSeq，防止“全部键删除后”
	//    重开导致 nextSeq 倒退、后续序号与历史冲突。
	tf, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("kv: compact create tmp: %w", err)
	}
	writeErr := func() error {
		defer func() { _ = tf.Close() }()

		cp, err := encodeFrame(frame{typ: recCheckpoint, seq: s.nextSeq})
		if err != nil {
			return err
		}
		if _, err := tf.Write(cp); err != nil {
			return fmt.Errorf("kv: compact write checkpoint: %w", err)
		}
		for _, it := range live {
			fr := frame{typ: recPut, seq: it.e.seq, key: []byte(it.k), value: it.e.value}
			enc, err := encodeFrame(fr)
			if err != nil {
				return err
			}
			if _, err := tf.Write(enc); err != nil {
				return fmt.Errorf("kv: compact write: %w", err)
			}
			newIndex[it.k] = it.e
		}
		if hook := s.opts.compactHook; hook != nil {
			// 模拟“新内容已写好但尚未切换”时进程被杀。
			hook("before-tmp-sync")
		}
		if err := tf.Sync(); err != nil {
			return fmt.Errorf("kv: compact sync tmp: %w", err)
		}
		return nil
	}()
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return writeErr
	}
	// 关闭 tmp（确保所有脏页归属落盘后的文件），rename 要求文件已关闭或至少不影响。
	// 上面 deferred Close 已执行。

	// 4) 原子替换并持久化目录项。
	if hook := s.opts.compactHook; hook != nil {
		// 模拟“rename 与目录 fsync 之间”被杀。
		hook("before-rename")
	}
	if err := os.Rename(tmpPath, s.logPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("kv: compact rename: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		// rename 本身在大多数文件系统上已随目录日志落盘；fsync 目录失败仍然返回错误，
		// 但磁盘上的日志只会是旧文件或新文件二者之一，不会出现中间态。
		return fmt.Errorf("kv: compact sync dir: %w", err)
	}

	// 5) 切换内存与文件句柄。
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("kv: compact close old handle: %w", err)
	}
	nf, err := os.OpenFile(s.logPath, os.O_RDWR, 0o644)
	if err != nil {
		// 文件已替换成功，仅句柄打开失败：标记 broken 要求重开。
		s.broken = true
		return fmt.Errorf("kv: compact reopen: %w", err)
	}
	st, err := nf.Stat()
	if err != nil {
		_ = nf.Close()
		s.broken = true
		return fmt.Errorf("kv: compact stat: %w", err)
	}
	s.f = nf
	s.index = newIndex
	s.offset = st.Size()
	return nil
}

// removeStaleTmp 清理上次回收在 rename 之前崩溃遗留的临时文件。
func removeStaleTmp(dir string) error {
	err := os.Remove(filepath.Join(dir, logFileName+tmpSuffix))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("kv: remove stale compaction tmp: %w", err)
	}
	return nil
}
