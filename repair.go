package kv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Repair 对数据目录执行显式修复。
//
// 规则（不会静默跳过任何异常）：
//   - 仅允许处理 KindIncompleteTail：截掉最后一帧写了一半的字节；
//   - KindChecksum / KindSequence 一律原样返回错误，不做任何修改。
//
// 截尾完成后会 fsync 文件与目录，保证修复结果本身也是崩溃安全的。
func Repair(dir string) (RepairReport, error) {
	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RepairReport{}, nil
		}
		return RepairReport{}, fmt.Errorf("kv: repair open: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return RepairReport{}, fmt.Errorf("kv: repair read: %w", err)
	}
	oldSize := int64(len(data))

	pos := 0
	var maxSeq uint64
	for pos < len(data) {
		fr, n, scanErr := scanFrame(data[pos:])
		if scanErr == io.EOF {
			break
		}
		var cerr *CorruptionError
		if errors.As(scanErr, &cerr) {
			cerr.Offset = int64(pos)
			if cerr.Kind != KindIncompleteTail {
				// 校验不一致 / 序号回退：拒绝猜测、拒绝修改。
				return RepairReport{}, cerr
			}
			if err := f.Truncate(int64(pos)); err != nil {
				return RepairReport{}, fmt.Errorf("kv: repair truncate: %w", err)
			}
			if err := f.Sync(); err != nil {
				return RepairReport{}, fmt.Errorf("kv: repair sync file: %w", err)
			}
			if err := syncDir(dir); err != nil {
				return RepairReport{}, err
			}
			return RepairReport{
				Truncated: true,
				Offset:    int64(pos),
				OldSize:   oldSize,
				NewSize:   int64(pos),
			}, nil
		}
		if scanErr != nil {
			return RepairReport{}, scanErr
		}
		if fr.typ == recCheckpoint {
			if pos != 0 {
				return RepairReport{}, &CorruptionError{
					Kind:   KindChecksum,
					Offset: int64(pos),
					Detail: "checkpoint frame only allowed at log start",
				}
			}
			pos += n
			continue
		}
		if fr.seq <= maxSeq {
			return RepairReport{}, &CorruptionError{
				Kind:   KindSequence,
				Offset: int64(pos),
				Detail: fmt.Sprintf("seq %d is not greater than previous max %d", fr.seq, maxSeq),
			}
		}
		maxSeq = fr.seq
		pos += n
	}
	return RepairReport{OldSize: oldSize, NewSize: oldSize}, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("kv: open dir for fsync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("kv: fsync dir: %w", err)
	}
	return d.Close()
}
