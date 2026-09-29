package kv

import (
	"errors"
	"fmt"
)

// CorruptionKind 标识重启恢复时发现的数据异常类别。
type CorruptionKind int

const (
	KindUnknown CorruptionKind = iota
	// KindIncompleteTail：文件尾部存在写了一半的记录（写入中途崩溃/掉电）。
	KindIncompleteTail
	// KindChecksum：记录头部完整但内容校验不一致（非尾部的损坏）。
	KindChecksum
	// KindSequence：记录版本序号小于等于已见序号（序号回退）。
	KindSequence
)

// 不同异常类别的哨兵错误，便于 errors.Is 判定。
var (
	ErrIncompleteTail     = errors.New("kv: incomplete trailing record")
	ErrChecksumMismatch   = errors.New("kv: record checksum mismatch")
	ErrSequenceRegression = errors.New("kv: record sequence regression")
)

// CorruptionError 携带异常类别、文件偏移与细节，恢复时不会静默跳过任何异常。
type CorruptionError struct {
	Kind   CorruptionKind
	Offset int64
	Detail string
}

func (e *CorruptionError) Error() string {
	var name string
	switch e.Kind {
	case KindIncompleteTail:
		name = "incomplete-tail"
	case KindChecksum:
		name = "checksum-mismatch"
	case KindSequence:
		name = "sequence-regression"
	default:
		name = "unknown"
	}
	return fmt.Sprintf("kv: corruption(%s) at offset %d: %s", name, e.Offset, e.Detail)
}

// Is 让具体错误可被 errors.Is 匹配到对应类别哨兵。
func (e *CorruptionError) Is(target error) bool {
	switch target {
	case ErrIncompleteTail:
		return e.Kind == KindIncompleteTail
	case ErrChecksumMismatch:
		return e.Kind == KindChecksum
	case ErrSequenceRegression:
		return e.Kind == KindSequence
	default:
		return false
	}
}

// RepairReport 描述显式修复动作实际做了什么。
type RepairReport struct {
	Truncated bool
	Offset    int64
	OldSize   int64
	NewSize   int64
}

func (r RepairReport) String() string {
	if !r.Truncated {
		return "repair: no change"
	}
	return fmt.Sprintf("repair: truncated %d bytes (old size %d -> %d) at offset %d",
		r.OldSize-r.NewSize, r.OldSize, r.NewSize, r.Offset)
}
