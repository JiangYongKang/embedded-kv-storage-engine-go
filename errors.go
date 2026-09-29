package kv

import "fmt"

// CorruptionKind 标识恢复阶段发现的数据损坏类别，
// 不同原因必须能够被调用方区分，而不是笼统地"跳过坏数据"。
type CorruptionKind int

const (
	KindNone CorruptionKind = iota
	// KindIncompleteTail 活动 WAL 尾部存在被截断的半条记录（写入中途崩溃）。
	KindIncompleteTail
	// KindChecksumMismatch 记录完整可读但 CRC 校验不一致（内容损坏）。
	KindChecksumMismatch
	// KindSequenceRegression 记录序号相对于前一条回退（日志拼接/伪造异常）。
	KindSequenceRegression
	// KindManifestCorrupt MANIFEST 文件损坏或无法解析。
	KindManifestCorrupt
	// KindMissingFile MANIFEST 引用的文件在目录中不存在。
	KindMissingFile
)

func (k CorruptionKind) String() string {
	switch k {
	case KindIncompleteTail:
		return "incomplete-tail"
	case KindChecksumMismatch:
		return "checksum-mismatch"
	case KindSequenceRegression:
		return "sequence-regression"
	case KindManifestCorrupt:
		return "manifest-corrupt"
	case KindMissingFile:
		return "missing-file"
	default:
		return "none"
	}
}

// CorruptionError 在打开/恢复数据库时描述具体的损坏原因。
// 调用方可用 errors.As 取出 *CorruptionError，并依据 Kind 区分触发条件。
type CorruptionError struct {
	Kind   CorruptionKind
	File   string
	Detail string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("kv: corruption in %q: kind=%s: %s", e.File, e.Kind, e.Detail)
}

// Is 支持 errors.Is(err, &CorruptionError{}) 判定是否属于损坏类错误；
// Kind 不同的两个 CorruptionError 仍视为同类，具体类别看 Kind 字段。
func (e *CorruptionError) Is(target error) bool {
	_, ok := target.(*CorruptionError)
	return ok
}

// newCorrupt 是构造损坏错误的快捷函数。
func newCorrupt(kind CorruptionKind, file, format string, args ...any) error {
	return &CorruptionError{Kind: kind, File: file, Detail: fmt.Sprintf(format, args...)}
}
