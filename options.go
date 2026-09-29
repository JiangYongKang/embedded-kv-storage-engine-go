package kv

const (
	defaultSegmentSize = 4 * 1024 * 1024 // 4 MiB
)

// Options 控制引擎的持久化与回收行为。
type Options struct {
	// Dir 数据目录，必须可由进程独占读写。
	Dir string
	// SegmentSize 单个 WAL 段文件的目标大小（字节），超过后滚动新段。
	// 0 或负值使用默认值 4MiB。
	SegmentSize int
	// Fsync 为 true 时每次提交都 fsync，保证强杀/掉电后已提交数据不丢；
	// 为 false 时崩溃可能丢失最近未刷盘的已确认写入（仅建议测试使用）。
	Fsync bool
	// RepairTornTail 为 true 时，恢复遇到活动 WAL 尾部半条记录会截断修复，
	// 而不是返回 KindIncompleteTail 错误；修复事件仍会通过 db.Repair 暴露。
	RepairTornTail bool
}

// DefaultOptions 返回带默认值的配置：每次提交 fsync，尾部截断默认开启，
// 因为"半截帧"只可能来自未提交的写入，丢弃它与崩溃前提交点严格一致。
func DefaultOptions(dir string) *Options {
	return &Options{
		Dir:            dir,
		SegmentSize:    defaultSegmentSize,
		Fsync:          true,
		RepairTornTail: true,
	}
}

func (o *Options) segmentSize() int {
	if o.SegmentSize <= 0 {
		return defaultSegmentSize
	}
	return o.SegmentSize
}
