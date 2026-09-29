package kv

// options 为引擎可调参数。
type options struct {
	// fsync：每次提交写入是否强制落盘（默认 true）。
	fsync bool
	// autoCompactThreshold：日志文件大小超过该字节数后，写提交时自动触发一次回收。
	autoCompactThreshold int64
	// compactHook：仅供测试，在回收各阶段注入回调（例如在此处强杀进程）。
	compactHook func(phase string)
	// commitHook：仅供测试，在提交关键点注入回调；参数为本次写使用的序号。
	commitHook func(seq uint64, phase string)
}

// Option 配置 Open 的行为。
type Option func(*options)

// WithFSync 设置每次提交是否 fsync（默认开启；关闭仅用于测试注入写后崩溃）。
func WithFSync(enabled bool) Option {
	return func(o *options) { o.fsync = enabled }
}

// WithAutoCompactThreshold 设置日志文件超过指定字节数后在写提交时自动回收；
// <=0 表示关闭自动回收（默认关闭）。
func WithAutoCompactThreshold(bytes int64) Option {
	return func(o *options) { o.autoCompactThreshold = bytes }
}

// withCompactHook 仅供测试：在回收各阶段注入回调（例如强杀进程）。
func withCompactHook(hook func(phase string)) Option {
	return func(o *options) { o.compactHook = hook }
}

// withCommitHook 仅供测试：在提交关键点（before-write / after-sync）注入回调。
func withCommitHook(hook func(seq uint64, phase string)) Option {
	return func(o *options) { o.commitHook = hook }
}

func defaultOptions() options {
	return options{
		fsync:                true,
		autoCompactThreshold: 0,
	}
}
