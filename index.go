package kv

// indexEntry 是内存索引中的一个键版本。
// deleted=true 表示该键已被删除（墓碑），删除不能直接把键抹掉，
// 否则重放旧段时旧值会"复活"。
type indexEntry struct {
	value   []byte
	deleted bool
	seq     uint64
}

// memIndex 是当前已提交状态的内存视图，由 RWMutex 保护。
type memIndex map[string]indexEntry
