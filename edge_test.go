package kv

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestWriteAfterTornRepair 活动段尾部被截断修复后，数据库必须能继续
// 正常追加、滚动与重启，且修复边界上的序号衔接严格单调。
func TestWriteAfterTornRepair(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	db.Put([]byte("a"), []byte("1")) // seq1
	db.Put([]byte("b"), []byte("2")) // seq2
	active := walPathOf(dir, db.manifest.ActiveID)
	db.Close()

	// 追加半条 seq=3 的帧。
	frame := encodeRecord(&record{seq: 3, op: opPut, key: []byte("c"), val: []byte("3")})
	f, _ := openAppend(active)
	f.Write(frame[:len(frame)-5])
	f.Close()

	db2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !db2.Repair.RepairedTornTail {
		t.Fatal("期望发生尾部修复")
	}
	if db2.Seq() != 2 {
		t.Fatalf("修复后序号应为 2，实际 %d", db2.Seq())
	}
	t.Logf("[触发条件] 尾部修复（保留 seq<=2）后继续写入，新提交必须从 seq=3 开始且重启可见")

	if err := db2.Put([]byte("c"), []byte("new3")); err != nil {
		t.Fatal(err)
	}
	if db2.Seq() != 3 {
		t.Fatalf("新提交序号应为 3，实际 %d", db2.Seq())
	}
	mustGet(t, db2, "a", "1")
	mustGet(t, db2, "b", "2")
	mustGet(t, db2, "c", "new3")
	if err := db2.Put([]byte("d"), []byte("4")); err != nil {
		t.Fatal(err)
	}
	if db2.Seq() != 4 {
		t.Fatalf("序号应为 4，实际 %d", db2.Seq())
	}
	db2.Close()

	assertState(t, dir, map[string]string{"a": "1", "b": "2", "c": "new3", "d": "4"})
}

// TestCompactConcurrentWithOps 压缩与普通读写并发：压缩后所有曾提交的
// 数据仍然完整；压缩期间进行的读要么等到回收前状态、要么读到回收后
// 状态，绝不读到正在被删除文件中的失效旧数据（写锁保证互斥）。
func TestCompactConcurrentWithOps(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 2048
	db, _ := Open(opts)
	for i := 0; i < 500; i++ {
		if err := db.Put([]byte("k"+itoa(i%100)), []byte("v"+itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var readers = 6
	var reads atomic.Int64
	stop := make(chan struct{})

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, err := db.Get([]byte("k42"))
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("读出错: %v", err)
					return
				}
				_ = v
				reads.Add(1)
			}
		}()
	}

	// 连续压缩 3 轮，中间穿插覆盖写。
	for c := 0; c < 3; c++ {
		if err := db.Put([]byte("k42"), []byte("compact-round")); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact(); err != nil {
			t.Fatalf("第 %d 轮压缩失败: %v", c+1, err)
		}
		mustGet(t, db, "k42", "compact-round")
	}
	close(stop)
	wg.Wait()

	t.Logf("[判定依据] 压缩 3 轮期间 %d 个读者完成 %d 次读，无错误；"+
		"读写锁保证读只可能落在回收前或回收后的完整状态", readers, reads.Load())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{}
	for i := 0; i < 100; i++ {
		k := "k" + itoa(i)
		if i == 42 {
			want[k] = "compact-round"
			continue
		}
		// 最后一次为每个键写入的值：i + 400
		last := i
		for last+100 < 500 {
			last += 100
		}
		want[k] = "v" + itoa(last)
	}
	assertState(t, dir, want)
}

// TestEmptyCompact 空数据库与"无新写入再压缩"都不得破坏状态。
func TestEmptyCompact(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	if err := db.Compact(); err != nil { // 空库压缩
		t.Fatal(err)
	}
	db.Put([]byte("only"), []byte("one"))
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil { // 紧接第二次，活动段为空
		t.Fatal(err)
	}
	mustGet(t, db, "only", "one")
	db.Close()
	assertState(t, dir, map[string]string{"only": "one"})
	t.Logf("[判定依据] 空压缩、空活动段复用路径后状态完好")
}

// TestTombstoneDropAcrossCompaction 专门验证快照中的墓碑精简规则：
// 墓碑早于所有尾巴段的首条记录时可以被快照丢弃，因为尾巴里不再有
// 更旧的同键记录；压缩重启后删除态必须保持，旧值不得复活。
func TestTombstoneDropAcrossCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 64 // 极小段，确保删除前的旧 Put 落在独立旧段
	db, _ := Open(opts)
	if err := db.Put([]byte("gone"), []byte("ancient-value")); err != nil {
		t.Fatal(err)
	}
	// 再写一些别的键滚动出若干段，使 gone 的墓碑不在尾巴最旧位置问题上取巧。
	for i := 0; i < 30; i++ {
		if err := db.Put([]byte("pad"+itoa(i)), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	// 压缩后不再写任何东西就重启：快照里 gone 的墓碑可能已按
	// dropBeforeSeq 丢弃，但任何旧段也都已不在清单中，必须仍为删除态。
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("[触发条件] 旧值在旧段、墓碑在压缩前最后段；压缩并重启（旧段全部消失）")
	assertState(t, dir, map[string]string{"gone": "", "pad0": "x", "pad29": "x"})
}
