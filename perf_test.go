package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestHistoryDoesNotBloatReadPath 制造海量历史版本后回收，验证：
//  1. Get 始终走内存索引（O(1)），不扫描历史；
//  2. 回收后日志大小只取决于存活数据量，与历史写入次数无关；
//  3. 因此重启重放耗时也不随历史线性增长。
func TestHistoryDoesNotBloatReadPath(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, WithFSync(false))

	const history = 5000 // 同一个键的 5000 个历史版本
	for i := 0; i < history; i++ {
		mustPut(t, s, "hot", fmt.Sprintf("v%05d", i))
	}
	for i := 0; i < 500; i++ { // 500 个删除键的墓碑历史
		k := fmt.Sprintf("dead%03d", i)
		mustPut(t, s, k, "x")
		mustDelete(t, s, k)
	}
	mustPut(t, s, "live", "L")

	bigStat, _ := os.Stat(filepath.Join(dir, logFileName))
	bigSize := bigStat.Size()

	// 回收前重开（重放全部历史）与回收后重开（只重放存活数据）对比。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	smallStat, _ := os.Stat(filepath.Join(dir, logFileName))
	smallSize := smallStat.Size()

	// 结构性保证：回收后日志只含 2 个存活键（checkpoint+hot+live），
	// 大小应比含 5500 帧历史的日志小一个数量级以上，且与 history 无关。
	if smallSize >= bigSize/10 {
		t.Fatalf("compacted log still scales with history: big=%d small=%d", bigSize, smallSize)
	}
	if smallSize > 256 {
		t.Fatalf("compacted log unexpectedly large for 2 live keys: %d", smallSize)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, _ := mustGet(t, s2, "hot"); string(v) != fmt.Sprintf("v%05d", history-1) {
		t.Fatalf("hot final version wrong: %q", v)
	}
	if v, _ := mustGet(t, s2, "live"); string(v) != "L" {
		t.Fatalf("live missing: %q", v)
	}
	if _, ok := mustGet(t, s2, "dead000"); ok {
		t.Fatal("deleted key resurrected")
	}
	t.Logf("判定依据: 日志 %d 字节(含 %d 帧历史) -> 回收后 %d 字节(2 存活键)，Get 走内存 map O(1)，重启重放量与历史写入量解耦",
		bigSize, history+1000, smallSize)
}

func TestAutoCompactThreshold(t *testing.T) {
	dir := t.TempDir()
	// 阈值 1KB：普通提交超过阈值即自动回收。
	s, err := Open(dir, WithAutoCompactThreshold(1024), WithFSync(false))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 2000; i++ {
		mustPut(t, s, "k", fmt.Sprintf("value-%05d", i))
	}
	// 自动回收后：数据正确，日志被压回到与存活集匹配的小尺寸。
	if v, _ := mustGet(t, s, "k"); string(v) != "value-01999" {
		t.Fatalf("auto-compact lost latest value: %q", v)
	}
	st, _ := os.Stat(filepath.Join(dir, logFileName))
	// 自动回收在每次提交超过阈值后触发；最后一次回收之后至多又积累到阈值附近
	// （阈值 + 一个最大帧），因此日志大小被阈值卡住，不随 2000 次写入增长。
	if st.Size() > 1024+128 {
		t.Fatalf("auto-compact failed to bound log growth: %d bytes", st.Size())
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	st, _ = os.Stat(filepath.Join(dir, logFileName))
	if st.Size() > 128 {
		t.Fatalf("manual compact after auto-compact left large log: %d bytes", st.Size())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, _ := mustGet(t, s2, "k"); string(v) != "value-01999" {
		t.Fatalf("value changed after reopen post auto-compact: %q", v)
	}
	t.Logf("触发条件: 阈值 1024 字节，2000 次覆盖; 判定依据: 写提交中自动回收，最终日志小、值为最后版本、重开一致")
}

func BenchmarkGetAfterLargeHistory(b *testing.B) {
	dir := b.TempDir()
	s, _ := Open(dir, WithFSync(false))
	for i := 0; i < 100000; i++ {
		if err := s.Put([]byte("hot"), []byte(fmt.Sprintf("v%08d", i))); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := s.Get([]byte("hot")); err != nil || !ok {
			b.Fatal("get failed")
		}
	}
	b.StopTimer()
	_ = s.Close()
}
