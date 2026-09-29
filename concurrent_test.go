package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentReadWrite 多读单写并发：
//   - 写者持续覆盖同一组键，每个值自带序号；
//   - 读者读到的值必须严格等于某一次完整提交写入的字节串，
//     不允许撕裂值、不允许空值以外的中间态；
//   - 并发结束后重启，状态等于最后一个提交点。
func TestConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	const writers = 1
	const readers = 8
	const writesEach = 300
	const keys = 20

	// 初始版本 0：每个键先放一个合法旧值，读者任何时刻都只能看到
	// "某个完整旧值"或"某个完整新值"。
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		if err := db.Put([]byte(key), []byte(padVal(key, 0))); err != nil {
			t.Fatal(err)
		}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	var observed int64

	// 读者：校验读到的字节必须匹配 <key 对应前缀>-v<数字> 的完整格式，
	// 且数字必须落在已提交版本范围内（0..当前最大版本，由写者单调推进）。
	var maxVer int64
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for !stop.Load() {
				k := int(fastrand() % keys)
				key := fmt.Sprintf("key-%02d", k)
				val, err := db.Get([]byte(key))
				if err != nil {
					t.Errorf("reader %d 意外错误: %v", id, err)
					return
				}
				v, ok := parseVal(key, string(val))
				if !ok {
					t.Errorf("reader %d 读到非完整中间态: %q", id, val)
					return
				}
				mv := atomic.LoadInt64(&maxVer)
				// 合法上界是 mv+1：引擎在 Put 内完成提交（索引更新）后、
				// 测试计数器 maxVer 累加前存在一个极窄窗口，读者在该窗口
				// 读到的是"已提交但计数器尚未发布"的新值，序号恰为 mv+1。
				// 任何 v > mv+1 都不可能由提交顺序解释，属于真正的越界可见。
				if int64(v) > mv+1 {
					t.Errorf("reader %d 读到不可能的版本 %d（已发布上界 %d）", id, v, mv)
					return
				}
				atomic.AddInt64(&observed, 1)
			}
		}(r)
	}

	// 单写者：覆盖写。每次成功后才推进 maxVer 上界，
	// 因此读者不可能观察到尚未提交成功的版本。
	wg.Add(writers)
	go func() {
		defer wg.Done()
		for i := 1; i <= writesEach; i++ {
			k := i % keys
			key := fmt.Sprintf("key-%02d", k)
			if err := db.Put([]byte(key), []byte(padVal(key, i))); err != nil {
				t.Errorf("writer put: %v", err)
				return
			}
			// 对每个键而言版本并非连续，但全局最大版本单调即可。
			atomic.StoreInt64(&maxVer, int64(i))
		}
	}()

	// 等待写者完成后再让读者停，保证读写确实在时间上重叠。
	// （写者通过 wg 计数，这里单独等待它：简单起见 sleep 到足够写入量）
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt64(&maxVer) < writesEach && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()

	if observed == 0 {
		t.Fatal("读者一次有效观测都没有，并发场景未真正发生")
	}
	t.Logf("[判定依据] %d 个读协程与 1 个写协程并发，共完成 %d 次只读观测、%d 次覆盖写；"+
		"每次观测都等于某个已提交完整版本，无撕裂值/无超前版本", readers, observed, writesEach)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启后内容必须与崩溃前提交一致：逐键重放校验最后版本。
	db2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	finalVer := map[int]int{}
	for i := 1; i <= writesEach; i++ {
		finalVer[i%keys] = i // 每个键最后一次写入的全局序号
	}
	for k, v := range finalVer {
		key := fmt.Sprintf("key-%02d", k)
		mustGet(t, db2, key, padVal(key, v))
	}
	t.Logf("[结果] 并发结束后重启，每个键都等于其最后一次已提交覆盖值")
	db2.Close()
}

// padVal 构造带固定填充的长值，让"撕裂写"更容易暴露：
// 格式 <key>|vNNN|<200 字节填充>，parseVal 严格校验填充完整性。
func padVal(key string, ver int) string {
	body := fmt.Sprintf("%s|v%06d|", key, ver)
	for len(body) < 256 {
		body += "x"
	}
	return body
}

func parseVal(key, val string) (int, bool) {
	prefix := key + "|v"
	if len(val) != 256 || val[:len(key)+2] != prefix {
		return 0, false
	}
	ver := 0
	for i := len(prefix); i < len(prefix)+6; i++ {
		if val[i] < '0' || val[i] > '9' {
			return 0, false
		}
		ver = ver*10 + int(val[i]-'0')
	}
	if val[len(prefix)+6] != '|' {
		return 0, false
	}
	for i := len(prefix) + 7; i < 256; i++ {
		if val[i] != 'x' {
			return 0, false
		}
	}
	return ver, true
}

// fastrand 避免给测试引入 math/rand 的全局锁竞争。
func fastrand() uint32 {
	return xorshift()
}

var rngState uint32 = 2463534242

func xorshift() uint32 {
	// 测试辅助：多个 goroutine 同时读写一个状态只影响随机性，
	// 不影响正确性；为干净起见用原子操作。
	for {
		old := atomic.LoadUint32(&rngState)
		x := old
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		if atomic.CompareAndSwapUint32(&rngState, old, x) {
			return x
		}
	}
}
