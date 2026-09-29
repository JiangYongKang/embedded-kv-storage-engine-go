package kv

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// validValue 把写入序号编码成固定格式；读者拿到的值必须完整匹配某一个已提交版本，
// 绝不能看到半截字节或拼接出来的中间态。
func validValue(v []byte) (int, bool) {
	if len(v) != 12 || string(v[:4]) != "val=" {
		return 0, false
	}
	n, err := strconv.Atoi(string(v[4:]))
	if err != nil {
		return 0, false
	}
	return n, true
}

func makeValue(i int) []byte {
	return []byte(fmt.Sprintf("val=%08d", i))
}

func TestConcurrentReadersSingleWriterVisibility(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, WithFSync(false))
	defer s.Close()
	if err := s.Put([]byte("hot"), makeValue(0)); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 多个读者并发：每次读到的必须是某一个完整的已提交版本。
	for g := 0; g < runtime.NumCPU()*2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localHigh int
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok, err := s.Get([]byte("hot"))
				if err != nil || !ok {
					t.Errorf("Get ok=%v err=%v", ok, err)
					return
				}
				n, valid := validValue(v)
				if !valid {
					t.Errorf("看到中间态/部分数据: %q", v)
					return
				}
				// 每个读者自身观察到的版本号单调不减：一旦新版本可见，旧版本不会再回来。
				if n < localHigh {
					t.Errorf("版本回退观测: %d -> %d", localHigh, n)
					return
				}
				localHigh = n
			}
		}()
	}

	// 单写者反复覆盖。
	const writes = 3000
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= writes; i++ {
			if err := s.Put([]byte("hot"), makeValue(i)); err != nil {
				t.Errorf("Put error: %v", err)
				close(stop)
				return
			}
		}
		close(stop)
	}()
	wg.Wait()

	v, _ := mustGet(t, s, "hot")
	n, valid := validValue(v)
	if !valid || n != writes {
		t.Fatalf("final value want val=%08d got %q valid=%v", writes, v, valid)
	}
	t.Logf("触发条件: %d 读者 + 1 写者, %d 次覆盖; 判定依据: 每次读取均为完整版本(格式校验通过)、观测版本不倒退，最终值=val=%08d",
		runtime.NumCPU()*2, writes, writes)
}

func TestConcurrentReadersWritersDistinctKeys(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, WithFSync(false))
	defer s.Close()

	const writers = 8
	const perWriter = 300
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 读者：任何时刻拿到的值要么不存在要么是完整值，且同一键版本不倒退。
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localHigh := make([]int, writers)
			for {
				select {
				case <-stop:
					return
				default:
				}
				for w := 0; w < writers; w++ {
					v, ok, err := s.Get([]byte(fmt.Sprintf("k%d", w)))
					if err != nil {
						t.Errorf("get: %v", err)
						return
					}
					if ok {
						n, valid := validValue(v)
						if !valid {
							t.Errorf("partial value: %q", v)
							return
						}
						if n < localHigh[w] {
							t.Errorf("key k%d version went backwards: %d -> %d", w, localHigh[w], n)
							return
						}
						localHigh[w] = n
					}
				}
			}
		}()
	}
	// 多个写者并发（写锁串行化），各写独立键。
	var writerWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
			for i := 1; i <= perWriter; i++ {
				if err := s.Put([]byte(fmt.Sprintf("k%d", w)), makeValue(w*100000+i)); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}(w)
	}
	writerWG.Wait()
	close(stop)
	wg.Wait()

	for w := 0; w < writers; w++ {
		v, ok := mustGet(t, s, fmt.Sprintf("k%d", w))
		want := w*100000 + perWriter
		if !ok {
			t.Fatalf("k%d missing", w)
		}
		if n, valid := validValue(v); !valid || n != want {
			t.Fatalf("k%d final want %d got %q(valid=%v)", w, want, v, valid)
		}
	}
	t.Logf("触发条件: %d 写者(独立键)+8 读者并发, 每键 %d 次写; 判定依据: 无部分数据、版本不倒退、最终值齐全",
		writers, perWriter)
}

// TestReadsAroundCompactionSwitch 验证回收切换点的读取语义：
// 回收开始前已在进行的读取先完成（看到回收前完整视图）；回收持写锁原子切换期间，
// 新读取只会阻塞等待；切换完成后的读取看到回收后完整视图。任何读取都拿不到失效数据。
func TestReadsAroundCompactionSwitch(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	for i := 0; i < 300; i++ {
		mustPut(t, s, "hot", fmt.Sprintf("hot-%d", i))
	}
	mustPut(t, s, "keep", "k")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var bad int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok, err := s.Get([]byte("hot"))
				if err != nil || !ok || string(v) != "hot-299" {
					atomic.AddInt64(&bad, 1)
					return
				}
			}
		}()
	}
	// 与持续读取并发执行回收：读者要么先于切换读到完整旧视图，要么阻塞后读到完整新视图。
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if atomic.LoadInt64(&bad) != 0 {
		t.Fatal("reader observed invalid/stale data around compaction switch")
	}
	v, ok, err := s.Get([]byte("hot"))
	if err != nil || !ok || string(v) != "hot-299" {
		t.Fatalf("post-compaction read wrong: %q ok=%v err=%v", v, ok, err)
	}
	if v2, ok2, _ := s.Get([]byte("keep")); !ok2 || string(v2) != "k" {
		t.Fatal("keep missing after compaction")
	}
	t.Logf("触发条件: 8 个持续读者与回收并发; 判定依据: 切换前读到回收前完整视图、切换后读到回收后完整视图，hot=hot-299，无失效数据")
}

// TestReadsDuringRepeatedCompactions 回收与读取并发压测：读只能阻塞或看到完整视图。
func TestReadsDuringRepeatedCompactions(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, WithAutoCompactThreshold(1<<30)) // 压测期间手动回收
	defer s.Close()
	mustPut(t, s, "hot", "stable")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var badReads int64
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok, err := s.Get([]byte("hot"))
				if err != nil || !ok || string(v) != "stable" {
					atomic.AddInt64(&badReads, 1)
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if atomic.LoadInt64(&badReads) != 0 {
		t.Fatal("reader saw invalid data during compaction")
	}
	t.Logf("触发条件: 50 次回收与 6 个持续读者并发; 判定依据: 全部读取得到一致完整值 stable")
}

// TestKillMidCommitPrefix 用子进程 SIGKILL 验证提交原子性：
// 在第 N 次写的“进入 write 调用之前”强杀，重启后可见的键必须恰好是
// k00..k(N-1) 的完整前缀，kN 及其后的任何帧都不能出现（没有未提交数据泄漏）。
func TestKillBeforeWriteHidesUncommitted(t *testing.T) {
	const total, killAt = 40, 20
	if os.Getenv("KV_KILL_HELPER") == "1" {
		dir := os.Getenv("KV_CRASH_DIR")
		s, err := Open(dir,
			WithFSync(false), // 进程被杀不丢页缓存；无需真实 fsync 也能验证提交点边界
			withCommitHook(func(seq uint64, phase string) {
				// seq 从 1 开始：seq==killAt+1 即“写第 killAt 个键的下一帧”。
				if phase == "before-write" && int(seq) == killAt+1 {
					p, _ := os.FindProcess(os.Getpid())
					_ = p.Kill()
					select {}
				}
			}))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		for i := 0; i < total; i++ {
			_ = s.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
		}
		select {}
	}

	dir := t.TempDir()
	_, testFile, _, _ := runtime.Caller(0)
	cmd := exec.Command(os.Args[0], "-test.run", "^TestKillBeforeWriteHidesUncommitted$", "-test.count=1", filepath.Dir(testFile))
	cmd.Env = append(os.Environ(), "KV_KILL_HELPER=1", "KV_CRASH_DIR="+dir)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if cmd.ProcessState != nil && cmd.ProcessState.Success() {
		t.Fatalf("subprocess unexpectedly succeeded: %s", out)
	}
	_ = err

	// 重启：若末帧因页缓存边界留下半截（模拟掉电），只能是尾部不完整，修复截掉。
	if _, oerr := Open(dir); oerr != nil {
		var cerr *CorruptionError
		if !errors.As(oerr, &cerr) || cerr.Kind != KindIncompleteTail {
			t.Fatalf("restart error must be incomplete-tail at worst, got %v", oerr)
		}
		rep, rerr := Repair(dir)
		if rerr != nil {
			t.Fatal(rerr)
		}
		t.Logf("判定依据: 尾部发现半截帧，%s", rep.String())
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// 可见键必须恰好是 0..M 的连续前缀，且 killAt 及其后的键一律不可见。
	visible := map[string]bool{}
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("k%02d", i)
		if v, ok, gerr := s2.Get([]byte(k)); gerr == nil && ok {
			if string(v) != fmt.Sprintf("v%02d", i) {
				t.Fatalf("partial/garbage value for %s: %q", k, v)
			}
			visible[k] = true
		}
	}
	m := 0
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("k%02d", i)
		if visible[k] {
			if i != m {
				t.Fatalf("visible keys are not a prefix: %s visible but k%02d is not", k, m)
			}
			m++
		}
	}
	if m == 0 || m > killAt {
		t.Fatalf("visible prefix length %d out of expected range [1,%d]", m, killAt)
	}
	t.Logf("触发条件: 子进程在第 %d 次写进入 write 前被 SIGKILL; 判定依据: 重启可见恰好 k00..k%02d 共 %d 个完整前缀帧，后续无任何泄漏",
		killAt+1, m-1, m)
}

// TestKillAfterSyncKeepsCommit 验证：write+sync 成功、但进程在内存索引更新前被杀，
// 重启仍能看到该已提交帧（持久性来源于日志而非内存状态）。
func TestKillAfterSyncKeepsCommit(t *testing.T) {
	if os.Getenv("KV_KILL_HELPER") == "1" {
		dir := os.Getenv("KV_CRASH_DIR")
		s, err := Open(dir,
			withCommitHook(func(seq uint64, phase string) {
				if phase == "after-sync" {
					p, _ := os.FindProcess(os.Getpid())
					_ = p.Kill()
					select {}
				}
			}))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		_ = s.Put([]byte("durable"), []byte("yes"))
		select {}
	}

	dir := t.TempDir()
	_, testFile, _, _ := runtime.Caller(0)
	cmd := exec.Command(os.Args[0], "-test.run", "^TestKillAfterSyncKeepsCommit$", "-test.count=1", filepath.Dir(testFile))
	cmd.Env = append(os.Environ(), "KV_KILL_HELPER=1", "KV_CRASH_DIR="+dir)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); cmd.ProcessState != nil && cmd.ProcessState.Success() {
		t.Fatalf("subprocess unexpectedly succeeded: %s %v", out, err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, ok := mustGet(t, s2, "durable"); !ok || string(v) != "yes" {
		t.Fatalf("post-sync commit lost: %q ok=%v", v, ok)
	}
	t.Logf("触发条件: fsync 成功返回钩子点立即 SIGKILL（内存索引未来得及更新）; 判定依据: 重启重放后 durable=yes")
}
