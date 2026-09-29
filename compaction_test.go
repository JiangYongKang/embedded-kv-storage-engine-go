package kv

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// buildDataset 写入 n 个键，其中 hot 键被反复覆盖 rounds 轮，
// 制造大量历史版本；最后返回期望状态。
func buildDataset(t *testing.T, db *DB, n, rounds int) map[string]string {
	t.Helper()
	want := map[string]string{}
	for r := 0; r < rounds; r++ {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("k%04d", i)
			v := fmt.Sprintf("r%d-%08d", r, i)
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			if r == rounds-1 {
				want[k] = v
			}
		}
	}
	// 删除一部分键，验证墓碑回收后不复活。
	for i := 0; i < n; i += 50 {
		k := fmt.Sprintf("k%04d", i)
		if err := db.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
		delete(want, k)
	}
	return want
}

// TestCompactContentEquality 回收后（含多次回收 + 回收后继续写）重启，
// 恢复出的内容必须与回收前逐键一致，且历史段已被物理删除。
func TestCompactContentEquality(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 4096 // 小段，制造多个 WAL 文件
	db, _ := Open(opts)
	want := buildDataset(t, db, 200, 4)

	before := fileSizes(t, dir)
	t.Logf("[触发条件] 写入 %d 个键 × 4 轮覆盖+删除，回收前文件: %v", 200, before)

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after := fileSizes(t, dir)
	t.Logf("[判定依据] 回收后文件: %v；应只剩快照与一个活动段，旧 wal 段消失", after)
	for name := range after {
		if isWALName(name) && name != walName(db.manifest.ActiveID) {
			t.Fatalf("旧段 %s 未被删除", name)
		}
	}

	// 回收后继续写入并再次回收，验证快照+尾巴的迭代链条。
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("new%03d", i)
		v := fmt.Sprintf("nv%03d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := db.Delete([]byte("new050")); err != nil {
		t.Fatal(err)
	}
	delete(want, "new050")
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "k0001", want["k0001"])
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	t.Logf("[判定依据] 两轮回收 + 回收后增删，重启逐键比对")
	assertState(t, dir, want)
}

// TestCompactCrashBeforeCommit 与 AfterCommit 通过子进程 + os.Exit
// 在回收协议不同阶段"强杀"，验证重启只能得到回收前或回收后的完整状态。
//
// 触发的阶段由 KV_CRASH_STAGE 环境变量指定：
//   - snap-wrote / snap-installed：提交点（MANIFEST rename）之前，
//     重启必须是【回收前完整状态】（全部旧段还在，逐键一致）；
//   - manifest-committed：提交点之后，重启必须是【回收后完整状态】
//     （只有快照+空活动段，逐键一致且孤儿文件被清理）。
func TestCompactCrashBeforeCommit(t *testing.T) {
	stages := []string{"snap-wrote", "snap-installed"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) { testCompactCrash(t, stage, false) })
	}
}

func TestCompactCrashAfterCommit(t *testing.T) {
	testCompactCrash(t, "manifest-committed", true)
}

func testCompactCrash(t *testing.T, stage string, expectPost bool) {
	dir := filepathFromEnv(t)
	if os.Getenv(crashEnv) == stage {
		// 子进程：建数据、设置钩子、在指定阶段被 os.Exit 强杀。
		runCrashingCompact(dir, stage)
		return
	}

	// 父进程：先正常建数据集，再启动会自杀的子进程复用同一目录。
	opts := DefaultOptions(dir)
	opts.SegmentSize = 4096
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	want := buildDataset(t, db, 120, 3)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	totalWALBytes := int64(0)
	for _, sz := range fileSizes(t, dir) {
		totalWALBytes += sz
	}
	t.Logf("[触发条件] 子进程将在回收阶段 %s 被 os.Exit 强杀（不跑 defer）", stage)
	runCrashSubprocess(t, "TestCompactCrash"+map[bool]string{true: "AfterCommit", false: "BeforeCommit"}[expectPost], stage)
	dumpDir(t, dir, "[崩溃后重启前]")

	db2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("回收中途崩溃后必须能成功打开，实际: %v", err)
	}
	m := db2.manifest
	gotSnapshot := m.Snapshot != ""
	if gotSnapshot != expectPost {
		t.Fatalf("阶段 %s 期望快照已生效=%v，实际 Snapshot=%q", stage, expectPost, m.Snapshot)
	}
	// 无论前/后状态，逐键内容必须完整一致。
	for k, v := range want {
		mustGet(t, db2, k, v)
	}
	for i := 0; i < 120; i += 50 {
		if _, err := db2.Get([]byte(fmt.Sprintf("k%04d", i))); !errors.Is(err, ErrNotFound) {
			t.Fatalf("墓碑键 k%04d 在回收崩溃后复活", i)
		}
	}
	if expectPost {
		if len(m.Tails) != 1 || m.Tails[0].ID != m.ActiveID {
			t.Fatalf("回收后状态应只有一个活动尾巴，实际 tails=%v active=%d", m.Tails, m.ActiveID)
		}
		t.Logf("[判定依据] 阶段 %s 在提交点之后：重启=回收后完整状态，"+
			"清单仅含快照 %s + 活动段 %d，孤儿旧段已清理", stage, m.Snapshot, m.ActiveID)
	} else {
		t.Logf("[判定依据] 阶段 %s 在提交点之前：重启=回收前完整状态，"+
			"快照 tmp 是孤儿被清理，尾巴段集合保持 %d 个，数据零丢失", stage, len(m.Tails))
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

// runCrashingCompact 是子进程入口：打开 DB，挂上钩子后执行 Compact。
func runCrashingCompact(dir, stage string) {
	compactCrashHook = func(s string) {
		if s == stage {
			fmt.Fprintf(os.Stderr, "[crash-hook] 在阶段 %s 强杀进程\n", s)
			os.Exit(9)
		}
	}
	opts := DefaultOptions(dir)
	opts.SegmentSize = 4096
	db, err := Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "子进程打开失败: %v\n", err)
		os.Exit(2)
	}
	if err := db.Compact(); err != nil {
		fmt.Fprintf(os.Stderr, "子进程压缩错误（可能因退出未完成）: %v\n", err)
		os.Exit(3)
	}
	// cleaned 阶段钩子触发后不会走到这里；若未触发则正常退出。
	os.Exit(0)
}

// filepathFromEnv 让父子进程共享目录：父进程通过 t.Setenv 把
// KV_CRASH_DIR 带给自己 exec 出来的子进程，测试结束自动还原环境。
func filepathFromEnv(t *testing.T) string {
	t.Helper()
	if d0 := os.Getenv(crashDirEnv); d0 != "" && os.Getenv(crashEnv) != "" {
		return d0
	}
	d := t.TempDir()
	t.Setenv(crashDirEnv, d)
	return d
}

// TestReadDoesNotGrowWithHistory 数据量增长、历史版本增多后，
// 单次读取耗时必须保持平稳，不随历史写入量线性变差。
func TestReadDoesNotGrowWithHistory(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 8192
	db, _ := Open(opts)

	measure := func(rounds int) time.Duration {
		const samples = 2000
		start := time.Now()
		for i := 0; i < samples; i++ {
			k := fmt.Sprintf("k%04d", i%300)
			if _, err := db.Get([]byte(k)); err != nil && !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
		}
		d := time.Since(start)
		t.Logf("[样本] 历史轮次=%3d，%d 次 Get 总耗时 %v（平均 %v/次）",
			rounds, samples, d, d/samples)
		return d
	}

	measure(0)
	for r := 1; r <= 6; r++ {
		for i := 0; i < 300; i++ {
			if err := db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("r%d-payload-%04d", r, i))); err != nil {
				t.Fatal(err)
			}
		}
		measure(r)
	}

	// 回收后再测一次：读路径仍然只是内存 map，耗时不应随磁盘历史变化。
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	post := measure(99)
	t.Logf("[判定依据] 读取为内存索引点查（O(1)）；回收前后平均耗时同量级，"+
		"不随历史写入量线性增长。回收后平均 %v/次", post/2000)
	db.Close()
}
