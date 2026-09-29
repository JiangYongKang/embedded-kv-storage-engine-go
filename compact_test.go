package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func snapshot(s *Store) map[string]string {
	out := map[string]string{}
	for _, k := range []string{"keep", "ver", "gone", "tomb"} {
		if v, ok, err := s.Get([]byte(k)); err != nil {
			panic(err)
		} else if ok {
			out[k] = string(v)
		}
	}
	return out
}

func TestCompactPreservesAndShrinks(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	// 同一键写很多旧版本，制造可回收历史；删除两个键（一个曾经存在，一个本不存在）。
	for i := 0; i < 200; i++ {
		mustPut(t, s, "ver", fmt.Sprintf("v%03d", i))
	}
	mustPut(t, s, "keep", "stable")
	mustPut(t, s, "tomb", "x")
	mustDelete(t, s, "tomb")
	mustDelete(t, s, "gone")
	want := snapshot(s)

	st, _ := os.Stat(filepath.Join(dir, logFileName))
	before := st.Size()

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(s); !mapsEqual(got, want) {
		t.Fatalf("post-compact view mismatch: got=%v want=%v", got, want)
	}
	st, _ = os.Stat(filepath.Join(dir, logFileName))
	after := st.Size()
	if after >= before {
		t.Fatalf("compaction did not shrink log: before=%d after=%d", before, after)
	}
	t.Logf("判定依据: 回收后存活视图逐键一致，日志 %d -> %d 字节，墓碑/旧版本已丢弃", before, after)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := snapshot(s2); !mapsEqual(got, want) {
		t.Fatalf("post-reopen view mismatch: got=%v want=%v", got, want)
	}
	// 回收后继续写入，序号不得与回收前冲突，覆盖语义仍正确。
	mustPut(t, s2, "ver", "after-compact")
	if v, ok := mustGet(t, s2, "ver"); !ok || string(v) != "after-compact" {
		t.Fatalf("post-compact overwrite broken: %q %v", v, ok)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if v, ok := mustGet(t, s3, "ver"); !ok || string(v) != "after-compact" {
		t.Fatalf("post-compact commit lost on reopen: %q %v", v, ok)
	}
	if _, ok := mustGet(t, s3, "tomb"); ok {
		t.Fatal("deleted key resurrected after compaction")
	}
	t.Logf("判定依据: 回收后重开并再次写入，新版本序号严格大于历史序号，墓碑不复活")
}

func TestCompactAllDeletedKeepsSeq(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	mustPut(t, s, "k", "v1")
	mustPut(t, s, "k", "v2")
	mustDelete(t, s, "k")
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("empty-after-compact log must replay: %v", err)
	}
	defer s2.Close()
	mustPut(t, s2, "k", "v3")
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	// 再次重开不能出现序号回退（v3 的序号必须大于回收前的 3）。
	s3, err := Open(dir)
	if err != nil {
		t.Fatalf("seq regression after all-deleted compaction: %v", err)
	}
	defer s3.Close()
	if v, ok := mustGet(t, s3, "k"); !ok || string(v) != "v3" {
		t.Fatalf("want v3, got %q ok=%v", v, ok)
	}
	t.Logf("判定依据: checkpoint 帧携带 nextSeq，全删回收后重开无 KindSequence")
}

func TestCompactCrashSubprocess(t *testing.T) {
	for _, phase := range []string{"before-tmp-sync", "before-rename"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			// 准备阶段：建立多版本与删除。
			s, _ := Open(dir)
			mustPut(t, s, "keep", "1")
			mustPut(t, s, "ver", "old")
			mustPut(t, s, "ver", "new")
			mustPut(t, s, "tomb", "x")
			mustDelete(t, s, "tomb")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			// 子进程在回收钩子点被强杀。
			runCrashSubprocess(t, dir, phase)
			t.Logf("触发条件: 子进程在 compact 钩子 %s 处被 SIGKILL", phase)

			// 判定：重开不报错，且状态只能是“回收前完整态”或“回收后完整态”。
			s2, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen after mid-compact kill failed: %v", err)
			}
			got := snapshot(s2)
			want := map[string]string{"keep": "1", "ver": "new"}
			if !mapsEqual(got, want) {
				t.Fatalf("state after crash is neither pre- nor post-compaction view: %v", got)
			}
			if v, ok := mustGet(t, s2, "tomb"); ok {
				t.Fatalf("deleted key visible: %q", v)
			}
			// 日志必须完整可重放：再来一轮回收 + 重开必须仍然一致。
			if err := s2.Compact(); err != nil {
				t.Fatal(err)
			}
			if err := s2.Close(); err != nil {
				t.Fatal(err)
			}
			s3, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s3.Close()
			if got := snapshot(s3); !mapsEqual(got, want) {
				t.Fatalf("second compaction diverged: %v", got)
			}
			entries, _ := os.ReadDir(dir)
			for _, e := range entries {
				if filepath.Ext(e.Name()) == tmpSuffix {
					t.Fatal("orphan tmp file survived Open cleanup")
				}
			}
			t.Logf("判定依据: 重开无损坏错误，存活键=%v，墓碑不复活，孤儿 tmp 被清理", got)
		})
	}
}

func TestCompactCrashSubprocessHelper(t *testing.T) {
	if os.Getenv("KV_CRASH_PHASE") == "" {
		return
	}
	dir := os.Getenv("KV_CRASH_DIR")
	phase := os.Getenv("KV_CRASH_PHASE")
	s, err := Open(dir, withCompactHook(func(p string) {
		if p == phase {
			// 立即、无 defer 地强杀自身，模拟掉电/进程被杀。
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Kill()
			select {} // 保险：不返回
		}
	}))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := s.Compact(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
