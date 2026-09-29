package kv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// crashEnv 让子进程知道"自己是崩溃模拟子进程"，值为要触发的崩溃阶段。
const crashEnv = "KV_CRASH_STAGE"

// runCrashSubprocess 以子进程方式运行指定用例，在压缩的指定阶段调用
// os.Exit（不跑 defer、不刷盘），语义等价于 SIGKILL：进程立刻消失。
func runCrashSubprocess(t *testing.T, testName, stage string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.v")
	cmd.Env = append(os.Environ(), crashEnv+"="+stage)
	out, _ := cmd.CombinedOutput()
	t.Logf("[crash-subprocess stage=%s] 子进程输出:\n%s", stage, indent(string(out)))
}

func indent(s string) string {
	return strings.TrimRight("    "+strings.ReplaceAll(s, "\n", "\n    "), " \n")
}

// appendRawRecord 直接向指定 WAL 段尾部追加一条记录（绕过引擎提交语义），
// 用于构造损坏/回退等异常磁盘布局。
func appendRawRecord(t *testing.T, path string, r *record) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if _, err := f.Write(encodeRecord(r)); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

// corruptionKind 从错误中取出损坏类别；非损坏错误返回 KindNone。
func corruptionKind(err error) CorruptionKind {
	var ce *CorruptionError
	if errors.As(err, &ce) {
		return ce.Kind
	}
	return KindNone
}

// dumpDir 列出数据目录文件名与大小，写进测试日志便于人工核对。
func dumpDir(t *testing.T, dir, prefix string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Logf("%s 读取目录失败: %v", prefix, err)
		return
	}
	var parts []string
	for _, e := range entries {
		if st, err := e.Info(); err == nil {
			parts = append(parts, fmt.Sprintf("%s(%d)", e.Name(), st.Size()))
		} else {
			parts = append(parts, e.Name())
		}
	}
	t.Logf("%s 目录内容: %s", prefix, strings.Join(parts, " "))
}

// assertState 重开数据库并逐键校验期望状态（"" 表示 ErrNotFound）。
func assertState(t *testing.T, dir string, want map[string]string) {
	t.Helper()
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("重开失败: %v", err)
	}
	defer db.Close()
	for k, v := range want {
		got, gerr := db.Get([]byte(k))
		if v == "" {
			if !errors.Is(gerr, ErrNotFound) {
				t.Fatalf("键 %s 期望不存在，实际得到 %q/%v", k, got, gerr)
			}
			continue
		}
		if gerr != nil || !bytes.Equal(got, []byte(v)) {
			t.Fatalf("键 %s 期望 %q，实际 %q/%v", k, v, got, gerr)
		}
	}
	if seq := db.Seq(); seq == 0 && len(want) > 0 {
		t.Fatalf("重开后序号为 0，但应有已提交数据")
	}
}

// fileSizes 返回目录中各数据文件大小。
func fileSizes(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	m := map[string]int64{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if st, err := e.Info(); err == nil {
			m[e.Name()] = st.Size()
		}
	}
	return m
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if want == "" {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("键 %s 期望 ErrNotFound，实际 %q/%v", key, got, err)
		}
		return
	}
	if err != nil || string(got) != want {
		t.Fatalf("键 %s 期望 %q，实际 %q/%v", key, want, got, err)
	}
}

// crashDirEnv 父子进程共享崩溃用例数据目录的环境变量名。
const crashDirEnv = "KV_CRASH_DIR"

func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
}
