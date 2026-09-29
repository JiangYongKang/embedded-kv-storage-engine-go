package kv

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// runCrashSubprocess 在子进程中运行 TestCompactCrashSubprocessHelper，
// 通过环境变量指定回收钩子阶段，子进程在该点 SIGKILL 自杀。
func runCrashSubprocess(t *testing.T, dir, phase string) {
	t.Helper()
	_, testFile, _, _ := runtime.Caller(0)
	cmd := exec.Command(os.Args[0],
		"-test.run", "^TestCompactCrashSubprocessHelper$",
		"-test.count=1",
		filepath.Dir(testFile))
	cmd.Env = append(os.Environ(),
		"KV_CRASH_DIR="+dir,
		"KV_CRASH_PHASE="+phase,
	)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if cmd.ProcessState != nil && cmd.ProcessState.Success() {
		t.Fatalf("subprocess unexpectedly succeeded, phase=%s\n%s", phase, out)
	}
	// SIGKILL（signal: killed）是预期；其它非零退出视为真实失败。
	if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == -1 {
		return
	}
	t.Fatalf("subprocess failed unexpectedly, phase=%s err=%v\n%s", phase, err, out)
}
