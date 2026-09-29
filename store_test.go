package kv

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPutGetDeleteOverwrite(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mustGet(t, s, "missing"); ok {
		t.Fatal("missing key reported present")
	}
	if err := s.Put([]byte("a"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if v, ok := mustGet(t, s, "a"); !ok || string(v) != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}
	if err := s.Put([]byte("a"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if v, _ := mustGet(t, s, "a"); string(v) != "v2" {
		t.Fatalf("expected v2 after overwrite, got %q", v)
	}
	if err := s.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, ok := mustGet(t, s, "a"); ok {
		t.Fatal("deleted key reported present")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 重开：删除必须仍是删除，旧值不能复活。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, ok := mustGet(t, s2, "a"); ok {
		t.Fatal("deleted key resurrected after reopen")
	}
	t.Logf("判定依据: 重放墓碑记录后索引中 a 为 deleted，Get 返回 ok=false")
}

func TestReopenRecommitsOnly(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	mustPut(t, s, "k1", "one")
	mustPut(t, s, "k2", "two")
	mustPut(t, s, "k1", "ONE")
	mustDelete(t, s, "k2")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, ok := mustGet(t, s2, "k1"); !ok || string(v) != "ONE" {
		t.Fatalf("k1 want ONE got %q", v)
	}
	if _, ok := mustGet(t, s2, "k2"); ok {
		t.Fatal("k2 should remain deleted")
	}
}

func TestReopenIncompleteTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	mustPut(t, s, "committed", "yes")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// 触发条件: 模拟写一帧后立即崩溃——追加半帧但不 fsync。
	mangled := append(append([]byte(nil), data...),
		[]byte(frameMagic)...)
	mangled = append(mangled, 1, 2, 3)
	if err := os.WriteFile(p, mangled, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = Open(dir)
	if !errors.Is(err, ErrIncompleteTail) {
		t.Fatalf("want ErrIncompleteTail, got %v", err)
	}
	t.Logf("触发条件: 完整帧后追加 7 字节半截帧; 判定依据: errors.Is(err, ErrIncompleteTail)=true")
	// 显式修复后重开，已提交数据必须完好。
	rep, err := Repair(dir)
	if err != nil || !rep.Truncated {
		t.Fatalf("repair failed: %+v err=%v", rep, err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, ok := mustGet(t, s2, "committed"); !ok || string(v) != "yes" {
		t.Fatalf("committed value lost after repair: %q ok=%v", v, ok)
	}
}

func TestReopenChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	mustPut(t, s, "a", "vvvv")
	mustPut(t, s, "b", "wwww")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, logFileName)
	data, _ := os.ReadFile(p)
	// 触发条件: 在第一帧载荷区翻转一个字节（非尾部）。
	data[headerSize] ^= 0xFF
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("want ErrChecksumMismatch, got %v", err)
	}
	if _, rerr := Repair(dir); !errors.Is(rerr, ErrChecksumMismatch) {
		t.Fatalf("repair must refuse checksum corruption, got %v", rerr)
	}
	t.Logf("触发条件: 翻转首帧载荷字节; 判定依据: CRC 不符且坏帧后还有数据 -> KindChecksum，Repair 拒绝修改")
}

func TestReopenSequenceRegression(t *testing.T) {
	dir := t.TempDir()
	// 手工构造两段日志：序号 5 在前，序号 3 在后（帧本身都完整且 CRC 正确）。
	f1, _ := encodeFrame(frame{typ: recPut, seq: 5, key: []byte("a"), value: []byte("1")})
	f2, _ := encodeFrame(frame{typ: recPut, seq: 3, key: []byte("b"), value: []byte("2")})
	if err := os.WriteFile(filepath.Join(dir, logFileName), append(f1, f2...), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir)
	if !errors.Is(err, ErrSequenceRegression) {
		t.Fatalf("want ErrSequenceRegression, got %v", err)
	}
	t.Logf("触发条件: 日志中序号 5 后出现序号 3; 判定依据: seq<=maxSeq -> KindSequence")
}

func mustGet(t *testing.T, s *Store, k string) ([]byte, bool) {
	t.Helper()
	v, ok, err := s.Get([]byte(k))
	if err != nil {
		t.Fatal(err)
	}
	return v, ok
}

func mustPut(t *testing.T, s *Store, k, v string) {
	t.Helper()
	if err := s.Put([]byte(k), []byte(v)); err != nil {
		t.Fatal(err)
	}
}

func mustDelete(t *testing.T, s *Store, k string) {
	t.Helper()
	if err := s.Delete([]byte(k)); err != nil {
		t.Fatal(err)
	}
}
