package kv

import (
	"errors"
	"os"
	"testing"
)

// corruptByte 翻转目标文件中某个字节，用来制造"帧完整但内容变了"。
func corruptByte(t *testing.T, path string, index int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if index >= len(data) {
		t.Fatalf("索引 %d 超出文件长度 %d", index, len(data))
	}
	data[index] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestChecksumMismatch 翻转活动段中间某条帧的 payload 字节：
// 帧边界与长度全部完整，唯独 CRC 对不上，必须报
// KindChecksumMismatch 而不是按"半截尾部"处理，更不能跳过。
func TestChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	db.Put([]byte("alpha"), []byte("aaaa")) // seq1，帧大约从偏移 0 开始
	db.Put([]byte("beta"), []byte("bbbb"))  // seq2
	active := walPathOf(dir, db.manifest.ActiveID)
	db.Close()

	// 翻转第一条帧 payload 中的一个字节（位于头之后的 key 区内，
	// 避开文件尾部——尾部问题由另一类用例负责）。
	corruptByte(t, active, 30)
	t.Logf("[触发条件] 翻转 %s 偏移 30 的字节：帧结构完整，仅内容变化", "wal-1")

	_, err := Open(DefaultOptions(dir))
	var ce *CorruptionError
	if !errors.As(err, &ce) || ce.Kind != KindChecksumMismatch {
		t.Fatalf("[判定依据] 期望 KindChecksumMismatch，实际: %v", err)
	}
	t.Logf("[判定依据] 返回类别 %s，文件 %s；长度完整但 CRC 失败必须中止恢复，"+
		"不得当作不完整尾部截断、也不得静默跳过", ce.Kind, ce.File)
}

// TestSequenceRegression 伪造一条序号回退的记录追加到活动段尾部，
// 且帧本身完全合法（CRC 正确）。必须报 KindSequenceRegression。
func TestSequenceRegression(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	db.Put([]byte("k1"), []byte("v1")) // seq1
	db.Put([]byte("k2"), []byte("v2")) // seq2
	active := walPathOf(dir, db.manifest.ActiveID)
	db.Close()

	// 合法帧，但 seq 回退为 1（<= 已见序号 2）。
	appendRawRecord(t, active, &record{seq: 1, op: opPut, key: []byte("k2"), val: []byte("old")})
	t.Logf("[触发条件] 在合法历史（最大序号 2）之后追加一条 CRC 合法但序号=1 的帧")

	_, err := Open(DefaultOptions(dir))
	var ce *CorruptionError
	if !errors.As(err, &ce) || ce.Kind != KindSequenceRegression {
		t.Fatalf("[判定依据] 期望 KindSequenceRegression，实际: %v", err)
	}
	t.Logf("[判定依据] 返回类别 %s，文件 %s；全局序号必须严格递增，"+
		"旧段拼回/伪造日志不能被接受", ce.Kind, ce.File)
}

// TestMissingFile 删掉 MANIFEST 引用的一个 WAL 段，必须报 KindMissingFile。
func TestMissingFile(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 1 // 每条记录都触发滚动，得到多个段
	db, _ := Open(opts)
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Put([]byte("c"), []byte("3"))
	db.Close()
	dumpDir(t, dir, "[准备]")

	// 找到清单中除活动段外的第一个段并删除。
	m, err := readManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	victim := walName(m.Tails[0].ID)
	if err := os.Remove(walPathOf(dir, m.Tails[0].ID)); err != nil {
		t.Fatal(err)
	}
	t.Logf("[触发条件] 删除 MANIFEST 仍在引用的段文件 %s", victim)

	_, err = Open(DefaultOptions(dir))
	var ce *CorruptionError
	if !errors.As(err, &ce) || ce.Kind != KindMissingFile {
		t.Fatalf("[判定依据] 期望 KindMissingFile，实际: %v", err)
	}
	t.Logf("[判定依据] 返回类别 %s，文件 %s；引用文件缺失不能静默忽略", ce.Kind, ce.File)
}

// TestManifestCorrupt 破坏 MANIFEST 的一个 CRC 行，必须报 KindManifestCorrupt，
// 而不是退回"猜目录"——猜测可能接受中间态文件集合。
func TestManifestCorrupt(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	db.Put([]byte("x"), []byte("y"))
	db.Close()

	p := manifestPath(dir)
	data, _ := os.ReadFile(p)
	// 翻转 version 逻辑行内的一个字节（第一行，CRC 行随即对不上）。
	data[10] ^= 0xFF
	os.WriteFile(p, data, 0o600)
	t.Logf("[触发条件] 翻转 manifest.toml 第 11 字节，使其与行内 CRC 不一致")

	_, err := Open(DefaultOptions(dir))
	var ce *CorruptionError
	if !errors.As(err, &ce) || ce.Kind != KindManifestCorrupt {
		t.Fatalf("[判定依据] 期望 KindManifestCorrupt，实际: %v", err)
	}
	t.Logf("[判定依据] 返回类别 %s；清单任何一行校验失败即整体拒绝", ce.Kind)
}

// TestOverwriteVersions 重复键反复覆盖，验证新旧版本语义：
// 读永远只返回最新已提交版本；删除后再写也正确；重启后保持。
func TestOverwriteVersions(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	t.Logf("[触发条件] 同一键 k 经历 v1->v2->删除->v3 四次带序号提交")
	mustGet(t, db, "k", "")
	db.Put([]byte("k"), []byte("v1"))
	mustGet(t, db, "k", "v1")
	db.Put([]byte("k"), []byte("v2"))
	mustGet(t, db, "k", "v2")
	db.Delete([]byte("k"))
	mustGet(t, db, "k", "")
	db.Put([]byte("k"), []byte("v3"))
	mustGet(t, db, "k", "v3")
	seq := db.Seq()
	if seq != 4 {
		t.Fatalf("四次提交后序号应为 4，实际 %d", seq)
	}
	db.Close()

	t.Logf("[判定依据] 重放后 k 只能是 v3；中间版本与删除墓碑都不会让旧值冒头")
	assertState(t, dir, map[string]string{"k": "v3"})
}

// TestDeleteDoesNotResurrect 明确验证删除不能把键"抹掉"：
// 构造清单中多个段、删除发生在最后，重放必须保持删除态。
func TestDeleteDoesNotResurrect(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SegmentSize = 1
	db, _ := Open(opts)
	db.Put([]byte("dup"), []byte("old"))
	db.Put([]byte("dup"), []byte("new"))
	db.Delete([]byte("dup"))
	db.Close()
	dumpDir(t, dir, "[准备] 多段历史+删除")
	t.Logf("[触发条件] 键 dup 在旧段有值、在最新记录是墓碑；整段重放")
	assertState(t, dir, map[string]string{"dup": ""})
}
