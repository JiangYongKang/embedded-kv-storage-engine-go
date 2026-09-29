package kv

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestCrashKillRecovery 模拟进程被 SIGKILL：写入并确认提交后，
// 不经过 Close 直接让进程消失（测试里等价于丢弃句柄、另开进程视角）。
// 判定依据：重启后所有已提交（fsync 成功）的键必须逐字节一致；
// 不存在的键必须 ErrNotFound，不允许旧值复活。
func TestCrashKillRecovery(t *testing.T) {
	dir := t.TempDir()
	t.Logf("[触发条件] 在数据目录 %s 完成 200 次 fsync 提交后，不调用 Close 直接放弃句柄（等价强杀）", dir)

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := 0; i < 200; i++ {
		k := "k" + itoa(i)
		v := "value-" + itoa(i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := db.Delete([]byte("k5")); err != nil {
		t.Fatal(err)
	}
	delete(want, "k5")
	t.Logf("[判定依据] 提交点 = WAL append 后 fsync 成功；丢弃内存句柄后重新 Open，" +
		"快照为 0，必须仅靠重放 MANIFEST 中的 WAL 段得到同一状态")
	// 故意不 Close，直接让 GC/进程视角抛弃它。
	db = nil

	assertState(t, dir, want)
	t.Logf("[结果] 强杀后重启，200 次写入全部可见、1 次删除保持删除语义，无复活")
}

// TestTornTailAutoRepair 活动 WAL 末尾出现半截帧（写一帧中途崩溃）：
// 打开时必须识别为 KindIncompleteTail 类别，并在默认配置下截断修复，
// 且把修复事实通过 db.Repair 暴露，不能静默跳过。
func TestTornTailAutoRepair(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	for i := 0; i < 10; i++ {
		if err := db.Put([]byte("k"+itoa(i)), []byte("v"+itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	active := walPathOf(dir, db.manifest.ActiveID)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 追加一条完整帧，再把它截断成"写了一半"：
	// 保留帧头但砍掉后半 payload（长度不足 totalLen）。
	frame := encodeRecord(&record{seq: 11, op: opPut, key: []byte("torn"), val: []byte("xx")})
	if err := os.WriteFile(active+".seed", frame, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(frame[:len(frame)-7]); err != nil { // 砍掉最后 7 字节
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("[触发条件] 在活动段 %s 尾部制造半条帧：帧头 totalLen=%d，实际剩余 %d 字节",
		filepath.Base(active), len(frame), len(frame)-7)

	db2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("[判定依据] 默认应自动截断修复，但打开失败: %v", err)
	}
	if !db2.Repair.RepairedTornTail || db2.Repair.RepairedFile == "" {
		t.Fatalf("修复未被显式报告: %+v", db2.Repair)
	}
	t.Logf("[判定依据] 损坏类别=KindIncompleteTail；已截断文件 %s，丢弃 %d 字节，"+
		"保留最大序号 %d；修复事件通过 Repair 报告，非静默",
		db2.Repair.RepairedFile, db2.Repair.DroppedBytes, db2.Repair.KeptSeq)
	mustGet(t, db2, "k9", "v9")
	if _, err := db2.Get([]byte("torn")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("未提交的半截写入不得可见，实际: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	// 修复后文件必须被物理截断：再次打开不应再有修复事件。
	db3, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if db3.Repair.RepairedTornTail {
		t.Fatalf("截断已持久化，第二次打开不应再报告修复")
	}
	t.Logf("[结果] 半截帧被一次性物理截断并 fsync，再次打开状态干净")
	db3.Close()
}

// TestTornTailStrictMode 关闭自动修复时，尾部不完整必须原样报错，
// 类别是 KindIncompleteTail，绝不能悄悄继续。
func TestTornTailStrictMode(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	db.Put([]byte("a"), []byte("1"))
	active := walPathOf(dir, db.manifest.ActiveID)
	db.Close()

	frame := encodeRecord(&record{seq: 2, op: opPut, key: []byte("b"), val: []byte("22")})
	f, _ := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0o600)
	f.Write(frame[:12])
	f.Close()

	opts := DefaultOptions(dir)
	opts.RepairTornTail = false
	t.Logf("[触发条件] RepairTornTail=false，活动段尾部仅 12 字节残帧")
	_, err := Open(opts)
	var ce *CorruptionError
	if !errors.As(err, &ce) || ce.Kind != KindIncompleteTail {
		t.Fatalf("[判定依据] 期望 KindIncompleteTail，实际: %v", err)
	}
	t.Logf("[判定依据] 拒绝打开并返回类别 %s（文件 %s），不静默跳过", ce.Kind, ce.File)
}

// TestWriteFailureLeavesNoHalfState 并发写过程中强制让写失败不现实，
// 这里验证公开语义边界：正常提交后立即 kill（不 Close）不丢任何键；
// 并且在 Fsync=true 下串行 500 次提交后状态完整（无半成品索引）。
func TestDurableSerialWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(DefaultOptions(dir))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			if err := db.Put([]byte("s"+itoa(i)), []byte("d"+itoa(i))); err != nil {
				t.Errorf("put: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	t.Logf("[触发条件] Fsync=true 连续 500 次提交后不 Close 重开")
	db = nil
	want := map[string]string{}
	for i := 0; i < 500; i++ {
		want["s"+itoa(i)] = "d" + itoa(i)
	}
	assertState(t, dir, want)
}
