# embedded-kv-storage-engine-go

一个可嵌入 Go 进程的持久化键值存储引擎。所有数据落在单个追加式日志文件中，
面向的核心目标是：**进程被强杀或写入中途失败后重启，数据状态依然可预期。**

- 包导入路径：`github.com/highcumontoa/embedded-kv-storage-engine-go`
- 无任何第三方依赖，仅使用 Go 标准库。

## 快速使用

```go
package main

import (
	"fmt"
	"log"

	kv "github.com/highcumontoa/embedded-kv-storage-engine-go"
)

func main() {
	// 打开（不存在会创建）数据目录；默认每次提交都 fsync。
	s, err := kv.Open("./data", kv.WithAutoCompactThreshold(4<<20))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("hello"), []byte("world")); err != nil {
		log.Fatal(err)
	}
	if v, ok, err := s.Get([]byte("hello")); err == nil && ok {
		fmt.Println(string(v)) // world
	}
	if err := s.Delete([]byte("hello")); err != nil {
		log.Fatal(err)
	}

	// 需要立即回收历史版本时手动触发；也可依赖阈值自动回收。
	if err := s.Compact(); err != nil {
		log.Fatal(err)
	}
}
```

## 磁盘格式

数据目录下只有一个日志文件 `wal.log`，由一系列定长头帧顺序组成：

```
[0:4]   magic "KVSG"
[4]     记录类型：1=PUT，2=DELETE（墓碑），3=CHECKPOINT（回收产物头帧）
[5:13]  版本序号 seq（uint64，大端）
[13:17] key 长度（uint32，大端）
[17:21] value 长度（uint32，大端；DELETE/CHECKPOINT 为 0）
[21:25] CRC32-IEEE，覆盖类型/序号/长度以及 key/value
随后紧跟 key、value 原始字节。
```

- 每条 **PUT/DELETE 是一次独立提交**：一次 `write` 写完整个帧，成功后 `fsync`，
  然后才更新内存索引。
- 正常日志中 seq **严格单调递增**。回收产物首帧是 CHECKPOINT，
  其 seq 字段记录回收后的 `nextSeq`，保证“所有键都被删除后再回收”也不会让序号倒退。
- 读取路径是纯内存 map 查找（O(1)），不触碰磁盘、不扫描历史；
  重启时顺序重放日志重建内存索引。

## 持久化与恢复保证

### 提交原子性（write 或完整生效，或完全不生效）

`Put`/`Delete` 的提交顺序固定为：

1. 在写锁内分配下一个 seq，编码整帧；
2. `write` 整帧（单帧一次 `Write` 调用，内核视角是一次连续追加）；
3. 默认 `fsync`（见 `WithFSync`）；
4. 以上全部成功后才更新内存索引并推进已提交偏移。

因此一次写入要么完整生效、要么完全不生效：

- 写失败 / fsync 失败 / 短写：日志被截回上一个已提交偏移，内存索引不动，
  调用方收到错误，磁盘与内存都停留在旧版本；若截断也失败，引擎进入
  `broken` 状态拒绝继续服务（返回错误要求重开），绝不产生状态分叉。
- 进程在步骤 4 之前被杀：该帧要么完整留在日志中（重启后正常生效），
  要么整体缺失；不会出现“半帧已提交”。
- 进程在返回给调用方之前被杀：只要 write+fsync 已完成，重启就能看到该数据；
  未进入 write 调用的数据绝不会出现。

### 覆盖写的版本语义

每个键在内存索引中只保留 seq 最大的版本；seq 全引擎严格递增。

- 覆盖成功后，旧版本立即对所有后续读取不可见；进行中的读要么拿到旧值、
  要么拿到新值，不可能拿到拼接或部分字节（值以独立拷贝返回）。
- 历史旧版本仅占用日志空间，由回收处理。

### 删除语义（墓碑）

`Delete` 写入一条 DELETE 墓碑记录，而不是物理抹去键：

- 墓碑在正常日志中长期保留，因此任何“重放更早数据”的路径都不会让旧值复活；
- 墓碑只在回收时随所有死亡数据一起被丢弃（丢弃时已不存在更早日志段）。

### 重启恢复与损坏分类（不静默跳过）

`Open` 顺序重放日志，发现任何异常都返回 `*CorruptionError` 并拒绝打开，
绝不静默跳过。异常通过 `errors.Is` 区分三类，测试按类别断言：

| 类别（`Kind`）              | 哨兵错误                   | 触发条件                                   | 处理方式                       |
| --------------------------- | -------------------------- | ------------------------------------------ | ------------------------------ |
| `KindIncompleteTail`        | `ErrIncompleteTail`        | 尾部最后一帧只有半截（写中途掉电/被杀）    | `Repair` 可截掉半截尾部        |
| `KindChecksum`              | `ErrChecksumMismatch`      | 帧完整但 magic/类型/CRC 不符（非尾部损坏） | `Repair` **拒绝修改**          |
| `KindSequence`              | `ErrSequenceRegression`    | seq 不大于已见最大 seq（顺序被破坏）       | `Repair` **拒绝修改**          |

```go
s, err := kv.Open(dir)
if errors.Is(err, kv.ErrIncompleteTail) {
	report, rerr := kv.Repair(dir) // 仅截掉尾部半截帧，并 fsync 文件与目录
	...
}
```

`Repair` 是保守的：只有“尾部不完整”可以确定性修复；CRC 错误或序号回退可能意味着
更深层的数据被破坏，贸然截断会丢数据，因此原样返回错误让调用方介入（如从备份恢复）。

重启后可见内容 = 崩溃前所有已完成 write+fsync 的提交序列，不多不少。

## 并发语义

- 内部使用 `sync.RWMutex`：**多个读与单个写** 同时进行。读持有读锁，写持有写锁。
- 提交的可见性切换（更新索引、推进偏移）发生在同一把写锁内，是一个原子点：
  - 读永远看不到未提交数据；
  - 同一键并发读，结果要么是完整旧值，要么是完整新值，无中间态/部分数据；
  - 单个读者连续观察到的版本号单调不减。
- 写失败不更新索引并回滚日志，不会留下“半个新值”。
- `Get` 返回值的拷贝，调用方可以自由修改。

## 数据回收（Compaction）规则

`Compact`（或写提交时由 `WithAutoCompactThreshold` 自动触发）在写锁内执行：

1. 从内存索引收集所有存活键值（丢弃墓碑与被覆盖旧版本），按 seq 升序排列；
2. 写入临时文件 `wal.log.compaction.tmp`：先写 CHECKPOINT（nextSeq），
   再写每个存活键的 PUT 帧；
3. `fsync` 临时文件 → `os.Rename` 原子替换 `wal.log` → `fsync` 所在目录；
4. 切换文件句柄与内存索引。

由此得到的保证：

- **回收后重启恢复出与回收前完全一样的逻辑内容**（键值逐字节一致）；
- **回收期间进行中的读取**：回收需要写锁，先开始的读先完成（看到回收前完整视图），
  切换期间的新读短暂等待，切换完成后看到回收后完整视图——任何读都拿不到已失效数据；
- **回收中途崩溃**以 rename 为唯一切换点：
  - rename 前（含 tmp 写一半、fsync 前）被杀：tmp 成为孤儿，`Open` 时直接删除，
    重启结果是**回收前完整状态**；
  - rename 后被杀：新日志在 rename 前已完整 fsync，重启结果是**回收后完整状态**；
  - 不存在“切换到一半”的中间状态；
- 回收后日志大小只取决于存活数据量，与历史写入总量无关，因此读取（O(1) 内存查找）
  与重启重放的开销都不随历史写入量线性增长。

## 配置项

| Option | 默认 | 说明 |
| --- | --- | --- |
| `WithFSync(bool)` | `true` | 每次提交是否 `fsync`。关闭可提速，但机器掉电可能丢失最近未刷盘的写；进程被 SIGKILL 不影响已进入页缓存的完整帧。生产环境应保持开启。 |
| `WithAutoCompactThreshold(int64)` | `0`（关闭） | 日志大小达到该字节数后，在写提交成功时自动触发一次回收。日志大小被“阈值 + 一个最大帧”夹住。`<=0` 关闭，仅手动 `Compact`。 |

## 本地验证

```bash
# 全部单元测试（含子进程 SIGKILL 强杀、三类损坏注入、并发读写、回收中途崩溃）
go test ./... -v

# 竞态检测
go test ./... -race

# 读取性能（十万次历史覆盖后单次 Get 仍是常数耗时，约几十 ns/op）
go test -bench BenchmarkGetAfterLargeHistory -run '^$'
go vet ./...
```

测试覆盖的关键场景：

- **强杀恢复**：`TestKillBeforeWriteHidesUncommitted`（写调用前 SIGKILL，未提交数据不泄漏、
  可见键恰为完整前缀）、`TestKillAfterSyncKeepsCommit`（fsync 后、索引更新前 SIGKILL，提交不丢）、
  `TestCompactCrashSubprocess`（回收两个阶段 SIGKILL，状态非回收前即回收后）；
- **损坏尾部与分类**：`TestReopenIncompleteTail` / `TestReopenChecksumMismatch` /
  `TestReopenSequenceRegression`；
- **重复键覆盖 / 删除不复活**：`TestPutGetDeleteOverwrite`、`TestReopenRecommitsOnly`；
- **并发读写可见性**：`TestConcurrentReadersSingleWriterVisibility`、
  `TestConcurrentReadersWritersDistinctKeys`（`-race` 下通过）；
- **回收规则**：`TestCompactPreservesAndShrinks`、`TestCompactAllDeletedKeepsSeq`、
  `TestReadsAroundCompactionSwitch`、`TestReadsDuringRepeatedCompactions`；
- **历史不拖慢读取**：`TestHistoryDoesNotBloatReadPath`、`BenchmarkGetAfterLargeHistory`。

每个关键测试都会用 `t.Logf` 打印**触发条件**与**判定依据**，`-v` 运行可见。

## API

| 方法 | 说明 |
| --- | --- |
| `Open(dir string, opts ...Option) (*Store, error)` | 打开/创建引擎并重放日志；损坏时返回分类错误 |
| `(*Store) Get(key []byte) (value []byte, ok bool, err error)` | 读取当前已提交值（返回拷贝） |
| `(*Store) Put(key, value []byte) error` | 写入/覆盖（一次原子提交） |
| `(*Store) Delete(key []byte) error` | 删除（写墓碑） |
| `(*Store) Compact() error` | 手动回收历史版本与墓碑 |
| `(*Store) Len() int` | 当前存活键数量 |
| `(*Store) Close() error` | 刷盘并关闭 |
| `Repair(dir string) (RepairReport, error)` | 显式修复：仅截掉尾部不完整帧 |
