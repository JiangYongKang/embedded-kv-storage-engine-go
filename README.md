# embedded-kv：可嵌入的持久化键值存储引擎（Go）

一个零依赖、可直接嵌入 Go 进程的单机 KV 存储。核心目标是：
**进程被 SIGKILL / 掉电 / 写入中途失败后重启，数据状态依然可预期**，
所有异常磁盘状态在恢复时按原因分类报告，绝不静默跳过坏数据。

```go
db, err := kv.Open(kv.DefaultOptions("/var/lib/myapp/kv"))
defer db.Close()

db.Put([]byte("user:1"), []byte("alice")) // 写入/覆盖
v, err := db.Get([]byte("user:1"))        // 读取
db.Delete([]byte("user:1"))               // 删除（墓碑）
db.Compact()                              // 回收历史数据
```

---

## 1. 存储结构与持久化模型

数据目录中的文件：

| 文件 | 作用 |
| --- | --- |
| `manifest.toml` | 清单：当前快照文件名、尾巴 WAL 段集合、活动段 ID。逐行自带 CRC，通过「临时文件 → rename → fsync 目录」原子提交 |
| `wal-<id>` | 追加式 WAL 段，ID 单调递增；最大的 ID 是活动段 |
| `snap-<id>` | 快照：某一提交时刻的完整状态镜像（头部 CRC + 记录帧） |
| `*.tmp` / 未引用文件 | 崩溃残留，打开时作为孤儿安全删除 |

**记录帧**（WAL 与快照共用，见 `record.go`）：

```
magic("KVR1") | totalLen | hdrLen | seq(u64) | op | klen | vlen | key | value | crc32
```

- 每条记录是一次 `Put` 或 `Delete`，携带全局**单调递增序号 `seq`**。
- 一帧在**单次 `Write`** 中追加落盘，因此崩溃后只有两种结果：
  帧完整可读，或在文件最末尾留下一个残缺前缀。
- CRC 使用 Castagnoli 多项式，覆盖序号与全部内容。

### 提交点（commit point）

1. 编码记录帧；
2. `write()` 追加到活动 WAL；
3. `fsync()`（`Options.Fsync=true` 时，默认开启）；
4. **fsync 成功后**才更新内存索引。

因此：

- 读在任何时刻都看不到尚未提交的数据；
- 写失败 / 进程强杀后，内存里没有半成品状态，磁盘上至多留下最后一帧的
  残缺前缀，恢复时按「不完整尾部」类别处理；
- 重启后可见的内容与崩溃前最后一个 fsync 成功的提交点**逐键一致**。

---

## 2. 崩溃恢复保证与损坏分类

`Open` 只信任 `manifest.toml` 登记的文件集合，顺序为：
装载快照 → 按 ID 顺序重放全部尾巴 WAL（活动段最后）。
重放时全局校验 `seq` 严格递增，并把每类异常包装成 `*kv.CorruptionError`，
可通过 `errors.As` 取出后按 `Kind` 区分：

| Kind | 触发条件 | 默认处理 |
| --- | --- | --- |
| `KindIncompleteTail` | 活动 WAL 尾部存在凑不齐整帧的残留字节（写一帧中途被杀） | **自动截断修复**并在 `db.Repair` 中显式报告；可通过 `RepairTornTail=false` 改为拒绝打开 |
| `KindChecksumMismatch` | 帧长度完整但 magic / CRC / 字段不一致（内容被改写） | 拒绝打开，不截断、不跳过 |
| `KindSequenceRegression` | 记录 `seq` 不大于前一条（旧段拼回、日志伪造、段被调换） | 拒绝打开 |
| `KindManifestCorrupt` | 清单缺失 CRC 行 / 行校验不一致 / 版本不支持 | 拒绝打开，不猜测目录结构 |
| `KindMissingFile` | 清单引用的快照或 WAL 段在磁盘上不存在 | 拒绝打开 |

设计要点：

- **只有活动段的文件末尾允许出现不完整帧**——已封存段出现残缺同样按
  损坏报错，因为它本该不可变。
- 自动修复只丢弃「未提交的半截写入」，截断后文件被 `fsync` 持久化，
  修复结果本身也是崩溃安全的；修复事件通过 `DB.Repair`
  （`RepairedTornTail / RepairedFile / KeptSeq / DroppedBytes`）暴露，
  不是静默跳过。
- 内容损坏与尾部不完整用不同路径判定（帧声明长度 > 剩余字节 才是尾部
  不完整；帧完整而 CRC 错就是内容损坏），调用方可以据此做不同处置。

### 删除不是物理抹除

`Delete` 写入一条 `opDelete` **墓碑**记录（带新的 `seq`），
键不会从状态中消失。这样重放更旧的段时，旧值不会「复活」；
覆盖写同理：任何键的当前值永远是其 `seq` 最大的那条记录。

---

## 3. 并发语义

- `DB` 内部是一把 `sync.RWMutex`：
  - `Get` 持**读锁**，多个读互不阻塞，可与任意多个读同时进行；
  - `Put` / `Delete` / `Compact` 持**写锁**，同一时刻只有一个写，
    且与所有读互斥。
- 一次读拿到的是某个提交点上的索引快照，因此同一个键在读写并发时，
  结果**要么是旧值、要么是新值**，不存在部分数据或中间态。
- 写流程「先落盘提交、后改内存」。append/fsync 失败时会尽力把活动文件
  截断回上一个提交边界；若截断也失败，引擎进入 fatal 状态
  （后续操作报错），必须重新 `Open`，避免带着未知状态继续服务。

---

## 4. 历史数据回收（Compaction）

`db.Compact()` 在写锁下执行：

1. 用当前内存索引生成 `snap-N.tmp`（每键仅最新版本；写完 `fsync`）；
2. `rename` 为正式快照名并 `fsync` 目录；
3. 滚动新活动 WAL 段（旧段自此封存）；
4. **原子替换 `manifest.toml`**（临时文件 rename + 目录 fsync）——
   这是回收唯一的**提交点**；
5. 删除被快照取代的旧 WAL 段与旧快照。

保证：

- **内容等价**：回收后重启，恢复出的键集合与回收前完全一致
  （墓碑按 `dropBeforeSeq` 安全精简：早于所有尾巴段首条记录的墓碑
  已无旧数据需要压制，可在快照中丢弃）。
- **崩溃原子性**：崩溃发生在提交点之前 ⇒ 重启得到「回收前完整状态」
  （新快照只是孤儿文件，被清理）；崩溃发生在提交点之后 ⇒ 得到
  「回收后完整状态」（旧文件只是没来得及删的孤儿，被清理）。
  不存在卡在中间的可能。
- **回收期间的读**：压缩与读互斥（写锁），进行中的读只会落在
  回收前或回收后的完整状态，绝不会读到正在被删除文件中的失效旧数据。
- **读取性能**：读取永远是内存 map 点查 O(1)。回收只是把重放起点
  从旧段集合换成快照，数据量与历史写入量增长后，`Get` 耗时不随
  历史量线性变差（打开恢复时间则与「快照 + 回收后新写入量」相关）。

---

## 5. 配置项（`kv.Options`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Dir` | 必填 | 数据目录，不存在会自动创建（0700） |
| `SegmentSize` | `4 MiB` | WAL 段目标大小，达到后滚动新段；滚动本身也是一次 MANIFEST 原子提交 |
| `Fsync` | `true` | 每次提交都 `fsync`，保证强杀/掉电不丢已确认写入。设为 `false` 可换取吞吐，但崩溃可能丢失最近未刷盘的提交（仅供测试/可容忍丢失的场景） |
| `RepairTornTail` | `true` | 恢复时是否自动截断活动 WAL 的不完整尾部；`false` 时遇到尾部残缺直接返回 `KindIncompleteTail` 错误 |

---

## 6. 本地验证

要求 Go 1.25+。

```bash
go build ./...
go vet ./...

# 常规全量测试
go test ./... -v

# 并发竞态检测（强烈建议）
go test -race ./... -count=3
```

测试覆盖的场景（测试日志中会打印每个用例的**触发条件**与**判定依据**）：

| 测试 | 场景 |
| --- | --- |
| `TestCrashKillRecovery` / `TestDurableSerialWrites` | 提交后不 Close 直接放弃进程（等价强杀），重启逐键比对 |
| `TestTornTailAutoRepair` / `TestTornTailStrictMode` | 活动 WAL 尾部半截帧：自动截断并报告，或严格模式按类别拒绝 |
| `TestChecksumMismatch` | 翻转帧内字节，帧完整但 CRC 不一致 ⇒ `KindChecksumMismatch` |
| `TestSequenceRegression` | 追加 CRC 合法但序号回退的帧 ⇒ `KindSequenceRegression` |
| `TestMissingFile` / `TestManifestCorrupt` | 引用段缺失 / 清单行校验失败 |
| `TestOverwriteVersions` / `TestDeleteDoesNotResurrect` | 重复键多版本覆盖、删除墓碑不复活 |
| `TestConcurrentReadWrite` | 8 读 1 写并发，值只可能是完整旧值/新值，`-race` 无竞态 |
| `TestCompactCrashBeforeCommit/AfterCommit` | 子进程 `os.Exit(9)` 模拟 SIGKILL，注入回收协议三个阶段，验证重启只能落在回收前/回收后完整状态 |
| `TestCompactContentEquality` | 多轮回收 + 回收后继续写，重启内容逐键一致 |
| `TestCompactConcurrentWithOps` | 回收与读写并发进行，读者无错误、状态一致 |
| `TestReadDoesNotGrowWithHistory` | 6 轮历史增长中采样 2000 次 Get，平均耗时稳定（~200ns），不随历史线性增长 |

回收崩溃测试采用「父测试 `exec` 子测试 + `KV_CRASH_STAGE` 环境变量」
的方式在真实独立进程中注入 `os.Exit`（不执行 defer、不刷盘），
比在进程内模拟更接近 SIGKILL 语义。

## 7. 适用范围与限制

- 单机、单进程独占目录；不提供网络接口与多机复制。
- 崩溃一致性依赖文件系统对 `write/fsync/rename` 的标准 POSIX 语义
  （Linux ext4/xfs 等本地文件系统）；网络磁盘或禁用 barrier 的配置
  不在保证范围内。
- 回收期间持写锁，数据量极大时会阻塞读写，可在低峰期手动调用
  `Compact()`；后续可扩展为后台节流回收。
