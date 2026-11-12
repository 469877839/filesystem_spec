# fsspec 事务能力审计：LocalFileSystem 与 MemoryFileSystem

- 审计对象：工作区 fsspec（`fsspec/transaction.py`、`fsspec/spec.py`、`fsspec/implementations/local.py`、`fsspec/implementations/memory.py`，版本 `0.0.post2+gd53423696`）
- 复现：`python audit/repro_transaction.py`（子进程硬崩溃 + 故障注入，实测约 0.16 秒，95/95 探针通过；完整输出见 `audit/repro_output.txt`）
- 上游自带事务测试：21 项全部通过（`test_local.py` / `test_memory.py`）

## 0. 结论先行

1. **这不是数据库事务，是“延迟写队列 + 逐个搬移”**。没有两阶段提交、没有回滚日志、没有锁、没有 fsync、没有崩溃恢复。官方注释自己的措辞是 *semi-atomically*（`fsspec/transaction.py:11`）。
2. **能承诺的只有两件事**：提交之前新数据不可见；正常退出时按打开顺序逐个提交、异常退出时逐个丢弃。单文件在“本地同设备 + `wb` + commit 成功”时接近原子发布；**跨文件不具备原子性**。
3. **提交中途抛错时，已提交的前缀永久可见**，后续文件被丢弃，状态机在 `finally` 中强制复位（避免后续写入被静默吞掉）。读到过的前缀不能靠事后删除描述成“从未可见”——本报告所有结论均以可观测探针为准。
4. **已打开的读取器持有旧快照**：本地靠 POSIX inode（rename 后旧 fd 继续读旧 inode），内存靠对象引用（store 换了 dict 值，旧 reader 还指向旧 `MemoryFile`）。新打开的读取器在每个 commit 之后立即看到该文件的新内容，所以提交窗口内可以读到**混合版本集**。
5. **两个后端都没有列表缓存参与**：`LocalFileSystem.ls`（`local.py:60`）和 `MemoryFileSystem.ls`（`memory.py:168`）直接读底层，从不查 `dircache`；`use_listings_cache` 对这两个后端是惰性空转。
6. 本地后端存在两个真实陷阱（已实测）：**只有含 `w` 的模式被延迟**，`ab`/`xb`/`a+b` 会直写目标且在 commit 时抛 `AttributeError`，`r+b` 静默绕过；**嵌套事务会丢数据并在退出时崩溃**。

## 1. 机制：代码里实际发生了什么

### 1.1 入队

`AbstractFileSystem.open()` 在事务中把写句柄塞进队列，条件是“非 autocommit 且模式不含 `r`”：

- `ac = kwargs.pop("autocommit", not self._intrans)`（`spec.py:1499`）
- `if not ac and "r" not in mode: self.transaction.files.append(f)`（`spec.py:1508-1509`）

事务对象是 filesystem 实例上的一个 `deque`（`transaction.py:22`），由 `fs.transaction` 懒创建（`spec.py:345-347`）。

### 1.2 完成 / 丢弃

`Transaction.complete()`（`transaction.py:42-69`）：

- `while self.files:` 从左端 `popleft()`，FIFO 顺序，逐个 `f.commit()` 或 `f.discard()`；
- 正在处理的文件出队后若抛错，`finally` 把它 `appendleft` 回去，再统一 `discard` 剩余项（`transaction.py:54-66`）；
- 无论成功失败，`finally` 都复位 `fs._intrans = False`、`fs._transaction = None`、`self.fs = None`（`transaction.py:67-69`）。这避免了“一次失败后所有后续写入都被静默延迟”的进程级污染，上游 `test_transaction_ends_when_a_commit_fails` 覆盖。

上下文退出只在无异常时提交：`self.complete(commit=exc_type is None)`（`transaction.py:31`）。异常退出时逐个 `discard()`。

### 1.3 本地后端：临时文件 + 改名的单文件发布

`LocalFileOpener._open()`（`local.py:405-421`）：

- `autocommit=True` 或模式不含 `w` → 直接 `open(self.path)`；
- 否则 `tempfile.mkstemp()` 建临时文件（`local.py:414-417`），写入全在临时文件里；
- `commit()` 先关闭句柄，再 `shutil.move(self.temp, self.path)`（`local.py:450-459`）；
- `discard()` 关闭句柄并 `os.remove(self.temp)`（`local.py:475-480`）。

`shutil.move` 的实际路径（实测）：同设备 → 1 次 `os.rename`、0 次 copy；跨设备 → `copy2` + `unlink`。`commit()` 不调用 `fsync`（源码中不存在），也不提交目录项。

### 1.4 内存后端：store 里换对象

`MemoryFileSystem._open()`（`memory.py:298-355`）：

- 写模式构造 `MemoryFile`，**只有非事务才立即 `m.commit()`**（`memory.py:346-352`）；
- `MemoryFile.commit()` 就是 `self.fs.store[self.path] = self`（`memory.py:486-488`）；
- `MemoryFile.discard()` 是 `pass`（`memory.py:483-484`）——没进 store 的对象本来就不需要“删”；
- `close()` 也是 `pass`（`memory.py:480-481`），未关闭的写句柄照样会在 commit 时进 store（已实测）。

`store` 默认是**进程内全局共享 dict**（`memory.py:25`，`global_store=True`）；事务标志是**实例级**的。因此两个实例共享数据但不共享事务。

## 2. 逐路径观察矩阵（实测）

“旧对象”指目标路径已存在的版本；“新对象”指事务内写入的新内容。所有行都对应 `audit/repro_transaction.py` 中的探针编号。

### 2.1 LocalFileSystem

| 路径 | 旧对象（已存在路径） | 新对象（不存在路径） | 已打开 reader | 新调用 ls/info/cat |
|---|---|---|---|---|
| 事务内写入后、提交前 | 旧内容原样可见（L1） | 不出现（L1） | 不受影响 | 旧状态 |
| 正常退出 complete | 被 rename 原子替换（同设备，实测 1 次 `os.rename`、0 次 copy） | 出现（L1） | 旧 fd 继续读旧 inode（L2） | 立即看到新内容（L2/L9） |
| 上下文异常退出 | 不变（L3） | 不出现（L3） | 不受影响 | 旧状态；临时文件被清理（L3） |
| 第 k 个 commit 抛错 | 前 k-1 个已替换且**永久可见**（L4） | 前 k-1 个已出现；第 k 个与后续被丢弃、不出现（L4） | 前缀的新 fd 可读到新内容 | 混合状态（L5） |
| 两次 commit 之间 | 前序已换、后续仍是旧（L5 快照） | 前序已出现、后续未出现 | — | **混合版本集**（L5） |
| 跨设备 commit（temp 与目标不同设备） | `copy2+unlink`；copy 中途失败 → **目标被截断为部分字节，旧对象已被破坏**（L6） | 部分文件可见（L6） | 旧 fd 若已打开仍读旧 inode | 部分内容 |
| discard 自身抛错 | 不变（L13） | 不出现（L13） | — | 不变；临时文件泄漏在磁盘（L13）；discard 异常会掩盖用户异常 |
| 进程硬崩溃于 complete 循环 | 前缀已替换（L14） | 前缀已出现；未处理项不出现 | — | 混合状态永久存在；临时文件全部孤儿化，**无恢复、无重放**（L14） |

### 2.2 MemoryFileSystem

| 路径 | 旧对象 | 新对象 | 已打开 reader | 新调用 ls/info/cat |
|---|---|---|---|---|
| 事务内写入后、提交前 | 旧内容可见（M1） | 不出现；store 未变（M1） | 不受影响 | 旧状态 |
| 正常退出 complete | store 换对象（M1） | 出现（M1） | 旧 reader 仍持旧对象、读旧内容（M2） | 立即看到新内容（M2） |
| 上下文异常退出 | 不变（M3） | 不出现（M3） | 不受影响 | 旧状态 |
| 第 k 个 commit 抛错 | 前缀已换且永久可见；第 k 个旧值保留（store 赋值在 commit 最后一步，注入点在赋值前）（M4） | 前缀已出现；第 k 个与后续不进 store（M4） | — | 混合状态（M5） |
| 两次 commit 之间 | 前序已换、后续仍旧（M5 快照） | 前序已出现、后续未出现 | — | **混合版本集**（M5） |
| 进程硬崩溃 | 前缀已换（进程内） | 前缀已出现；未处理项随进程消失 | — | 整个 store 随进程消失（内存后端无持久性） |

### 2.3 半提交时间线（本地，a→b→c 顺序提交，b 的 commit 抛错）

```
t0  事务内写入完成        a=OLD  b=OLD  c=不存在      （读者只能看到 OLD）
t1  complete() 开始
t2  a.commit() 完成       a=NEW  b=OLD  c=不存在      ← 此刻读者看到混合集
t3  b.commit() 抛错       a=NEW  b=OLD  c=不存在      ← 异常传播给调用方
t4  finally: b、c 丢弃    a=NEW  b=OLD  c=不存在      ← 临时文件已清理
t5  事后 rm(a)            a 被删                       ← 但 t2~t5 之间 NEW 已被读到
```

**t5 的删除不能改写 t2~t5 的观测历史**。数据集发布必须把“已提交前缀”当作已发布事实处理（见第 5 节）。

## 3. 能力表

### 3.1 单文件能力

| 能力 | LocalFileSystem | MemoryFileSystem | 证据 |
|---|---|---|---|
| 提交前新数据不可见 | 是（临时文件在系统临时目录，不在目标目录） | 是（对象未进 store） | L1 / M1 |
| 单文件发布原子性（读者不见半写内容） | 同设备：是（`os.rename`）；跨设备：**否**（copy2+unlink，实测留下部分文件） | 是（dict 赋值换对象，单进程内） | L2 / L5 / L6 / M2 |
| 覆盖已有对象时旧 fd 快照保持 | 是（POSIX inode 语义） | 是（reader 持旧 `MemoryFile`） | L2 / M2 |
| 提交失败时旧对象完好 | 同设备 commit 前失败：是；跨设备 copy 失败：**否**（旧对象已被截断覆盖） | 是（赋值在最后一步，异常注入时旧值保留） | L6 / M4 |
| 元数据/目录列表立即反映提交 | 是（stat 直读） | 是（store 直读） | L9 / M5 |
| 需要 fsync / 掉电持久 | **不提供**（源码无 fsync） | 不适用（进程内存储） | 源码 `local.py:450-473` |
| 未关闭的写句柄能提交 | 是（`commit()` 自行 close） | 是（`close()` 是 no-op） | 实测 |

### 3.2 跨文件能力

| 能力 | LocalFileSystem | MemoryFileSystem | 证据 |
|---|---|---|---|
| 全部提交或全不提交 | **否** | **否** | L4 / L5 / M4 / M5 |
| 提交顺序 | 打开顺序 FIFO | 打开顺序 FIFO | `transaction.py:46-52` |
| 中途失败：已提交前缀 | **永久可见**，不回滚 | **永久可见**，不回滚 | L4 / L5 / M4 / M5 |
| 中途失败：未处理项 | 逐个 discard，临时文件清理 | 未进 store，等同丢弃 | L4 / M4 |
| 中途失败：失败那一项 | 同设备：旧对象保留；跨设备：可能半写 | 旧值保留（注入点在赋值前） | L6 / M4 |
| 异常退出（上下文） | 全部丢弃；旧值不变；临时文件清理 | 全部丢弃；旧值不变 | L3 / M3 |
| 提交窗口内的读者 | 可读到**混合版本集**（前序新、后续旧） | 同左 | L5 / M5 |
| 失败后事务状态复位 | 是（`finally`） | 是（`finally`） | L4 / M4 |
| 并发/冲突检测 | **无** | **无** | L8（同路径两次写，后者胜出） |
| 嵌套事务 | **破坏性**：丢外层队列 + 外层退出抛 `AttributeError` | 共用同一 `Transaction` 类与队列机制，故障模式相同（本地实测，内存为代码同构推断） | L11 |

### 3.3 后端未承诺的行为（不要假设）

| 项 | 现状 | 证据 |
|---|---|---|
| 崩溃一致性 | 无日志/无重放；硬崩溃后前缀永久可见、临时文件孤儿化，新进程不做修复 | L14 |
| 跨设备原子性 | 依赖 `shutil.move` 的 copy 回退，非原子 | L6 |
| 跨进程事务 | 本地无锁；内存 store 仅进程内，事务更不跨进程 | 源码 + M9 |
| 多实例隔离 | 每个实例独立 `_intrans`/队列；内存共享 store 但事务不共享，交错提交无隔离 | M9 |
| `rm` / `mv` / `cp_file` / `touch` 的回滚 | 全部绕过事务、立即生效、异常退出不回滚（本地 `touch` 也绕过） | L7 / M7 |
| 非 `w` 写模式 | 本地 `ab`/`xb`/`a+b` 直写并导致 commit 抛 `AttributeError`；`r+b` 静默绕过。内存全部延迟，`r+b` 直接改共享对象 | L12 / M10 |
| 读缓存一致性 | 两个后端都不使用 `dircache`，`use_listings_cache` 空转 | L9 |
| 权限/属主 | 本地 commit 后按 umask 重设权限，不保留原文件权限 | `local.py:469-473` |
| Windows 覆盖语义 | `commit()` 吞掉 `PermissionError`（目标存在即视为成功），可能掩盖失败的 copy（未在本环境实测） | `local.py:460-466` |

## 4. 秒级复现

```bash
python audit/repro_transaction.py          # 约 0.16 秒，95 个探针
python audit/repro_transaction.py > audit/repro_output.txt 2>&1
python -m pytest fsspec/implementations/tests/test_local.py \
                 fsspec/implementations/tests/test_memory.py -k transaction -q
```

脚本不依赖 pytest，全部断言自带说明；任一探针不符则退出码为 1。故障注入点都是显式、可控的：

- 本地 commit 失败：替换 `LocalFileOpener.commit`，按文件名抛 `PermissionError`（L4/L5）；
- 本地跨设备：把 `tempfile.tempdir` 指向 `/dev/shm`（不同 `st_dev`），替换 `shutil.copyfile` 写一半后抛错（L6）；
- 本地 discard 失败：替换 `LocalFileOpener.discard` 抛 `OSError`（L13）；
- 本地硬崩溃：子进程 `os._exit(7)`（L14），验证没有 `finally`、没有清理、没有重放；
- 内存 commit 失败：替换 `MemoryFile.commit` 按路径抛错（M4）。

关键实测摘录：

```
L4  prefix committed, later files discarded, state reset
L5  mid-commit: first file already new / second still old / third still old
L6  target now holds PARTIAL bytes (old object destroyed)  -- b'PARTIAL'
L11 outer context exit crashes with AttributeError
L12 mode=ab  writes immediately AND commit raises AttributeError
L14 child died hard; a stays committed; b,c absent; 2 orphaned temp files; no auto-repair
M4  /a committed; /b, /c absent; state reset
M5  mid-commit: first object already new / second still old / third still old
```

## 5. 数据集发布的最小使用约束

目标不是把 fsspec 改成强事务，而是**在现有保证下安全地发布**。按优先级：

1. **把事务当“提交时机控制”，不当原子性开关。** 读者在 `with` 块退出前不要读；但必须接受“退出过程中可能读到前缀”。发布侧要按“可能部分成功”设计。
2. **数据对象不可变、带唯一版本名**（如 `shard-<uuid>.parquet`），不要覆盖旧路径。部分提交最多多出孤儿对象，不会破坏已发布版本；无效前缀可以安全清理。
3. **用清单（manifest）作为唯一发布开关，并且清单最后提交。** 先写分片，再写 manifest；读者只读 manifest 中列出的对象。注意：manifest 本身也在同一事务里，若它之前的某个分片失败，manifest 不会出现——但已提交分片会留下。发布流程应为“先写新版本目录/前缀 → 写 manifest → 清理孤儿”，而不是原地覆盖。
4. **一份数据只由一个进程、一个实例、一个线程写。** 事务状态在实例上，队列无锁，跨实例/跨进程没有隔离；内存后端更是进程内 store。
5. **只用 `wb` / `wb+` 写模式（本地必须含 `w`）。** 禁止 `ab`/`xb`/`a+b`/`r+b`：本地会直写目标并在 commit 时抛 `AttributeError`（L12）；内存 `r+b` 会直接改共享对象（M10）。
6. **禁止在事务内调用 `rm` / `mv` / `cp_file` / `touch`。** 它们立即生效且异常退出不回滚（L7/M7）；需要删除或改名时，放在事务成功之后。
7. **保证临时目录与目标同设备。** 设 `TMPDIR` 到目标所在文件系统；否则 commit 走 copy+unlink，非原子，失败会留下部分文件并破坏旧对象（L6）。
8. **不要嵌套事务，不要复用 Transaction 对象，不要用 `autocommit=True` 混写。** 嵌套会丢外层队列并在外层退出时抛 `AttributeError`（L11）；`autocommit` 会让写入立刻可见（L10）；`Transaction` 完成后 `self.fs=None`，对象不可复用。
9. **提交后校验，失败后按“已提交清单”处理。** commit 返回后重新 `info`/读回大小与摘要；失败时记录已成功的前缀，只清理确定未提交的对象。绝不可以用“事后 rm”把已读到的前缀说成从未可见（见 §2.3）。
10. **接受不持久：** 本地 commit 无 `fsync`；掉电/内核崩溃可能丢失已“提交”的数据。需要强持久时，在事务外用 `os.fsync` 对文件和目录再刷一次（这不改变事务语义，只补持久性）。
11. **内存后端只用于测试/单进程缓存。** 它没有跨进程可见性，进程退出即丢失；不要拿它做发布后端。

### 5.1 推荐的最小发布骨架（不改造 fsspec）

```python
# 伪代码：一次发布 = 新版本前缀 + 最后写 manifest
prefix = f"dataset/v{version}/"          # 不与旧版本共享路径
with fs.transaction:                     # 只用 wb；不要 rm/cp/mv/touch
    for i, shard in enumerate(shards):
        with fs.open(f"{prefix}part-{i:04d}.parquet", "wb") as f:
            f.write(shard)
with fs.transaction:                     # manifest 单独一个事务，最后提交
    with fs.open(f"{prefix}_MANIFEST", "wb") as f:
        f.write(manifest_bytes)
# 读者只认 _MANIFEST；失败时丢弃整个 prefix（未被 manifest 引用）
```

更严格（可选，不属于本题要求）：把整个版本目录先写到临时目录，再对目录做一次 `rename`。目录 rename 在同一文件系统内是原子的，可以把“跨文件”问题降级为“单目录”问题；但这不是 fsspec transaction 提供的，需要调用方自己做。

## 6. 实测到的具体缺陷（本版本）

| # | 缺陷 | 触发 | 后果 | 证据 |
|---|---|---|---|---|
| 1 | 本地事务只延迟含 `w` 的模式 | 事务内 `fs.open(p, "ab")` / `"xb"` / `"a+b"` | 写入**立刻落到目标**；退出时 `commit()` 访问不存在的 `self.temp`，抛 `AttributeError`；二者叠加造成“已可见 + 报错” | L12 |
| 2 | `r+b` 静默绕过事务 | 事务内 `fs.open(p, "r+b")` | 直接改真实文件，立即可见，退出不报错（因为 `"r" in mode` 不入队，`"w" not in mode` 走真实路径） | L12 |
| 3 | 嵌套事务丢数据并崩溃 | `with fs.transaction:` 内再 `with fs.transaction:` | 内层 `start()` 清空外层队列（外层已写文件永不提交、临时文件泄漏）；内层 `__exit__` 把 `self.fs=None`；外层 `complete()` 抛 `AttributeError` | L11 |
| 4 | discard 失败会掩盖用户异常 | 回滚时 `discard()` 抛错 | `OSError` 取代用户异常（原异常留在 `__context__`）；状态仍复位，但临时文件泄漏 | L13 |
| 5 | 跨设备 commit 非原子 | `TMPDIR` 与目标不同设备 | copy 回退，失败时目标被截断为部分字节，旧对象被破坏 | L6 |
| 6 | 硬崩溃无恢复 | 提交循环中进程被杀 | 前缀永久可见、未处理项丢失、临时文件孤儿化；新进程不修复 | L14 |

前 3 项与“接口保证”直接相关，建议在文档或封装层显式禁止；第 4~6 项属于发布流程必须接受的边界。

## 7. 适用边界总结

- **可以承诺**：提交前不可见；正常/异常退出时的队列处理；单文件同设备 rename 的原子替换；状态复位。
- **可以争取**：用“唯一版本名 + 最后写 manifest”把跨文件问题降级为“多出孤儿对象”。
- **不能承诺**：全有或全无、失败回滚、崩溃一致、跨设备原子、并发隔离、持久化。
- **不能做的事后叙事**：已经提交并被读到的前缀，删除它不等于“从未可见”。

## 附录 A：源码索引

| 主题 | 位置 |
|---|---|
| 队列与 complete/discard | `fsspec/transaction.py:7-69` |
| 入队条件 | `fsspec/spec.py:1499-1509` |
| transaction 属性 / start / end | `fsspec/spec.py:338-362` |
| 本地临时文件与 commit/discard | `fsspec/implementations/local.py:405-421`、`450-480` |
| 本地 rm/cp/mv/touch 旁路 | `local.py:133-143`、`165-176`、`191-200`、`230-241` |
| 内存写模式与 append 复制 | `fsspec/implementations/memory.py:298-355`（复制在 324-327） |
| 内存 commit/discard/close | `memory.py:480-488` |
| 内存 cp_file 直写 store | `memory.py:357-373` |
| 两端 ls 直读、不用 dircache | `local.py:60-77`、`memory.py:168-205` |

## 附录 B：完整探针输出

`audit/repro_output.txt`（95/95，含每条断言的实测值）。
