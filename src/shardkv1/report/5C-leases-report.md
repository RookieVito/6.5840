# Lab 5C 实现说明

## 目标

Lab 5B 已经解决了单 controller 崩溃后的恢复问题：通过持久化 `curCfg` 和 `nextCfg`，新的 controller 能继续完成未完成的配置迁移。

Lab 5C 进一步要求：

1. 允许多个 controller 并行运行。
2. 某个 controller 崩溃或被分区后，新的 controller 可以接管未完成任务。
3. 并发场景下，多个 controller 可能同时观察到同一个未完成配置；系统必须能依赖请求幂等与去重安全收敛。
4. 并发场景下，多个 controller 甚至可能在本地各自算出相同 `Num`、但内容不同的配置；系统必须保证同一时刻只有一个 controller 真正推进配置。

## 5C 的具体思路

### 1. 保留 5B 的恢复骨架

5C 没有推翻 5B 的设计，仍然保留两个持久化配置键：

- `curCfg`：已经完全生效的当前配置。
- `nextCfg`：已经登记、但可能尚未完成迁移的目标配置。

因此：

- 如果 controller 在迁移中崩溃，只要 `nextCfg.Num > curCfg.Num`，后续 controller 仍然可以从 `curCfg -> nextCfg` 继续收尾。
- shard 迁移 RPC 继续依赖 5B 中已经做好的幂等语义，允许恢复 controller 重复执行 Freeze / Install / Delete。

### 2. 在 5B 外面增加一层 leased leadership

5C 的核心新增点不是新的迁移协议，而是给 controller 增加一个持久化的“领导权租约”：

- 控制器状态额外新增 `ctrlerLease`。
- 其中保存：
  - `holder`：当前 lease 持有者的 controller 标识。
  - `epoch`：单调递增的 fencing token。
  - `deadline`：租约过期时间。

每个 controller 启动后，先去竞争 lease：

1. 读取当前 lease。
2. 如果 lease 还没过期且属于别人，则等待重试。
3. 如果 lease 已过期，或者当前持有者就是自己，则通过 `kvsrv` 的版本号 CAS 写入新的 lease。
4. 成功写入后，当前 controller 才能执行 `InitController()` 或 `ChangeConfigTo()`。

这个设计的含义是：

- 即使多个 controller 同时运行，也只有 lease 持有者能继续推进配置。
- 旧 controller 一旦分区，无法续租；租约过期后，新 controller 就能接管。
- 旧 controller 重新连通后，即使还在执行旧逻辑，也因为 lease/epoch 已失效而被 fencing 掉，不能继续推进配置。

### 3. 后台续租，失租即退出

controller 获取 lease 后，会启动一个后台 goroutine 周期性续租：

- 周期小于 TTL，例如 `renew interval < lease TTL`。
- 每次续租都要求：
  - 当前 lease 仍然是自己持有。
  - 当前 lease 仍未过期。
  - 通过 CAS 成功把 `deadline` 向后推进。

一旦续租失败，当前 controller 立刻认为自己失去领导权，并停止后续推进：

- 不再推进 `nextCfg`。
- 不再把 `curCfg` 提交为新配置。
- 不再无限重试 shard migration。

这样可以防止“旧 controller 在网络恢复后继续把过期的迁移做完”。

### 4. 所有关键路径都要带 lease 检查

只在进入 `ChangeConfigTo()` 时获取一次 lease 还不够，因为 controller 可能在执行过程中被分区。为此，5C 在关键路径增加了“带 lease 检查”的版本：

- `readConfigWithLease()`：读配置时，如果 lease 已经失效，直接返回。
- `tryAdvanceConfigWithLease()`：推进 `curCfg` / `nextCfg` 时，如果 lease 失效，停止写入。
- `configSupersededWithLease()`：判断目标配置是否已经被别人完成时，也要求当前 controller 仍持有 lease。
- `moveShard()` / `finishConfigChange()`：每个迁移重试循环都检查 lease，失租立即退出。

这一步是 5C 成功的关键。否则 controller 在分区期间卡在某个阻塞重试里，恢复网络后仍然可能继续写入旧状态。

### 5. 为什么可以解决“相同 num 不同内容”的冲突

这是 5C 最重要的问题之一。

多个 controller 并发时，可能都基于同一个 `curCfg` 算出“下一个配置号是 `curCfg.Num + 1`”，但它们本地生成的新配置内容不同。

解决方法不是比较“谁算得更对”，而是限制“谁有资格提交”：

- 只有持有有效 lease 的 controller 能把 `nextCfg` 从 `curCfg` CAS 推进到新的配置。
- 其他 controller 即使算出相同 `Num`，只要没有 lease，就写不进去。
- 即使旧 controller 曾经持有 lease，但在它真正提交前 lease 过期了，它也会在带 lease 检查的 CAS 中失败。

因此，同一时刻最多只有一个 controller 能推进某个 `Num` 对应的目标配置。

### 6. 为什么接管后仍然能继续完成旧任务

lease 只负责“谁有资格继续推进”，不负责保存“推进到哪一步”。

真正的恢复信息仍然在 5B 的持久化状态里：

- `curCfg` 表示系统目前已经完全提交到哪里。
- `nextCfg` 表示还有哪份配置正在进行中。

因此新的 controller 接管时，不需要知道旧 controller 在迁移的哪个子步骤中断：

1. 先拿到 lease。
2. 读出 `curCfg` 和 `nextCfg`。
3. 如果 `nextCfg.Num > curCfg.Num`，就重新执行一遍 `curCfg -> nextCfg` 的迁移。
4. shard group 依赖幂等 RPC 自动吸收重复执行。

## 实现步骤

### 1. 增加持久化 lease 状态

在 `shardctrler` 中新增：

- `leaseKey = "ctrlerLease"`
- `leaseState`
- `leaseSession`

并实现：

- `readLease()`
- `writeLease()`
- `acquireLease()`
- `renewLeaseLoop()`
- `refreshLease()`
- `releaseLease()`

### 2. 改造 InitController

`InitController()` 不再直接做恢复，而是：

1. 先获取 lease。
2. 在 lease 有效期间读取 `curCfg` 和 `nextCfg`。
3. 若发现存在 `nextCfg.Num > curCfg.Num`，继续完成迁移。
4. 一旦失租，立即退出。

### 3. 改造 ChangeConfigTo

`ChangeConfigTo(newCfg)` 改为：

1. 先获取 lease。
2. 若存在旧的 pending 配置，优先完成它。
3. 只有在 lease 有效时，才允许把 `nextCfg` 从 `curCfg` 推进到 `newCfg`。
4. 迁移完成后，再在 lease 有效时提交 `curCfg = newCfg`。

### 4. 在迁移循环中加入 fencing

对以下路径全部增加 lease 检查：

- 读取配置
- 推进配置
- 判断 superseded
- shard 迁移的重试循环

这样旧 controller 只要失租，就不会在恢复网络后继续推进旧迁移。

## 需要考虑的边界情况

### 1. 多个 controller 同时启动

- 现象：都去争抢 lease。
- 处理：依赖 `kvsrv` 上的 CAS，只有一个成功写入 `ctrlerLease`。

### 2. controller 在拿到 lease 后马上分区

- 现象：旧 controller 无法续租。
- 处理：lease 超时后由新 controller 接管；旧 controller 在本地 lease 失效后停止推进。

### 3. 旧 controller 分区恢复后继续执行旧逻辑

- 风险：它可能继续写 `nextCfg`、推进 `curCfg`，或者继续做 shard migration。
- 处理：所有关键读写路径都带 lease 检查；恢复连通后它会发现自己不再持有当前 lease，从而退出。

### 4. 新 controller 接管时，旧迁移只做了一半

- 现象：有些 shard 已冻结但未安装，有些已安装但未删除。
- 处理：重新执行迁移流程；依赖 shard RPC 的幂等语义安全收敛。

### 5. 多个 controller 观察到同一个未完成的 `nextCfg`

- 现象：它们都可能尝试从 `curCfg -> nextCfg` 做恢复。
- 处理：只有 lease 持有者会真正持续推进；其他 controller 即使开始执行，也会在后续 lease 检查中退出。

### 6. 多个 controller 计算出相同 `Num`、不同内容的新配置

- 风险：同一个配置号出现分叉。
- 处理：只有 lease 持有者有资格把 `nextCfg` 从 `curCfg` 推进到自己的候选配置；其他 controller 的 CAS 会失败或在失租后退出。

### 7. `Put()` 返回 `ErrMaybe`

- 风险：无法确定 lease / config 是否写成功。
- 处理：所有关键写入后都通过 `Get()` 校验当前值，若目标值已经存在则视为成功，否则按竞争失败处理。

### 8. 续租 goroutine 与主线程并发

- 风险：主线程已结束，但续租线程还在工作；或者主线程继续执行时 lease 实际已经失效。
- 处理：
  - 用 `leaseSession.stopCh/doneCh` 控制生命周期。
  - 主流程统一通过 `lease.active()` 判断是否还能继续。

### 9. controller 正在提交 `curCfg` 时 lease 过期

- 风险：旧 controller 在临界点把已经失效的迁移提交成功。
- 处理：提交 `curCfg` 使用 `tryAdvanceConfigWithLease()`；lease 失效后不会继续推进。

### 10. controller 主动结束时如何释放 lease

- 现象：如果不释放，只能等 TTL 自然超时。
- 处理：在 `lease.close()` 中尝试把 lease 清空，加快下一个 controller 接管速度。

## 结果

这一版 5C 的实现保留了 5B 的恢复与幂等迁移模型，同时通过 lease + fencing 增加了并发 controller 之间的排他推进能力：

- 可以支持多个 controller 并行运行。
- controller 崩溃或分区后，新的 controller 能接管未完成配置。
- 多个 controller 并发处理相同 pending 配置时，系统能依靠幂等迁移和 lease fencing 收敛。
- 对于同一个 `Num` 的竞争，只允许一个 controller 在同一时刻推进配置，避免分叉提交。
