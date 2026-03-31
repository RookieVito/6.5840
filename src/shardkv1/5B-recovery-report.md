# Lab 5B 实现流程记录

## 目标

本次实现的目标是补齐 Lab 5B 中 shard controller 的恢复能力，解决以下问题：

1. controller 在配置迁移过程中宕机后，新的 controller 能继续完成未完成的配置变更。
2. 配置迁移不能只依赖内存状态，必须把“当前配置”和“待完成配置”持久化到 controller 使用的 `kvsrv` 中。
3. shard 迁移 RPC 需要支持重复执行，否则恢复后的 controller 会因为重试旧步骤而卡住。

## 总体方案

核心思路是在 `kvsrv` 中维护两个配置键：

- `curCfg`：已经完全生效的当前配置。
- `nextCfg`：已经登记、但可能尚未完成迁移的目标配置。

配置变更流程改为三阶段：

1. 先把 `nextCfg` 从 `curCfg` 原子推进到新配置。
2. 按 `old=curCfg`、`new=nextCfg` 执行分片迁移。
3. 所有迁移完成后，再把 `curCfg` 推进到 `nextCfg`。

这样即使 controller 在第 2 步中途崩溃，只要 `nextCfg.Num > curCfg.Num`，新 controller 就能知道有一个未完成的配置变更需要接管。

## 具体实现步骤

### 1. 扩展 controller 状态存储

在 `src/shardkv1/shardctrler/shardctrler.go` 中新增：

- `curCfgKey = "curCfg"`
- `nextCfgKey = "nextCfg"`

并实现以下辅助函数：

- `ensureConfig()`：初始化或覆盖配置键。
- `readConfig()`：循环从 `kvsrv` 读取配置。
- `tryAdvanceConfig()`：基于版本号和旧值做 CAS 推进，避免多个 controller 并发写坏配置。

`InitConfig()` 不再只写 `curCfg`，而是同时写入 `curCfg` 和 `nextCfg`，确保初始状态一致。

### 2. 实现 InitController 恢复逻辑

`InitController()` 启动时执行：

1. 读取 `curCfg`。
2. 读取 `nextCfg`。
3. 如果 `nextCfg.Num > curCfg.Num`，说明上一个 controller 在配置变更过程中中断。
4. 调用恢复流程继续完成 `curCfg -> nextCfg` 的迁移。

这个逻辑使得新 controller 不需要知道旧 controller 执行到了哪一步，只要重复执行迁移协议即可。

### 3. 重构 ChangeConfigTo

`ChangeConfigTo(newCfg)` 被改造成以下流程：

1. 读取当前 `curCfg` 和 `nextCfg`。
2. 如果已经存在 `nextCfg.Num > curCfg.Num`，优先完成这次未完成的变更。
3. 如果没有 pending 配置，并且 `newCfg.Num == curCfg.Num + 1`，则先把 `nextCfg` 推进到 `newCfg`。
4. 调用统一的 `finishConfigChange(old, new)` 执行迁移。
5. 迁移完成后把 `curCfg` 推进到 `newCfg`。

这样 `ChangeConfigTo()` 和 `InitController()` 共用同一套收尾逻辑，不会出现两套恢复路径。

### 4. 抽出统一的迁移执行函数

新增两个函数：

- `finishConfigChange(old, new)`：并发处理所有发生变动的 shard。
- `moveShard(old, new, shard, getClerk)`：处理单个 shard 的 Freeze / Install / Delete 流程。

迁移规则保持原有语义：

- `oldGid == 0`：目标组直接安装空 shard。
- `newGid == 0`：源组冻结并删除 shard。
- 普通迁移：`FreezeShard -> InstallShard -> DeleteShard`。

## 为恢复做的幂等改造

恢复意味着 controller 会重复发起迁移 RPC，因此 shardgrp 必须支持幂等。

### 1. FreezeShard 幂等化

在 `src/shardkv1/shardgrp/server.go` 中增加处理：

- 当 `args.Num < shardNum[shard]` 时，仍然返回 `ErrWrongGroup`，表示请求过旧。
- 当 `args.Num == shardNum[shard]` 时，直接返回 `OK`，表示该 shard 已在这个配置号完成迁移。

这一步很关键。否则恢复中的 controller 可能对已经删除的 shard 再次执行 freeze，导致旧组重新创建一个空 shard，破坏状态。

### 2. 让 Install/Delete 的 ErrWrongGroup 可返回上层

在 `src/shardkv1/shardgrp/client.go` 中修改：

- `InstallShard()` 遇到 `ErrWrongGroup` 时直接返回。
- `DeleteShard()` 遇到 `ErrWrongGroup` 时直接返回。

之前这两个 RPC clerk 会无限重试，不适合恢复场景。恢复后如果配置已经被更新或旧 controller 已经被取代，继续重试没有意义，只会把线程卡死。

## 并发与 supersede 处理

实现中增加了 `configSuperseded(targetNum)` 检查：

- 如果 `curCfg.Num >= targetNum`，说明目标配置已经完成。
- 如果 `nextCfg.Num > targetNum`，说明有更新的 controller 抢先推进了配置。

当迁移 RPC 返回 `ErrWrongGroup` 时，controller 会结合 `configSuperseded()` 判断自己是否已经过期；若已过期则直接退出，而不是继续执着于旧配置。

## 测试验证

本次实现后验证了以下测试：

```bash
cd src
go test ./shardkv1 -run 'Test(JoinLeave5B|RecoverCtrler5B)' -count=1
go test ./shardkv1 -run 'Test(JoinBasic5A|JoinLeaveBasic5A|Shutdown5A|JoinLeave5B|RecoverCtrler5B)' -count=1
```

验证结果：

- 5B 的 join/leave 恢复通过。
- controller 宕机后的恢复通过。
- 若干 5A 基础回归未被破坏。

## 当前结论

这一版实现已经满足 5B 的基本要求：

- controller 状态具备持久化恢复能力。
- 配置迁移具备“未完成任务接管”能力。
- 关键迁移 RPC 支持恢复场景下的重复执行。

后续如果继续做 5C，还需要在此基础上增加更严格的 controller 领导权控制与 fencing 机制，避免旧 controller 在网络恢复后继续推进过期配置。
