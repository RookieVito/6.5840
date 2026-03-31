# shardkv1 5A 修复记录

## 背景

在执行 `go test -run TestManyConcurrentClerkUnreliable5A` 时，测试偶发性失败，并由 Porcupine 报出 `history is not linearizable`。

问题只在“不可靠网络 + 并发 Clerk + join/leave 迁移”组合下稳定出现，和下面这组现象一致：

- `TestManyConcurrentClerkUnreliable5A` 失败。
- `TestManyConcurrentClerkReliable5A` 可以通过。
- `TestManyJoinLeaveUnreliable5A` 可以通过。

这说明基础的分片迁移流程本身大体可工作，但 `Put` 请求在不可靠 RPC 与迁移重试交叠时，返回语义存在错误。

## 失败根因

### 1. `Put` 的 `ErrMaybe` 判定过宽

原始实现里，组内 Clerk 在一次 `Put` 尝试中只要不是第一次重试，就会把某些后续错误过早提升为“这次写可能已经成功”。

这会产生两种对立的风险：

- 该返回 `ErrMaybe` 时却返回了 `ErrVersion`，让上层误以为写一定没成功。
- 该返回 `ErrVersion` 时却返回了 `ErrMaybe`，让 Porcupine 被迫把一次确定失败的写当成“可能已提交”。

这次真正卡住 witness 的，是第二种情况：一次 `Put` 先遇到 `ErrWrongLeader`，随后遇到 `ErrWrongGroup`，原实现把它错误算成了 ambiguous。实际上，`ErrWrongLeader` 本身并不代表这次写已经被某个 owner 接收，更不能单独推出 `ErrMaybe`。

### 2. 服务端对 shard ownership 的判断不够严格

原始服务端对 `Get`/`Put` 是否属于本组，主要依赖 `frozen`。这样会带来一个漏洞：

- 如果当前组从未安装过这个 shard，但 `frozen[shard] == false`，`Put(version=0)` 可能直接在本地创建空 shard 并接收写入。
- 这会让“未持有 shard 的组”误收请求，破坏迁移边界。

这不是最初 failing witness 的唯一根因，但属于明确的 correctness risk，所以一并修正。

## 修复内容

### 1. 将“组内 ambiguous 状态”与“顶层最终返回语义”拆开

修改位置：

- [client.go:90](/home/vito/workspace/6.5840/src/shardkv1/client.go#L90)
- [shardgrp/client.go:79](/home/vito/workspace/6.5840/src/shardkv1/shardgrp/client.go#L79)

修复思路：

- `shardgrp.Clerk.Put` 现在返回两个值：`(rpc.Err, bool)`。
- 第一个返回值仍然是当前轮 RPC 语义上的结果。
- 第二个返回值表示“这一轮里是否真的发生过可能丢回复的 RPC”。
- 只有 `ok == false` 的网络失败才会把 `maybeLost` 置为 `true`。
- 单纯的 `ErrWrongLeader` 不再被当作“可能已提交”。
- 顶层 `shardkv.Clerk.Put` 持续累积 `maybeLost`，并在刷新配置后根据最终结果决定：
  - 如果最终得到 `OK`，直接返回 `OK`。
  - 如果最终得到 `ErrVersion`，但之前确实存在可能丢回复，则升级为 `ErrMaybe`。
  - 如果最终得到 `ErrVersion`，且没有 ambiguous 历史，则保留 `ErrVersion`。

这样就避免了两边都出错：

- 不会把“其实已成功并迁移走”的写错报为 `ErrVersion`。
- 也不会把“其实没成功，只是换 leader/换 group 了”的写错报为 `ErrMaybe`。

### 2. 为 shard ownership 增加明确判定

修改位置：

- [shardgrp/server.go:100](/home/vito/workspace/6.5840/src/shardkv1/shardgrp/server.go#L100)
- [shardgrp/server.go:105](/home/vito/workspace/6.5840/src/shardkv1/shardgrp/server.go#L105)
- [shardgrp/server.go:138](/home/vito/workspace/6.5840/src/shardkv1/shardgrp/server.go#L138)

修复思路：

- 新增 `ownsShardLocked(shard)`，要求：
  - `kv.data[shard]` 已存在。
  - `kv.frozen[shard] == false`。
- `Get` 和 `Put` 都必须先通过 ownership 检查，否则统一返回 `ErrWrongGroup`。
- 这样可以避免未安装 shard 的组把请求误收为本地写。

### 3. 让初始配置中的 `Gid1` 显式拥有全部 shard

修改位置：

- [shardgrp/server.go:347](/home/vito/workspace/6.5840/src/shardkv1/shardgrp/server.go#L347)

修复思路：

- 当服务器是全新启动的 `Gid1`，且 `Persister` 里没有 raft state / snapshot 时，初始化全部 shard 的空 map，并把版本号记为 `shardcfg.NumFirst`。
- 这样不会被新的 ownership 检查误判成“从未安装过 shard”。
- 同时不会影响重启恢复路径，因为只有“完全无持久化状态”的 brand-new server 才会走这个初始化分支。

## 回归执行

执行命令：

```bash
go test -run '5A' -count=1
```

整套 5A 用例包含：

- `TestInitQuery5A`
- `TestStaticOneShardGroup5A`
- `TestJoinBasic5A`
- `TestDeleteBasic5A`
- `TestJoinLeaveBasic5A`
- `TestManyJoinLeaveReliable5A`
- `TestManyJoinLeaveUnreliable5A`
- `TestShutdown5A`
- `TestProgressShutdown5A`
- `TestProgressJoin5A`
- `TestOneConcurrentClerkReliable5A`
- `TestManyConcurrentClerkReliable5A`
- `TestOneConcurrentClerkUnreliable5A`
- `TestManyConcurrentClerkUnreliable5A`

### 三轮结果

| 轮次 | 结果 | 总耗时 |
| --- | --- | --- |
| Round 1 | PASS | `246.704s` |
| Round 2 | PASS | `245.276s` |
| Round 3 | PASS | `241.635s` |

三轮里最关键的回归信号是：

- `TestManyConcurrentClerkUnreliable5A` 已连续 3 轮通过。
- `TestManyConcurrentClerkReliable5A` 持续通过，说明没有把 reliable 路径带坏。
- `TestManyJoinLeaveUnreliable5A` 持续通过，说明 shard ownership 修正没有破坏迁移主流程。

## 结论

这次修复本质上做了两件事：

- 把 `Put` 的“不确定是否已提交”判定从“粗糙地依赖重试次数”，修正为“只依赖真正可能丢回复的 RPC 历史”。
- 把 shard ownership 从“默认未冻结就算持有”，修正为“必须已经安装且未冻结才算持有”。

修复后，之前会触发线性一致性失败的 `many concurrent clerks unreliable` 已经稳定通过，多轮整套 5A 回归也没有发现新的回归问题。
