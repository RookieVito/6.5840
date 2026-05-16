# 6.5840 分布式系统实验仓库

这个仓库包含 MIT 6.5840（2026 年）分布式系统实验的代码实现，主要使用 Go 语言完成。项目源代码位于 `src` 目录下。

## 项目结构

- `src/`：主代码目录
  - `kvsrv1/`：KV 服务端与客户端实现
  - `raft1/`：Raft 共识算法实现
  - `kvraft1/`：基于 Raft 的 KV 存储实现
  - `shardkv1/`：分片 KV 存储实现
  - `mr/`：MapReduce 框架与任务实现
  - `main/`：可执行程序入口文件
  - `labgob/`、`labrpc/`：辅助库
  - `kvtest1/`：测试工具与线性化检查
  - `tester1/`：实验测试框架

## 编译与测试

进入 `src` 目录后执行：

```bash
cd src
make
```

这个命令会构建大部分实验目标并运行测试。

常见构建/测试目标：

```bash
cd src
make mr
make kvsrv1
make lock1
make raft1
make rsm1
make kvraft1 make shardkv ``` `make` 也支持通过 `RUN` 环境变量运行特定测试，例如： ```bash cd src make kvsrv1 RUN="-run Wc" ``` ## 说明 - `src/main/` 下包含各个实验可执行程序的入口文件，如 `kvsrv1d.go`、`raft1d.go`、`kvraft1d.go`、`mrcoordinator.go`、`mrsequential.go`、`mrworker.go`、`shardgrp1d.go` 等。 项目主要用于实验与学习，不同子目录对应不同的实验模块。 ## 目录导航 - `src/kvsrv1/`：KV 服务与锁模块 `src/raft1/`：Raft 算法实现与测试 `src/kvraft1/`：Raft + KV 存储集成 `src/shardkv1/`：分片 KV 存储与配置管理 `src/mr/`：MapReduce 框架、Worker、Coordinator `src/mrapps/`：MapReduce 应用插件 `src/kvtest1/`：线性化测试与模型检查 `src/tester1/`：实验测试环境与网络模拟 ## 待办事项 - [x] Lab 1: MapReduce [x] Lab 2: Key/Value Server [x] Lab 3A: Raft leader election [x] Lab 3B: Raft log [x] Lab 3C: Raft persistence [x] Lab 3D: Raft log compaction [x] Lab 4A: FT KV Service replicated state machine (RSM) [x] Lab 4B: FT KV Service Key/value service without snapshots [x] Lab 4C: FT KV Key/value service with snapshots [x] Lab 5A: Sharded KV Service Moving shards [x] Lab 5B: Sharded KV Handling a failed controller [x] Lab 5C: Sharded KV Concurrent configuration changes [ ] Lab 5D: extend
```

## 总结
当前项目确实流量控制、各种raft相关的优化、kv相关功能（范围查找、前缀查找等）等问题。
另外，本项目的raft采取的是传递绝对日志索引，而链接"https://gitee.com/YuXinAndYang/6.824.git"采取相对索引（更加复杂，更难管理，实际是未考虑uint64大小而做的多余设计），本项目重构了部分代码，并修改了原仓库的部分错误，并继续完成了课程大部分内容。
