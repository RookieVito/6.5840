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
make kvraft1
make shardkv
```

`make` 也支持通过 `RUN` 环境变量运行特定测试，例如：

```bash
cd src
make kvsrv1 RUN="-run Wc"
```

## 说明

- `src/main/` 下包含各个实验可执行程序的入口文件，如 `kvsrv1d.go`、`raft1d.go`、`kvraft1d.go`、`mrcoordinator.go`、`mrsequential.go`、`mrworker.go`、`shardgrp1d.go` 等。
- 项目主要用于实验与学习，不同子目录对应不同的实验模块。

## 目录导航

- `src/kvsrv1/`：KV 服务与锁模块
- `src/raft1/`：Raft 算法实现与测试
- `src/kvraft1/`：Raft + KV 存储集成
- `src/shardkv1/`：分片 KV 存储与配置管理
- `src/mr/`：MapReduce 框架、Worker、Coordinator
- `src/mrapps/`：MapReduce 应用插件
- `src/kvtest1/`：线性化测试与模型检查
- `src/tester1/`：实验测试环境与网络模拟

## 待办事项

- [x] Lab 1: MapReduce
- [x] Lab 2: Key/Value Server
- [x] Lab 3A: Raft leader election
- [x] Lab 3B: Raft log
- [x] Lab 3C: Raft persistence
- [x] Lab 3D: Raft log compaction
- [x] Lab 4A: FT KV Service replicated state machine (RSM)
- [x] Lab 4B: FT KV Service Key/value service without snapshots
- [x] Lab 4C: FT KV Key/value service with snapshots
- [x] Lab 5A: Sharded KV Service Moving shards
- [x] Lab 5B: Sharded KV Handling a failed controller
- [x] Lab 5C: Sharded KV Concurrent configuration changes
- [ ] Lab 5D: extend

## 总结
本项目完成了大部分的raft主题，但是依旧存在很多问题，例如缺乏流量控制、读取优化、范围查询、两阶段选举、快照隔离级别的事务等等，本项目依旧有很多很多的优化空间。阅读和学习etcd-raft、etcd、tikv、rocksdb等优秀的工业设计，有助于深入了解raft的优化、应用场景，并且，将不在局限于存储这一个部分，包括但不限于压缩、加密、rpc网络连接的处理、大数据量的分块传递、像tcp中熟悉的流量控制拥塞控制等等，甚至如果你不局限于只做内存型存储引擎而转眼于持久化大量数据的存储引擎，将可能会接触到优秀的LSM、B+树存储引擎设计方法，挣扎于磁盘、内存、网络、cpu等等数据会触及等任何主题。
https://gitee.com/YuXinAndYang/6.824 这个链接是过去繁忙时做的，有很多的设计不足和过度设计（比如相对索引使得实现相当麻烦，本意是为了让index有足够大的空间，但是实际上可能不需要那么大或者有其他更优秀的设计，浪费了很多时间）。在lab1，lab2的实现中，似乎也有部分理解不清，所以，本项目尝试重新开始并实现大部分功能，对raft有了更深的理解，同时也涉猎了一部分存储引擎设计（不愧是数据密集型应用系统设计）。
正如我的github用户名，我依旧是Rookie。
