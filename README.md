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

## 依赖

- Go 1.22
- `github.com/anishathalye/porcupine`（已在 `src/go.mod` 中声明）

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

## 提交打包

根目录下的 `Makefile` 提供了打包提交的目标。可用目标包括：

`lab1 lab2 lab3a lab3b lab3c lab3d lab4a lab4b lab4c lab5a lab5b lab5c`

例如打包 `lab1`：

```bash
make lab1
```

这会生成 `lab1-handin.tar.gz`，用于上传到 Gradescope。

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
