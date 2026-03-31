# Repository Guidelines

## 项目结构与模块组织

本仓库用于 MIT 6.5840 分布式系统实验开发，主要代码位于 [`src/`](/home/vito/workspace/6.5840/src)，这里也是 Go 模块根目录（`module 6.5840`）。各实验按目录拆分：`mr/` 为 MapReduce，`kvsrv1/` 为键值服务与锁服务，`raft1/` 为 Raft，`kvraft1/` 为基于 Raft 的 KV，`shardkv1/` 为分片 KV。公共支撑代码位于 `labgob/`、`labrpc/`、`tester1/`、`kvtest1/` 与 `raftapi/`。可执行入口和实验输入样例位于 `src/main/`。仓库根目录下的 [`Makefile`](/home/vito/workspace/6.5840/Makefile) 主要用于打包提交文件。

## 构建、测试与开发命令

除特别说明外，以下命令均在 [`src/`](/home/vito/workspace/6.5840/src) 目录执行。

- `make`：构建实验二进制并运行默认测试集合。
- `make mr`、`make kvsrv1`、`make lock1`、`make raft1`、`make rsm1`、`make kvraft1`、`make shardkv`：运行单个实验目标，默认开启 `-race`。
- `make raft1 RUN="-run 3C"`：只运行指定测试子集，适合回归验证。
- `go test ./...`：按包执行全量测试，适合做通用检查。
- 在仓库根目录执行 `make lab5a`：在构建检查通过后生成 Gradescope 提交压缩包。

## 代码风格与命名约定

提交前请对改动文件执行 `gofmt -w`，保持标准 Go 格式和 import 排序。遵循现有 Go 风格：使用制表符缩进，导出标识符采用 MixedCaps，局部变量使用 lowerCamelCase，接收者名称保持简短且有语义，例如 `rf` 表示 `Raft`。包名应与目录一致，例如 `raft1`、`kvsrv1`、`shardkv1`。优先做与实验目标直接相关的小范围改动，避免影响测试框架依赖的公开接口。

## 测试规范

测试文件与实现文件放在同一目录下，并使用 Go 的 `*_test.go` 约定。现有测试命名通常带实验阶段后缀，例如 `TestInitialElection3A`、`TestBasic4B`、`TestJoinLeave5B`。修改代码后，至少运行对应实验的 `make` 目标，并补充一条聚焦回归命令验证受影响场景。仓库没有强制覆盖率阈值，但合并前应确保相关包通过带 `-race` 的测试。

## 提交与 Pull Request 规范

最近提交历史以简短祈使句为主，常配合 `feat:`、`fix:` 等前缀。请延续这一风格，并在标题中直接标明实验阶段或子系统，例如 `fix: 稳定 shardkv 5A clerk 去重逻辑`。Pull Request 应包含问题背景、涉及的实验或包、实际执行过的测试命令，以及仍存在的限制或风险。如果终端输出或失败日志对评审有帮助，可以附上关键片段；一般不需要截图。
