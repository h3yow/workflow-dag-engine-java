# Workflow DAG Engine (Java / Spring Boot)

一个本地可运行的 DAG 工作流调度服务：按步骤间的依赖关系调度执行，支持受控并发、
失败传播、超时归因、断点恢复与幂等重复提交。所有状态变更都原子化落盘为 JSON 文件，
因此结果可解释、可复现，且在服务重启后保持一致。

## 1. 构建与运行

要求 JDK 21+、Maven 3.9+。

```bash
# 编译并运行全部测试（21 个用例）
mvn test

# 打包并启动（默认端口 8080）
mvn package -DskipTests
java -jar target/workflow-dag-engine-java-1.0-SNAPSHOT.jar

# 可选参数
java -jar target/workflow-dag-engine-java-1.0-SNAPSHOT.jar \
  --server.port=18091 \
  --workflow.data-dir=/tmp/wf-data \
  --workflow.default-concurrency=4
```

## 2. HTTP API

### 提交工作流 `POST /api/workflows`

```json
{
  "workflowId": "order-flow",
  "maxConcurrency": 2,
  "steps": [
    {"id": "a", "action": "echo"},
    {"id": "b", "action": "sleep", "timeoutMillis": 5000, "dependsOn": ["a"]},
    {"id": "c", "action": "noop", "dependsOn": ["a"]}
  ]
}
```

- 执行是**异步**的：提交后立即返回当前快照（新定义返回 `201`）。
- `maxConcurrency` 为该工作流的并发上限；省略或 `<=0` 时使用
  `workflow.default-concurrency`。
- `timeoutMillis` 为单步骤超时；`0` 或缺省使用引擎默认值 10s。
- 内置 action：`echo`（回显步骤 id）、`noop`（立即成功）、
  `sleep`（休眠毫秒数取步骤 id 最后一个 `-` 之后的数字，缺省 200ms）、
  `fail`（始终抛异常，用于演示失败传播）。
- 扩展步骤：实现 `StepHandler` 接口并注册为 Spring Bean，`action()` 即动作名。

### 查询结果 `GET /api/workflows/{workflowId}`

返回快照：工作流状态、每个步骤的状态/详情/失败分类/尝试次数/起止时间戳。
步骤状态：`PENDING / RUNNING / SUCCEEDED / FAILED / TIMEOUT / SKIPPED`；
工作流状态：`PENDING / RUNNING / SUCCEEDED / FAILED`；
定义被拒绝时工作流不会落盘，由 HTTP 错误码表达。

## 3. 依赖校验规则（拒绝原因可区分）

提交后按固定、确定性的顺序校验，与步骤的提交顺序无关。非法定义返回
`422 Unprocessable Entity`，body 中 `error` 为稳定机器码：

| 错误码 | 触发条件 |
|---|---|
| `EMPTY_DEFINITION` | 无定义或步骤列表为空 |
| `EMPTY_WORKFLOW_ID` | workflowId 为空白 |
| `EMPTY_STEP_ID` | 任一步骤 id 为空白 |
| `DUPLICATE_STEP_ID` | 两个步骤使用相同 id |
| `SELF_DEPENDENCY` | 步骤 dependsOn 包含自身 |
| `MISSING_DEPENDENCY` | dependsOn 引用了不存在的步骤 |
| `CYCLIC_DEPENDENCY` | 依赖图存在环（Kahn 拓扑排序无法消费全部节点） |

其他错误：同 id 但定义不同返回 `409 DUPLICATE_WORKFLOW`；查询未知 id 返回
`404 WORKFLOW_NOT_FOUND`；非法 JSON 返回 `400 MALFORMED_REQUEST`。

合法定义会被**规范化（canonicalize）**：步骤重排为拓扑序（并列按 id 排序）、
重复依赖去重、依赖列表排序。因此执行顺序只由 DAG 决定，与提交顺序无关。

## 4. 并发执行与失败传播语义

- 所有上游均 `SUCCEEDED` 的步骤进入就绪集合（按 id 排序），但同一时刻运行中的
  步骤数不超过 `maxConcurrency`（公平信号量）。
- 任一步骤**首次失败**后：不再启动任何新步骤；所有（传递）依赖于非成功步骤的
  下游步骤标记为 `SKIPPED`，其 handler 绝不会执行；与失败分支无关的步骤若已在
  运行中会正常跑完（例如 a→b(fail)→c 与 a→d 两条分支，d 仍可成功）。
- 已 `SUCCEEDED` 的步骤**永远不会重跑**。
- 工作流最终为 `FAILED`，并带可区分的失败分类 `failureKind`：
  - `STEP_EXCEPTION`：步骤 handler 抛出异常（步骤状态 `FAILED`）；
  - `TIMEOUT`：步骤在 `timeoutMillis` 内未完成（步骤状态 `TIMEOUT`）。
    超时通过调度任务赢得 CompletableFuture 完成竞争并中断执行线程实现，
    即使 handler 吞掉中断，超时归因仍然成立；
  - `DEFINITION_ERROR`：定义类错误（提交期以 4xx 拒绝码表达）。

## 5. 断点恢复与幂等提交

- **持久化**：每个工作流对应 `workflow.data-dir` 下一个 JSON 文件；
  每次状态流转（步骤 RUNNING / 终态、工作流状态）都通过临时文件 + 原子 move
  立即刷盘。
- **重启恢复**：启动时扫描数据目录，`PENDING/RUNNING` 的工作流自动恢复调度：
  - 已 `SUCCEEDED` 步骤直接沿用，不重复执行（attempts 保持 1，溯源详情不变）；
  - 崩溃时停留在 `RUNNING` 的步骤重置为 `PENDING` 并补跑一次（attempts +1，
    可在记录中区分“崩溃前尝试”与“恢复后重试”）；
  - 已处于终态（SUCCEEDED/FAILED）的工作流重启后不再调度。
- **幂等提交**：相同 workflowId + 相同定义再次提交时，返回既有快照（HTTP 200），
  绝不产生第二次执行；正在运行中的重复提交同样复用当前执行，不会重复启动调度器。
  相同 id 但定义不同返回 `409`，避免“同 id 不同图”造成歧义。

## 6. 本地手工验证示例

```bash
java -jar target/workflow-dag-engine-java-1.0-SNAPSHOT.jar --server.port=18091 &

# 合法 DAG
curl -s -X POST localhost:18091/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"demo","maxConcurrency":2,
  "steps":[{"id":"a","action":"echo"},
           {"id":"b","action":"fail","dependsOn":["a"]},
           {"id":"c","action":"echo","dependsOn":["b"]},
           {"id":"d","action":"echo","dependsOn":["a"]}]}'
curl -s localhost:18091/api/workflows/demo   # a/d SUCCEEDED, b FAILED, c SKIPPED

# 成环 -> 422 CYCLIC_DEPENDENCY
curl -i -X POST localhost:18091/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId":"cyc",
  "steps":[{"id":"a","dependsOn":["b"]},{"id":"b","dependsOn":["a"]}]}'

# 杀掉进程后重启，GET 结果与重启前一致；
# 也可手工把 data/workflows/<id>.json 改为 SUCCEEDED+RUNNING 的中间态，
# 重启后观察已成功步骤不重跑、中断步骤补跑。
```

## 7. 测试覆盖

```bash
mvn test
```

- `WorkflowValidationTest`（6 例）：自引用、缺失依赖、成环、重复步骤 id、
  空定义/空 id、提交顺序无关的拓扑规范化。
- `WorkflowEngineTest`（7 例）：依赖序成功路径、失败传播与下游跳过、
  超时归因、并发上限实测峰值、基于落盘快照的重启恢复（已成功不重跑）、
  RUNNING 中断步骤恢复重试、重复提交幂等。
- `WorkflowControllerTest`（7 例）：三类拒绝码、畸形 JSON、404、
  提交+轮询至成功+幂等 200 重放、定义冲突 409。

测试日志使用 SLF4J 打印**工作流标识、步骤依赖关系与最终状态**，
调度器在启动/恢复与结束时也会输出完整的步骤依赖与终态列表，可在 surefire
报告或控制台中查看。

## 8. 项目结构

```
src/main/java/com/github/h3yow/workflowdagenginejava/
├── model/        # 枚举(状态/失败分类/拒绝码)与记录类型(StepDef/Snapshot/...)
├── validation/   # WorkflowValidator：缺失/自引用/成环校验 + 规范化
├── handler/      # StepHandler SPI、注册表与内置 echo/noop/sleep/fail
├── store/        # WorkflowStore 接口与 JSON 文件原子落盘实现
├── engine/       # WorkflowEngine（提交/去重/启动恢复）与 WorkflowDispatcher（调度）
├── service/      # 应用门面
├── web/          # REST 控制器、请求 DTO、统一异常处理
└── config/       # data-dir / 默认并发配置
```
