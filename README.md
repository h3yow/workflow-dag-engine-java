# DAG 工作流调度服务（本地可运行）

一个基于 Spring Boot 3 / Java 21 的本地 DAG 工作流调度服务：按步骤依赖关系调度执行，
在**步骤失败、服务重启、重复提交**三种情况下都给出可解释、可复现的结果；
并支持**步骤自动重试**与**失败工作流续跑**。

## 1. 模型与状态

- `WorkflowDef`：`workflowId`（客户端提供的**幂等键**）+ 步骤列表；
  每个步骤 `StepDef` 包含 `id`、`type`、`dependsOn`、`param`、`timeoutMs`，
  以及可选的自动重试配置 `maxRetries`、`retryDelayMs`（见第 4 节）。
- 步骤状态 `StepStatus`：`PENDING → RUNNING → SUCCESS / FAILED`，失败后未执行的步骤为 `SKIPPED`。
  `SUCCESS / FAILED / SKIPPED` 均为终态，**终态永不重跑**（续跑是唯一例外入口，见第 5 节：
  续跑只把 `FAILED/SKIPPED` 重置为 `PENDING`，`SUCCESS` 依然绝不重跑）。
- 工作流状态 `WorkflowStatus`：`RUNNING / SUCCESS / FAILED`。
- 每次状态流转都带时间戳、`attempts` 与失败溯源 `FailureInfo`，并以 JSON 落盘；
  每次尝试（含重试）在 `steps[id].attemptRecords` 中留下一条记录：
  第几次尝试、起止时间、是否成功、失败分类与消息，`finishedAt=0` 表示该次尝试尚无结果
  （执行中，或服务重启被中断——恢复时会被补记为"中断"）。

内置步骤类型：

| type | 行为 | param |
| --- | --- | --- |
| `noop` | 立即成功 | 忽略 |
| `echo` | 打印一行含工作流/步骤标识的日志 | 文本 |
| `sleep` | 休眠后成功（响应中断），用于并发/超时验证 | 毫秒数，默认 100 |
| `fail` | 直接抛出异常，用于失败传播验证 | 异常消息 |

## 2. 依赖校验规则（提交即拒绝，HTTP 400）

提交时按以下顺序校验，拒绝原因 `RejectReason` 互相可区分，响应体形如
`{"rejectReason":"CYCLE_DETECTED","message":"..."}`：

1. `INVALID_ID`：`workflowId`/步骤 `id` 为空，或步骤 id 重复；
2. `EMPTY_WORKFLOW`：没有任何步骤；
3. `INVALID_STEP`：步骤缺少 `type`，或 `type` 没有对应的处理器；
4. `SELF_REFERENCE`：某步骤把自己列入 `dependsOn`；
5. `MISSING_DEPENDENCY`：依赖了工作流中不存在的步骤 id；
6. `CYCLE_DETECTED`：依赖图存在环（Kahn 拓扑排序无法消费全部节点）。

合法 DAG 的执行顺序由拓扑序决定：**同层就绪步骤按 id 字典序派发**，
因此执行顺序只依赖 DAG 结构，与步骤的提交顺序无关（见 `DagValidationTest`）。

## 3. 受控并发与失败传播

- 全局固定大小步骤线程池 + 公平信号量，并发上限为 `workflow.max-concurrency`（默认 4），
  对所有工作流统一生效；无依赖关系的步骤可并行（测试实测 4 个 200ms 步骤、并发 2 时约 400ms 完成）。
- 每个工作流由一个单线程"调度泵"驱动：收集已完成步骤 → 发现就绪步骤 → 按空闲槽位派发。
- **失败传播**：任一步骤 `FAILED` 后
  - 不再派发任何新步骤；
  - 尚未派发的 `PENDING` 步骤（无论是否为失败步骤的直接下游）一律置为 `SKIPPED`；
  - 失败瞬间已在执行中的独立步骤允许自然跑完（不强制中断线程），其真实结果被记录；
  - 已 `SUCCESS` 的步骤保持终态，绝不重跑。
- 工作流根失败记录在 `execution.failure`，失败步骤记录在各自的 `steps[id].failure`。

### 失败原因三分类（`FailureType`）

| 分类 | 场景 | 结果 |
| --- | --- | --- |
| `DEFINITION_ERROR` | 定义非法 / 类型不支持 | 提交被拒绝，不产生执行记录 |
| `STEP_EXCEPTION` | 步骤处理器内部抛异常 | 步骤 FAILED，工作流 FAILED |
| `TIMEOUT` | 执行超过 `timeoutMs`（默认 `workflow.default-timeout-ms=5000`） | 步骤 FAILED，工作流 FAILED |

## 4. 步骤自动重试

步骤定义可携带两个可选字段（缺省均为 0，即不重试）：

| 字段 | 含义 |
| --- | --- |
| `maxRetries` | 允许的重试次数（不含首次执行），总尝试上限 = `1 + maxRetries` |
| `retryDelayMs` | 两次尝试之间的等待间隔（毫秒） |

语义：

- 步骤因 `STEP_EXCEPTION`（内部报错）或 `TIMEOUT`（超时）失败时，若已派发次数
  未达到 `1 + maxRetries`，步骤回到 `PENDING`，等待 `retryDelayMs` 后由调度泵重新派发；
  只有次数用尽才置为 `FAILED`，并按第 3 节的既有规则停掉后续步骤。
- 重试的每次派发与首次执行走同一条路径：同样占用全局并发槽位，
  **不会突破 `workflow.max-concurrency` 上限**（有单测实测）。
- 每次尝试都追加一条 `attemptRecords` 记录并落盘，`attempts` 同步累加；
  最终失败时 `steps[id].failure` 保留最后一次尝试的失败分类，
  历史每次尝试的分类可在 `attemptRecords` 中逐条查看。
- 重试等待期间服务重启：步骤以 `PENDING` 落盘，恢复后按既有规则重新派发，
  已累加的 `attempts` 不回退、不重复累加。
- **重试预算按执行批次计算**：每个步骤随状态持久化一个批次基线
  （`steps[id].batchBaseAttempts`，首次执行为 0），"本批次已用尝试数 =
  `attempts - batchBaseAttempts`"，只有它达到 `1 + maxRetries` 才算真正失败。
  续跑会把被重置步骤的基线推到当时的 `attempts`，因此续跑批次重新获得
  定义里配置的全部重试次数（见第 5 节），而历史 `attempts` 与
  `attemptRecords` 原样保留、不累加也不丢。

**兼容性**：未配置重试的老定义（`maxRetries=0`）行为与引入重试之前完全一致——
失败一次即终态（有专门用例 `RetryTest.definitionWithoutRetryKeepsLegacyBehavior` 守护）；
老版本落盘的状态文件不含重试字段、尝试记录与批次基线，反序列化后按缺省值
（基线 0）处理，语义与引入该字段之前一致，可直接加载。

## 5. 失败工作流续跑

`POST /api/workflows/{workflowId}/resume` 对一个已 `FAILED` 的工作流发起续跑，
**只补跑未成功的部分**：

- 已 `SUCCESS` 的步骤原样保留、绝不重跑，其 `attempts` 与 `attemptRecords` 不动；
- `FAILED` 与被级联 `SKIPPED` 的步骤重置为 `PENDING`，按依赖关系重新调度
  （上游已成功的直接复用结果，被跳过的步骤这次会补跑）；
- **续跑批次里的自动重试**：被重置的步骤把重试预算基线推到当前 `attempts`，
  本轮重新获得 `1 + maxRetries` 次尝试——续跑里重试到成功，或把这一轮的重试
  次数也用尽才算真正失败、再按既有规则停掉后续步骤，与首次执行的表现完全一致；
  续跑里的重试同样占用全局并发槽位，不突破 `workflow.max-concurrency` 上限；
- 重置与状态落盘在同一把锁内完成，续跑期间服务重启按第 6 节断点恢复规则继续：
  已成功的不重跑、已记录的尝试次数不重复累加，批次基线随状态落盘，
  重启后续跑批次剩余的重试预算仍然有效；
- 续跑结束后的整体状态与每步结果通过 `GET /api/workflows/{workflowId}` 正常查询，
  与首次执行的呈现完全一致。

**幂等语义**：

- 并发/重复触发续跑：只有一个请求真正受理（`deduplicated=false`），
  其余请求命中幂等（`deduplicated=true`），不会各跑一遍；
  命中幂等的请求会**等待这一批续跑跑完**，返回与真正触发者同一次执行的
  **终态结果**（终态快照一致），而不是执行中的半截快照；
  若跟随的批次最终仍是 `FAILED`，可再次发起续跑，语义同上；
- 对 `SUCCESS` 工作流续跑：无操作，直接返回既有执行（`deduplicated=true`）；
- 对不存在的 `workflowId` 续跑：HTTP 404 `WORKFLOW_NOT_FOUND`；
- 已结束工作流的**重复提交**（`POST /api/workflows`）行为不变，仍是返回既有执行，
  不会触发第二次执行——续跑只能显式通过 resume 接口发起。

## 6. 断点恢复与幂等提交

**持久化**：每个工作流落盘为 `<data-dir>/<workflowId>.json`，写入采用"临时文件 + 原子 rename"，
每次步骤状态变化都会持久化，因此任意时刻磁盘上都是完整状态。

**重启恢复**（`@PostConstruct recoverOnStartup`）：

- `SUCCESS`/`FAILED` 工作流直接以终态呈现，不重新调度；
- `RUNNING` 工作流中：
  - 崩溃时停留在 `RUNNING` 的步骤没有可信的完成结果，重置为 `PENDING` 并重新执行（`attempts` 累加）；
  - 已 `SUCCESS` 的步骤**不会重跑**（有单测用执行计数器验证）。

**幂等提交**（`POST /api/workflows`）：

- 相同 `workflowId` + 语义相同的定义（步骤顺序无关，比较 id/type/deps/param/timeout/重试配置）：
  返回 `{"deduplicated":true,"execution":...}`，即既有执行结果，**不产生第二次执行**；
- 相同 `workflowId` 但定义不同：HTTP 409 `DEFINITION_CONFLICT`。

## 7. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/workflows` | 提交工作流（异步执行，立即返回当前状态） |
| POST | `/api/workflows/{workflowId}/resume` | 失败续跑（幂等，只补跑未成功步骤；不存在返回 404） |
| GET | `/api/workflows/{workflowId}` | 查询执行进度与溯源（不存在返回 404） |
| GET | `/api/workflows` | 列出全部工作流 |

提交示例：

```json
{
  "workflowId": "demo-1",
  "steps": [
    {"id": "a", "type": "echo", "param": "start", "dependsOn": []},
    {"id": "b", "type": "sleep", "param": "100", "dependsOn": ["a"]},
    {"id": "c", "type": "fail", "param": "simulated-bug", "dependsOn": ["a"]},
    {"id": "d", "type": "noop", "dependsOn": ["b", "c"]}
  ]
}
```

## 8. 本地验证方法

需要 JDK 21、Maven 3.6+。

### 8.1 运行测试

```bash
mvn test
```

测试清单（共 34 个用例，日志中统一打印**工作流标识、步骤依赖关系、各步骤最终状态**）：

- `DagValidationTest`：缺失依赖 / 自引用 / 成环拒绝原因可区分；拓扑序满足依赖且与提交顺序无关；
- `StepFailureTest`：STEP_EXCEPTION 传播、TIMEOUT 分类、下游 SKIPPED、并发上限实测、
  在途独立步骤跑完且不重跑、未知类型作为 DEFINITION_ERROR 被拒；
- `RetryTest`：重试最后成功、次数耗尽才算失败、超时也走重试、重试不突破并发上限、
  未配重试的老定义行为不变；
- `ResumeTest`：续跑只补未成功部分、并发续跑只生效一次、已成功工作流续跑为空操作、
  未知 id 404、续跑中途重启可断点恢复且不重复执行；
- `ResumeRetryTest`：续跑批次里带重试的步骤重新获得定义里的重试次数并最终重试成功、
  续跑轮重试次数用尽才算真正失败并停掉下游、并发续跑的每个请求拿到同一次续跑的
  一致终态结果、续跑批次中途重启后剩余重试预算有效且尝试记录如实保留；
- `RestartRecoveryTest`：重复提交幂等、同 id 不同定义 409、kill 后重启已完成步骤不重跑、
  RUNNING 步骤恢复重跑、终态工作流重启后保持一致；
- `WorkflowApiTest`：基于 MockMvc 的 HTTP 端到端冒烟（成功、成环 400、幂等、409、
  续跑受理/幂等空操作/未知 id 404）。

测试不依赖上一次运行留下的状态：使用固定路径数据目录的用例会在 Spring 上下文创建前
先清理旧状态文件，干净环境与连续多次运行结果一致。

### 8.2 启动服务并用 curl 验证

```bash
mvn spring-boot:run \
  -Dspring-boot.run.arguments="--workflow.data-dir=/tmp/dag-demo --workflow.max-concurrency=2"

# 提交
curl -s -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' \
  -d '{"workflowId":"demo","steps":[{"id":"a","type":"noop","dependsOn":[]}]}'

# 查询
curl -s localhost:8080/api/workflows/demo

# 成环必须 400，rejectReason=CYCLE_DETECTED
curl -i -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' \
  -d '{"workflowId":"cyc","steps":[{"id":"a","type":"noop","dependsOn":["b"]},{"id":"b","type":"noop","dependsOn":["a"]}]}'

# 再次提交相同定义：deduplicated=true
curl -s -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' \
  -d '{"workflowId":"demo","steps":[{"id":"a","type":"noop","dependsOn":[]}]}'
```

### 8.3 手工验证自动重试与失败续跑

```bash
# 自动重试：fail 步骤配 maxRetries=2，会尝试 3 次后 FAILED；
# GET 可见 steps.bad.attempts=3 与 3 条 attemptRecords
curl -s -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' \
  -d '{"workflowId":"retry-demo","steps":[
        {"id":"bad","type":"fail","param":"boom","dependsOn":[],"maxRetries":2,"retryDelayMs":500},
        {"id":"after","type":"noop","dependsOn":["bad"]}]}'
sleep 3 && curl -s localhost:8080/api/workflows/retry-demo

# 失败续跑：把上面的 fail 换成会成功的类型无法直接改定义（同 id 不同定义会 409），
# 因此用新 id 演示完整链路——先失败，再续跑（此处 after 依赖 bad，bad 仍会失败，
# 续跑用于演示幂等：重复调用返回 deduplicated=true 的同一份执行）
curl -s -X POST localhost:8080/api/workflows/retry-demo/resume
curl -s -X POST localhost:8080/api/workflows/retry-demo/resume   # 幂等命中，不会各跑一遍

# 续跑批次里的重试预算：retry-demo 的 bad 配了 maxRetries=2，
# 上面每次续跑都会再完整尝试 3 次（GET 可见 attempts 每次续跑 +3、
# attemptRecords 逐条追加、batchBaseAttempts 记录本轮基线），
# 而不是只跑一次就失败
sleep 3 && curl -s localhost:8080/api/workflows/retry-demo

# 并发续跑一致性：同时发多个续跑请求，只有一个 deduplicated=false，
# 其余请求会等这一批跑完，返回与触发者同一次执行的终态结果
for i in 1 2 3 4; do
  curl -s -X POST localhost:8080/api/workflows/retry-demo/resume &
done; wait

# 不存在的 id 续跑 → 404 WORKFLOW_NOT_FOUND
curl -i -X POST localhost:8080/api/workflows/no-such/resume
```

### 8.4 手工验证重启恢复

```bash
mvn package -DskipTests
java -jar target/workflow-dag-engine-java-1.0-SNAPSHOT.jar --workflow.data-dir=/tmp/dag-demo
# 提交一个含 sleep 步骤的工作流，执行中 kill -9 <pid>
# 重新启动同一命令：观察日志 "断点恢复：N 个中断在 RUNNING 的步骤回到 PENDING，SUCCESS 步骤保持不重跑"，
# GET 接口可见已完成步骤 attempts=1 保持不变，被中断步骤 attempts 增加并继续执行。
```

## 9. 配置项（application.properties）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `workflow.data-dir` | `./data/workflows` | JSON 状态落盘目录，启动时自动创建 |
| `workflow.max-concurrency` | `4` | 步骤执行全局并发上限 |
| `workflow.default-timeout-ms` | `5000` | 步骤默认超时；步骤 `timeoutMs>0` 时单步覆盖 |

## 10. 代码结构

```
model/       枚举与状态模型（定义、执行记录、失败溯源）
validation/  DAG 校验（Kahn 拓扑排序）
engine/      WorkflowEngine 调度核心、步骤处理器、内置处理器、幂等/冲突异常
store/       文件存储（原子写 JSON）
web/         REST 控制器与异常映射
config/      Spring 装配与配置属性
```
