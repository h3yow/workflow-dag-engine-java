# DAG 工作流调度服务（本地可运行）

一个基于 Spring Boot 3 / Java 21 的本地 DAG 工作流调度服务：按步骤依赖关系调度执行，
在**步骤失败、服务重启、重复提交**三种情况下都给出可解释、可复现的结果；并支持
**步骤自动重试**与**失败工作流手动续跑**。

## 1. 模型与状态

- `WorkflowDef`：`workflowId`（客户端提供的**幂等键**）+ 步骤列表；
  每个步骤 `StepDef` 包含 `id`、`type`、`dependsOn`、`param`、`timeoutMs`，
  以及重试配置 `maxRetries`、`retryBackoffMs`（见第 3.1 节）。
- 步骤状态 `StepStatus`：`PENDING → RUNNING → SUCCESS / FAILED`，失败后未执行的步骤为 `SKIPPED`。
  `SUCCESS / FAILED / SKIPPED` 均为终态；其中 **SUCCESS 永不重跑**，FAILED/SKIPPED 只在显式续跑时回到 PENDING（见第 4.1 节）。
- 工作流状态 `WorkflowStatus`：`RUNNING / SUCCESS / FAILED`。
- 每次状态流转都带时间戳、`attempts` 与失败溯源 `FailureInfo`，并以 JSON 落盘。
- **每次尝试**都有一条 `StepAttempt` 明细：`stepId / attemptNo（步骤内连续序号）/
  runNo（执行批次，首次=1，每次续跑+1）/ startedAt / finishedAt / result / message`，
  结果取值 `SUCCESS / STEP_EXCEPTION / TIMEOUT / DEFINITION_ERROR / INTERRUPTED`。
  明细跨自动重试、跨续跑按时间追加，可事后看出试了几次、每次什么结果、最终失败属于哪类原因。

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

### 3.1 步骤自动重试

步骤定义上可配置：

| 字段 | 含义 | 默认 |
| --- | --- | --- |
| `maxRetries` | 因内部异常或超时失败后，**额外**自动重试的次数；总尝试次数 = `1 + maxRetries` | `0`（不重试） |
| `retryBackoffMs` | 两次尝试之间的等待间隔（毫秒） | `0`（立即重试） |

语义：

- 触发重试的失败类型是 `STEP_EXCEPTION` 与 `TIMEOUT`；`DEFINITION_ERROR`（类型不受支持）不重试，直接终态失败。
- **每次尝试都独立施加完整的 `timeoutMs` 超时预算**；每次尝试的开始/结束时间、结果分类、失败消息都落入
  `steps[id].attemptRecords`，`attempts` 等于明细条数。只有全部尝试都失败，步骤才算 `FAILED`，
  其 `failure` 取**最后一次**尝试的失败类型与消息，之后仍按原失败传播规则跳过下游。
- 任一次尝试成功即步骤成功，后续尝试不再发生，工作流继续向下调度。
- **重试不突破并发上限**：整组尝试（含退避等待）在同一个全局并发槽位内完成，槽位在整组尝试结束后才释放；
  退避期间响应中断。
- 重试只在同一次执行批次内发生；进程在重试期间崩溃，恢复时未完成的尝试补记为 `INTERRUPTED`，
  步骤回到 PENDING 再执行一次（见第 4 节），次数不重复累加。
- **兼容老定义**：未提供这两个字段（或均为 0）时，行为与结果与旧版本完全一致——一次失败即 FAILED、
  一次尝试一条明细，`attempts=1`。

`maxRetries` / `retryBackoffMs` 为负数会在构造定义时即被拒绝；它们与 type/param/timeout/deps 一样
纳入"同 id 不同定义"的冲突判定。

## 4. 失败工作流续跑（resume）

对一个已经 `FAILED` 的工作流发起续跑（`POST /api/workflows/{workflowId}/resume`），只把没成功的部分重跑：

- 已 `SUCCESS` 的步骤**原样保留、绝不重跑**：状态、`failure=null`、`attempts` 与全部 `attemptRecords` 保持不变；
- 上一批 `FAILED` 的步骤与因上游失败被 `SKIPPED` 的步骤全部回到 `PENDING`，由调度泵按**原依赖关系**补跑
  （被跳过的步骤只有在依赖都成功后才会派发，因此不会越过仍失败的上游）；
- 续跑是一个新的执行批次：工作流 `resumeCount + 1`（持久化），新发起的尝试 `runNo` 等于新批次号，
  尝试序号与明细在历史记录之后**连续追加**，`attempts` 只增新次数、不重复累加旧次数；
- 续跑开始前先把重置动作（含 `resumeCount`）原子落盘；续跑跑到一半服务重启，恢复逻辑会把中断在 RUNNING
  的步骤（最后一次尝试补记 `INTERRUPTED`）重新执行，SUCCESS 步骤依旧不重跑，最终能继续跑到终态；
- 续跑成功则整体 `SUCCESS`；仍有步骤失败则再次 `FAILED`、新的下游被 SKIPPED，可以继续再发起续跑。
  续跑结束后的整体状态、每步结果、尝试明细与根失败的查询方式与正常执行完全相同。

### 4.1 续跑的幂等与兼容

- **并发只生效一次**：同一工作流并发重复发起续跑，只有第一个请求真正重置并执行；其余请求不触发第二次执行，
  阻塞等待并拿到与发起方**同一份终态结果**（响应 `deduplicated=true`）。
- **已结束工作流**：对 `SUCCESS` 工作流续跑，行为与现在的重复提交完全一致——返回既有执行
  （`deduplicated=true`），不产生第二次执行；对仍在首次执行中的工作流续跑同样不重复触发。
- 对不存在的 workflowId 续跑返回 404 `WORKFLOW_NOT_FOUND`。
- 首次执行的提交/重复提交/409 冲突规则不受任何影响（见第 5 节）。

## 5. 断点恢复与幂等提交

**持久化**：每个工作流落盘为 `<data-dir>/<workflowId>.json`，写入采用"临时文件 + 原子 rename"，
每次步骤状态变化（含每次尝试开始/结束）都会持久化，因此任意时刻磁盘上都是完整状态。

**重启恢复**（`@PostConstruct recoverOnStartup`）：

- `SUCCESS`/`FAILED` 工作流直接以终态呈现，不重新调度；续跑中崩溃留下的 `RUNNING` 工作流照常恢复调度；
- `RUNNING` 工作流中：
  - 崩溃时停留在 `RUNNING` 的步骤没有可信的完成结果，重置为 `PENDING` 并重新执行；
    其最后一条"已开始未结束"的尝试补记为 `INTERRUPTED`，随后只再追加一次新尝试，
    因此 `attempts` 与明细条数一致、**不会把已记录的尝试重复累加**；
  - 已 `SUCCESS` 的步骤**不会重跑**（有单测用执行计数器验证）。

**幂等提交**（`POST /api/workflows`）：

- 相同 `workflowId` + 语义相同的定义（步骤顺序无关，比较 id/type/deps/param/timeout/maxRetries/retryBackoffMs）：
  返回 `{"deduplicated":true,"execution":...}`，即既有执行结果，**不产生第二次执行**；
- 相同 `workflowId` 但定义不同：HTTP 409 `DEFINITION_CONFLICT`。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/workflows` | 提交工作流（异步执行，立即返回当前状态） |
| POST | `/api/workflows/{workflowId}/resume` | 失败续跑：同步阻塞到本次执行终态并返回；并发重复续跑只执行一次 |
| GET | `/api/workflows/{workflowId}` | 查询执行进度与溯源（不存在返回 404） |
| GET | `/api/workflows` | 列出全部工作流 |

提交示例（含重试配置）：

```json
{
  "workflowId": "demo-1",
  "steps": [
    {"id": "a", "type": "echo", "param": "start", "dependsOn": []},
    {"id": "b", "type": "sleep", "param": "100", "dependsOn": ["a"],
     "maxRetries": 2, "retryBackoffMs": 200},
    {"id": "c", "type": "fail", "param": "simulated-bug", "dependsOn": ["a"]},
    {"id": "d", "type": "noop", "dependsOn": ["b", "c"]}
  ]
}
```

## 7. 本地验证方法

需要 JDK 21、Maven 3.6+。

### 7.1 运行测试

```bash
mvn test
```

测试清单（共 29 个用例，每个用例使用独立临时目录，不依赖上一次运行留下的状态；
日志中统一打印**工作流标识、步骤依赖关系、各步骤最终状态**）：

- `DagValidationTest`：缺失依赖 / 自引用 / 成环拒绝原因可区分；拓扑序满足依赖且与提交顺序无关；
- `StepFailureTest`：STEP_EXCEPTION 传播、TIMEOUT 分类、下游 SKIPPED、并发上限实测、
  在途独立步骤跑完且不重跑、未知类型作为 DEFINITION_ERROR 被拒；
- `RetryTest`（新）：前两次异常第三次成功且每次尝试都有明细、重试次数耗尽才 FAILED 且保留最后一次失败类型、
  超时也走重试且每次尝试独立超时预算、退避等待占用槽位不突破并发上限、无重试配置行为与旧版完全一致；
- `ResumeTest`（新）：续跑只补未成功步骤且成功步骤不重跑/尝试历史连续保留、
  8 路并发续跑只执行一次且拿到同一份终态、续跑中途"硬杀"进程后重启继续跑完且不重复执行/不重复计数、
  未知 id 续跑 404、修复前可反复续跑且历史连续累计；
- `RestartRecoveryTest`：重复提交幂等、同 id 不同定义 409、kill 后重启已完成步骤不重跑、
  RUNNING 步骤恢复重跑、终态工作流重启后保持一致；
- `WorkflowApiTest`：基于 MockMvc 的 HTTP 端到端冒烟（成功、成环 400、幂等、409、失败续跑与 404）。

### 7.2 启动服务并用 curl 验证

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

验证自动重试（b 内部异常重试 2 次、间隔 200ms，重试仍失败，下游被跳过；
观察返回中的 `attempts=3` 与 `attemptRecords` 三条明细）：

```bash
curl -s -X POST localhost:8080/api/workflows -H 'Content-Type: application/json' -d '{
  "workflowId": "retry-demo",
  "steps": [
    {"id": "b", "type": "fail", "param": "boom", "dependsOn": [],
     "maxRetries": 2, "retryBackoffMs": 200},
    {"id": "c", "type": "noop", "dependsOn": ["b"]}
  ]
}'
curl -s localhost:8080/api/workflows/retry-demo
```

验证失败续跑（接口同步阻塞到终态；`resumeCount=1`，SUCCESS 步骤 attempts 不变）：

```bash
curl -s -X POST localhost:8080/api/workflows/try-demo -H 'Content-Type: application/json' -d '{
  "workflowId": "try-demo",
  "steps": [
    {"id": "a", "type": "noop", "dependsOn": []},
    {"id": "b", "type": "fail", "param": "need-fix", "dependsOn": ["a"]},
    {"id": "c", "type": "noop", "dependsOn": ["b"]}
  ]
}'
# 修复处理器/数据后（例如重启加载了新版本处理器），只补跑 b 与 c，a 不会再执行：
curl -s -X POST localhost:8080/api/workflows/try-demo/resume
# 对已成功工作流再续跑：deduplicated=true，不产生第二次执行
# 对不存在 id 续跑：HTTP 404，rejectReason=WORKFLOW_NOT_FOUND
```

### 7.3 手工验证重启恢复

```bash
mvn package -DskipTests
java -jar target/workflow-dag-engine-java-1.0-SNAPSHOT.jar --workflow.data-dir=/tmp/dag-demo
# 首次执行或续跑过程中 kill -9 <pid>
# 重新启动同一命令：观察日志 "断点恢复：N 个中断在 RUNNING 的步骤回到 PENDING，SUCCESS 步骤保持不重跑"，
# GET 接口可见已完成步骤 attempts 保持不变，被中断步骤多一条 result=INTERRUPTED 的明细，随后继续执行。
```

## 8. 配置项（application.properties）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `workflow.data-dir` | `./data/workflows` | JSON 状态落盘目录，启动时自动创建 |
| `workflow.max-concurrency` | `4` | 步骤执行全局并发上限（重试与退避等待同样占用槽位） |
| `workflow.default-timeout-ms` | `5000` | 步骤默认超时；步骤 `timeoutMs>0` 时单步覆盖；每次重试尝试各自独立计时 |

## 9. 代码结构

```
model/       枚举与状态模型（定义、执行记录、失败溯源、逐次尝试明细 StepAttempt）
validation/  DAG 校验（Kahn 拓扑排序）
engine/      WorkflowEngine 调度核心、重试循环、失败续跑、步骤处理器、内置处理器、幂等/冲突/未找到异常
store/       文件存储（原子写 JSON）
web/         REST 控制器（提交/续跑/查询）与异常映射
config/      Spring 装配与配置属性
```
