# DAG 工作流调度服务（本地可运行）

一个基于 Spring Boot 3 / Java 21 的本地 DAG 工作流调度服务：按步骤依赖关系调度执行，
在**步骤失败、服务重启、重复提交**三种情况下都给出可解释、可复现的结果。

## 1. 模型与状态

- `WorkflowDef`：`workflowId`（客户端提供的**幂等键**）+ 步骤列表；
  每个步骤 `StepDef` 包含 `id`、`type`、`dependsOn`、`param`、`timeoutMs`。
- 步骤状态 `StepStatus`：`PENDING → RUNNING → SUCCESS / FAILED`，失败后未执行的步骤为 `SKIPPED`。
  `SUCCESS / FAILED / SKIPPED` 均为终态，**终态永不重跑**。
- 工作流状态 `WorkflowStatus`：`RUNNING / SUCCESS / FAILED`。
- 每次状态流转都带时间戳、`attempts` 与失败溯源 `FailureInfo`，并以 JSON 落盘。

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

## 4. 断点恢复与幂等提交

**持久化**：每个工作流落盘为 `<data-dir>/<workflowId>.json`，写入采用"临时文件 + 原子 rename"，
每次步骤状态变化都会持久化，因此任意时刻磁盘上都是完整状态。

**重启恢复**（`@PostConstruct recoverOnStartup`）：

- `SUCCESS`/`FAILED` 工作流直接以终态呈现，不重新调度；
- `RUNNING` 工作流中：
  - 崩溃时停留在 `RUNNING` 的步骤没有可信的完成结果，重置为 `PENDING` 并重新执行（`attempts` 累加）；
  - 已 `SUCCESS` 的步骤**不会重跑**（有单测用执行计数器验证）。

**幂等提交**（`POST /api/workflows`）：

- 相同 `workflowId` + 语义相同的定义（步骤顺序无关，比较 id/type/deps/param/timeout）：
  返回 `{"deduplicated":true,"execution":...}`，即既有执行结果，**不产生第二次执行**；
- 相同 `workflowId` 但定义不同：HTTP 409 `DEFINITION_CONFLICT`。

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/workflows` | 提交工作流（异步执行，立即返回当前状态） |
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

## 6. 本地验证方法

需要 JDK 21、Maven 3.6+。

### 6.1 运行测试

```bash
mvn test
```

测试清单（共 19 个用例，日志中统一打印**工作流标识、步骤依赖关系、各步骤最终状态**）：

- `DagValidationTest`：缺失依赖 / 自引用 / 成环拒绝原因可区分；拓扑序满足依赖且与提交顺序无关；
- `StepFailureTest`：STEP_EXCEPTION 传播、TIMEOUT 分类、下游 SKIPPED、并发上限实测、
  在途独立步骤跑完且不重跑、未知类型作为 DEFINITION_ERROR 被拒；
- `RestartRecoveryTest`：重复提交幂等、同 id 不同定义 409、kill 后重启已完成步骤不重跑、
  RUNNING 步骤恢复重跑、终态工作流重启后保持一致；
- `WorkflowApiIT`：基于 MockMvc 的 HTTP 端到端冒烟（成功、成环 400、幂等、409）。

### 6.2 启动服务并用 curl 验证

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

### 6.3 手工验证重启恢复

```bash
mvn package -DskipTests
java -jar target/workflow-dag-engine-java-1.0-SNAPSHOT.jar --workflow.data-dir=/tmp/dag-demo
# 提交一个含 sleep 步骤的工作流，执行中 kill -9 <pid>
# 重新启动同一命令：观察日志 "断点恢复：N 个中断在 RUNNING 的步骤回到 PENDING，SUCCESS 步骤保持不重跑"，
# GET 接口可见已完成步骤 attempts=1 保持不变，被中断步骤 attempts 增加并继续执行。
```

## 7. 配置项（application.properties）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `workflow.data-dir` | `./data/workflows` | JSON 状态落盘目录，启动时自动创建 |
| `workflow.max-concurrency` | `4` | 步骤执行全局并发上限 |
| `workflow.default-timeout-ms` | `5000` | 步骤默认超时；步骤 `timeoutMs>0` 时单步覆盖 |

## 8. 代码结构

```
model/       枚举与状态模型（定义、执行记录、失败溯源）
validation/  DAG 校验（Kahn 拓扑排序）
engine/      WorkflowEngine 调度核心、步骤处理器、内置处理器、幂等/冲突异常
store/       文件存储（原子写 JSON）
web/         REST 控制器与异常映射
config/      Spring 装配与配置属性
```
