package com.github.h3yow.workflowdagenginejava.engine;

import com.github.h3yow.workflowdagenginejava.model.FailureInfo;
import com.github.h3yow.workflowdagenginejava.model.FailureType;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.RejectReason;
import com.github.h3yow.workflowdagenginejava.model.StepRun;
import com.github.h3yow.workflowdagenginejava.model.StepStatus;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;
import com.github.h3yow.workflowdagenginejava.model.WorkflowStatus;
import com.github.h3yow.workflowdagenginejava.store.WorkflowStateStore;
import com.github.h3yow.workflowdagenginejava.validation.DagValidator;
import com.github.h3yow.workflowdagenginejava.validation.InvalidWorkflowException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DAG 工作流调度核心。
 *
 * <p>并发模型：一个全局固定大小的步骤线程池（大小 = maxConcurrency）是明确的并发上限；
 * 每个 RUNNING 工作流由一个单线程调度泵驱动，泵只做"判断就绪 → 提交到线程池 → 回收结果"，
 * 步骤本体在线程池中执行。</p>
 *
 * <p>失败传播：任一步骤 FAILED 后，工作流进入"不再派发新步骤"的收敛阶段，
 * 尚未派发的 PENDING 步骤一律 SKIPPED，已在执行中的步骤允许跑完（不杀线程）；
 * 已 SUCCESS 的步骤保持终态，任何情况下都不会重跑。</p>
 */
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private final WorkflowStateStore store;
    private final StepHandlerRegistry registry;
    private final int maxConcurrency;
    private final long defaultTimeoutMs;

    private final ConcurrentHashMap<String, WorkflowExecution> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PumpState> pumps = new ConcurrentHashMap<>();

    private ExecutorService stepPool;
    private ScheduledExecutorService pumpPool;
    private java.util.concurrent.Semaphore stepSlots;

    public WorkflowEngine(WorkflowStateStore store, StepHandlerRegistry registry,
                          int maxConcurrency, long defaultTimeoutMs) {
        this.store = store;
        this.registry = registry;
        this.maxConcurrency = Math.max(1, maxConcurrency);
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    /** 启动时加载持久化状态，恢复未完成工作流；SUCCESS/FAILED 保持终态不重跑。 */
    @PostConstruct
    public void recoverOnStartup() {
        ensurePools();
        List<WorkflowExecution> loaded = store.findAll();
        log.info("启动恢复：从存储加载到 {} 个工作流", loaded.size());
        for (WorkflowExecution exec : loaded) {
            cache.put(exec.getWorkflowId(), exec);
            if (exec.getStatus() == WorkflowStatus.RUNNING) {
                recoverRunning(exec);
                startPump(exec.getWorkflowId());
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        if (pumpPool != null) {
            pumpPool.shutdownNow();
        }
        if (stepPool != null) {
            stepPool.shutdownNow();
        }
    }

    /**
     * 幂等提交：
     * <ul>
     *   <li>workflowId 不存在：校验定义、创建执行并立即开始调度；</li>
     *   <li>workflowId 已存在且定义相同：返回既有执行（deduplicated=true），不产生第二次执行；</li>
     *   <li>workflowId 已存在但定义不同：抛出 {@link DuplicateWorkflowException}（HTTP 409）。</li>
     * </ul>
     * 定义错误以 {@link InvalidWorkflowException} 拒绝（HTTP 400），失败类型 DEFINITION_ERROR。
     */
    public SubmitOutcome submit(WorkflowDef def) {
        DagValidator.validate(def);
        // 处理器类型在定义层之外再校验一次，归类为定义错误
        for (StepDef step : def.steps()) {
            if (!registry.supports(step.type())) {
                throw new InvalidWorkflowException(
                        RejectReason.INVALID_STEP,
                        "步骤 " + step.id() + " 的类型不受支持: " + step.type());
            }
        }
        ensurePools();

        Object lock = lockFor(def.workflowId());
        synchronized (lock) {
            WorkflowExecution existing = cache.get(def.workflowId());
            if (existing != null) {
                if (!sameDefinition(existing.getDefinition(), def)) {
                    throw new DuplicateWorkflowException(
                            "workflowId=" + def.workflowId() + " 已存在，但提交的定义与既有定义不一致");
                }
                log.info("[workflowId={}] 幂等命中，返回既有执行，状态={}",
                        def.workflowId(), existing.getStatus());
                return new SubmitOutcome(true, snapshot(existing));
            }

            long now = System.currentTimeMillis();
            WorkflowExecution exec = new WorkflowExecution();
            exec.setWorkflowId(def.workflowId());
            exec.setDefinition(def);
            exec.setStatus(WorkflowStatus.RUNNING);
            exec.setCreatedAt(now);
            exec.setUpdatedAt(now);
            for (StepDef step : def.steps()) {
                exec.getSteps().put(step.id(), new StepRun(step.id()));
            }
            persist(exec);
            cache.put(def.workflowId(), exec);
            log.info("[workflowId={}] 新工作流已接收，步骤数={}", def.workflowId(), def.steps().size());

            startPump(def.workflowId());
            return new SubmitOutcome(false, snapshot(exec));
        }
    }

    public Optional<WorkflowExecution> get(String workflowId) {
        WorkflowExecution exec = cache.get(workflowId);
        return exec == null ? Optional.empty() : Optional.of(snapshot(exec));
    }

    public List<WorkflowExecution> list() {
        return cache.values().stream().map(this::snapshot).toList();
    }

    // ------------------------------------------------------------------
    // 调度泵
    // ------------------------------------------------------------------

    private void startPump(String workflowId) {
        PumpState state = new PumpState();
        pumps.put(workflowId, state);
        pumpPool.schedule(() -> pumpOnce(workflowId, state), 0, TimeUnit.MILLISECONDS);
    }

    private void pumpOnce(String workflowId, PumpState state) {
        boolean stillRunning = false;
        try {
            Object lock = lockFor(workflowId);
            synchronized (lock) {
                WorkflowExecution exec = cache.get(workflowId);
                if (exec != null && exec.getStatus() == WorkflowStatus.RUNNING) {
                    pump(exec, state);
                    stillRunning = exec.getStatus() == WorkflowStatus.RUNNING;
                }
            }
        } catch (Exception e) {
            log.error("[workflowId={}] 调度泵异常", workflowId, e);
            stillRunning = true; // 意外异常后继续尝试，避免工作流永久挂起
        }
        if (stillRunning) {
            pumpPool.schedule(() -> pumpOnce(workflowId, state), 5, TimeUnit.MILLISECONDS);
        } else {
            pumps.remove(workflowId);
        }
    }

    private void pump(WorkflowExecution exec, PumpState state) {
        drainCompleted(exec, state);

        if (state.failed) {
            // 失败收敛：还有步骤在跑就等其结束；没有在跑的了，剩余 PENDING 全部 SKIPPED
            boolean anyInFlight = state.inflight.values().stream().anyMatch(f -> !f.isDone());
            if (anyInFlight) {
                return;
            }
            int skipped = 0;
            for (StepRun run : exec.getSteps().values()) {
                if (run.getStatus() == StepStatus.PENDING) {
                    run.setStatus(StepStatus.SKIPPED);
                    skipped++;
                }
            }
            finishAs(exec, WorkflowStatus.FAILED, skipped);
            return;
        }

        dispatchReady(exec, state);

        boolean terminal = exec.getSteps().values().stream()
                .allMatch(r -> r.getStatus() == StepStatus.SUCCESS);
        if (terminal) {
            finishAs(exec, WorkflowStatus.SUCCESS, 0);
        }
    }

    private void drainCompleted(WorkflowExecution exec, PumpState state) {
        var iterator = state.inflight.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Future<StepOutcome>> entry = iterator.next();
            Future<StepOutcome> future = entry.getValue();
            if (!future.isDone()) {
                continue;
            }
            String stepId = entry.getKey();
            iterator.remove();
            try {
                StepOutcome outcome = future.get();
                applyOutcome(exec, state, outcome);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                // 正常情况下步骤异常已在 task 内部捕获；走到这里属于调度意外
                applyOutcome(exec, state, StepOutcome.failure(stepId, FailureType.STEP_EXCEPTION,
                        String.valueOf(e.getCause())));
            }
        }
    }

    private void applyOutcome(WorkflowExecution exec, PumpState state, StepOutcome outcome) {
        StepRun run = exec.getSteps().get(outcome.stepId());
        if (run.getStatus() == StepStatus.SUCCESS) {
            return; // 防御：终态不覆盖
        }
        run.setFinishedAt(System.currentTimeMillis());
        if (outcome.success()) {
            run.setStatus(StepStatus.SUCCESS);
            run.setFailure(null);
            log.info("[workflowId={}] stepId={} 最终状态=SUCCESS",
                    exec.getWorkflowId(), outcome.stepId());
        } else {
            run.setStatus(StepStatus.FAILED);
            run.setFailure(new FailureInfo(outcome.failureType(), outcome.stepId(), outcome.message()));
            state.failed = true;
            log.warn("[workflowId={}] stepId={} 最终状态=FAILED/{} {}",
                    exec.getWorkflowId(), outcome.stepId(),
                    outcome.failureType(), outcome.message());
        }
        persist(exec);
    }

    private void dispatchReady(WorkflowExecution exec, PumpState state) {
        Map<String, StepDef> byId = DagValidator.indexById(exec.getDefinition());
        TreeSet<String> ready = new TreeSet<>();
        for (StepDef step : exec.getDefinition().steps()) {
            StepRun run = exec.getSteps().get(step.id());
            if (run.getStatus() != StepStatus.PENDING || state.dispatched.contains(step.id())) {
                continue;
            }
            boolean depsOk = true;
            for (String dep : step.dependsOn()) {
                if (exec.getSteps().get(dep).getStatus() != StepStatus.SUCCESS) {
                    depsOk = false;
                    break;
                }
            }
            if (depsOk) {
                ready.add(step.id());
            }
        }

        // 并发上限是跨所有工作流共享的全局信号量；本工作流已占的槽位要扣除
        int freeSlots = stepSlots.drainPermits();
        stepSlots.release(freeSlots);
        int availableSlots = Math.max(0, freeSlots - state.inflight.size());
        int toDispatch = Math.min(availableSlots, ready.size());
        int dispatched = 0;
        for (String stepId : ready) {
            if (dispatched >= toDispatch) {
                break;
            }
            if (!stepSlots.tryAcquire()) {
                break; // 槽位被其他工作流抢走，等下一拍
            }
            StepDef step = byId.get(stepId);
            StepRun run = exec.getSteps().get(stepId);
            long now = System.currentTimeMillis();
            run.setStatus(StepStatus.RUNNING);
            run.setStartedAt(now);
            run.setAttempts(run.getAttempts() + 1);
            persist(exec);
            state.dispatched.add(stepId);
            log.info("[workflowId={}] stepId={} 派发执行，依赖={}",
                    exec.getWorkflowId(), stepId, step.dependsOn());

            StepTask task = new StepTask(exec.getWorkflowId(), step,
                    resolveTimeout(step), registry, stepSlots);
            Future<StepOutcome> future = stepPool.submit(task);
            state.inflight.put(stepId, future);
            dispatched++;
        }
    }

    private long resolveTimeout(StepDef step) {
        return step.timeoutMs() > 0 ? step.timeoutMs() : defaultTimeoutMs;
    }

    private void finishAs(WorkflowExecution exec, WorkflowStatus workflowStatus, int skipped) {
        exec.setStatus(workflowStatus);
        exec.setUpdatedAt(System.currentTimeMillis());
        if (workflowStatus == WorkflowStatus.FAILED) {
            FailureInfo root = exec.getSteps().values().stream()
                    .filter(r -> r.getStatus() == StepStatus.FAILED && r.getFailure() != null)
                    .map(StepRun::getFailure)
                    .findFirst()
                    .orElse(new FailureInfo(FailureType.STEP_EXCEPTION, null, "未知失败"));
            exec.setFailure(root);
        }
        persist(exec);
        log.info("[workflowId={}] 工作流结束，最终状态={}{}",
                exec.getWorkflowId(), workflowStatus,
                skipped > 0 ? "，跳过步骤数=" + skipped : "");
    }

    // ------------------------------------------------------------------
    // 恢复
    // ------------------------------------------------------------------

    private void recoverRunning(WorkflowExecution exec) {
        int reset = 0;
        for (StepRun run : exec.getSteps().values()) {
            if (run.getStatus() == StepStatus.RUNNING) {
                // 进程崩溃时正在执行的步骤没有可信的完成结果，回到 PENDING 重新执行
                run.setStatus(StepStatus.PENDING);
                run.setStartedAt(0);
                reset++;
            }
        }
        // 极端情况：崩溃发生在"标记 RUNNING"之前，无需处理；SUCCESS 保持不重跑
        persist(exec);
        log.info("[workflowId={}] 断点恢复：{} 个中断在 RUNNING 的步骤回到 PENDING，"
                        + "SUCCESS 步骤保持不重跑",
                exec.getWorkflowId(), reset);
    }

    // ------------------------------------------------------------------
    // 基础设施
    // ------------------------------------------------------------------

    private void ensurePools() {
        if (stepPool == null) {
            ThreadFactory daemon = r -> {
                Thread t = new Thread(r, "dag-step");
                t.setDaemon(true);
                return t;
            };
            stepPool = Executors.newFixedThreadPool(maxConcurrency, daemon);
            stepSlots = new java.util.concurrent.Semaphore(maxConcurrency, true);
        }
        if (pumpPool == null) {
            pumpPool = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "dag-pump");
                t.setDaemon(true);
                return t;
            });
        }
    }

    private void persist(WorkflowExecution exec) {
        exec.setUpdatedAt(System.currentTimeMillis());
        store.save(exec);
    }

    private Object lockFor(String workflowId) {
        return locks.computeIfAbsent(workflowId, k -> new Object());
    }

    /** 幂等键冲突判定：按 (id,type,deps,param,timeoutMs) 做语义比较，忽略步骤数组顺序。 */
    private boolean sameDefinition(WorkflowDef a, WorkflowDef b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (!java.util.Objects.equals(a.workflowId(), b.workflowId())) {
            return false;
        }
        Map<String, StepDef> ma = DagValidator.indexById(a);
        Map<String, StepDef> mb = DagValidator.indexById(b);
        if (!ma.keySet().equals(mb.keySet())) {
            return false;
        }
        for (String id : ma.keySet()) {
            StepDef x = ma.get(id);
            StepDef y = mb.get(id);
            if (!java.util.Objects.equals(x.type(), y.type())
                    || !java.util.Objects.equals(x.param(), y.param())
                    || x.timeoutMs() != y.timeoutMs()
                    || !new TreeSet<>(x.dependsOn()).equals(new TreeSet<>(y.dependsOn()))) {
                return false;
            }
        }
        return true;
    }

    /** 返回深拷贝快照，避免调用方看到执行过程中的中间态。 */
    private WorkflowExecution snapshot(WorkflowExecution exec) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.readValue(mapper.writeValueAsString(exec), WorkflowExecution.class);
        } catch (Exception e) {
            throw new IllegalStateException("状态快照失败", e);
        }
    }

    /** 单个步骤的执行结果，在任务线程内产出。 */
    private record StepOutcome(boolean success, String stepId, FailureType failureType,
                               String message) {
        static StepOutcome ok(String stepId) {
            return new StepOutcome(true, stepId, null, null);
        }

        static StepOutcome failure(String stepId, FailureType type, String message) {
            return new StepOutcome(false, stepId, type, message);
        }
    }

    /** 带超时控制的步骤任务；执行结束（含异常/超时）后释放并发槽位。 */
    private static final class StepTask implements java.util.concurrent.Callable<StepOutcome> {
        private final String workflowId;
        private final StepDef step;
        private final long timeoutMs;
        private final StepHandlerRegistry registry;
        private final java.util.concurrent.Semaphore slots;

        StepTask(String workflowId, StepDef step, long timeoutMs, StepHandlerRegistry registry,
                 java.util.concurrent.Semaphore slots) {
            this.workflowId = workflowId;
            this.step = step;
            this.timeoutMs = timeoutMs;
            this.registry = registry;
            this.slots = slots;
        }

        @Override
        public StepOutcome call() {
            try {
                return doCall();
            } finally {
                slots.release();
            }
        }

        private StepOutcome doCall() {
            StepHandler handler = registry.find(step.type()).orElse(null);
            if (handler == null) {
                return StepOutcome.failure(step.id(), FailureType.DEFINITION_ERROR,
                        "步骤类型不受支持: " + step.type());
            }
            ExecutorService oneShot = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "dag-step-" + step.id());
                t.setDaemon(true);
                return t;
            });
            try {
                Future<?> f = oneShot.submit(() -> {
                    try {
                        handler.execute(workflowId, step.id(), step.param());
                    } catch (Exception e) {
                        throw new StepExecutionException(e);
                    }
                });
                try {
                    f.get(timeoutMs, TimeUnit.MILLISECONDS);
                    return StepOutcome.ok(step.id());
                } catch (TimeoutException e) {
                    f.cancel(true);
                    return StepOutcome.failure(step.id(), FailureType.TIMEOUT,
                            "步骤执行超过超时时间 " + timeoutMs + "ms");
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof StepExecutionException see && see.getCause() != null) {
                        cause = see.getCause();
                    }
                    return StepOutcome.failure(step.id(), FailureType.STEP_EXCEPTION,
                            cause.getClass().getSimpleName() + ": " + cause.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return StepOutcome.failure(step.id(), FailureType.STEP_EXCEPTION, "调度被中断");
                }
            } finally {
                oneShot.shutdownNow();
            }
        }
    }

    /** 在 submit 的 lambda 里把受检异常包成运行时异常跨线程传递。 */
    private static final class StepExecutionException extends RuntimeException {
        StepExecutionException(Throwable cause) {
            super(cause);
        }
    }

    /** 每个工作流调度泵的可变状态。 */
    private static final class PumpState {
        final Map<String, Future<StepOutcome>> inflight = new java.util.HashMap<>();
        final java.util.Set<String> dispatched = new java.util.HashSet<>();
        volatile boolean failed = false;
    }
}
