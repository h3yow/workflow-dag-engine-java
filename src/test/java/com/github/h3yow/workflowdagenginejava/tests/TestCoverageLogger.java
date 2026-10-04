package com.github.h3yow.workflowdagenginejava.tests;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 全局测试覆盖日志（通过 junit-platform 自动注册，无需逐类注解）：
 * 每个测试类开始时打印其覆盖的业务面，每个用例结束时打印通过/失败，
 * 使 `mvn test` 的日志能直接看出覆盖了哪些业务、各业务下有哪些用例及其结果。
 */
public class TestCoverageLogger implements BeforeAllCallback, TestWatcher {

    private static final Logger log = LoggerFactory.getLogger("workflow-test");

    /** 测试类 → 覆盖的业务面说明。 */
    private static final Map<String, String> BUSINESS_AREAS = Map.of(
            "DagValidationTest", "依赖校验：缺失依赖/自引用/成环拒绝原因可区分，拓扑序与提交顺序无关",
            "StepFailureTest", "受控并发与失败传播：异常/超时分类、下游 SKIPPED、并发上限、在途步骤跑完",
            "RetryTest", "步骤自动重试：重试成功/次数耗尽/超时重试/重试不突破并发上限/老定义行为不变",
            "ResumeTest", "失败续跑：只补未成功步骤、并发只生效一次、成功态空操作、未知 id、中途重启恢复",
            "ResumeRetryTest", "续跑批次里的自动重试：续跑重新获得重试预算、用尽才算失败、"
                    + "并发续跑结果一致、重启后预算与记录保留、续跑重试不突破并发上限",
            "RestartRecoveryTest", "断点恢复与幂等提交：重复提交幂等、定义冲突 409、重启后已完成步骤不重跑",
            "WorkflowApiTest", "HTTP 端到端：提交/查询/续跑/幂等/400/404/409",
            "ApplicationTests", "Spring 上下文加载冒烟");

    @Override
    public void beforeAll(ExtensionContext context) {
        String simpleName = context.getRequiredTestClass().getSimpleName();
        String area = BUSINESS_AREAS.getOrDefault(simpleName, "（未登记的业务面）");
        log.info("==== 业务测试开始：{} | 覆盖业务：{} ====", simpleName, area);
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        log.info("  [通过] {}.{}", context.getRequiredTestClass().getSimpleName(),
                context.getRequiredTestMethod().getName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        log.info("  [失败] {}.{} → {}", context.getRequiredTestClass().getSimpleName(),
                context.getRequiredTestMethod().getName(), cause.getMessage());
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        log.info("  [中止] {}.{}", context.getRequiredTestClass().getSimpleName(),
                context.getRequiredTestMethod().getName());
    }

    @Override
    public void testDisabled(ExtensionContext context, java.util.Optional<String> reason) {
        log.info("  [跳过] {}.{}", context.getRequiredTestClass().getSimpleName(),
                context.getRequiredTestMethod().getName());
    }
}
