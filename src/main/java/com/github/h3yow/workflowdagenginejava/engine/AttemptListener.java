package com.github.h3yow.workflowdagenginejava.engine;

import com.github.h3yow.workflowdagenginejava.model.AttemptResult;
import com.github.h3yow.workflowdagenginejava.model.StepAttempt;

/**
 * 步骤每次尝试（含自动重试）的生命周期回调，由调度泵实现并持久化。
 * 调用发生在步骤执行线程上；一个步骤的各次尝试严格串行，回调不会并发。
 */
public interface AttemptListener {

    /** 一次尝试开始：发起执行前回调，attemptNo 由实现方分配（从 1 连续递增，跨续跑连续）。 */
    StepAttempt onAttemptStart(String stepId, long startedAt);

    /** 一次尝试结束：记录结果分类与失败原因；attempt 为 onAttemptStart 返回的对象。 */
    void onAttemptFinish(StepAttempt attempt, AttemptResult result, String message, long finishedAt);
}
