package com.github.h3yow.workflowdagenginejava.model;

/**
 * 步骤一次"尝试"的完整溯源记录，会被持久化。
 * 同一个步骤跨多次工作流续跑（resume）的所有尝试按时间顺序追加，不重复累加、不覆盖。
 *
 * @param stepId      所属步骤 id
 * @param attemptNo   该步骤的全局尝试序号，从 1 开始连续编号（跨续跑连续）
 * @param runNo       发生本次尝试的工作流执行批次：1=首次执行，之后每次 resume +1
 * @param startedAt   本次尝试开始时间（epoch 毫秒）
 * @param finishedAt  本次尝试结束时间（epoch 毫秒，0 表示未正常结束，如进程崩溃）
 * @param result      本次尝试结果分类（开始时为 null）
 * @param message     失败时的可读原因；成功为 null
 */
public record StepAttempt(String stepId, int attemptNo, int runNo, long startedAt, long finishedAt,
                          AttemptResult result, String message) {
}
