package com.github.h3yow.workflowdagenginejava.model;

/**
 * 单个步骤的生命周期状态。
 */
public enum StepStatus {
    /** 等待调度 */
    PENDING,
    /** 已提交到执行池，正在执行 */
    RUNNING,
    /** 执行成功，终态，不会重跑 */
    SUCCESS,
    /** 执行失败（内部异常或超时），终态 */
    FAILED,
    /** 因上游失败而未执行，终态 */
    SKIPPED
}
