package com.github.h3yow.workflowdagenginejava.model;

/**
 * 单次尝试（attempt）的结果分类，用于逐次尝试溯源。
 */
public enum AttemptResult {
    /** 本次尝试成功 */
    SUCCESS,
    /** 本次尝试因处理器内部异常失败（可重试） */
    STEP_EXCEPTION,
    /** 本次尝试超时失败（可重试） */
    TIMEOUT,
    /** 步骤类型等定义层错误（不重试，直接终态失败） */
    DEFINITION_ERROR,
    /** 进程崩溃导致尝试中断、没有可信结果（恢复时补记） */
    INTERRUPTED
}
