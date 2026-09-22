package com.github.h3yow.workflowdagenginejava.model;

/**
 * 工作流定义被拒绝的可区分原因。
 */
public enum RejectReason {
    /** 工作流或步骤标识缺失/非法 */
    INVALID_ID,
    /** 依赖了不存在的步骤 */
    MISSING_DEPENDENCY,
    /** 步骤依赖自身 */
    SELF_REFERENCE,
    /** 依赖关系中存在环 */
    CYCLE_DETECTED,
    /** 步骤定义为空或类型不受支持 */
    INVALID_STEP,
    /** 工作流定义不完整（无步骤等） */
    EMPTY_WORKFLOW
}
