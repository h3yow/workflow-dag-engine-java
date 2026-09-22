package com.github.h3yow.workflowdagenginejava.model;

/**
 * 工作流整体状态。
 */
public enum WorkflowStatus {
    /** 定义已接收并通过校验，等待/正在执行 */
    RUNNING,
    /** 所有步骤成功 */
    SUCCESS,
    /** 至少一个步骤失败（其余步骤被跳过） */
    FAILED
}
