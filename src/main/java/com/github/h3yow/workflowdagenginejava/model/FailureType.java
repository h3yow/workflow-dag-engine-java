package com.github.h3yow.workflowdagenginejava.model;

/**
 * 失败原因的分类，用于可解释地展示结果：
 * <ul>
 *   <li>{@link #DEFINITION_ERROR}：工作流定义错误（提交时即被拒绝，不会产生执行）；</li>
 *   <li>{@link #STEP_EXCEPTION}：步骤执行过程中抛出内部异常；</li>
 *   <li>{@link #TIMEOUT}：步骤执行超过其约定的超时时间。</li>
 * </ul>
 */
public enum FailureType {
    DEFINITION_ERROR,
    STEP_EXCEPTION,
    TIMEOUT
}
