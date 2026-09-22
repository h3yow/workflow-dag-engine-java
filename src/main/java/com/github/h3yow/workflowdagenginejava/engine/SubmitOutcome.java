package com.github.h3yow.workflowdagenginejava.engine;

import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;

/**
 * 提交结果：deduplicated=true 表示命中幂等（重复提交），
 * 返回的是既有执行，没有产生第二次执行。
 */
public record SubmitOutcome(boolean deduplicated, WorkflowExecution execution) {
}
