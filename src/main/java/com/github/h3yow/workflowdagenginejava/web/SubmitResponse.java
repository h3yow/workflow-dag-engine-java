package com.github.h3yow.workflowdagenginejava.web;

import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;

/**
 * 提交响应：既有执行（幂等命中）时 deduplicated=true。
 */
public record SubmitResponse(boolean deduplicated, WorkflowExecution execution) {
}
