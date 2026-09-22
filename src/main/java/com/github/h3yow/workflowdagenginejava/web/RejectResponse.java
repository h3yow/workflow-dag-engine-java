package com.github.h3yow.workflowdagenginejava.web;

/**
 * 定义被拒绝时的响应体：可区分的拒绝原因 + 可读说明。
 */
public record RejectResponse(String workflowId, String rejectReason, String message) {
}
