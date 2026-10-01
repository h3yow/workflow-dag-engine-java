package com.github.h3yow.workflowdagenginejava.engine;

/**
 * 对不存在的 workflowId 发起操作（如续跑）时抛出，HTTP 层映射为 404。
 */
public class WorkflowNotFoundException extends RuntimeException {
    public WorkflowNotFoundException(String message) {
        super(message);
    }
}
