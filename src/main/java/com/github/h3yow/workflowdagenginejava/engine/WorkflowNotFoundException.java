package com.github.h3yow.workflowdagenginejava.engine;

/**
 * 对不存在的工作流发起查询/续跑。
 */
public class WorkflowNotFoundException extends RuntimeException {

    public WorkflowNotFoundException(String workflowId) {
        super("工作流不存在: " + workflowId);
    }
}
