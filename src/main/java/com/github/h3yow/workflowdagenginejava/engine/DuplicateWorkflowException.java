package com.github.h3yow.workflowdagenginejava.engine;

/**
 * 相同 workflowId 提交了不一致的定义（破坏幂等键语义）。
 */
public class DuplicateWorkflowException extends RuntimeException {

    public DuplicateWorkflowException(String message) {
        super(message);
    }
}
