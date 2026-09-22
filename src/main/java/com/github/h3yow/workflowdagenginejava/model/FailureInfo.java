package com.github.h3yow.workflowdagenginejava.model;

/**
 * 失败溯源信息：失败分类、发生步骤与可读消息。
 */
public class FailureInfo {
    private FailureType failureType;
    private String stepId;
    private String message;

    public FailureInfo() {
    }

    public FailureInfo(FailureType failureType, String stepId, String message) {
        this.failureType = failureType;
        this.stepId = stepId;
        this.message = message;
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public void setFailureType(FailureType failureType) {
        this.failureType = failureType;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
