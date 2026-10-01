package com.github.h3yow.workflowdagenginejava.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一次工作流执行的完整状态：定义 + 进度，按 workflowId 持久化。
 */
public class WorkflowExecution {
    private String workflowId;
    private WorkflowDef definition;
    private WorkflowStatus status;
    private Map<String, StepRun> steps = new LinkedHashMap<>();
    private FailureInfo failure;
    private long createdAt;
    private long updatedAt;
    /** 失败后续跑（resume）的次数：首次执行为 0，每次续跑 +1，会被持久化用于重启后判断批次。 */
    private int resumeCount;

    public WorkflowExecution() {
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public WorkflowDef getDefinition() {
        return definition;
    }

    public void setDefinition(WorkflowDef definition) {
        this.definition = definition;
    }

    public WorkflowStatus getStatus() {
        return status;
    }

    public void setStatus(WorkflowStatus status) {
        this.status = status;
    }

    public FailureInfo getFailure() {
        return failure;
    }

    public void setFailure(FailureInfo failure) {
        this.failure = failure;
    }

    public Map<String, StepRun> getSteps() {
        return steps;
    }

    public void setSteps(Map<String, StepRun> steps) {
        this.steps = steps;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public int getResumeCount() {
        return resumeCount;
    }

    public void setResumeCount(int resumeCount) {
        this.resumeCount = resumeCount;
    }
}
