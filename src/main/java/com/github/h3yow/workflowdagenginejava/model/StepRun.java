package com.github.h3yow.workflowdagenginejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个步骤的执行进度与溯源记录，会被持久化。
 * startedAt/finishedAt 为 epoch 毫秒，0 表示尚未发生。
 * attempts 为已派发的尝试次数（含自动重试），attemptRecords 记录每次尝试的起止与结果。
 */
public class StepRun {
    private String stepId;
    private StepStatus status;
    private long startedAt;
    private long finishedAt;
    private int attempts;
    private FailureInfo failure;
    private List<AttemptRecord> attemptRecords = new ArrayList<>();

    public StepRun() {
    }

    public StepRun(String stepId) {
        this.stepId = stepId;
        this.status = StepStatus.PENDING;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public StepStatus getStatus() {
        return status;
    }

    public void setStatus(StepStatus status) {
        this.status = status;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(long startedAt) {
        this.startedAt = startedAt;
    }

    public long getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(long finishedAt) {
        this.finishedAt = finishedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public FailureInfo getFailure() {
        return failure;
    }

    public void setFailure(FailureInfo failure) {
        this.failure = failure;
    }

    public List<AttemptRecord> getAttemptRecords() {
        return attemptRecords;
    }

    public void setAttemptRecords(List<AttemptRecord> attemptRecords) {
        this.attemptRecords = attemptRecords == null ? new ArrayList<>() : attemptRecords;
    }
}
