package com.github.h3yow.workflowdagenginejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个步骤的执行进度与溯源记录，会被持久化。
 * startedAt/finishedAt 为 epoch 毫秒，0 表示尚未发生。
 *
 * <p>{@code attempts} 为已发起的尝试总次数（跨自动重试、跨续跑连续累计）；
 * {@code attemptRecords} 保留每一次尝试的耗时与结果明细，事后可看出试了几次、每次什么结果。</p>
 */
public class StepRun {
    private String stepId;
    private StepStatus status;
    private long startedAt;
    private long finishedAt;
    private int attempts;
    private FailureInfo failure;
    private List<StepAttempt> attemptRecords = new ArrayList<>();

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

    public List<StepAttempt> getAttemptRecords() {
        return attemptRecords;
    }

    public void setAttemptRecords(List<StepAttempt> attemptRecords) {
        this.attemptRecords = attemptRecords == null ? new ArrayList<>() : attemptRecords;
    }
}
