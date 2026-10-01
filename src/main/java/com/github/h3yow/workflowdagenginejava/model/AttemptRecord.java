package com.github.h3yow.workflowdagenginejava.model;

/**
 * 一次步骤尝试的溯源记录：第几次尝试、起止时间、结果与失败分类。
 * 每次派发（含自动重试）都会产生一条记录并随工作流持久化，
 * finishedAt=0 表示该次尝试尚未产生结果（例如执行中或服务重启被中断）。
 */
public class AttemptRecord {
    private int attemptNo;
    private long startedAt;
    private long finishedAt;
    private boolean success;
    private FailureType failureType;
    private String message;

    public AttemptRecord() {
    }

    public AttemptRecord(int attemptNo, long startedAt) {
        this.attemptNo = attemptNo;
        this.startedAt = startedAt;
    }

    public int getAttemptNo() {
        return attemptNo;
    }

    public void setAttemptNo(int attemptNo) {
        this.attemptNo = attemptNo;
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

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public void setFailureType(FailureType failureType) {
        this.failureType = failureType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
