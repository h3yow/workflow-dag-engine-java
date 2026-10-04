package com.github.h3yow.workflowdagenginejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个步骤的执行进度与溯源记录，会被持久化。
 * startedAt/finishedAt 为 epoch 毫秒，0 表示尚未发生。
 * attempts 为已派发的尝试次数（含自动重试），attemptRecords 记录每次尝试的起止与结果。
 * batchBaseAttempts 为当前执行批次（首次执行或一次续跑）开始时的 attempts 基线：
 * 本批次已用尝试次数 = attempts - batchBaseAttempts，自动重试的预算按批次计算，
 * 续跑时把基线推到当时的 attempts，使续跑批次重新获得定义里配置的全部重试次数，
 * 同时历史 attempts 与 attemptRecords 原样保留、不累加也不丢失。
 * 老版本状态文件没有该字段，反序列化为 0，语义与引入该字段之前完全一致。
 */
public class StepRun {
    private String stepId;
    private StepStatus status;
    private long startedAt;
    private long finishedAt;
    private int attempts;
    private int batchBaseAttempts;
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

    public int getBatchBaseAttempts() {
        return batchBaseAttempts;
    }

    public void setBatchBaseAttempts(int batchBaseAttempts) {
        this.batchBaseAttempts = batchBaseAttempts;
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
