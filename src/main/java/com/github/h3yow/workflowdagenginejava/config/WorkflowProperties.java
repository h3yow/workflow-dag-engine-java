package com.github.h3yow.workflowdagenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 可通过 application.properties 覆盖的引擎配置：
 * workflow.data-dir / workflow.max-concurrency / workflow.default-timeout-ms
 */
@ConfigurationProperties(prefix = "workflow")
public class WorkflowProperties {

    private String dataDir = "./data/workflows";
    private int maxConcurrency = 4;
    private long defaultTimeoutMs = 5000L;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public long getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }

    public void setDefaultTimeoutMs(long defaultTimeoutMs) {
        this.defaultTimeoutMs = defaultTimeoutMs;
    }
}
