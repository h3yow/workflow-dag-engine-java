package com.github.h3yow.workflowdagenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration knobs for the workflow engine. */
@ConfigurationProperties(prefix = "workflow")
public class EngineProperties {

    private String dataDir = "data/workflows";
    private int defaultConcurrency = 4;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getDefaultConcurrency() {
        return defaultConcurrency;
    }

    public void setDefaultConcurrency(int defaultConcurrency) {
        this.defaultConcurrency = defaultConcurrency;
    }
}
