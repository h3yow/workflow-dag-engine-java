package com.github.h3yow.workflowdagenginejava.model;

import java.util.List;

/**
 * 步骤定义：标识、处理器类型、依赖列表、入参、超时（毫秒）与自动重试配置。
 *
 * <p>maxRetries 为允许的重试次数（不含首次执行），retryDelayMs 为两次尝试之间的等待间隔；
 * 两者缺省为 0，即不重试，行为与引入重试能力之前完全一致。</p>
 */
public record StepDef(String id, String type, List<String> dependsOn, String param, long timeoutMs,
                      int maxRetries, long retryDelayMs) {
    public StepDef {
        if (dependsOn == null) {
            dependsOn = List.of();
        }
        if (maxRetries < 0) {
            maxRetries = 0;
        }
        if (retryDelayMs < 0) {
            retryDelayMs = 0;
        }
    }

    public StepDef(String id, String type, List<String> dependsOn, String param, long timeoutMs) {
        this(id, type, dependsOn, param, timeoutMs, 0, 0L);
    }

    public StepDef(String id, String type, List<String> dependsOn) {
        this(id, type, dependsOn, null, 0L);
    }
}
