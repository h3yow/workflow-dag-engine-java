package com.github.h3yow.workflowdagenginejava.model;

import java.util.List;

/**
 * 步骤定义：标识、处理器类型、依赖列表、入参、超时（毫秒）与自动重试配置。
 *
 * @param maxRetries      内部异常/超时失败后额外的自动重试次数；0 表示不重试（老定义行为）。
 *                        总尝试次数 = 1 + maxRetries，全部失败才算步骤 FAILED。
 * @param retryBackoffMs  两次尝试之间的等待间隔（毫秒），0 表示立即重试。
 *                        等待期间步骤仍占用并发槽位，因此重试不会突破并发上限。
 */
public record StepDef(String id, String type, List<String> dependsOn, String param, long timeoutMs,
                      int maxRetries, long retryBackoffMs) {
    public StepDef {
        if (dependsOn == null) {
            dependsOn = List.of();
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries 不能为负数: " + id);
        }
        if (retryBackoffMs < 0) {
            throw new IllegalArgumentException("retryBackoffMs 不能为负数: " + id);
        }
    }

    /** 不含重试配置的构造器：老用法，maxRetries/retryBackoffMs 均为 0。 */
    public StepDef(String id, String type, List<String> dependsOn, String param, long timeoutMs) {
        this(id, type, dependsOn, param, timeoutMs, 0, 0L);
    }

    public StepDef(String id, String type, List<String> dependsOn) {
        this(id, type, dependsOn, null, 0L, 0, 0L);
    }
}
