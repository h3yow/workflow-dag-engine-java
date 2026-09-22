package com.github.h3yow.workflowdagenginejava.model;

import java.util.List;

/**
 * 步骤定义：标识、处理器类型、依赖列表、入参与超时（毫秒）。
 */
public record StepDef(String id, String type, List<String> dependsOn, String param, long timeoutMs) {
    public StepDef {
        if (dependsOn == null) {
            dependsOn = List.of();
        }
    }

    public StepDef(String id, String type, List<String> dependsOn) {
        this(id, type, dependsOn, null, 0L);
    }
}
