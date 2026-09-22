package com.github.h3yow.workflowdagenginejava.engine;

import java.util.Map;
import java.util.Optional;

/**
 * 按步骤 type 查找处理器。
 */
public class StepHandlerRegistry {

    private final Map<String, StepHandler> handlers;

    public StepHandlerRegistry(Map<String, StepHandler> handlers) {
        this.handlers = Map.copyOf(handlers);
    }

    public Optional<StepHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public boolean supports(String type) {
        return handlers.containsKey(type);
    }
}
