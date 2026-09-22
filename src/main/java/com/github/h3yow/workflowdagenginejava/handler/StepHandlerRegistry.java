package com.github.h3yow.workflowdagenginejava.handler;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resolves a step's {@code action} name to its registered {@link StepHandler}.
 * Spring injects every handler bean (built-ins plus any test/custom bean).
 */
@Component
public class StepHandlerRegistry {

    private final Map<String, StepHandler> handlers;

    public StepHandlerRegistry(List<StepHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(StepHandler::action, Function.identity()));
    }

    public Optional<StepHandler> find(String action) {
        return Optional.ofNullable(handlers.get(action));
    }
}
