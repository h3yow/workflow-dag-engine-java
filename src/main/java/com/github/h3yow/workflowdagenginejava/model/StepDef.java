package com.github.h3yow.workflowdagenginejava.model;

import java.util.List;

/** Declarative definition of one workflow step. */
public record StepDef(
        String id,
        String action,
        List<String> dependsOn,
        long timeoutMillis
) {
}
