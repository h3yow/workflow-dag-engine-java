package com.github.h3yow.workflowdagenginejava.model;

import java.util.List;

/** Declarative definition of a workflow: an id plus a list of steps. */
public record WorkflowDef(
        String workflowId,
        List<StepDef> steps,
        int maxConcurrency
) {
}
