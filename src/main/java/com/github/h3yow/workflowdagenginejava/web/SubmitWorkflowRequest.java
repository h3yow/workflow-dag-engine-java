package com.github.h3yow.workflowdagenginejava.web;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.util.List;

/** JSON body shape for POST /api/workflows. */
public record SubmitWorkflowRequest(
        @JsonAlias({"workflow_id", "workflowId"})
        String workflowId,
        List<SubmitStepRequest> steps,
        @JsonAlias({"max_concurrency", "maxConcurrency"})
        Integer maxConcurrency
) {
}
