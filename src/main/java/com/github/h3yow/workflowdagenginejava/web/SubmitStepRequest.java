package com.github.h3yow.workflowdagenginejava.web;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.util.List;

/** JSON body shape for one step in a submission request. */
public record SubmitStepRequest(
        String id,
        String action,
        @JsonAlias({"depends_on", "dependsOn"})
        List<String> dependsOn,
        @JsonAlias({"timeout_millis", "timeoutMillis"})
        Long timeoutMillis
) {
}
