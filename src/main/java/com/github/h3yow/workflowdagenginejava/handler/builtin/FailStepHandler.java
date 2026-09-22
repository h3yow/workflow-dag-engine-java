package com.github.h3yow.workflowdagenginejava.handler.builtin;

import com.github.h3yow.workflowdagenginejava.handler.StepHandler;
import org.springframework.stereotype.Component;

/** Built-in handler: always throws, used to demonstrate failure propagation. */
@Component
public class FailStepHandler implements StepHandler {

    @Override
    public String action() {
        return "fail";
    }

    @Override
    public String execute(String stepId) {
        throw new RuntimeException("forced failure in step " + stepId);
    }
}
