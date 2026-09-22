package com.github.h3yow.workflowdagenginejava.handler.builtin;

import com.github.h3yow.workflowdagenginejava.handler.StepHandler;
import org.springframework.stereotype.Component;

/** Built-in handler: returns an echo string for the step. */
@Component
public class EchoStepHandler implements StepHandler {

    @Override
    public String action() {
        return "echo";
    }

    @Override
    public String execute(String stepId) {
        return "echo:" + stepId;
    }
}
