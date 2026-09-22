package com.github.h3yow.workflowdagenginejava.handler.builtin;

import com.github.h3yow.workflowdagenginejava.handler.StepHandler;
import org.springframework.stereotype.Component;

/** Built-in handler: completes immediately. */
@Component
public class NoopStepHandler implements StepHandler {

    @Override
    public String action() {
        return "noop";
    }

    @Override
    public String execute(String stepId) {
        return "ok";
    }
}
