package com.github.h3yow.workflowdagenginejava.handler.builtin;

import com.github.h3yow.workflowdagenginejava.handler.StepHandler;
import org.springframework.stereotype.Component;

/**
 * Built-in handler: sleeps for {@code millis} milliseconds (suffixed in the
 * step id, e.g. {@code sleep-300}) then succeeds. Useful for testing
 * concurrency limits and timeouts.
 */
@Component
public class SleepStepHandler implements StepHandler {

    @Override
    public String action() {
        return "sleep";
    }

    @Override
    public String execute(String stepId) throws Exception {
        long millis = 200;
        int dash = stepId == null ? -1 : stepId.lastIndexOf('-');
        if (dash >= 0) {
            try {
                millis = Long.parseLong(stepId.substring(dash + 1));
            } catch (NumberFormatException ignored) {
                // keep default
            }
        }
        Thread.sleep(millis);
        return "slept " + millis + "ms";
    }
}
