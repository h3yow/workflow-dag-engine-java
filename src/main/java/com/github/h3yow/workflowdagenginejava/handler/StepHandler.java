package com.github.h3yow.workflowdagenginejava.handler;

/** A piece of executable business logic bound to a step's {@code action}. */
public interface StepHandler {

    /** Action name this handler is registered under. */
    String action();

    /** Execute the step; throwing means STEP_EXCEPTION failure. */
    String execute(String stepId) throws Exception;
}
