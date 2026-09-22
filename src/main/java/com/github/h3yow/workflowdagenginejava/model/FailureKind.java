package com.github.h3yow.workflowdagenginejava.model;

/**
 * Distinguishable failure categories reported on failed steps/workflows.
 */
public enum FailureKind {
    /** Submitted workflow definition is invalid (missing dep, cycle, ...). */
    DEFINITION_ERROR,
    /** Step handler threw an exception while executing. */
    STEP_EXCEPTION,
    /** Step did not finish within its configured timeout. */
    TIMEOUT
}
