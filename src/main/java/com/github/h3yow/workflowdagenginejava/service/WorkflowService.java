package com.github.h3yow.workflowdagenginejava.service;

import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine.SubmissionResult;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowSnapshot;
import com.github.h3yow.workflowdagenginejava.store.WorkflowNotFoundException;
import org.springframework.stereotype.Service;

/**
 * Application facade over {@link WorkflowEngine}. Submission is idempotent:
 * resubmitting the same id with the same definition returns the existing
 * snapshot and never triggers a second execution of finished steps.
 */
@Service
public class WorkflowService {

    private final WorkflowEngine engine;

    public WorkflowService(WorkflowEngine engine) {
        this.engine = engine;
    }

    public SubmissionResult submit(WorkflowDef def) {
        return engine.submit(def);
    }

    public WorkflowSnapshot get(String workflowId) {
        return engine.get(workflowId)
                .orElseThrow(() -> new WorkflowNotFoundException(workflowId));
    }
}
