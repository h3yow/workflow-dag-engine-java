package com.github.h3yow.workflowdagenginejava.store;

import com.github.h3yow.workflowdagenginejava.model.WorkflowSnapshot;

import java.util.List;
import java.util.Optional;

/** Persistence boundary for workflow snapshots (survives process restart). */
public interface WorkflowStore {

    /** Atomically insert; returns false if a workflow with the same id already exists. */
    boolean insertIfAbsent(WorkflowSnapshot snapshot);

    /** Persist the current snapshot state. */
    void save(WorkflowSnapshot snapshot);

    /** Load one workflow by id. */
    Optional<WorkflowSnapshot> find(String workflowId);

    /** Load all persisted workflows (used for recovery on startup). */
    List<WorkflowSnapshot> findAll();
}
