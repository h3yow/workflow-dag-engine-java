package com.github.h3yow.workflowdagenginejava.web;

import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine.SubmissionResult;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowSnapshot;
import com.github.h3yow.workflowdagenginejava.service.WorkflowService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * HTTP API:
 * <ul>
 *   <li>{@code POST /api/workflows} - submit (or idempotently resubmit) a workflow;</li>
 *   <li>{@code GET  /api/workflows/{id}} - fetch current snapshot / final result.</li>
 * </ul>
 * New submissions return 201; idempotent replays of an existing id return 200.
 */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowService service;

    public WorkflowController(WorkflowService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<WorkflowSnapshot> submit(@RequestBody SubmitWorkflowRequest request) {
        List<StepDef> steps = request.steps() == null ? List.of()
                : request.steps().stream()
                .map(s -> new StepDef(s.id(), s.action(),
                        s.dependsOn() == null ? List.of() : List.copyOf(s.dependsOn()),
                        s.timeoutMillis() == null ? 0L : s.timeoutMillis()))
                .toList();
        int concurrency = request.maxConcurrency() == null ? 0 : request.maxConcurrency();
        WorkflowDef def = new WorkflowDef(request.workflowId(), steps, concurrency);
        SubmissionResult result = service.submit(def);
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK)
                .body(result.snapshot());
    }

    @GetMapping("/{id}")
    public WorkflowSnapshot get(@PathVariable String id) {
        return service.get(id);
    }
}
