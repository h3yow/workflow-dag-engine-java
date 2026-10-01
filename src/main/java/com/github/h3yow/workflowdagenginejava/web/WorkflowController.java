package com.github.h3yow.workflowdagenginejava.web;

import com.github.h3yow.workflowdagenginejava.engine.SubmitOutcome;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 工作流 HTTP 接口：
 * <ul>
 *   <li>POST /api/workflows 提交（幂等）；</li>
 *   <li>GET  /api/workflows/{id} 查询进度与溯源；</li>
 *   <li>GET  /api/workflows 列出全部。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowEngine engine;

    public WorkflowController(WorkflowEngine engine) {
        this.engine = engine;
    }

    @PostMapping
    public SubmitResponse submit(@RequestBody WorkflowDef def) {
        SubmitOutcome outcome = engine.submit(def);
        return new SubmitResponse(outcome.deduplicated(), outcome.execution());
    }

    /**
     * 失败续跑：只重新执行未成功的步骤。接口异步，立即返回当前状态；
     * 并发重复续跑只有第一次生效，其余请求拿到的是同一份执行（deduplicated=true）。
     */
    @PostMapping("/{workflowId}/resume")
    public SubmitResponse resume(@PathVariable String workflowId) {
        SubmitOutcome outcome = engine.resume(workflowId);
        return new SubmitResponse(outcome.deduplicated(), outcome.execution());
    }

    @GetMapping("/{workflowId}")
    public ResponseEntity<WorkflowExecution> get(@PathVariable String workflowId) {
        return engine.get(workflowId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<WorkflowExecution> list() {
        return engine.list();
    }
}
