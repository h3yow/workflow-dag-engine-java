package com.github.h3yow.workflowdagenginejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 端到端冒烟：合法 DAG 执行成功、重复提交幂等、成环 400 且原因可区分、id 冲突 409。
 * 使用独立临时目录，避免污染默认 data 目录。
 */
@SpringBootTest(properties = "workflow.data-dir=target/it-data/workflows")
@AutoConfigureMockMvc
class WorkflowApiTest {

    // Spring 上下文在测试类加载之后才创建，此处先清掉上一次运行遗留的状态文件，
    // 保证干净环境与连续多次运行下结果一致（幂等命中不依赖旧状态）。
    static {
        TestSupport.deleteRecursively(java.nio.file.Path.of("target/it-data/workflows"));
    }

    @Autowired
    private MockMvc mvc;

    @Test
    void submitRunQueryAndIdempotentReplay() throws Exception {
        String body = """
                {
                  "workflowId": "api-wf-1",
                  "steps": [
                    {"id": "a", "type": "noop", "dependsOn": []},
                    {"id": "b", "type": "echo", "param": "hi", "dependsOn": ["a"]}
                  ]
                }
                """;
        mvc.perform(post("/api/workflows").contentType("application/json").content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deduplicated").value(false));

        // 等待异步执行完成
        Thread.sleep(300);

        mvc.perform(get("/api/workflows/api-wf-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.steps.a.status").value("SUCCESS"))
                .andExpect(jsonPath("$.steps.b.status").value("SUCCESS"));

        // 重复提交：返回既有结果，deduplicated=true
        mvc.perform(post("/api/workflows").contentType("application/json").content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deduplicated").value(true))
                .andExpect(jsonPath("$.execution.status").value("SUCCESS"));
    }

    @Test
    void cycleIsRejectedWithDistinguishableReason() throws Exception {
        String body = """
                {
                  "workflowId": "api-wf-cycle",
                  "steps": [
                    {"id": "a", "type": "noop", "dependsOn": ["b"]},
                    {"id": "b", "type": "noop", "dependsOn": ["a"]}
                  ]
                }
                """;
        mvc.perform(post("/api/workflows").contentType("application/json").content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.rejectReason").value("CYCLE_DETECTED"));
    }

    @Test
    void sameIdDifferentDefinitionConflicts() throws Exception {
        String v1 = """
                {"workflowId": "api-wf-conflict",
                 "steps": [{"id": "a", "type": "noop", "dependsOn": []}]}
                """;
        String v2 = """
                {"workflowId": "api-wf-conflict",
                 "steps": [{"id": "a", "type": "fail", "dependsOn": []}]}
                """;
        mvc.perform(post("/api/workflows").contentType("application/json").content(v1))
                .andExpect(status().isOk());
        Thread.sleep(300);
        mvc.perform(post("/api/workflows").contentType("application/json").content(v2))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.rejectReason").value("DEFINITION_CONFLICT"));
    }

    @Test
    void resumeViaHttp() throws Exception {
        // 不存在的 id → 404 WORKFLOW_NOT_FOUND
        mvc.perform(post("/api/workflows/no-such-wf/resume"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.rejectReason").value("WORKFLOW_NOT_FOUND"));

        // 失败工作流：续跑受理（deduplicated=false），只补跑未成功部分
        String body = """
                {"workflowId": "api-wf-resume",
                 "steps": [{"id": "a", "type": "fail", "param": "boom", "dependsOn": [],
                            "maxRetries": 1, "retryDelayMs": 50},
                           {"id": "b", "type": "noop", "dependsOn": ["a"]}]}
                """;
        mvc.perform(post("/api/workflows").contentType("application/json").content(body))
                .andExpect(status().isOk());
        Thread.sleep(500);
        mvc.perform(get("/api/workflows/api-wf-resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.steps.a.attempts").value(2))
                .andExpect(jsonPath("$.steps.b.status").value("SKIPPED"));

        mvc.perform(post("/api/workflows/api-wf-resume/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deduplicated").value(false))
                .andExpect(jsonPath("$.execution.status").value("RUNNING"));

        // 已成功的工作流续跑 → 幂等空操作
        String ok = """
                {"workflowId": "api-wf-resume-ok",
                 "steps": [{"id": "a", "type": "noop", "dependsOn": []}]}
                """;
        mvc.perform(post("/api/workflows").contentType("application/json").content(ok))
                .andExpect(status().isOk());
        Thread.sleep(300);
        mvc.perform(post("/api/workflows/api-wf-resume-ok/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deduplicated").value(true))
                .andExpect(jsonPath("$.execution.status").value("SUCCESS"));
    }
}
