package com.github.h3yow.workflowdagenginejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 端到端冒烟：合法 DAG 执行成功、重复提交幂等、成环 400 且原因可区分、id 冲突 409。
 * 使用独立临时目录，避免污染默认 data 目录。
 */
@SpringBootTest
@AutoConfigureMockMvc
class WorkflowApiTest {

    /** 每次测试运行使用独立数据目录，不依赖上一次运行留下的状态。 */
    private static final Path DATA_DIR;

    static {
        try {
            DATA_DIR = Files.createTempDirectory("dag-api-it-");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("workflow.data-dir", () -> DATA_DIR.toString());
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
    void failedWorkflowCanResumeViaHttp() throws Exception {
        String body = """
                {
                  "workflowId": "api-wf-resume",
                  "steps": [
                    {"id": "ok", "type": "noop", "dependsOn": []},
                    {"id": "bad", "type": "fail", "param": "boom", "dependsOn": ["ok"]},
                    {"id": "after", "type": "noop", "dependsOn": ["bad"]}
                  ]
                }
                """;
        mvc.perform(post("/api/workflows").contentType("application/json").content(body))
                .andExpect(status().isOk());
        Thread.sleep(300);
        mvc.perform(get("/api/workflows/api-wf-resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.steps.after.status").value("SKIPPED"));

        // 修复后的步骤只能换处理器，内置 fail 无法变成功；这里验证续跑接口的状态契约：
        // 不存在的工作流 → 404；已失败工作流续跑 → 同步阻塞返回终态（仍 FAILED，但 attempts 增加）
        mvc.perform(post("/api/workflows/no-such/resume"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.rejectReason").value("WORKFLOW_NOT_FOUND"));

        mvc.perform(post("/api/workflows/api-wf-resume/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deduplicated").value(false))
                .andExpect(jsonPath("$.execution.status").value("FAILED"))
                .andExpect(jsonPath("$.execution.resumeCount").value(1))
                .andExpect(jsonPath("$.execution.steps.ok.status").value("SUCCESS"))
                .andExpect(jsonPath("$.execution.steps.bad.attempts").value(2));

        // 并发/重复续跑幂等：再次续跑，仍是一次真实执行，结果一致
        mvc.perform(post("/api/workflows/api-wf-resume/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.execution.resumeCount").value(2));
    }

    @Test
    void sameIdDifferentDefinitionConflicts() throws Exception {        String v1 = """
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
}
