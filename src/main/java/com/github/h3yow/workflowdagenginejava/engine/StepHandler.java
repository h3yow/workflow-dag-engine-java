package com.github.h3yow.workflowdagenginejava.engine;

/**
 * 步骤处理器：每种 type 对应一个可执行实现。
 */
public interface StepHandler {

    /**
     * 执行一步。
     *
     * @param workflowId 所属工作流标识（用于日志溯源）
     * @param stepId     步骤标识
     * @param param      定义中的入参
     * @throws Exception 步骤内部异常，会被分类为 STEP_EXCEPTION
     */
    void execute(String workflowId, String stepId, String param) throws Exception;
}
