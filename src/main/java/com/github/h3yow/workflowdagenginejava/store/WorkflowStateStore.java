package com.github.h3yow.workflowdagenginejava.store;

import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;

import java.util.List;
import java.util.Optional;

/**
 * 工作流执行状态的持久化存储。
 */
public interface WorkflowStateStore {

    /** 保存（创建或覆盖）一次执行状态。 */
    void save(WorkflowExecution execution);

    /** 按工作流标识读取。 */
    Optional<WorkflowExecution> find(String workflowId);

    /** 列出全部已持久化的执行（主要用于启动恢复）。 */
    List<WorkflowExecution> findAll();
}
