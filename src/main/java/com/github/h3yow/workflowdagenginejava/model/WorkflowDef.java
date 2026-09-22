package com.github.h3yow.workflowdagenginejava.model;

import java.util.List;

/**
 * 工作流定义：客户端提供的幂等键 workflowId 与步骤集合。
 */
public record WorkflowDef(String workflowId, List<StepDef> steps) {
}
