package com.github.h3yow.workflowdagenginejava.config;

import com.github.h3yow.workflowdagenginejava.engine.BuiltInHandlers;
import com.github.h3yow.workflowdagenginejava.engine.StepHandlerRegistry;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.store.FileWorkflowStateStore;
import com.github.h3yow.workflowdagenginejava.store.WorkflowStateStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 引擎 Bean 装配：文件存储 + 内置处理器 + 调度核心。
 */
@Configuration
@EnableConfigurationProperties(WorkflowProperties.class)
public class WorkflowConfig {

    @Bean
    public WorkflowStateStore workflowStateStore(WorkflowProperties props) {
        return new FileWorkflowStateStore(props.getDataDir());
    }

    @Bean
    public StepHandlerRegistry stepHandlerRegistry() {
        return new StepHandlerRegistry(BuiltInHandlers.defaults());
    }

    @Bean
    public WorkflowEngine workflowEngine(WorkflowStateStore store, StepHandlerRegistry registry,
                                         WorkflowProperties props) {
        return new WorkflowEngine(store, registry, props.getMaxConcurrency(),
                props.getDefaultTimeoutMs());
    }
}
