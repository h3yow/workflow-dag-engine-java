package com.github.h3yow.workflowdagenginejava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Wires engine configuration properties. */
@Configuration
@EnableConfigurationProperties(EngineProperties.class)
public class EngineConfig {
}
