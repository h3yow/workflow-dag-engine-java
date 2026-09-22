package com.github.h3yow.workflowdagenginejava.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 内置步骤类型：
 * <ul>
 *   <li>noop：空操作，立即成功；</li>
 *   <li>sleep：休眠 param 指定毫秒（默认 100ms），用于并发/超时验证，响应中断；</li>
 *   <li>fail：直接抛出异常，用于失败传播验证；</li>
 *   <li>echo：打印一行包含工作流/步骤标识的日志。</li>
 * </ul>
 */
public final class BuiltInHandlers {

    private static final Logger log = LoggerFactory.getLogger(BuiltInHandlers.class);

    private BuiltInHandlers() {
    }

    public static Map<String, StepHandler> defaults() {
        Map<String, StepHandler> handlers = new LinkedHashMap<>();
        handlers.put("noop", (workflowId, stepId, param) -> {
            // 立即成功
        });
        handlers.put("echo", (workflowId, stepId, param) ->
                log.info("[workflowId={}] stepId={} echo={}", workflowId, stepId,
                        param == null ? "" : param));
        handlers.put("sleep", (workflowId, stepId, param) -> {
            long ms = parsePositive(param, 100L);
            log.info("[workflowId={}] stepId={} sleep {}ms 开始", workflowId, stepId, ms);
            Thread.sleep(ms);
            log.info("[workflowId={}] stepId={} sleep {}ms 结束", workflowId, stepId, ms);
        });
        handlers.put("fail", (workflowId, stepId, param) -> {
            String msg = param == null || param.isBlank()
                    ? "步骤 " + stepId + " 被 fail 处理器显式置失败"
                    : param;
            throw new IllegalStateException(msg);
        });
        return Map.copyOf(handlers);
    }

    private static long parsePositive(String param, long fallback) {
        if (param == null || param.isBlank()) {
            return fallback;
        }
        try {
            long v = Long.parseLong(param.trim());
            return v > 0 ? v : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
