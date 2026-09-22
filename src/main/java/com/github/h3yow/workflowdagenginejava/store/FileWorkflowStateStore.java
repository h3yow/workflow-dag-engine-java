package com.github.h3yow.workflowdagenginejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 基于本地 JSON 文件的存储实现：每个工作流一个 {@code <workflowId>.json} 文件。
 * 先写临时文件再原子 rename，保证任意时刻落盘状态都是完整 JSON，重启后可恢复。
 */
public class FileWorkflowStateStore implements WorkflowStateStore {

    private static final Logger log = LoggerFactory.getLogger(FileWorkflowStateStore.class);

    private final Path dataDir;
    private final ObjectMapper mapper;

    public FileWorkflowStateStore(String dataDir) {
        this.dataDir = Path.of(dataDir);
        this.mapper = new ObjectMapper();
        try {
            Files.createDirectories(this.dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建数据目录: " + this.dataDir, e);
        }
    }

    @Override
    public synchronized void save(WorkflowExecution execution) {
        String fileName = sanitize(execution.getWorkflowId()) + ".json";
        Path target = dataDir.resolve(fileName);
        Path tmp = dataDir.resolve(fileName + ".tmp");
        try {
            String json = mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(execution);
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化工作流状态失败: " + execution.getWorkflowId(), e);
        }
    }

    @Override
    public synchronized Optional<WorkflowExecution> find(String workflowId) {
        Path target = dataDir.resolve(sanitize(workflowId) + ".json");
        if (!Files.isRegularFile(target)) {
            return Optional.empty();
        }
        return Optional.of(read(target));
    }

    @Override
    public synchronized List<WorkflowExecution> findAll() {
        List<WorkflowExecution> all = new ArrayList<>();
        try (Stream<Path> files = Files.list(dataDir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .forEach(p -> {
                        try {
                            all.add(read(p));
                        } catch (UncheckedIOException e) {
                            log.warn("跳过无法读取的状态文件: {}", p, e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("读取数据目录失败: " + dataDir, e);
        }
        return all;
    }

    private WorkflowExecution read(Path file) {
        try {
            return mapper.readValue(file.toFile(), WorkflowExecution.class);
        } catch (IOException e) {
            throw new UncheckedIOException("读取状态文件失败: " + file, e);
        }
    }

    /** 防止 workflowId 中出现路径分隔符等危险字符。 */
    private static String sanitize(String workflowId) {
        if (workflowId == null || workflowId.isBlank()) {
            throw new IllegalArgumentException("workflowId 不能为空");
        }
        return workflowId.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
