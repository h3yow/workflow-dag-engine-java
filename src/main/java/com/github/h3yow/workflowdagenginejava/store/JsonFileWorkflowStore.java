package com.github.h3yow.workflowdagenginejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.workflowdagenginejava.config.EngineProperties;
import com.github.h3yow.workflowdagenginejava.model.WorkflowSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * File-system backed {@link WorkflowStore}: each workflow is one JSON file
 * under {@code workflow.data-dir} (default {@code data/workflows}). Writes go
 * through a temp file + atomic move, and every state transition is flushed to
 * disk, so a process restart can reconstruct progress exactly.
 */
@Component
public class JsonFileWorkflowStore implements WorkflowStore {

    private static final Logger log = LoggerFactory.getLogger(JsonFileWorkflowStore.class);
    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9._-]+");
    private static final String SUFFIX = ".json";

    private final Path dataDir;
    private final ObjectMapper mapper;

    public JsonFileWorkflowStore(EngineProperties properties, ObjectMapper mapper) {
        this.dataDir = Path.of(properties.getDataDir());
        this.mapper = mapper;
        try {
            Files.createDirectories(this.dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create data dir " + this.dataDir, e);
        }
    }

    private Path fileFor(String workflowId) {
        String name = SAFE_NAME.matcher(workflowId).matches()
                ? workflowId
                : "wf-" + toHex(workflowId);
        return dataDir.resolve(name + SUFFIX);
    }

    private static String toHex(String s) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public synchronized boolean insertIfAbsent(WorkflowSnapshot snapshot) {
        Path target = fileFor(snapshot.workflowId());
        if (Files.exists(target)) {
            return false;
        }
        try {
            Files.writeString(target, mapper.writeValueAsString(snapshot), StandardCharsets.UTF_8);
            log.info("persisted new workflow {} -> {}", snapshot.workflowId(), target);
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist workflow " + snapshot.workflowId(), e);
        }
    }

    @Override
    public synchronized void save(WorkflowSnapshot snapshot) {
        Path target = fileFor(snapshot.workflowId());
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            Files.writeString(tmp, mapper.writeValueAsString(snapshot), StandardCharsets.UTF_8);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to save workflow " + snapshot.workflowId(), e);
        }
    }

    @Override
    public synchronized Optional<WorkflowSnapshot> find(String workflowId) {
        Path target = fileFor(workflowId);
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(Files.readString(target, StandardCharsets.UTF_8),
                    WorkflowSnapshot.class));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read workflow " + workflowId, e);
        }
    }

    @Override
    public synchronized List<WorkflowSnapshot> findAll() {
        if (!Files.isDirectory(dataDir)) {
            return List.of();
        }
        List<WorkflowSnapshot> out = new ArrayList<>();
        try (Stream<Path> files = Files.list(dataDir)) {
            for (Path f : files.filter(p -> p.getFileName().toString().endsWith(SUFFIX)).sorted().toList()) {
                try {
                    out.add(mapper.readValue(Files.readString(f, StandardCharsets.UTF_8),
                            WorkflowSnapshot.class));
                } catch (IOException e) {
                    log.warn("skipping unreadable workflow file {}", f, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list workflows in " + dataDir, e);
        }
        return out;
    }
}
