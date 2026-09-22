package com.github.h3yow.workflowdagenginejava.web;

import com.github.h3yow.workflowdagenginejava.engine.DuplicateWorkflowException;
import com.github.h3yow.workflowdagenginejava.validation.InvalidWorkflowException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 把领域异常映射为可解释的 HTTP 响应：
 * 定义错误 → 400 + RejectReason；幂等键冲突 → 409。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InvalidWorkflowException.class)
    public ResponseEntity<RejectResponse> handleInvalid(InvalidWorkflowException ex) {
        RejectResponse body = new RejectResponse(null, ex.getReason().name(), ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    @ExceptionHandler(DuplicateWorkflowException.class)
    public ResponseEntity<RejectResponse> handleDuplicate(DuplicateWorkflowException ex) {
        RejectResponse body = new RejectResponse(null, "DEFINITION_CONFLICT", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }
}
