package com.github.h3yow.workflowdagenginejava.web;

import com.github.h3yow.workflowdagenginejava.engine.DuplicateWorkflowException;
import com.github.h3yow.workflowdagenginejava.store.WorkflowNotFoundException;
import com.github.h3yow.workflowdagenginejava.validation.WorkflowValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps domain failures to distinguishable HTTP responses:
 * <ul>
 *   <li>422 + stable reject code - invalid workflow definition;</li>
 *   <li>409 DUPLICATE_WORKFLOW - id exists with a different definition;</li>
 *   <li>404 WORKFLOW_NOT_FOUND - unknown workflow id;</li>
 *   <li>400 MALFORMED_REQUEST - unreadable JSON body.</li>
 * </ul>
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(WorkflowValidationException.class)
    public ResponseEntity<ErrorResponse> handleValidation(WorkflowValidationException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new ErrorResponse(ex.reason().code(), ex.getMessage()));
    }

    @ExceptionHandler(DuplicateWorkflowException.class)
    public ResponseEntity<ErrorResponse> handleDuplicate(DuplicateWorkflowException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("DUPLICATE_WORKFLOW", ex.getMessage()));
    }

    @ExceptionHandler(WorkflowNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(WorkflowNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("WORKFLOW_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse("MALFORMED_REQUEST", "request body is not valid JSON"));
    }
}
