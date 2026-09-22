package com.github.h3yow.workflowdagenginejava.web;

/** Stable error body: machine-readable code plus human explanation. */
public record ErrorResponse(String error, String message) {
}
