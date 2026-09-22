package com.github.h3yow.workflowdagenginejava.tests.engine;

import com.github.h3yow.workflowdagenginejava.model.RejectReason;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.validation.WorkflowValidationException;
import com.github.h3yow.workflowdagenginejava.validation.WorkflowValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers dependency validation: each rejection must carry a distinguishable
 * {@link RejectReason}, and valid DAGs are canonicalized independent of the
 * submission order of their steps.
 */
class WorkflowValidationTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowValidationTest.class);

    private final WorkflowValidator validator = new WorkflowValidator();

    private static StepDef step(String id, String... deps) {
        return new StepDef(id, "noop", List.of(deps), 0L);
    }

    @Test
    @DisplayName("self dependency is rejected with SELF_DEPENDENCY")
    void rejectsSelfDependency() {
        WorkflowDef def = new WorkflowDef("wf-self",
                List.of(step("a", "a")), 1);
        WorkflowValidationException ex = assertThrows(WorkflowValidationException.class,
                () -> validator.validate(def));
        assertEquals(RejectReason.SELF_DEPENDENCY, ex.reason());
        log.info("workflow=wf-self deps=[a->a] result=REJECTED reason={} message={}",
                ex.reason().code(), ex.getMessage());
    }

    @Test
    @DisplayName("missing dependency is rejected with MISSING_DEPENDENCY")
    void rejectsMissingDependency() {
        WorkflowDef def = new WorkflowDef("wf-missing",
                List.of(step("a"), step("b", "a"), step("c", "ghost")), 2);
        WorkflowValidationException ex = assertThrows(WorkflowValidationException.class,
                () -> validator.validate(def));
        assertEquals(RejectReason.MISSING_DEPENDENCY, ex.reason());
        log.info("workflow=wf-missing deps=[b->a,c->ghost] result=REJECTED reason={} message={}",
                ex.reason().code(), ex.getMessage());
    }

    @Test
    @DisplayName("cyclic dependency is rejected with CYCLIC_DEPENDENCY")
    void rejectsCycle() {
        // a -> b -> c -> a
        WorkflowDef def = new WorkflowDef("wf-cycle",
                List.of(step("a", "c"), step("b", "a"), step("c", "b")), 2);
        WorkflowValidationException ex = assertThrows(WorkflowValidationException.class,
                () -> validator.validate(def));
        assertEquals(RejectReason.CYCLIC_DEPENDENCY, ex.reason());
        log.info("workflow=wf-cycle deps=[a->c,b->a,c->b] result=REJECTED reason={} message={}",
                ex.reason().code(), ex.getMessage());
    }

    @Test
    @DisplayName("duplicate step id is rejected with DUPLICATE_STEP_ID")
    void rejectsDuplicateStepId() {
        WorkflowDef def = new WorkflowDef("wf-dup-step",
                List.of(step("a"), step("a")), 1);
        WorkflowValidationException ex = assertThrows(WorkflowValidationException.class,
                () -> validator.validate(def));
        assertEquals(RejectReason.DUPLICATE_STEP_ID, ex.reason());
        log.info("workflow=wf-dup-step deps=[] result=REJECTED reason={} message={}",
                ex.reason().code(), ex.getMessage());
    }

    @Test
    @DisplayName("empty definition and blank workflow id are distinguishable")
    void rejectsEmptyDefs() {
        WorkflowValidationException empty = assertThrows(WorkflowValidationException.class,
                () -> validator.validate(new WorkflowDef("x", List.of(), 1)));
        assertEquals(RejectReason.EMPTY_DEFINITION, empty.reason());

        WorkflowValidationException blank = assertThrows(WorkflowValidationException.class,
                () -> validator.validate(new WorkflowDef("  ", List.of(step("a")), 1)));
        assertEquals(RejectReason.EMPTY_WORKFLOW_ID, blank.reason());
        log.info("workflow=<empty/blank> result=REJECTED reasons={}/{}",
                empty.reason().code(), blank.reason().code());
    }

    @Test
    @DisplayName("execution order is independent of submission order")
    void canonicalizesIndependentOfSubmissionOrder() {
        List<StepDef> orderOne = List.of(step("a", "c"), step("b", "a"), step("c"));
        List<StepDef> orderTwo = List.of(step("c"), step("b", "a"), step("a", "c"));

        var v1 = validator.validate(new WorkflowDef("wf-order", orderOne, 2));
        var v2 = validator.validate(new WorkflowDef("wf-order", orderTwo, 2));

        List<String> ids1 = v1.def().steps().stream().map(StepDef::id).toList();
        List<String> ids2 = v2.def().steps().stream().map(StepDef::id).toList();
        assertEquals(ids1, ids2);
        assertEquals(List.of("c", "a", "b"), ids1);
        assertTrue(ids1.indexOf("c") < ids1.indexOf("a"));
        assertTrue(ids1.indexOf("a") < ids1.indexOf("b"));
        log.info("workflow=wf-order deps=[a->c,b->a] canonicalOrder={} finalStatus(expectation)=TOPOLOGICAL",
                ids1);
    }
}
