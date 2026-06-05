package io.kestra.plugin.temporal;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Input-validation tests for ScheduleWorkflow that run without a real Temporal server.
 * They fail before making any network call, so no server setup is needed.
 */
@KestraTest
class ScheduleWorkflowValidationTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void missingCronAndInterval_throwsIllegalArgument() {
        var task = ScheduleWorkflow.builder()
            .id("sched-val-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.of("localhost:7233"))
            .scheduleId(Property.of("bad-" + UUID.randomUUID()))
            .workflowType(Property.of("TestWorkflow"))
            .taskQueue(Property.of("test-queue"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }

    @Test
    void bothCronAndInterval_throwsIllegalArgument() {
        var task = ScheduleWorkflow.builder()
            .id("sched-val2-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.of("localhost:7233"))
            .scheduleId(Property.of("both-" + UUID.randomUUID()))
            .cron(Property.of("0 9 * * *"))
            .intervalSeconds(Property.of(300L))
            .workflowType(Property.of("TestWorkflow"))
            .taskQueue(Property.of("test-queue"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }
}
