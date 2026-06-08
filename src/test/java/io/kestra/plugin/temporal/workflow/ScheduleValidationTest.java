package io.kestra.plugin.temporal.workflow;

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
 * Input-validation tests for Schedule that run without a real Temporal server.
 * They fail before making any network call, so no server setup is needed.
 */
@KestraTest
class ScheduleValidationTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void missingCronAndInterval_throwsIllegalArgument() {
        var task = Schedule.builder()
            .id("sched-val-" + UUID.randomUUID())
            .type(Schedule.class.getName())
            .endpoint(Property.ofValue("localhost:7233"))
            .scheduleId(Property.ofValue("bad-" + UUID.randomUUID()))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("test-queue"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }

    @Test
    void bothCronAndInterval_throwsIllegalArgument() {
        var task = Schedule.builder()
            .id("sched-val2-" + UUID.randomUUID())
            .type(Schedule.class.getName())
            .endpoint(Property.ofValue("localhost:7233"))
            .scheduleId(Property.ofValue("both-" + UUID.randomUUID()))
            .cron(Property.ofValue("0 9 * * *"))
            .intervalSeconds(Property.ofValue(300L))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("test-queue"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }
}
