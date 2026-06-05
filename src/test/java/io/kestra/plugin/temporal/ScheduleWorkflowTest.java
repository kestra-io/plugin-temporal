package io.kestra.plugin.temporal;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for ScheduleWorkflow.
 *
 * Temporal's in-process TestWorkflowEnvironment does not support the Schedules
 * API (requires a real Temporal server with the scheduler service). These tests
 * are guarded by the system property `temporal.integration.enabled=true` so
 * they are skipped in CI unless a real server is available.
 *
 * To run locally:
 *   docker compose -f docker-compose-ci.yml up -d
 *   ./gradlew test -Dtemporal.integration.enabled=true
 */
@KestraTest
@EnabledIfSystemProperty(named = "temporal.integration.enabled", matches = "true")
class ScheduleWorkflowTest {

    private static final String SERVER = System.getProperty("temporal.server", "localhost:7233");

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void createSchedule_happyPath() throws Exception {
        var scheduleId = "test-schedule-" + UUID.randomUUID();

        var task = ScheduleWorkflow.builder()
            .id("schedule-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.ofValue(SERVER))
            .scheduleId(Property.ofValue(scheduleId))
            .cron(Property.ofValue("0 9 * * *"))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("integration-queue"))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getScheduleId(), is(scheduleId));
    }

    @Test
    void duplicateSchedule_withoutOverwrite_throws() throws Exception {
        var scheduleId = "dup-schedule-" + UUID.randomUUID();

        var task = ScheduleWorkflow.builder()
            .id("schedule-first-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.ofValue(SERVER))
            .scheduleId(Property.ofValue(scheduleId))
            .cron(Property.ofValue("0 9 * * *"))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("integration-queue"))
            .build();

        task.run(runContextFactory.of());

        var task2 = ScheduleWorkflow.builder()
            .id("schedule-second-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.ofValue(SERVER))
            .scheduleId(Property.ofValue(scheduleId))
            .cron(Property.ofValue("0 10 * * *"))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("integration-queue"))
            .overwrite(Property.ofValue(false))
            .build();

        var ex = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalStateException.class,
            () -> task2.run(runContextFactory.of())
        );
        assertThat(ex.getMessage(), containsString(scheduleId));
    }

    @Test
    void duplicateSchedule_withOverwrite_updatesSchedule() throws Exception {
        var scheduleId = "overwrite-schedule-" + UUID.randomUUID();

        var task = ScheduleWorkflow.builder()
            .id("schedule-create-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.ofValue(SERVER))
            .scheduleId(Property.ofValue(scheduleId))
            .cron(Property.ofValue("0 9 * * *"))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("integration-queue"))
            .build();

        task.run(runContextFactory.of());

        var task2 = ScheduleWorkflow.builder()
            .id("schedule-update-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.ofValue(SERVER))
            .scheduleId(Property.ofValue(scheduleId))
            .cron(Property.ofValue("0 10 * * *"))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("integration-queue"))
            .overwrite(Property.ofValue(true))
            .build();

        var output = task2.run(runContextFactory.of());

        assertThat(output.getScheduleId(), is(scheduleId));
    }

    @Test
    void missingCronAndInterval_throwsIllegalArgument() {
        var task = ScheduleWorkflow.builder()
            .id("schedule-bad-" + UUID.randomUUID())
            .type(ScheduleWorkflow.class.getName())
            .endpoint(Property.ofValue(SERVER))
            .scheduleId(Property.ofValue("bad-" + UUID.randomUUID()))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("integration-queue"))
            .build();

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContextFactory.of())
        );
    }
}
