package io.kestra.plugin.temporal.workflow;

import io.kestra.plugin.temporal.AbstractTemporalTask;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.schedules.*;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create or update a Temporal schedule.",
    description = """
        Creates a new Temporal schedule that triggers a workflow on a cron expression or
        fixed interval. If the schedule already exists, the task fails unless `overwrite`
        is set to `true`, in which case the existing schedule is updated to the new spec.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Schedule a report workflow to run daily at 9 AM UTC.",
            full = true,
            code = """
                id: schedule_daily_report
                namespace: company.team

                tasks:
                  - id: schedule
                    type: io.kestra.plugin.temporal.workflow.Schedule
                    endpoint: "localhost:7233"
                    scheduleId: "daily-report"
                    cron: "0 9 * * *"
                    workflowType: "ReportWorkflow"
                    taskQueue: "report-queue"
                    args:
                      - '"daily"'
                """
        ),
        @Example(
            title = "Schedule a workflow on a fixed interval, updating if it already exists.",
            full = true,
            code = """
                id: schedule_heartbeat
                namespace: company.team

                tasks:
                  - id: schedule
                    type: io.kestra.plugin.temporal.workflow.Schedule
                    endpoint: "localhost:7233"
                    scheduleId: "heartbeat-check"
                    intervalSeconds: 300
                    workflowType: "HeartbeatWorkflow"
                    taskQueue: "health-queue"
                    overwrite: true
                """
        )
    }
)
public class Schedule extends AbstractTemporalTask implements RunnableTask<Schedule.Output> {

    @Schema(
        title = "Schedule ID.",
        description = "Unique identifier for the schedule within the namespace."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> scheduleId;

    @Schema(
        title = "Cron expression.",
        description = "Standard 5-field cron, e.g. `0 9 * * *`. Mutually exclusive with `intervalSeconds`."
    )
    @PluginProperty(group = "main")
    private Property<String> cron;

    @Schema(
        title = "Fixed interval in seconds.",
        description = "Run the workflow every N seconds. Mutually exclusive with `cron`."
    )
    @PluginProperty(group = "main")
    private Property<Long> intervalSeconds;

    @Schema(
        title = "Workflow type name.",
        description = "Registered workflow type to execute on each schedule tick."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workflowType;

    @Schema(
        title = "Task queue name.",
        description = "The worker polling this queue will execute the scheduled workflow."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> taskQueue;

    @Schema(
        title = "Workflow input arguments.",
        description = "List of JSON-encoded argument strings passed to each workflow execution."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> args;

    @Schema(
        title = "Overwrite existing schedule.",
        description = "When true, updates the schedule if it already exists. Defaults to false."
    )
    @PluginProperty(group = "reliability")
    @Builder.Default
    private Property<Boolean> overwrite = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rScheduleId    = runContext.render(scheduleId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("scheduleId is required"));
        var rCron          = runContext.render(cron).as(String.class).orElse(null);
        var rIntervalSecs  = runContext.render(intervalSeconds).as(Long.class).orElse(null);
        var rWorkflowType  = runContext.render(workflowType).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("workflowType is required"));
        var rTaskQueue     = runContext.render(taskQueue).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("taskQueue is required"));
        var rArgs          = runContext.render(args).asList(String.class);
        var rOverwrite     = runContext.render(overwrite).as(Boolean.class).orElse(false);

        if (rCron == null && rIntervalSecs == null) {
            throw new IllegalArgumentException("Either cron or intervalSeconds must be provided");
        }
        if (rCron != null && rIntervalSecs != null) {
            throw new IllegalArgumentException("cron and intervalSeconds are mutually exclusive");
        }

        var logger = runContext.logger();
        logger.info("Configuring schedule scheduleId={} workflowType={}", rScheduleId, rWorkflowType);

        try (var conn = connect(runContext)) {
            var client = conn.client();
            var scheduleClientOptions = ScheduleClientOptions.newBuilder()
                .setNamespace(client.getOptions().getNamespace())
                .build();
            var scheduleClient = ScheduleClient.newInstance(
                client.getWorkflowServiceStubs(), scheduleClientOptions
            );

            var spec    = buildSpec(rCron, rIntervalSecs);
            var action  = ScheduleActionStartWorkflow.newBuilder()
                .setWorkflowType(rWorkflowType)
                .setOptions(
                    WorkflowOptions.newBuilder()
                        // base ID for scheduled runs, Temporal appends the fire timestamp per execution
                        .setWorkflowId(rScheduleId + "-workflow")
                        .setTaskQueue(rTaskQueue)
                        .build()
                )
                .setArguments(rArgs.toArray())
                .build();

            var schedule = io.temporal.client.schedules.Schedule.newBuilder()
                .setAction(action)
                .setSpec(spec)
                .build();

            try {
                scheduleClient.createSchedule(rScheduleId, schedule, ScheduleOptions.newBuilder().build());
                logger.info("Schedule created scheduleId={}", rScheduleId);
            } catch (ScheduleAlreadyRunningException e) {
                if (!rOverwrite) {
                    throw new IllegalStateException(
                        "Schedule '" + rScheduleId + "' already exists. Set overwrite: true to update it.",
                        e
                    );
                }
                var handle = scheduleClient.getHandle(rScheduleId);
                handle.update(input -> new ScheduleUpdate(
                    io.temporal.client.schedules.Schedule.newBuilder(input.getDescription().getSchedule())
                        .setSpec(spec)
                        .setAction(action)
                        .build()
                ));
                logger.info("Schedule updated scheduleId={}", rScheduleId);
            }

            return Output.builder().scheduleId(rScheduleId).build();
        }
    }

    private ScheduleSpec buildSpec(String rCron, Long rIntervalSecs) {
        var specBuilder = ScheduleSpec.newBuilder();
        if (rCron != null) {
            specBuilder.setCronExpressions(List.of(rCron));
        } else {
            specBuilder.setIntervals(List.of(new ScheduleIntervalSpec(Duration.ofSeconds(rIntervalSecs))));
        }
        return specBuilder.build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Schedule ID.",
            description = "The ID of the created or updated schedule."
        )
        private final String scheduleId;
    }
}
