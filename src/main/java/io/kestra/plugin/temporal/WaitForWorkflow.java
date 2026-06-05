package io.kestra.plugin.temporal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for a Temporal workflow to reach a terminal state.",
    description = """
        Polls `DescribeWorkflowExecution` at a configurable interval until the workflow
        completes, fails, is canceled, terminated, or times out.

        On COMPLETED, retrieves and returns the workflow result as JSON.
        On other terminal states, fails the task by default; set `failOnNonCompleted: false`
        to emit the status instead.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger an order workflow and wait for it to finish.",
            full = true,
            code = """
                id: wait_order_workflow
                namespace: company.team

                tasks:
                  - id: trigger
                    type: io.kestra.plugin.temporal.TriggerWorkflow
                    endpoint: "localhost:7233"
                    workflowType: "OrderWorkflow"
                    taskQueue: "order-queue"
                    workflowId: "order-{{ execution.id }}"

                  - id: wait
                    type: io.kestra.plugin.temporal.WaitForWorkflow
                    endpoint: "localhost:7233"
                    workflowId: "{{ outputs.trigger.workflowId }}"
                    runId: "{{ outputs.trigger.runId }}"
                    pollInterval: "PT5S"
                    waitTimeout: "PT1H"
                """
        )
    }
)
public class WaitForWorkflow extends AbstractTemporalTask implements RunnableTask<WaitForWorkflow.Output> {

    private static final Set<WorkflowExecutionStatus> TERMINAL_STATUSES = Set.of(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
    );

    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    @Schema(
        title = "Workflow ID.",
        description = "The ID of the workflow execution to wait on."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workflowId;

    @Schema(
        title = "Run ID.",
        description = "Optional. When omitted, waits on the latest run of the given workflow ID."
    )
    @PluginProperty(group = "main")
    private Property<String> runId;

    @Schema(
        title = "Poll interval.",
        description = "How often to check the workflow status. Defaults to 5 seconds."
    )
    @PluginProperty(group = "processing")
    private Property<Duration> pollInterval;

    @Schema(
        title = "Maximum wait time.",
        description = """
            Total time to wait before giving up. Defaults to 1 hour.
            Named waitTimeout because timeout is a reserved Kestra task property
            that kills the task run instead of returning an output.
            """
    )
    @PluginProperty(group = "reliability")
    private Property<Duration> waitTimeout;

    @Schema(
        title = "Fail if the workflow ends in a non-COMPLETED state.",
        description = """
            When true (default), the task throws an exception for FAILED, CANCELED,
            TERMINATED, or TIMED_OUT terminal states.
            When false, the task succeeds and reports the status in the `status` output.
            """
    )
    @PluginProperty(group = "reliability")
    @Builder.Default
    private Property<Boolean> failOnNonCompleted = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWorkflowId       = runContext.render(workflowId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("workflowId is required"));
        var rRunId            = runContext.render(runId).as(String.class).orElse(null);
        var rPollInterval     = runContext.render(pollInterval).as(Duration.class).orElse(Duration.ofSeconds(5));
        var rTimeout          = runContext.render(waitTimeout).as(Duration.class).orElse(Duration.ofHours(1));
        var rFailOnNonCompleted = runContext.render(failOnNonCompleted).as(Boolean.class).orElse(true);

        var logger = runContext.logger();
        logger.info("Waiting for workflowId={} runId={} timeout={}",
            rWorkflowId, rRunId != null ? rRunId : "<latest>", rTimeout);

        var client = buildClient(runContext);
        try {
            var serviceStubs = client.getWorkflowServiceStubs();
            var namespace    = client.getOptions().getNamespace();
            var deadline     = Instant.now().plus(rTimeout);

            var execBuilder = WorkflowExecution.newBuilder().setWorkflowId(rWorkflowId);
            if (rRunId != null) {
                execBuilder.setRunId(rRunId);
            }

            WorkflowExecutionStatus finalStatus = null;
            while (true) {
                var request = DescribeWorkflowExecutionRequest.newBuilder()
                    .setNamespace(namespace)
                    .setExecution(execBuilder.build())
                    .build();

                var response = serviceStubs.blockingStub().describeWorkflowExecution(request);
                var status = response.getWorkflowExecutionInfo().getStatus();
                logger.debug("workflowId={} status={}", rWorkflowId, status);

                if (TERMINAL_STATUSES.contains(status)) {
                    finalStatus = status;
                    break;
                }

                if (Instant.now().isAfter(deadline)) {
                    if (rFailOnNonCompleted) {
                        throw new IllegalStateException(
                            "Timed out waiting for workflowId='" + rWorkflowId + "' after " + rTimeout
                        );
                    }
                    return Output.builder().status("TIMED_OUT").result(null).build();
                }

                Thread.sleep(rPollInterval.toMillis());
            }

            var statusName = terminalStatusName(finalStatus);

            if (finalStatus != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED) {
                if (rFailOnNonCompleted) {
                    throw new IllegalStateException(
                        "Workflow ended with status=" + statusName + " (workflowId='" + rWorkflowId + "')"
                    );
                }
                return Output.builder().status(statusName).result(null).build();
            }

            // Workflow completed: fetch the result via a non-blocking getResult with a short timeout.
            var stub = client.newUntypedWorkflowStub(rWorkflowId, Optional.ofNullable(rRunId), Optional.empty());
            String resultJson;
            try {
                var rawResult = stub.getResult(1, java.util.concurrent.TimeUnit.SECONDS, Object.class);
                resultJson = rawResult == null ? "null" : MAPPER.writeValueAsString(rawResult);
            } catch (java.util.concurrent.TimeoutException e) {
                // Already confirmed COMPLETED via DescribeWorkflowExecution,
                // so a short getResult timeout here is unexpected but non-fatal.
                resultJson = "null";
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Workflow completed but result is not JSON-serializable: " + e.getMessage(),
                    e
                );
            }

            logger.info("workflowId={} completed", rWorkflowId);
            return Output.builder().status("COMPLETED").result(resultJson).build();
        } finally {
            closeClient(client);
        }
    }

    private static String terminalStatusName(WorkflowExecutionStatus status) {
        return switch (status) {
            case WORKFLOW_EXECUTION_STATUS_COMPLETED   -> "COMPLETED";
            case WORKFLOW_EXECUTION_STATUS_FAILED      -> "FAILED";
            case WORKFLOW_EXECUTION_STATUS_CANCELED    -> "CANCELED";
            case WORKFLOW_EXECUTION_STATUS_TERMINATED  -> "TERMINATED";
            case WORKFLOW_EXECUTION_STATUS_TIMED_OUT   -> "TIMED_OUT";
            default -> status.name();
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Workflow terminal status.",
            description = "One of: COMPLETED, FAILED, CANCELED, TERMINATED, TIMED_OUT."
        )
        private final String status;

        @Schema(
            title = "Workflow result.",
            description = "JSON-encoded return value of the workflow. Null for non-COMPLETED states."
        )
        private final String result;
    }
}
