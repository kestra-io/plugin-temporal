package io.kestra.plugin.temporal.workflow;

import io.kestra.plugin.temporal.AbstractTemporalTask;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a Temporal workflow execution.",
    description = """
        Starts a new Temporal workflow and returns its workflow ID and run ID.
        If a workflow with the given ID is already running, the task fails with a
        clear error message.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger an order-processing workflow on a local Temporal server.",
            full = true,
            code = """
                id: trigger_order_workflow
                namespace: company.team

                tasks:
                  - id: start
                    type: io.kestra.plugin.temporal.workflow.Trigger
                    endpoint: "localhost:7233"
                    namespace: "default"
                    workflowType: "OrderWorkflow"
                    taskQueue: "order-queue"
                    workflowId: "order-{{ execution.id }}"
                    input:
                      - '{"orderId": "ORD-123", "amount": 99.99}'
                    executionTimeout: "PT1H"
                """
        ),
        @Example(
            title = "Trigger a workflow on Temporal Cloud using an API key.",
            full = true,
            code = """
                id: trigger_cloud_workflow
                namespace: company.team

                tasks:
                  - id: start
                    type: io.kestra.plugin.temporal.workflow.Trigger
                    endpoint: "myns.tmprl.cloud:7233"
                    namespace: "myns.accountId"
                    apiKey: "{{ secret('TEMPORAL_API_KEY') }}"
                    workflowType: "OrderWorkflow"
                    taskQueue: "order-queue"
                """
        )
    }
)
public class Trigger extends AbstractTemporalTask implements RunnableTask<Trigger.Output> {

    @Schema(
        title = "Registered workflow type name.",
        description = "Must match the `@WorkflowMethod` implementation registered on the worker."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workflowType;

    @Schema(
        title = "Task queue name.",
        description = "The worker polling this queue will pick up the workflow."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> taskQueue;

    @Schema(
        title = "Workflow ID.",
        description = "Optional. A UUID is generated when absent."
    )
    @PluginProperty(group = "main")
    private Property<String> workflowId;

    @Schema(
        title = "Workflow input arguments.",
        description = """
            List of JSON-encoded argument strings, one per workflow parameter.
            Example: `["{\"key\": \"value\"}", "42"]`
            """
    )
    @PluginProperty(group = "main")
    private Property<List<String>> input;

    @Schema(
        title = "Maximum time allowed for the workflow to complete.",
        description = "ISO-8601 duration, e.g. `PT1H` for one hour."
    )
    @PluginProperty(group = "main")
    private Property<Duration> executionTimeout;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWorkflowType = runContext.render(workflowType).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("workflowType is required"));
        var rTaskQueue = runContext.render(taskQueue).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("taskQueue is required"));
        var rWorkflowId = runContext.render(workflowId).as(String.class)
            .orElseGet(() -> UUID.randomUUID().toString());
        var rInput = runContext.render(input).asList(String.class);
        var rTimeout = runContext.render(executionTimeout).as(Duration.class).orElse(null);

        var logger = runContext.logger();
        logger.info("Starting workflow type={} taskQueue={} workflowId={}", rWorkflowType, rTaskQueue, rWorkflowId);

        var optionsBuilder = WorkflowOptions.newBuilder()
            .setWorkflowId(rWorkflowId)
            .setTaskQueue(rTaskQueue);
        if (rTimeout != null) {
            optionsBuilder.setWorkflowExecutionTimeout(rTimeout);
        }

        try (var conn = connect(runContext)) {
            var stub = conn.client().newUntypedWorkflowStub(rWorkflowType, optionsBuilder.build());
            io.temporal.api.common.v1.WorkflowExecution execution;
            try {
                execution = stub.start(rInput.toArray());
            } catch (WorkflowExecutionAlreadyStarted e) {
                throw new IllegalStateException(
                    "Workflow with ID '" + rWorkflowId + "' is already running (runId=" + e.getExecution().getRunId() + ")",
                    e
                );
            }

            logger.info("Workflow started workflowId={} runId={}", execution.getWorkflowId(), execution.getRunId());
            return Output.builder()
                .workflowId(execution.getWorkflowId())
                .runId(execution.getRunId())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Workflow ID.", description = "The ID of the started workflow execution.")
        private final String workflowId;

        @Schema(title = "Run ID.", description = "The unique run ID assigned by Temporal to this execution.")
        private final String runId;
    }
}
