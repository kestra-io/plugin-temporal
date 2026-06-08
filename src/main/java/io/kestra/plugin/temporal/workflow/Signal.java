package io.kestra.plugin.temporal.workflow;

import io.kestra.plugin.temporal.AbstractTemporalTask;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.temporal.client.WorkflowNotFoundException;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a signal to a running Temporal workflow.",
    description = """
        Sends a named signal with optional arguments to an existing workflow execution.
        The task fails with a clear error if the workflow is not found or has already completed.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Send an approval signal to a running workflow.",
            full = true,
            code = """
                id: approve_order
                namespace: company.team

                tasks:
                  - id: signal
                    type: io.kestra.plugin.temporal.workflow.Signal
                    endpoint: "localhost:7233"
                    workflowId: "order-{{ inputs.orderId }}"
                    signalName: "approve"
                    args:
                      - '"approved"'
                """
        )
    }
)
public class Signal extends AbstractTemporalTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Workflow ID.",
        description = "The ID of the workflow execution to signal."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workflowId;

    @Schema(
        title = "Run ID.",
        description = "Optional. When omitted, the signal targets the latest open run of the given workflow ID."
    )
    @PluginProperty(group = "main")
    private Property<String> runId;

    @Schema(
        title = "Signal name.",
        description = "Must match the signal handler name registered on the workflow."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> signalName;

    @Schema(
        title = "Signal arguments.",
        description = "List of JSON-encoded argument strings, one per signal parameter."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> args;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rWorkflowId = runContext.render(workflowId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("workflowId is required"));
        var rRunId = runContext.render(runId).as(String.class).orElse(null);
        var rSignalName = runContext.render(signalName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("signalName is required"));
        var rArgs = runContext.render(args).asList(String.class);

        var logger = runContext.logger();
        logger.info("Sending signal={} to workflowId={} runId={}", rSignalName, rWorkflowId, rRunId != null ? rRunId : "<latest>");

        try (var conn = connect(runContext)) {
            var stub = conn.client().newUntypedWorkflowStub(rWorkflowId, Optional.ofNullable(rRunId), Optional.empty());
            try {
                stub.signal(rSignalName, rArgs.toArray());
            } catch (WorkflowNotFoundException e) {
                throw new IllegalStateException(
                    "Workflow not found: workflowId='" + rWorkflowId + "'"
                        + (rRunId != null ? " runId='" + rRunId + "'" : ""),
                    e
                );
            }
            logger.info("Signal '{}' sent successfully", rSignalName);
        }
        return null;
    }
}
