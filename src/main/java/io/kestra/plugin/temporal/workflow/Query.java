package io.kestra.plugin.temporal.workflow;

import io.kestra.plugin.temporal.AbstractTemporalTask;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
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
    title = "Query a running Temporal workflow",
    description = """
        Sends a named query to a workflow and returns the result as a JSON string.
        The task fails if the workflow is not found or the query handler returns an error.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Query the current status of an order workflow.",
            full = true,
            code = """
                id: query_order_status
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.temporal.workflow.Query
                    endpoint: "localhost:7233"
                    workflowId: "order-{{ inputs.orderId }}"
                    queryType: "getStatus"
                """
        )
    }
)
public class Query extends AbstractTemporalTask implements RunnableTask<Query.Output> {

    @Schema(
        title = "Workflow ID",
        description = "The ID of the workflow execution to query."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workflowId;

    @Schema(
        title = "Run ID",
        description = "Optional. When omitted, targets the latest open run of the given workflow ID."
    )
    @PluginProperty(group = "main")
    private Property<String> runId;

    @Schema(
        title = "Query type name",
        description = "Must match a registered `@QueryMethod` on the workflow."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> queryType;

    @Schema(
        title = "Query arguments",
        description = "List of JSON-encoded argument strings, one per query parameter."
    )
    @PluginProperty(group = "main")
    private Property<List<String>> args;

    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWorkflowId = runContext.render(workflowId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("workflowId is required"));
        var rRunId = runContext.render(runId).as(String.class).orElse(null);
        var rQueryType = runContext.render(queryType).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("queryType is required"));
        var rArgs = runContext.render(args).asList(String.class);

        var logger = runContext.logger();
        logger.info("Querying workflowId={} queryType={}", rWorkflowId, rQueryType);

        try (var conn = connect(runContext)) {
            var client = conn.client();
            var stub = client.newUntypedWorkflowStub(rWorkflowId, Optional.ofNullable(rRunId), Optional.empty());
            // Query returns the result deserialized as Object (Map/List/primitive via Jackson).
            // We re-serialize it to a canonical JSON string so the output is always valid JSON.
            Object rawResult;
            try {
                rawResult = stub.query(rQueryType, Object.class, rArgs.toArray());
            } catch (WorkflowNotFoundException e) {
                throw new IllegalStateException(
                    "Workflow not found: workflowId='" + rWorkflowId + "'",
                    e
                );
            }

            String resultJson;
            try {
                resultJson = rawResult == null ? "null" : MAPPER.writeValueAsString(rawResult);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Query result for queryType='" + rQueryType + "' is not JSON-serializable: " + e.getMessage(),
                    e
                );
            }

            logger.info("Query '{}' returned result", rQueryType);
            return Output.builder()
                .result(resultJson)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Query result",
            description = "JSON-encoded result returned by the workflow query handler."
        )
        private final String result;
    }
}
