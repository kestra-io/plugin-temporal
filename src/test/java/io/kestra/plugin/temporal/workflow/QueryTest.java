package io.kestra.plugin.temporal.workflow;

import io.kestra.plugin.temporal.TemporalTestServer;
import io.kestra.plugin.temporal.TestWorkflows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class QueryTest {

    private static final String TASK_QUEUE = "test-query-" + UUID.randomUUID();

    @Inject
    RunContextFactory runContextFactory;

    private TemporalTestServer temporalServer;

    @BeforeEach
    void setUp() throws Exception {
        temporalServer = TemporalTestServer.start();
        var worker = temporalServer.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(TestWorkflows.LongRunningWorkflowImpl.class);
        temporalServer.startWorkers();
    }

    @AfterEach
    void tearDown() {
        temporalServer.close();
    }

    @Test
    void happyPath_returnsQueryResult() throws Exception {
        var client = temporalServer.getClient();
        var workflowId = "query-target-" + UUID.randomUUID();
        var stub = client.newWorkflowStub(
            TestWorkflows.LongRunningWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build()
        );
        WorkflowClient.start(stub::run);

        var task = Query.builder()
            .id("query-" + UUID.randomUUID())
            .type(Query.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowId(Property.ofValue(workflowId))
            .queryType(Property.ofValue("getStatus"))
            .build();

        var output = task.run(runContextFactory.of());

        // Workflow is waiting for a signal; status is "waiting".
        assertThat(output.getResult(), is("\"waiting\""));
    }

    @Test
    void nonExistentWorkflow_throwsWithClearMessage() {
        var task = Query.builder()
            .id("query-missing-" + UUID.randomUUID())
            .type(Query.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowId(Property.ofValue("non-existent-" + UUID.randomUUID()))
            .queryType(Property.ofValue("getStatus"))
            .build();

        var ex = Assertions.assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));
        assertThat(ex.getMessage(), containsString("not found"));
    }
}
