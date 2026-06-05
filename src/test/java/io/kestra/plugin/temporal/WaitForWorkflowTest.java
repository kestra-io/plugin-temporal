package io.kestra.plugin.temporal;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class WaitForWorkflowTest {

    private static final String TASK_QUEUE = "test-wait-" + UUID.randomUUID();

    @Inject
    RunContextFactory runContextFactory;

    private TemporalTestServer temporalServer;

    @BeforeEach
    void setUp() throws Exception {
        temporalServer = TemporalTestServer.start();
        var worker = temporalServer.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(
            TestWorkflows.GreetingWorkflowImpl.class,
            TestWorkflows.NoResultWorkflowImpl.class
        );
        temporalServer.startWorkers();
    }

    @AfterEach
    void tearDown() {
        temporalServer.close();
    }

    @Test
    void happyPath_completedWorkflow_returnsResultAndStatus() throws Exception {
        var client = temporalServer.getClient();
        var workflowId = "wait-greet-" + UUID.randomUUID();
        var stub = client.newWorkflowStub(
            TestWorkflows.GreetingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build()
        );
        // Wait synchronously for the workflow to complete before polling.
        var result = stub.greet("Kestra");
        assertThat(result, notNullValue());

        var task = WaitForWorkflow.builder()
            .id("wait-" + UUID.randomUUID())
            .type(WaitForWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowId(Property.ofValue(workflowId))
            .pollInterval(Property.ofValue(Duration.ofMillis(200)))
            .timeout(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getStatus(), is("COMPLETED"));
        assertThat(output.getResult(), notNullValue());
    }

    @Test
    void voidWorkflow_completesSuccessfully() throws Exception {
        var client = temporalServer.getClient();
        var workflowId = "wait-noret-" + UUID.randomUUID();
        var stub = client.newWorkflowStub(
            TestWorkflows.NoResultWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build()
        );
        stub.run();

        var task = WaitForWorkflow.builder()
            .id("wait-noret-" + UUID.randomUUID())
            .type(WaitForWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowId(Property.ofValue(workflowId))
            .pollInterval(Property.ofValue(Duration.ofMillis(200)))
            .timeout(Property.ofValue(Duration.ofSeconds(30)))
            .failOnNonCompleted(Property.ofValue(false))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getStatus(), notNullValue());
    }

    @Test
    void nonExistentWorkflow_throws() {
        var task = WaitForWorkflow.builder()
            .id("wait-missing-" + UUID.randomUUID())
            .type(WaitForWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowId(Property.ofValue("no-such-workflow-" + UUID.randomUUID()))
            .pollInterval(Property.ofValue(Duration.ofMillis(100)))
            .timeout(Property.ofValue(Duration.ofMillis(300)))
            .failOnNonCompleted(Property.ofValue(true))
            .build();

        // gRPC NOT_FOUND or timeout exceeded.
        Assertions.assertThrows(Exception.class, () -> task.run(runContextFactory.of()));
    }
}
