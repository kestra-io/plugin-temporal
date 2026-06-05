package io.kestra.plugin.temporal;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@KestraTest
class SignalWorkflowTest {

    private static final String TASK_QUEUE = "test-signal-" + UUID.randomUUID();

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
    void happyPath_sendsSignalToRunningWorkflow() throws Exception {
        var client = temporalServer.getClient();
        var workflowId = "signal-target-" + UUID.randomUUID();
        var stub = client.newWorkflowStub(
            TestWorkflows.LongRunningWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(TASK_QUEUE)
                .build()
        );
        // Fire-and-forget so the workflow stays in RUNNING state while we signal it.
        WorkflowClient.start(stub::run);

        var task = SignalWorkflow.builder()
            .id("signal-" + UUID.randomUUID())
            .type(SignalWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowId(Property.ofValue(workflowId))
            .signalName(Property.ofValue("unlock"))
            .args(Property.ofValue(List.of("\"approved\"")))
            .build();

        // Should not throw.
        task.run(runContextFactory.of());
    }

    @Test
    void nonExistentWorkflow_throwsWithClearMessage() {
        var task = SignalWorkflow.builder()
            .id("signal-missing-" + UUID.randomUUID())
            .type(SignalWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowId(Property.ofValue("non-existent-" + UUID.randomUUID()))
            .signalName(Property.ofValue("unlock"))
            .build();

        var ex = Assertions.assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));
        assertThat(ex.getMessage(), containsString("not found"));
    }
}
