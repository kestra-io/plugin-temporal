package io.kestra.plugin.temporal;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class TriggerWorkflowTest {

    private static final String TASK_QUEUE = "test-trigger-" + UUID.randomUUID();

    @Inject
    RunContextFactory runContextFactory;

    private TemporalTestServer temporalServer;

    @BeforeEach
    void setUp() throws Exception {
        temporalServer = TemporalTestServer.start();
        var worker = temporalServer.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(
            TestWorkflows.GreetingWorkflowImpl.class,
            TestWorkflows.LongRunningWorkflowImpl.class
        );
        temporalServer.startWorkers();
    }

    @AfterEach
    void tearDown() {
        temporalServer.close();
    }

    @Test
    void happyPath_startsWorkflowAndReturnsIds() throws Exception {
        var task = TriggerWorkflow.builder()
            .id("trigger-" + UUID.randomUUID())
            .type(TriggerWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowType(Property.ofValue("GreetingWorkflow"))
            .taskQueue(Property.ofValue(TASK_QUEUE))
            .input(Property.ofValue(List.of("\"World\"")))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getWorkflowId(), notNullValue());
        assertThat(output.getRunId(), not(emptyString()));
    }

    @Test
    void customWorkflowId_isPreservedInOutput() throws Exception {
        var customId = "my-workflow-" + UUID.randomUUID();
        var task = TriggerWorkflow.builder()
            .id("trigger-custom-" + UUID.randomUUID())
            .type(TriggerWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowType(Property.ofValue("GreetingWorkflow"))
            .taskQueue(Property.ofValue(TASK_QUEUE))
            .workflowId(Property.ofValue(customId))
            .input(Property.ofValue(List.of("\"Kestra\"")))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getWorkflowId(), is(customId));
        assertThat(output.getRunId(), notNullValue());
    }

    @Test
    void duplicateWorkflowId_whileRunning_throwsWithClearMessage() throws Exception {
        // Use a long-running workflow so it's still RUNNING when we try to start the second.
        var sharedId = "dup-" + UUID.randomUUID();

        var task = TriggerWorkflow.builder()
            .id("trigger-dup-" + UUID.randomUUID())
            .type(TriggerWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowType(Property.ofValue("LongRunningWorkflow"))
            .taskQueue(Property.ofValue(TASK_QUEUE))
            .workflowId(Property.ofValue(sharedId))
            .build();

        task.run(runContextFactory.of());

        var task2 = TriggerWorkflow.builder()
            .id("trigger-dup2-" + UUID.randomUUID())
            .type(TriggerWorkflow.class.getName())
            .endpoint(Property.ofValue(temporalServer.getTarget()))
            .workflowType(Property.ofValue("LongRunningWorkflow"))
            .taskQueue(Property.ofValue(TASK_QUEUE))
            .workflowId(Property.ofValue(sharedId))
            .build();

        var ex = Assertions.assertThrows(IllegalStateException.class, () -> task2.run(runContextFactory.of()));
        assertThat(ex.getMessage(), containsString(sharedId));
    }
}
