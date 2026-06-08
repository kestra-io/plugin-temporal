package io.kestra.plugin.temporal;

import io.temporal.workflow.*;

/**
 * Minimal workflow definitions used by unit tests.
 * These run inside the port-bound test server.
 */
public final class TestWorkflows {

    private TestWorkflows() {}

    @WorkflowInterface
    public interface GreetingWorkflow {
        @WorkflowMethod
        String greet(String name);

        @SignalMethod
        void updateGreeting(String newGreeting);

        @QueryMethod
        String getCurrentGreeting();
    }

    /**
     * A workflow that waits for an "unlock" signal before completing.
     * Used for signal and duplicate-ID tests where the workflow must still be running.
     */
    @WorkflowInterface
    public interface LongRunningWorkflow {
        @WorkflowMethod
        String run();

        @SignalMethod
        void unlock(String message);

        @QueryMethod
        String getStatus();
    }

    @WorkflowInterface
    public interface NoResultWorkflow {
        @WorkflowMethod
        void run();
    }

    public static class GreetingWorkflowImpl implements GreetingWorkflow {
        private String greeting = "Hello";

        @Override
        public String greet(String name) {
            return greeting + ", " + name + "!";
        }

        @Override
        public void updateGreeting(String newGreeting) {
            this.greeting = newGreeting;
        }

        @Override
        public String getCurrentGreeting() {
            return greeting;
        }
    }

    public static class LongRunningWorkflowImpl implements LongRunningWorkflow {
        // Temporal-safe mutable state: simple String field, mutated only by signal.
        private String unlockMessage = null;

        @Override
        public String run() {
            // Await until the unlock signal sets the message.
            Workflow.await(() -> unlockMessage != null);
            return "done: " + unlockMessage;
        }

        @Override
        public void unlock(String message) {
            this.unlockMessage = message;
        }

        @Override
        public String getStatus() {
            return unlockMessage == null ? "waiting" : "unlocked";
        }
    }

    public static class NoResultWorkflowImpl implements NoResultWorkflow {
        @Override
        public void run() {
            // intentionally empty
        }
    }
}
