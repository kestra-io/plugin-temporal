package io.kestra.plugin.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.testserver.TestServer;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;

/**
 * Wraps a port-bound Temporal test server so task tests can connect via TCP.
 *
 * TestWorkflowEnvironment uses in-process channels by default, which tasks
 * cannot reach via host:port. This helper starts a real TCP server and wires
 * a WorkflowClient and WorkerFactory to it.
 */
public final class TemporalTestServer implements Closeable {

    private final TestServer.PortBoundTestServer portServer;
    private final WorkflowServiceStubs serviceStubs;
    private final WorkflowClient client;
    private final WorkerFactory workerFactory;
    private final int port;

    private TemporalTestServer(int port) throws Exception {
        this.port = port;
        this.portServer = TestServer.createPortBoundServer(port);

        var stubsOptions = WorkflowServiceStubsOptions.newBuilder()
            .setTarget("127.0.0.1:" + port)
            .build();
        this.serviceStubs = WorkflowServiceStubs.newServiceStubs(stubsOptions);

        var clientOptions = WorkflowClientOptions.newBuilder()
            .setNamespace("default")
            .build();
        this.client = WorkflowClient.newInstance(serviceStubs, clientOptions);
        this.workerFactory = WorkerFactory.newInstance(client);
    }

    public static TemporalTestServer start() throws Exception {
        return new TemporalTestServer(findFreePort());
    }

    /**
     * Creates a new worker that polls the given task queue.
     * Register workflow implementations before calling {@link #startWorkers()}.
     */
    public Worker newWorker(String taskQueue) {
        return workerFactory.newWorker(taskQueue);
    }

    public void startWorkers() {
        workerFactory.start();
    }

    public WorkflowClient getClient() {
        return client;
    }

    public String getTarget() {
        return "127.0.0.1:" + port;
    }

    @Override
    public void close() {
        try {
            workerFactory.shutdown();
        } catch (Exception ignored) {}
        try {
            serviceStubs.shutdown();
        } catch (Exception ignored) {}
        try {
            portServer.close();
        } catch (Exception ignored) {}
    }

    private static int findFreePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
