package io.kestra.plugin.temporal;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.SimpleSslContextBuilder;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import jakarta.annotation.Nullable;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;

/**
 * Shared connection and auth configuration for all Temporal tasks.
 *
 * Two auth modes are supported:
 * - API key (Bearer token): used with Temporal Cloud; requires TLS.
 * - mTLS: client certificate and key PEM contents, with optional CA cert.
 *
 * If neither mode is configured, a plaintext connection is used (suitable
 * for local dev servers).
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTemporalTask extends Task {

    @Schema(
        title = "Temporal server endpoint.",
        description = """
            Host and port of the Temporal frontend service in `host:port` format.
            For Temporal Cloud, use the gRPC endpoint for your namespace,
            e.g. `<namespace>.tmprl.cloud:7233`.
            """
    )
    private Property<String> endpoint;

    @Schema(
        title = "Temporal namespace.",
        description = "Defaults to `default`. For Temporal Cloud, typically `<namespace>.<accountId>`."
    )
    private Property<String> namespace;

    @Schema(
        title = "API key for Bearer token authentication.",
        description = """
            Used with Temporal Cloud. Sent as `Authorization: Bearer <apiKey>` on every
            gRPC call; TLS is enabled automatically when this property is set.
            Mutually exclusive with mTLS properties.
            """
    )
    private Property<String> apiKey;

    @Schema(
        title = "PEM-encoded CA certificate.",
        description = "Used in mTLS mode to verify the server certificate."
    )
    private Property<String> caCert;

    @Schema(
        title = "PEM-encoded client certificate.",
        description = "Used in mTLS mode. Must be provided together with `clientKey`."
    )
    private Property<String> clientCert;

    @Schema(
        title = "PEM-encoded client private key.",
        description = "Used in mTLS mode. Must be provided together with `clientCert`."
    )
    private Property<String> clientKey;

    /**
     * Builds a connected WorkflowClient. The caller must call
     * {@link #closeClient(WorkflowClient)} in a finally block.
     */
    protected WorkflowClient buildClient(RunContext runContext) throws Exception {
        var rEndpoint   = runContext.render(endpoint).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("endpoint is required"));
        var rNamespace  = runContext.render(namespace).as(String.class).orElse("default");
        var rApiKey     = runContext.render(apiKey).as(String.class).orElse(null);
        var rCaCert     = runContext.render(caCert).as(String.class).orElse(null);
        var rClientCert = runContext.render(clientCert).as(String.class).orElse(null);
        var rClientKey  = runContext.render(clientKey).as(String.class).orElse(null);

        validateAuthConfig(rApiKey, rClientCert, rClientKey);

        var stubsOptions = buildStubsOptions(rEndpoint, rApiKey, rCaCert, rClientCert, rClientKey);
        var stubs = WorkflowServiceStubs.newServiceStubs(stubsOptions);

        var clientOptions = WorkflowClientOptions.newBuilder()
            .setNamespace(rNamespace)
            .build();

        return WorkflowClient.newInstance(stubs, clientOptions);
    }

    private void validateAuthConfig(
        @Nullable String rApiKey,
        @Nullable String rClientCert,
        @Nullable String rClientKey
    ) {
        if (rApiKey != null && (rClientCert != null || rClientKey != null)) {
            throw new IllegalArgumentException(
                "apiKey and mTLS properties (clientCert/clientKey) are mutually exclusive"
            );
        }
        if ((rClientCert == null) != (rClientKey == null)) {
            throw new IllegalArgumentException(
                "clientCert and clientKey must both be provided for mTLS"
            );
        }
    }

    private WorkflowServiceStubsOptions buildStubsOptions(
        String rEndpoint,
        @Nullable String rApiKey,
        @Nullable String rCaCert,
        @Nullable String rClientCert,
        @Nullable String rClientKey
    ) throws Exception {
        var builder = WorkflowServiceStubsOptions.newBuilder()
            .setTarget(rEndpoint);

        if (rApiKey != null) {
            var key = rApiKey;
            builder.addApiKey(() -> key);
            // Temporal Cloud requires TLS with API keys.
            builder.setEnableHttps(true);
        } else if (rClientCert != null) {
            var sslBuilder = SimpleSslContextBuilder.forPKCS8(
                new ByteArrayInputStream(rClientCert.getBytes(StandardCharsets.UTF_8)),
                new ByteArrayInputStream(rClientKey.getBytes(StandardCharsets.UTF_8))
            );
            if (rCaCert != null) {
                sslBuilder.setTrustManager(buildTrustManager(rCaCert));
            }
            builder.setSslContext(sslBuilder.build());
        }

        return builder.build();
    }

    /**
     * Parses a PEM-encoded X.509 CA certificate and wraps it in a TrustManager.
     */
    private static X509TrustManager buildTrustManager(String caCertPem) throws Exception {
        var certFactory = CertificateFactory.getInstance("X.509");
        var cert = certFactory.generateCertificate(
            new ByteArrayInputStream(caCertPem.getBytes(StandardCharsets.UTF_8))
        );
        var keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("ca", cert);
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        for (var tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager x509tm) {
                return x509tm;
            }
        }
        throw new IllegalStateException("No X509TrustManager found after loading CA cert");
    }

    /**
     * Shuts down the workflow client and its underlying service stubs.
     * Must be called in a finally block after each task run.
     */
    protected void closeClient(WorkflowClient client) {
        try {
            client.getWorkflowServiceStubs().shutdown();
        } catch (Exception ignored) {
            // best-effort shutdown
        }
    }
}
