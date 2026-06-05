package io.kestra.plugin.temporal;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Validates auth configuration rules in AbstractTemporalTask without a live server.
 * The validation is done inside buildClient() before any network call is made.
 */
@KestraTest
class AbstractTemporalTaskTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void apiKeyAndMtls_mutuallyExclusive_throwsIllegalArgument() {
        // TriggerWorkflow is used here as a concrete subclass for testing the abstract base.
        var task = TriggerWorkflow.builder()
            .id("auth-conflict-" + UUID.randomUUID())
            .type(TriggerWorkflow.class.getName())
            .endpoint(Property.ofValue("localhost:7233"))
            .namespace(Property.ofValue("default"))
            .apiKey(Property.ofValue("some-key"))
            .clientCert(Property.ofValue("-----BEGIN CERTIFICATE-----"))
            .clientKey(Property.ofValue("-----BEGIN PRIVATE KEY-----"))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("test-queue"))
            .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(ex.getMessage(), containsString("mutually exclusive"));
    }

    @Test
    void clientCertWithoutClientKey_throwsIllegalArgument() {
        var task = TriggerWorkflow.builder()
            .id("mtls-incomplete-" + UUID.randomUUID())
            .type(TriggerWorkflow.class.getName())
            .endpoint(Property.ofValue("localhost:7233"))
            .clientCert(Property.ofValue("-----BEGIN CERTIFICATE-----"))
            .workflowType(Property.ofValue("TestWorkflow"))
            .taskQueue(Property.ofValue("test-queue"))
            .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(ex.getMessage(), containsString("clientCert and clientKey"));
    }
}
