# Kestra Temporal Plugin

## What

Provides Kestra tasks for interacting with Temporal workflow orchestration under `io.kestra.plugin.temporal`.

Key task classes:
- `io.kestra.plugin.temporal.workflow.Trigger`
- `io.kestra.plugin.temporal.workflow.Signal`
- `io.kestra.plugin.temporal.workflow.Query`
- `io.kestra.plugin.temporal.workflow.Wait`
- `io.kestra.plugin.temporal.workflow.Schedule`
- `io.kestra.plugin.temporal.AbstractTemporalTask` (shared base)

## Why

Teams that use Temporal for workflow orchestration can trigger, signal, query, poll, and schedule Temporal workflows directly from Kestra flows without writing custom code.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `temporal` (shared base, connection and auth)
- `temporal.workflow` (the five tasks)

All tasks extend `AbstractTemporalTask` which handles gRPC connection and auth (API key + TLS for Temporal Cloud, or mTLS via PEM contents).

The SDK version is `io.temporal:temporal-sdk:1.28.4` (latest version compatible with `protobuf-java:3.25.x` enforced by the Kestra platform BOM at version 1.3.13).

### Key Plugin Classes

- `io.kestra.plugin.temporal.AbstractTemporalTask` - shared endpoint, namespace, and auth config
- `io.kestra.plugin.temporal.workflow.Trigger` - starts a Temporal workflow
- `io.kestra.plugin.temporal.workflow.Signal` - sends a named signal to a running workflow
- `io.kestra.plugin.temporal.workflow.Query` - queries a workflow and returns JSON result
- `io.kestra.plugin.temporal.workflow.Wait` - polls DescribeWorkflowExecution until terminal
- `io.kestra.plugin.temporal.workflow.Schedule` - creates or updates a Temporal schedule

### Test Infrastructure

- Unit tests use `TemporalTestServer` (port-bound in-process Temporal test server) so tasks can connect via TCP.
- Integration tests for `Schedule` are guarded by `@EnabledIfSystemProperty(named = "temporal.integration.enabled", matches = "true")`. Require a real server via `docker compose -f docker-compose-ci.yml up -d`.

### Project Structure

```
plugin-temporal/
├── src/main/java/io/kestra/plugin/temporal/
│   ├── AbstractTemporalTask.java
│   ├── package-info.java
│   └── workflow/
│       ├── Trigger.java
│       ├── Signal.java
│       ├── Query.java
│       ├── Wait.java
│       ├── Schedule.java
│       └── package-info.java
├── src/test/java/io/kestra/plugin/temporal/
│   ├── TemporalTestServer.java
│   ├── TestWorkflows.java
│   ├── AbstractTemporalTaskTest.java
│   └── workflow/
│       ├── TriggerTest.java
│       ├── SignalTest.java
│       ├── QueryTest.java
│       ├── WaitTest.java
│       ├── ScheduleTest.java
│       └── ScheduleValidationTest.java
├── src/main/resources/icons/
├── .github/
│   ├── setup-unit.sh
│   └── cleanup-unit.sh
├── build.gradle
├── docker-compose-ci.yml
└── README.md
```

## Local rules

- Temporal SDK version must stay compatible with the Kestra platform BOM's protobuf version.
- Do not upgrade `temporal-sdk` beyond what works with `protobuf-java:3.25.x` without also bumping the Kestra platform version.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
