# How to use the Temporal plugin

Trigger, signal, query, wait on, and schedule [Temporal](https://temporal.io/) workflows from Kestra flows.

## Authentication

All tasks require `endpoint` (the Temporal frontend service in `host:port` format, e.g. `localhost:7233` or `<namespace>.tmprl.cloud:7233` for Temporal Cloud, required). Optionally set `namespace` (default `default`; for Temporal Cloud, typically `<namespace>.<accountId>`).

Three connection modes are supported:

- **Plaintext** — set neither `apiKey` nor the mTLS properties. Suitable for local dev servers.
- **API key (Bearer token)** — set `apiKey`. Used with Temporal Cloud; sent as `Authorization: Bearer <apiKey>` and TLS is enabled automatically. Mutually exclusive with the mTLS properties.
- **mTLS** — set `clientCert` and `clientKey` (both PEM-encoded; both must be provided together).

Optionally set `caCert` (a PEM-encoded CA certificate) to verify the server certificate — either on its own for one-way TLS with a custom CA, or alongside `clientCert`/`clientKey` for mTLS. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`workflow.Trigger` starts a new workflow execution — set `workflowType` (the registered workflow type name, required) and `taskQueue` (the queue a worker polls to pick up the workflow, required). Optionally set `workflowId` (a UUID is generated when omitted), `input` (a list of JSON-encoded argument strings, one per workflow parameter), and `executionTimeout` (an ISO-8601 duration, e.g. `PT1H`). Fails if a workflow with the given ID is already running. Outputs `workflowId` and `runId`.

`workflow.Signal` sends a named signal to a running workflow — set `workflowId` and `signalName` (both required). Optionally set `runId` (targets the latest open run when omitted) and `args` (a list of JSON-encoded argument strings, one per signal parameter). Fails if the workflow is not found.

`workflow.Query` sends a named query to a workflow and returns the result as a JSON string — set `workflowId` and `queryType` (must match a registered `@QueryMethod` on the workflow, both required). Optionally set `runId` (targets the latest open run when omitted) and `args` (a list of JSON-encoded argument strings, one per query parameter). Outputs `result` (the JSON-encoded query result).

`workflow.Wait` polls `DescribeWorkflowExecution` until the workflow reaches a terminal state — set `workflowId` (required). Optionally set `runId` (waits on the latest run when omitted), `pollInterval` (how often to check, default `PT5S`), `waitTimeout` (total time to wait, default `PT1H`), and `failOnNonCompleted` (default `true`; when `true`, the task throws for any non-`COMPLETED` terminal state; when `false`, it succeeds and reports the status). Outputs `status` (one of `COMPLETED`, `FAILED`, `CANCELED`, `TERMINATED`, `TIMED_OUT`, `CONTINUED_AS_NEW`) and `result` (the JSON-encoded workflow result, `null` for non-`COMPLETED` states).

`workflow.Schedule` creates or updates a Temporal schedule that triggers a workflow on a recurring basis — set `scheduleId` (required), `workflowType` (required), and `taskQueue` (required). Provide exactly one of `cron` (a standard 5-field cron expression, e.g. `0 9 * * *`) or `intervalSeconds` (run every N seconds) — the two are mutually exclusive and one is required. Optionally set `args` (a list of JSON-encoded argument strings passed to each execution) and `overwrite` (default `false`; when `true`, updates the schedule if it already exists rather than failing). Outputs `scheduleId`.
