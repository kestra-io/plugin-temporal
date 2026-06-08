<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Kestra Temporal Plugin
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a>
</div>

<br />

# Kestra Temporal Plugin

Kestra tasks for interacting with [Temporal](https://temporal.io) workflow orchestration via the official Java SDK.

## Tasks

| Task | Purpose |
|---|---|
| `Trigger` | Start a new Temporal workflow execution |
| `Signal` | Send a signal to a running workflow |
| `Query` | Query a running workflow and return the result as JSON |
| `Wait` | Poll until a workflow reaches a terminal state |
| `Schedule` | Create or update a Temporal schedule |

## Connection

All tasks share common connection properties:

| Property | Description |
|---|---|
| `endpoint` | `host:port` of the Temporal frontend (e.g. `localhost:7233`) |
| `namespace` | Temporal namespace (default: `default`) |
| `apiKey` | Bearer API key for Temporal Cloud; enables TLS automatically |
| `clientCert` / `clientKey` | PEM-encoded mTLS client certificate and key |
| `caCert` | PEM-encoded CA certificate for mTLS server verification |

`apiKey` and mTLS properties are mutually exclusive.

## Example: trigger, signal, wait

```yaml
id: order_processing
namespace: company.team

tasks:
  - id: trigger
    type: io.kestra.plugin.temporal.workflow.Trigger
    endpoint: "localhost:7233"
    workflowType: "OrderWorkflow"
    taskQueue: "order-queue"
    workflowId: "order-{{ execution.id }}"
    input:
      - '{"orderId": "ORD-123"}'

  - id: approve
    type: io.kestra.plugin.temporal.workflow.Signal
    endpoint: "localhost:7233"
    workflowId: "{{ outputs.trigger.workflowId }}"
    signalName: "approve"

  - id: wait
    type: io.kestra.plugin.temporal.workflow.Wait
    endpoint: "localhost:7233"
    workflowId: "{{ outputs.trigger.workflowId }}"
    runId: "{{ outputs.trigger.runId }}"
    pollInterval: "PT5S"
    waitTimeout: "PT1H"
```

## Integration tests

`Schedule` tests require a real Temporal server. To run them:

```bash
docker compose -f docker-compose-ci.yml up -d
./gradlew test -Dtemporal.integration.enabled=true
```

## License

Apache 2.0 (c) [Kestra Technologies](https://kestra.io)
