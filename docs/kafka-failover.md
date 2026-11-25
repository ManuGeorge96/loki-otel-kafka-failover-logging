# Loki logging pipeline: Loki-first with durable MSK failover

## Overview

Logs are delivered to Loki over a **direct (live) path** during normal operation.
If Loki becomes unreachable, a **failover connector** automatically diverts logs
to a durable **MSK (Kafka) buffer** instead of dropping them. When Loki recovers,
the live path resumes **and** the buffered logs are replayed (drained) back into
Loki — so there is no gap and no data loss.

Kafka is a **fallback**, not the normal route. This keeps the extra hop (and its
latency) off the happy path while preserving durability during a Loki outage or
backpressure event.

A single **`otel-failover-collector`** owns the whole thing. It is an
OpenTelemetry Collector (contrib distribution) that runs two roles at once and
therefore replaces both a producer-side Kafka export and a separate Kafka consumer
deployment.

> **Why contrib, not Alloy:** `otelcol.connector.failover`, the `awsfirehose`
> receiver, and Kafka `AWS_MSK_IAM_OAUTHBEARER` auth exist **only** in
> `otel/opentelemetry-collector-contrib`. Grafana Alloy does not wrap the failover
> connector, so the hub runs the contrib image (`0.128.0`).

## Components

| Component | Role |
|-----------|------|
| **Alloy producers** (`api-v3`, `data-platform`, `payments`) | Ingest from Kinesis Firehose, apply labels + service identity, then **export OTLP** to the hub (they no longer write to Kafka). |
| **`otel-failover-collector`** (Deployment/ConfigMap/SA/Service, ns `loki`) | The hub. Live routing (failover) **and** buffer drain. |
| **MSK topic `loki-logs`** | Durable buffer, used only during a Loki outage. |
| **Loki** (`http://loki-gateway.loki.svc.cluster.local/otlp/v1/logs`) | Final destination, native OTLP ingest. |

Inside the hub:

- **Role A — live routing:** `otlp` receiver → `otelcol.connector.failover` →
  - priority 1: `otlphttp/loki-live` → Loki
  - priority 2: `kafka/msk` → MSK `loki-logs` (only while Loki is unhealthy)
- **Role B — drain / backfill:** `kafka/drain` receiver (group
  `loki-forward-consumer`) → `otlphttp/loki-drain` → Loki

```
                    ┌─────────────────── otel-failover-collector ───────────────────┐
 Firehose           │                                                                │
    │               │                          ┌─▶ otlphttp/loki-live ─▶ Loki  (P1)  │
    ▼               │  otlp ─▶ failover ────────┤                                    │
 Alloy producers ───┼──OTLP─▶ (:4317/:4318)     └─▶ kafka/msk ─▶ MSK loki-logs (P2)  │
 (label + identity) │                                                    │           │
                    │                                                    ▼           │
                    │  kafka/drain (group loki-forward-consumer) ─▶ otlphttp/loki-drain ─▶ Loki
                    └────────────────────────────────────────────────────────────────┘
```

## Failover-critical configuration

These few settings are what make the failover actually work — get them wrong and
it silently fails to fail over.

| Setting | Where | Why |
|---------|-------|-----|
| `sending_queue.enabled: false` | `otlphttp/loki-live` | A sending queue ACKs data immediately and **hides** the Loki error, so the connector never sees a failure. Must be off. |
| `retry_on_failure.max_elapsed_time: 30s` | `otlphttp/loki-live` | Rides out brief Loki blips; the connector diverts to Kafka only after 30s of sustained failure. |
| `priority_levels: [[logs/loki],[logs/kafka]]` | `failover` connector | Tries Loki first, Kafka only while Loki's pipeline is unhealthy. |
| `retry_interval: 1m` | `failover` connector | How often it re-probes Loki to fail back once it recovers. |
| `message_marking: {after: true, on_error: false}` | `kafka/drain` | At-least-once: commit the offset only **after** Loki acks; never on a failed push. |
| `retry_on_failure.max_elapsed_time: 0s` | `otlphttp/loki-drain` | Retry forever, so the buffer survives a long outage. |
| `producer.required_acks: -1` | `kafka/msk` | Wait for in-sync replicas before a record is considered buffered. |

---

## How logs flow — the three scenarios

### Scenario 1 — Normal operation (Loki healthy)

```
Alloy producer ─OTLP─▶ hub: otlp ─▶ failover ─▶ otlphttp/loki-live ─▶ Loki ✅
                                        │
                                        └─ kafka/msk        : idle (nothing produced)
hub: kafka/drain ─▶ otlphttp/loki-drain : idle (nothing to consume)
```

1. Producers push OTLP to the hub on `:4317`.
2. The failover connector routes to priority 1 (`logs/loki`).
3. `otlphttp/loki-live` pushes straight to Loki. Kafka is untouched.

**Signature (metrics on `:8888`):**
`otelcol_exporter_sent_log_records_total{exporter="otlphttp/loki-live"}` rising;
`{exporter="kafka/msk"}` flat at its last value.

### Scenario 2 — Loki outage (failover to the buffer)

```
Alloy producer ─OTLP─▶ hub: otlp ─▶ failover ─▶ otlphttp/loki-live ─✗ (Loki down)
                                        │  (after ≤30s of retry)
                                        └────▶ kafka/msk ─▶ MSK loki-logs  ✅ (buffering)

hub: kafka/drain ─▶ otlphttp/loki-drain ─✗ (Loki down) ─▶ offset NOT committed ─▶ data retained
```

1. `otlphttp/loki-live` starts failing. It retries for up to `max_elapsed_time`
   (30s), then returns an error to the failover connector.
2. The connector marks `logs/loki` unhealthy and diverts **all** subsequent logs
   to priority 2 (`logs/kafka`) → `kafka/msk` writes them to `loki-logs`.
3. The drain path (`kafka/drain`) reads those same records but **cannot** push to
   Loki. Because of at-least-once marking it does **not** commit the offset → the
   records stay in Kafka and are redelivered later.

Net: nothing is lost; logs accumulate in `loki-logs`.

**Signature:** `{exporter="kafka/msk"}` rising; `send_failed{loki-live}` rising;
consumer-group lag for `loki-forward-consumer` climbing;
`{exporter="otlphttp/loki-drain"}` flat. New logs stop appearing in Grafana.

> The first batch at outage onset blocks up to ~30s (queue is off, so it's
> synchronous) before the connector diverts — expect brief backpressure toward
> producers at the transition only.

### Scenario 3 — Loki recovery (failback + backfill)

```
Alloy producer ─OTLP─▶ hub: otlp ─▶ failover ─▶ otlphttp/loki-live ─▶ Loki ✅ (new logs, within retry_interval)

hub: kafka/drain ─▶ otlphttp/loki-drain ─▶ Loki ✅ (replays the buffered backlog, original timestamps)
```

1. **Backfill:** the drain path's retry finally succeeds, so it pushes the
   buffered records to Loki and advances its offset — the backlog drains and lag
   falls to 0. Timestamps are preserved, so logs land in their correct place on
   the timeline (out-of-order arrival, in-order storage).
2. **Failback:** within `retry_interval` (1m) the failover connector re-probes
   `logs/loki`, finds it healthy, and routes new logs directly to Loki again.
   `kafka/msk` goes idle.

**Signature:** `{exporter="otlphttp/loki-drain"}` rising then plateauing (backlog
cleared); lag → 0; `{exporter="otlphttp/loki-live"}` rising again;
`{exporter="kafka/msk"}` flat.

---

## Cluster: 2 brokers

This cluster has **2 brokers**, which caps the topic at **replication-factor 2**
and is below the usual 3-broker quorum. See the durability tradeoff below before
choosing `min.insync.replicas`.

## 2-broker durability tradeoff (choose `min.insync.replicas`)

With RF=2 and the producer set to `required_acks = -1` (already configured):

| `min.insync.replicas` | One broker down | Risk |
|----|----|----|
| **2** | **Produces stop** (ISR < 2) | No data loss, but buffering halts during any single-broker maintenance |
| **1** | Produces continue on the survivor | Available, but a leader crash before replication can lose a small in-flight slice |

For a build buffer, `min.insync.replicas=1` (availability) is the pragmatic
default; choose `2` if zero-loss matters more than staying writable during a
broker restart. A 3rd broker later lets you move to RF=3 / ISR=2 and get both.

## IAM role (IRSA) for MSK

Only the **hub** talks to MSK now (producers export OTLP, not Kafka), so a single
role bound to the `otel-failover-collector` service account suffices — it needs
**both** produce (failover) and consume (drain) permissions. Trust it to the EKS
OIDC provider for `system:serviceaccount:loki:otel-failover-collector`, and attach
(scope the resource ARNs to the actual cluster/topic/group):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Connect",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:WriteDataIdempotently"
      ],
      "Resource": "arn:aws:kafka:<AWS_REGION>:<AWS_ACCOUNT_ID>:cluster/<CLUSTER_NAME>/*"
    },
    {
      "Sid": "TopicReadWrite",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:CreateTopic"
      ],
      "Resource": "arn:aws:kafka:<AWS_REGION>:<AWS_ACCOUNT_ID>:topic/<CLUSTER_NAME>/*/loki-logs"
    },
    {
      "Sid": "GroupRead",
      "Effect": "Allow",
      "Action": ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"],
      "Resource": "arn:aws:kafka:<AWS_REGION>:<AWS_ACCOUNT_ID>:group/<CLUSTER_NAME>/*/loki-forward-consumer"
    }
  ]
}
```

> **`WriteDataIdempotently` is required, not optional.** The Kafka exporter uses an
> idempotent producer, whose `InitProducerID` request is authorized at the
> **cluster** level. Without this action every produce fails permanently with
> `CLUSTER_AUTHORIZATION_FAILED` (topic-level `WriteData` alone is not enough).

## Create the topic

Auto-creation is left off in the exporter config, so create it explicitly.
**RF=2** (the max with 2 brokers); partitions ≥ hub replicas so the drain
consumers parallelize:

```bash
kafka-topics.sh --create --topic loki-logs \
  --partitions 6 --replication-factor 2 \
  --config min.insync.replicas=1 \          # or 2 — see durability tradeoff above
  --config retention.ms=86400000 \          # 24h buffer window
  --bootstrap-server <KAFKA_BROKERS> [--command-config client.properties]
```

> `--command-config` is only needed for authenticated clusters (IAM/SASL/TLS).
> Get the IAM bootstrap brokers with:
> `aws kafka get-bootstrap-brokers --cluster-arn <ARN> --region <AWS_REGION> --query BootstrapBrokerStringSaslIam --output text` (port `9098`).

## Deploy

Order matters: the hub must exist before producers start sending to it.

```bash
# 1. Hub first (so producers have somewhere to send)
kubectl apply -f otel-collector/otel-failover-collector.yaml   # SA + ConfigMap + Deployment + Service
kubectl rollout status deploy/otel-failover-collector -n loki

# 2. Producers (OTLP -> hub)
helm upgrade --install alloy-api-v3        grafana/alloy -n loki -f alloy/alloy-api-v3.yaml
helm upgrade --install alloy-data-platform grafana/alloy -n loki -f alloy/alloy-data-platform.yaml
helm upgrade --install alloy-payments      grafana/alloy -n loki -f alloy/alloy-payments.yaml
```

> **Migration note:** in the original environment the buffer was first drained by a
> standalone Alloy Kafka consumer (`grafana-alloy-kafka-consumer`). The hub's Role B
> reuses the same consumer group (`loki-forward-consumer`), so the cut-over was:
> deploy the hub → move producers to OTLP → scale the old consumer to 0. On a fresh
> build there is no old consumer — skip that step.

## Verify

Scrape hub metrics and read the per-path counters (see the "Signature" lines
above):

```bash
kubectl port-forward -n loki deploy/otel-failover-collector 8888:8888
curl -s localhost:8888/metrics | grep -E 'otelcol_exporter_sent_log_records|otelcol_exporter_send_failed_log_records|otelcol_receiver_accepted_log_records'
```

Confirm end-to-end in Grafana: `count_over_time({app="api-v3"}[5m])` returns data,
and `app`/`env` show up as labels (not just structured metadata).

### Failover smoke test

1. Make sure nothing else consumes the `loki-forward-consumer` group (it would
   split partitions with the hub and mask the test).
2. Optionally scale the hub to **1 replica** so all metrics land in one pod.
3. **Break Loki (from the hub's view):** edit the ConfigMap, point **both**
   `otlphttp/loki-live` and `otlphttp/loki-drain` `logs_endpoint` at a dead
   address (e.g. `http://127.0.0.1:1/otlp/v1/logs`), then
   `kubectl rollout restart deploy/otel-failover-collector -n loki`.
4. Watch `{exporter="kafka/msk"}` climb (Scenario 2). Optionally inject a marker
   via `:4318` `/v1/logs` with a unique `service.name`.
5. **Restore** the endpoints and `rollout restart`. Watch
   `{exporter="otlphttp/loki-drain"}` climb, lag fall to 0, and the marker appear
   in Loki with its original timestamp (Scenario 3).
6. Scale the hub back to 2 replicas.

## Label model note

After the OTLP round-trip, only `service.name`, `app`, and `env` are **index
labels** (configured in `values.yaml` via `limits_config.otlp_config`). The
dynamic labels from the producers' `loki.process` stages (`apigName`, `apiName`,
`apiId`, `raw_log`, `eventSource`, `detailType`, `event`, `log_group`,
`log_stream`) arrive as **structured metadata** — still queryable with
`| field="value"` but not indexed. If any of those must remain index labels, add
them to the `index_label` list in `limits_config.otlp_config`, but keep
cardinality in mind (`raw_log`/`event` are risky).

---

## Troubleshooting

### 1. Logs aren't reaching Loki at all

- **Which path is active?** Scrape `:8888`. If the `otlp` receiver is flat, the
  hub isn't receiving — check the producers (below). If `otlp` is rising but
  `loki-live` is flat with `send_failed{loki-live}` rising, Loki is unreachable
  from the hub.
- **2-replica metric trap:** `kubectl port-forward deploy/...` pins to **one**
  pod, and gRPC pins a producer to one pod, so you may be scraping an idle replica
  and see frozen counters. Scrape each pod by name, or scale to 1 replica while
  testing.
- **Producer not cut over:** confirm the running Alloy config exports OTLP to the
  hub, not Kafka:
  `kubectl get cm -n loki -l app.kubernetes.io/instance=alloy-api-v3 -o yaml | grep -A5 failover_hub`

### 2. Failover never triggers (Loki down but nothing goes to Kafka)

- **`sending_queue` is enabled on `loki-live`.** This is the #1 cause — the queue
  ACKs data and the connector never sees the failure. It must be
  `sending_queue.enabled: false`.
- **You're within the 30s retry window.** `max_elapsed_time: 30s` means the
  connector waits up to 30s of sustained failure before diverting. Wait it out or
  lower the value.
- **Verify the outage is a hard error.** Failover triggers on connection refused /
  5xx / timeout. A Loki that returns 200 while silently dropping won't trip it.

### 3. Logs failed over but were lost (marker never buffered)

- **Check `send_failed{kafka/msk}`.** If it's >0, the **produce to MSK is
  failing** — the records were never buffered. Almost always IAM:
  - Hub SA missing the role:
    `kubectl get sa otel-failover-collector -n loki -o jsonpath='{.metadata.annotations}'`
    → expect `eks.amazonaws.com/role-arn: arn:aws:iam::<AWS_ACCOUNT_ID>:role/<LOKI_KAFKA_ROLE_NAME>`.
  - The role's OIDC trust policy must include
    `system:serviceaccount:loki:otel-failover-collector`.
  - The IAM policy needs `WriteDataIdempotently` at the **cluster** level, or every
    produce fails with `CLUSTER_AUTHORIZATION_FAILED` (see IAM section above).

### 4. Buffer doesn't drain back after Loki recovers

- **Another consumer in the same group.** Anything else using the
  `loki-forward-consumer` group splits the partitions and can drain the backlog
  through a different path, so the hub's drain looks stuck. Scale it to 0.
- **Drain endpoint typo.** Confirm both endpoints reverted correctly:
  `kubectl get cm otel-failover-collector -n loki -o yaml | grep logs_endpoint`
  → both should be `http://loki-gateway.loki.svc.cluster.local/otlp/v1/logs`.
- **Consume-side IAM.** `kafka/drain` needs `ReadData` + `*Group` on the cluster.
  Check `receiver_accepted_log_records{receiver="kafka/drain"}` — if flat, it isn't
  consuming (auth / group issue).
- **ConfigMap edit didn't apply.** The collector does **not** hot-reload; you must
  `kubectl rollout restart` after editing the ConfigMap.

### 5. Hub pod is crashing / not ready

- **Bad config from a hand edit.** Editing the embedded `config.yaml` easily breaks
  indentation. Check:
  `kubectl get pods -n loki -l app=otel-failover-collector` and
  `kubectl logs -n loki deploy/otel-failover-collector --tail=80` — a config parse
  error means the last edit is malformed.
- **Health:** liveness/readiness hit the `health_check` extension on `:13133`.

### 6. Consumer-group lag stays high

- Loki is down or slow (drain can't keep up), **or** the drain path is failing to
  push (see #4). During a real outage rising lag is expected; it should fall to 0
  after recovery. The `KafkaConsumerLagHigh` alert covers group
  `loki-forward-consumer`.

### 7. Duplicate or oddly-labelled logs around a transition

- A batch that partially landed in Loki before failing over is replayed from Kafka
  → a few duplicates at the cutover. Loki dedupes identical
  (timestamp+labels+line) entries **within a stream**.
- If you add a `via="kafka"` marker on the drain pipeline, buffered logs form a
  separate stream from live ones (so they won't dedupe against each other) — a
  deliberate trade-off for being able to tell the paths apart in Grafana.

### 8. Loki gets knocked over right after recovering

- On recovery the live path **and** the drain backfill hit Loki at once
  (thundering herd). Add a `batch` processor to the drain pipeline and/or Loki
  ingestion rate limits to smooth it.

---

## Hardening TODO

- Add a `memory_limiter` processor (guards OOM on backfill at the 512Mi limit).
- Add a `batch` processor on the drain pipeline.
- Add `topologySpreadConstraints` + a `PodDisruptionBudget` so the 2 replicas
  don't share a node.
