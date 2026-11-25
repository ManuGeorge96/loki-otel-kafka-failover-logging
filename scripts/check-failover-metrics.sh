#!/usr/bin/env bash
# Print the per-path log counters of every otel-failover-collector pod.
# Read with docs/kafka-failover.md -> "How logs flow — the three scenarios".
#
#   otlphttp/loki-live  rising  -> normal operation
#   kafka/msk           rising  -> Loki is down, logs are being buffered
#   otlphttp/loki-drain rising  -> Loki recovered, backlog is being replayed
set -euo pipefail
NS="${NS:-loki}"
PORT=18888

for pod in $(kubectl get pods -n "$NS" -l app=otel-failover-collector -o name); do
  echo "=== $pod"
  kubectl port-forward -n "$NS" "$pod" "$PORT:8888" >/dev/null 2>&1 &
  PF=$!
  sleep 2
  curl -s "localhost:$PORT/metrics" | grep -E \
    'otelcol_exporter_sent_log_records|otelcol_exporter_send_failed_log_records|otelcol_receiver_accepted_log_records' \
    | grep -v '^#' || echo "(no samples yet)"
  kill "$PF" 2>/dev/null || true
  wait "$PF" 2>/dev/null || true
done
