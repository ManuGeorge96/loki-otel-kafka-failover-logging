#!/usr/bin/env bash
# Create the MSK buffer topic used by the failover collector.
#
# Usage:
#   export MSK_CLUSTER_ARN=arn:aws:kafka:<region>:<account>:cluster/<name>/<id>
#   export AWS_REGION=<region>
#   ./kafka/create-topic.sh
#
# Needs: kafka-topics.sh (Apache Kafka CLI) + aws-msk-iam-auth jar on the classpath,
# and network access to the MSK brokers (run from a pod or bastion inside the VPC).
set -euo pipefail

: "${MSK_CLUSTER_ARN:?set MSK_CLUSTER_ARN}"
: "${AWS_REGION:?set AWS_REGION}"

TOPIC="${TOPIC:-loki-logs}"
PARTITIONS="${PARTITIONS:-6}"            # >= number of collector replicas so the drain parallelises
REPLICATION="${REPLICATION:-2}"          # max possible on a 2-broker cluster
MIN_ISR="${MIN_ISR:-1}"                  # 1 = stay writable during broker restarts; 2 = zero-loss (see docs)
RETENTION_MS="${RETENTION_MS:-86400000}" # 24h buffer window

BROKERS=$(aws kafka get-bootstrap-brokers \
  --cluster-arn "$MSK_CLUSTER_ARN" --region "$AWS_REGION" \
  --query BootstrapBrokerStringSaslIam --output text)

CLIENT_PROPS=$(mktemp)
cat > "$CLIENT_PROPS" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
PROPS

kafka-topics.sh --create --if-not-exists --topic "$TOPIC" \
  --partitions "$PARTITIONS" --replication-factor "$REPLICATION" \
  --config min.insync.replicas="$MIN_ISR" \
  --config retention.ms="$RETENTION_MS" \
  --bootstrap-server "$BROKERS" --command-config "$CLIENT_PROPS"

kafka-topics.sh --describe --topic "$TOPIC" \
  --bootstrap-server "$BROKERS" --command-config "$CLIENT_PROPS"

rm -f "$CLIENT_PROPS"
