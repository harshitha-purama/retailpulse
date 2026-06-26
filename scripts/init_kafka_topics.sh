#!/bin/bash
set -e

BOOTSTRAP="kafka:9092"
TOPICS=("retail.clickstream" "retail.orders" "retail.inventory")

echo "Waiting for Kafka to be ready..."
sleep 15

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  kafka-topics --bootstrap-server $BOOTSTRAP \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 1
done

echo "Topics created:"
kafka-topics --bootstrap-server $BOOTSTRAP --list
