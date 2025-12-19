#!/usr/bin/env bash
set -euo pipefail
kafka-topics.sh --create --if-not-exists --topic raw_trades --bootstrap-server ${1:-localhost:9092} --replication-factor 1 --partitions 3
kafka-topics.sh --create --if-not-exists --topic enriched_trades --bootstrap-server ${1:-localhost:9092} --replication-factor 1 --partitions 3
kafka-topics.sh --create --if-not-exists --topic candles_1m --bootstrap-server ${1:-localhost:9092} --replication-factor 1 --partitions 3
