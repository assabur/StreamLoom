#!/usr/bin/env bash
set -euo pipefail
COMPOSE=docker-compose
$COMPOSE up -d --build kafka clickhouse
sleep 10
bash scripts/create-topics.sh kafka:9092
# Produce a fake event
echo '{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","p":"42000.0","q":"0.1","m":true,"a":1}' | docker exec -i kafka kafka-console-producer.sh --broker-list kafka:9092 --topic raw_trades
sleep 2
# Simple ClickHouse ping
curl -s "http://localhost:8123/?query=SELECT%201" || true
