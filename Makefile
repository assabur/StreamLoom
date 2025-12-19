SHELL := /bin/bash

COMPOSE := docker-compose

.PHONY: up down build test demo create-topics fmt

up:
$(COMPOSE) up -d --build

build:
$(COMPOSE) build

create-topics:
docker exec kafka kafka-topics.sh --create --if-not-exists --topic raw_trades --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3
docker exec kafka kafka-topics.sh --create --if-not-exists --topic enriched_trades --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3
docker exec kafka kafka-topics.sh --create --if-not-exists --topic candles_1m --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3
docker exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092

fmt:
cd services/binance-ingestor && gofmt -w .
cd services/clickhouse-sink && gofmt -w .
cd services/api && gofmt -w .

clean:
$(COMPOSE) down -v
rm -rf services/*/bin services/*/build

smoke-test:
bash scripts/smoke-test.sh

demo: up create-topics
@echo "Waiting for services..."
sleep 10
curl -s http://localhost:8088/health || true

