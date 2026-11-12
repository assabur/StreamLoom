.DEFAULT_GOAL := help

SYMBOLS ?= AAPL.US,MSFT.US
POLL_INTERVAL ?= 5s

.PHONY: help up topics producer processor test test-unit test-bdd build connect connector connector-update superset superset-env superset-setup logs down

help:
	@printf '%s\n' \
	  'Démarrer : make up (Kafka + Superset), make topics, puis make processor et make producer dans deux terminaux' \
	  'Tester   : make test | make test-unit | make test-bdd' \
	  'Compiler : make build' \
	  'Stocker  : make connect, puis make connector quand son API est disponible' \
	  'Afficher : make superset' \
	  'Observer : make logs' \
	  'Arrêter  : make down'

up: superset-env
	docker compose up -d --build --wait --wait-timeout 240 zookeeper kafka kafka-ui superset

topics:
	@set -e; for topic in eod eod-agg-1m eod-agg-1h eod-agg-daily eod-dlt; do \
	  docker compose exec -T kafka kafka-topics \
	    --bootstrap-server kafka:29092 --create --if-not-exists \
	    --topic "$$topic" --partitions 1 --replication-factor 1; \
	done

producer:
	cd apps/producer && go run . -stooq -symbols "$(SYMBOLS)" -poll-interval "$(POLL_INTERVAL)"

processor:
	cd apps/processor && sbt run

test:
	cd apps/producer && go test ./...
	cd apps/processor && sbt test

test-unit:
	cd apps/producer && go test ./...
	cd apps/processor && sbt unitTest

test-bdd:
	cd apps/processor && sbt bddTest

build:
	mkdir -p build
	cd apps/producer && go build -o ../../build/producer .
	cd apps/processor && sbt test package

connect:
	docker compose up -d --build kafka-connect

connector:
	curl --fail-with-body -X POST http://localhost:8083/connectors \
	  -H 'Content-Type: application/json' \
	  --data @infra/kafka-connect/clickhouse-sink.json

connector-update:
	python3 -c 'import json; print(json.dumps(json.load(open("infra/kafka-connect/clickhouse-sink.json"))["config"]))' | \
	  curl --fail-with-body -X PUT http://localhost:8083/connectors/clickhouse-sink/config \
	    -H 'Content-Type: application/json' --data-binary @-

superset-env:
	python3 infra/superset/init_env.py

superset: superset-env
	docker compose up -d --build --wait --wait-timeout 240 superset

superset-setup:
	docker compose exec -T superset python /app/streamloom/provision.py

logs:
	docker compose logs --tail=100 -f

down:
	docker compose down
