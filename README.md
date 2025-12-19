# streamloom-markets

Pipeline de marché temps réel utilisant Kafka Streams, ClickHouse et Go pour ingérer les agrégats Binance Spot (aggTrade) et exposer des métriques prêtes pour un entretien.

## Stack
- Kafka (mode KRaft) + Kafka UI
- Kafka Streams (Java 17)
- Go microservices : `binance-ingestor`, `clickhouse-sink`, `api`
- ClickHouse pour le stockage analytique
- Prometheus + Grafana pour l'observabilité
- Docker Compose pour un run local complet

## Démarrage rapide
```bash
make up
make create-topics
```
Les services sont disponibles sur :
- API : http://localhost:8088/health
- Kafka UI : http://localhost:8080
- Grafana : http://localhost:3000 (admin/admin)
- Prometheus : http://localhost:9090

## Architecture
1. **binance-ingestor** (Go) : WebSocket Binance `aggTrade` (BTCUSDT, ETHUSDT, SOLUSDT) -> Kafka topic `raw_trades`. Gestion reconnection/ping et métriques Prometheus.
2. **trade-streams** (Kafka Streams) : nettoyage, typage, enrichissement (base/quote, notional, session), déduplication (symbol + trade_id), fenêtres 1m pour OHLC/volume/VWAP/volatilité. Sortie vers topics `enriched_trades` et `candles_1m`.
3. **clickhouse-sink** (Go) : consommateurs Kafka -> ClickHouse (tables `trades_enriched`, `candles_1m`) avec batch + retries + métriques.
4. **api** (Go) : REST stateless qui lit ClickHouse.
5. **Observabilité** : Prometheus scrape (ports 2112) + dashboard Grafana fourni.

## Topics Kafka
- `raw_trades`: events aggTrade bruts.
- `enriched_trades`: trades enrichis normalisés.
- `candles_1m`: agrégations 1 minute.

## Schéma JSON `raw_trades`
```json
{
  "event_time": 1700000000000,
  "symbol": "BTCUSDT",
  "price": 42000.5,
  "qty": 0.01,
  "trade_id": 12345,
  "is_buyer_maker": true,
  "source": "binance",
  "ingest_ts": 1700000000500
}
```

## ClickHouse
Voir `clickhouse/schema.sql` pour DDL et requêtes prêtes à copier-coller.

## Endpoints API
- `GET /health`
- `GET /symbols`
- `GET /candles?symbol=BTCUSDT&from=...&to=...&interval=1m`
- `GET /stats/volume?symbol=BTCUSDT&window=1h`
- `GET /stats/spikes?symbol=BTCUSDT&threshold=0.03`

## Observabilité
Chaque service Go exporte des compteurs/histogrammes Prometheus (`events_received_total`, `events_published_total`, `insert_latency_ms`, `http_requests_total`, ...). Dashboard JSON dans `deploy/grafana/dashboard.json`.

## Tests
- Tests unitaires Kafka Streams (Topologie OHLC/VWAP) via Gradle.
- Script `scripts/smoke-test.sh` pour vérifier Kafka + ClickHouse + production d'un event.

## Design decisions
- KRaft simplifie le bootstrap local sans Zookeeper.
- kafka-go utilisé côté Go pour limiter les dépendances.
- ReplacingMergeTree pour `candles_1m` afin d'accepter les corrections tardives.
- Pas de Schema Registry pour réduire la surface; JSON explicite documenté.

## Trade-offs
- Déduplication simple par fenêtre (symbol, trade_id) peut laisser passer des duplicats hors fenêtre.
- Pas d'exactly-once : au moins une fois avec idempotence côté ClickHouse via clés.
- Tests d'intégration limités à un smoke test pour garder la CI rapide.

## Notes légales
- Données publiques Binance Spot via WebSocket officiel `wss://stream.binance.com:9443`. Pas de scraping, pas de conseil ou exécution de trading.

## Next steps
- Ajouter Schema Registry + Avro/Protobuf.
- Ajouter backfill/batch vers ClickHouse et compaction.
- Sécuriser Kafka (SASL/ACL) et gérer les secrets via Vault.
- Exactly-once (transactions Kafka ou outbox) et réplication multi-AZ.

