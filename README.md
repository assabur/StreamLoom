
# StreamLoom

StreamLoom recupere en temps reel des actifs de marche (Stooq), les agrege via Kafka Streams,
puis stocke les resultats dans ClickHouse Cloud.

Ce README explique comment lancer le producer Go et l'architecture de bout en bout.

## Architecture (bout en bout)

```text
Go producer (Stooq / stdin)
            |
            v
Kafka topic: eod
            |
            v
Kafka Streams (parse + validation)
            |                         \
            | valides                  \ invalides
            v                          v
normalisation                     DLT topic: eod-dlt
            |
            v
aggregations (fenetres 1m, 1d)
            |                 \
            v                  v
eod-agg-1m                  eod-agg-daily
            \                 /
             v               v
        Kafka Connect (sink)
                  |
                  v
ClickHouse Cloud: eod_agg_1m, eod_agg_daily, eod_dlt
```

## Lire ce README

Depuis la racine du projet :

```bash
cat README.md
```

Ou ouvre le fichier dans ton editeur.

## Prerequis

- Docker + Docker Compose (pour Kafka en local)
- Go 1.21+

## Lancer Kafka en local

Depuis la racine du projet :

```bash
docker compose up -d
```

Kafka est expose sur `localhost:9092` et l'interface Kafka UI sur `http://localhost:8080`.

## Lancer le producer

Le producer se trouve dans `src/go/eod-producer`.

```bash
cd src/go/eod-producer
go run .
```

Par defaut, il envoie 1 message vers le topic `eod` sur `localhost:9092`.

### Env vars disponibles

- `BROKERS` (ex: `localhost:9092`)
- `TOPIC` (ex: `eod`)
- `CLIENT_ID` (ex: `eod-producer`)
- `SYMBOLS` (ex: `AAPL.US,MSFT.US`)
- `STOOQ_URL` (ex: `https://stooq.com/q/l/?s=%s&f=sd2t2ohlcv&h&e=csv`)

### Exemples d'utilisation

Envoyer un message simple :

```bash
go run . -message "hello"
```

Envoyer plusieurs messages avec intervalle :

```bash
go run . -message "ping" -count 5 -interval 1s
```

Lire depuis stdin (1 ligne = 1 message) :

```bash
printf "a\nb\nc\n" | go run . -stdin
```

Publier des quotes Stooq en continu :

```bash
go run . -stooq -symbols "AAPL.US,MSFT.US" -poll-interval 5s
```

### Parametres principaux

- `-brokers` : liste de brokers (virgulee)
- `-topic` : topic Kafka
- `-client-id` : client id Kafka
- `-key` : cle des messages
- `-message` : payload du message
- `-count` : nombre de messages
- `-interval` : delai entre messages
- `-stdin` : lit depuis stdin
- `-stooq` : active la source Stooq
- `-symbols` : symboles Stooq (virgulee)
- `-poll-interval` : frequence de polling Stooq
- `-stooq-url` : template URL Stooq avec `%s`

## Kafka Streams

Le module Kafka Streams lit `eod` et ecrit `eod-agg-1m`, `eod-agg-daily` et `eod-dlt`.

```bash
cd src/java/eod-aggregator-streams
./gradlew run
```

## Kafka Connect -> ClickHouse Cloud

Kafka Connect consomme `eod-agg-1m`, `eod-agg-daily`, `eod-dlt` et ecrit dans ClickHouse.
Les tables cibles sont mappees vers :

- `eod-agg-1m` -> `eod_agg_1m`
- `eod-agg-daily` -> `eod_agg_daily`
- `eod-dlt` -> `eod_dlt`

### Configuration

1) Cree un fichier `.env` a partir de `.env.example`.
2) Renseigne :

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`

### Demarrer Kafka Connect

```bash
docker compose up -d kafka-connect
```

### Creer le connector

```bash
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @connectors/clickhouse-sink.json
```

## Grafana (dashboard demo)

Un dashboard Grafana est disponible dans `dashboards/grafana-eod-agg-1m.json`.

### Demarrer Grafana

```bash
docker compose up -d grafana
```

### Importer le dashboard

1) Ouvre `http://localhost:3000` (login: `admin` / `admin`).
2) Va sur **+ -> Import**.
3) Charge `dashboards/grafana-eod-agg-1m.json`.
4) Selectionne la datasource ClickHouse.
