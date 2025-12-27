
# Streamloon a pour but de recuperer en temps réels les actifs sur le marché notament sur stook les agreger et ensuite les stocker sur Clikhousse



Ce README explique comment lancer le producer Go qui publie des messages EOD (end of day) sur Kafka.

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

## Notes

- Le topic `eod` doit exister sur Kafka (l'auto-creation est generalement active en local).
