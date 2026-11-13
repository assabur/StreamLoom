# StreamLoom

StreamLoom génère des cotations boursières simulées, les transforme en résumés par minute, par heure et par jour, puis permet de les consulter dans Apache Superset.

Une **bougie OHLCV** contient le prix d’ouverture, le plus haut, le plus bas, le prix de clôture et le volume d’une période.

## Le projet en un schéma

```mermaid
flowchart LR
    source["Cotations fictives"] --> producer["Go : simuler et publier"]
    producer --> kafka["Kafka : transporter"]
    kafka --> aggregator["Scala : calculer"]
    aggregator -->|"via Kafka Connect"| database[("ClickHouse : stocker")]
    database --> superset["Apache Superset : visualiser"]
```

Go et Scala tournent dans deux terminaux. Docker lance Kafka, ClickHouse, Kafka Connect et Superset. Les cotations sont stockées localement dans un volume ClickHouse persistant ; les tables sont créées automatiquement.

## Se repérer dans les dossiers

```text
StreamLoom/
├── apps/                       # Les deux applications
│   ├── producer/               # Go : simulation et envoi vers Kafka
│   │   ├── main.go
│   │   └── go.mod
│   └── processor/              # Scala : validation et calcul des bougies
│       ├── build.sbt
│       ├── project/            # Version de sbt et plugins
│       └── src/
│           ├── main/scala/      # Code de l’application
│           └── test/           # Tests unitaires et scénarios BDD
├── infra/                      # Configuration des services
│   ├── clickhouse/             # Schéma des tables et initialisation
│   ├── kafka-connect/          # Dockerfile et connecteur ClickHouse
│   └── superset/               # Image, configuration et dashboard
├── docs/                       # Configuration et explications du code
├── .env.example                # Modèle de connexion ClickHouse
├── docker-compose.yml          # Services Docker locaux
└── Makefile                    # Commandes à lancer depuis la racine
```

| Je veux… | Je vais dans… |
| --- | --- |
| Modifier la collecte ou les symboles | [apps/producer](apps/producer) |
| Modifier la validation ou les calculs | [apps/processor](apps/processor) |
| Lire les scénarios métier en français | [cotations.feature](apps/processor/src/test/resources/features/cotations.feature) |
| Configurer ClickHouse et Apache Superset | [docs/configuration.md](docs/configuration.md) |
| Comprendre le code et les tests | [docs/developpement.md](docs/developpement.md) |

## Démarrer le pipeline local

Prérequis : **Docker Compose**, **Go 1.21+**, **JDK 17**, **sbt**, **Python 3** et **Make**. sbt télécharge le compilateur Scala et les dépendances lors du premier lancement.

Toutes les commandes `make` se lancent depuis la racine du projet. `make help` affiche les commandes disponibles.

### 1. Lancer Kafka, ClickHouse et Superset

```bash
make up
make topics
```

`make up` génère les secrets locaux manquants dans `.env`, puis démarre ZooKeeper, Kafka, Kafka UI, ClickHouse et Superset avec sa base PostgreSQL. La commande attend que ClickHouse et Superset soient prêts. Attendre que Kafka soit prêt avant `make topics` ; si la commande échoue pendant le démarrage du broker, la relancer. La création des cinq topics peut être répétée sans les recréer.

`make up producer` et `make producer up` préparent tous deux les services avant de lancer le producteur. Pour garder le processor et le producteur actifs ensemble, utiliser deux terminaux comme ci-dessous.

### 2. Lancer l’agrégateur Scala

Dans un premier terminal :

```bash
make processor
```

### 3. Lancer le producteur Go

Dans un deuxième terminal :

```bash
make producer
```

Par défaut, il génère des cotations fictives pour `AAPL.US` et `MSFT.US` toutes les 5 secondes, sans CSV ni API. Les prix évoluent par symbole et les volumes sont générés par intervalle. Tous les messages portent `source=simulation`. Pour choisir les symboles et l’intervalle :

```bash
make producer SYMBOLS=AAPL.US POLL_INTERVAL=10s
```

Pour une simulation courte de dix tours :

```bash
make producer SIMULATION_ROUNDS=10 POLL_INTERVAL=1s
```

`Ctrl+C` arrête la simulation continue. Le mode Stooq reste accessible explicitement avec `make producer-stooq` si le service redevient disponible. Le [guide de configuration](docs/configuration.md#simulation-locale) détaille aussi l’accélération du temps simulé.

### 4. Observer les résultats

Ouvrir [Kafka UI](http://localhost:8080) pour lire les topics :

| Topic | Contenu |
| --- | --- |
| `eod` | Cotations reçues |
| `eod-agg-1m` | Bougies par minute |
| `eod-agg-1h` | Bougies par heure |
| `eod-agg-daily` | Bougies par jour |
| `eod-dlt` | Messages invalides avec leur erreur |

Les bougies sont publiées après fermeture de la fenêtre et du délai de grâce : 1 minute pour les fenêtres minute, 5 minutes pour les fenêtres horaires et journalières. Le temps du flux avance avec les messages reçus ; attendre sans recevoir de nouveaux messages ne ferme pas une fenêtre.

## Que devient une cotation ?

```mermaid
flowchart TD
    input["Cotation reçue"] --> valid{"JSON et données valides ?"}
    valid -->|"Non"| errors["eod-dlt : conserver l’erreur"]
    valid -->|"Oui"| normalize["Normaliser le symbole et les dates"]
    normalize --> calculate["Regrouper par symbole et calculer les bougies"]
    calculate --> output["Publier les résultats minute, heure et jour"]
```

Les fenêtres suivent les timestamps Kafka. Les volumes reçus sont additionnés, y compris les cotations répétées. Les règles détaillées et un exemple JSON sont dans le [guide de développement](docs/developpement.md).

## Lancer les tests

```bash
make test        # Tous les tests Scala et vérification du producteur Go
make test-unit   # Tests unitaires et de topologie et vérification Go
make test-bdd    # Scénarios Cucumber en français uniquement
```

Les tests fonctionnent sans Docker, Stooq ou ClickHouse. Le flux Kafka est simulé en mémoire. Le module Scala comprend **36 tests unitaires et de topologie** et **6 scénarios BDD** ; le producteur Go comprend **12 tests** de collecte HTTP, de génération de cotations et de publication simulée.

Pour exécuter les tests Scala, générer le JAR dans `apps/processor/target/scala-2.13/` et compiler le binaire Go dans `build/producer` :

```bash
make build
```

## Ajouter le stockage et les graphiques

1. Lancer `make up` : les identifiants locaux manquants et les tables ClickHouse sont préparés automatiquement. `.env.example` permet de personnaliser le nom de la base et de l’utilisateur avant le premier démarrage.
2. Lancer `make connect`, puis `make connector` quand l’API Kafka Connect est disponible. Si le connecteur existe déjà, utiliser `make connector-update` pour appliquer la connexion locale.
3. Lancer `make superset-setup` pour créer les datasets et les graphiques, ou reconnecter le dashboard existant au ClickHouse local.

| Service | Adresse locale |
| --- | --- |
| Kafka | `localhost:9092` |
| Kafka UI | <http://localhost:8080> |
| Kafka Connect | <http://localhost:8083> |
| ClickHouse HTTP | <http://localhost:8123> |
| Apache Superset | <http://localhost:8088> |

Se connecter avec `admin` et la valeur `SUPERSET_ADMIN_PASSWORD` du fichier `.env`. Le [guide de configuration](docs/configuration.md#tableaux-de-bord-apache-superset) détaille les datasets et les graphiques.

## Arrêter et diagnostiquer

`Ctrl+C` arrête les applications Go et Scala dans leurs terminaux.

```bash
make down       # Arrêter les services Docker en conservant les volumes Kafka
make logs       # Suivre les logs des services Docker
```

Les cotations ClickHouse et les métadonnées Superset sont conservées dans leurs volumes Docker. Garder aussi les secrets du fichier `.env`. Les commandes détaillées et les pistes de diagnostic sont dans [docs/configuration.md](docs/configuration.md#arrêt-et-diagnostic).
