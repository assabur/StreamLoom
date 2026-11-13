# Configuration et exploitation

[Retour au README](../README.md) · [Comprendre le code et les tests](developpement.md)

Toutes les commandes Docker et curl de ce guide se lancent depuis la racine du dépôt. Les commandes `go run` se lancent dans `apps/producer`.

## Variables des applications

### Producteur Go

| Variable | Valeur par défaut | Option équivalente |
| --- | --- | --- |
| `BROKERS` | `localhost:9092` | `-brokers` |
| `TOPIC` | `eod` | `-topic` |
| `CLIENT_ID` | `eod-producer` | `-client-id` |
| `SYMBOLS` | `AAPL.US` | `-symbols` |
| `STOOQ_URL` | `https://stooq.com/q/l/?s=%s&f=sd2t2ohlcv&h&e=csv` | `-stooq-url` |

Les options de ligne de commande prennent le pas sur ces variables. `-poll-interval` vaut `1s` par défaut. `-key` permet de fixer la clé Kafka ; le traitement Scala regroupe ensuite par symbole.

Depuis la racine, entrer dans `apps/producer` avec `cd apps/producer`, puis publier un message de diagnostic :

```bash
go run . -message "hello"
```

Ce texte ne respecte pas le format d’une cotation : il sera envoyé dans `eod-dlt` si Kafka Streams est actif.

Publier plusieurs messages texte :

```bash
go run . -message "ping" -count 5 -interval 1s
```

`-count` ajoute un suffixe de numérotation à chaque message lorsqu’il dépasse 1. Pour publier des cotations JSON, utiliser le mode simulation, Stooq ou `-stdin` (une ligne JSON par message, lignes vides ignorées) :

```bash
printf '%s\n' '{"symbol":"AAPL.US","date":"2026-11-12","time":"15:30:00","open":225,"high":226,"low":224,"close":225.5,"volume":1000}' \
  | go run . -stdin
```

### Simulation locale

`make producer` utilise le mode `-simulate` et ne contacte aucune API. Les cours et les volumes sont fictifs, avec `source=simulation`. Chaque symbole démarre avec un prix arbitraire ; chaque tour fait varier son prix autour du précédent. `open` est la clôture du tour précédent, `high` et `low` encadrent ouverture et clôture, et le volume est un nombre positif généré pour cet intervalle.

```bash
make producer SYMBOLS=AAPL.US,MSFT.US POLL_INTERVAL=1s
```

| Variable Make | Option Go | Défaut | Rôle |
| --- | --- | --- | --- |
| `SYMBOLS` | `-symbols` | `AAPL.US,MSFT.US` via Make | Symboles générés |
| `POLL_INTERVAL` | `-poll-interval` | `5s` via Make | Attente réelle entre les tours |
| `SEED` | `-seed` | `1` | Reproduire les prix et volumes d’une exécution |
| `SIMULATION_ROUNDS` | `-simulation-rounds` | `0` | Nombre de tours ; zéro signifie continu |
| `SIMULATION_STEP` | `-simulation-step` | `0s` | Pas temporel fictif ; zéro utilise l’heure réelle |
| `SIMULATION_START` | `-simulation-start` | Vide | Début fictif RFC3339, nécessite un pas positif |

Le producteur affiche le nombre de cotations publiées et leur timestamp. Par défaut, les timestamps du JSON et de Kafka suivent l’heure réelle. Les clés Kafka correspondent aux symboles, sauf si `-key` est explicitement fourni. Une seule source peut être sélectionnée : `-simulate`, `-stooq`, `-stdin` ou `-message`.

Pour tester rapidement les agrégations horaires et journalières, choisir un début récent dans le passé et faire avancer le temps plus vite que la publication :

```bash
make producer POLL_INTERVAL=100ms SIMULATION_STEP=1m SIMULATION_ROUNDS=1446 \
  SIMULATION_START="$(date -u -v-2d +%Y-%m-%dT00:00:00Z)"
```

Cet exemple utilise la commande `date` de macOS : il rejoue environ une journée en quelques minutes. Les fenêtres suivent les timestamps Kafka simulés. Un processor dont le temps du flux est déjà plus récent peut rejeter ces événements comme tardifs : pour ce replay, démarrer un processor distinct avec un nouvel `APP_ID` avant la simulation. En mode normal, laisser `SIMULATION_STEP=0s`.

Une simulation finie ne ferme pas automatiquement toutes les dernières fenêtres : il faut des événements plus récents que la fin de fenêtre et sa grâce. Dans Superset, choisir une plage couvrant les dates simulées ; des dates futures sont hors de la plage par défaut.

Pour la collecte Stooq optionnelle, lancer `make producer-stooq`. Les erreurs de disponibilité de cette API n’affectent pas le mode simulation.

### Kafka Streams

| Variable | Valeur par défaut |
| --- | --- |
| `BROKERS` | `localhost:9092` |
| `APP_ID` | `eod-aggregator-streams` |
| `INPUT_TOPIC` | `eod` |
| `OUTPUT_1M_TOPIC` | `eod-agg-1m` |
| `OUTPUT_1H_TOPIC` | `eod-agg-1h` |
| `OUTPUT_DAILY_TOPIC` | `eod-agg-daily` |
| `DLT_TOPIC` | `eod-dlt` |

Ces variables doivent être définies dans le terminal qui lance l’application. Le fichier `.env` de Docker Compose n’est pas chargé automatiquement par les programmes Go et Scala.

## Stockage dans ClickHouse Docker

### Démarrage et tables

```bash
make up
```

ClickHouse démarre avec Kafka et Superset. Son image est `clickhouse/clickhouse-server:25.8`. Le volume `clickhouse-data` conserve les cotations. Le [script d’initialisation](../infra/clickhouse/init.sh) applique le [schéma SQL](../infra/clickhouse/schema.sql) lors de la création du volume.

| Topic Kafka | Table ClickHouse | Contenu |
| --- | --- | --- |
| `eod-agg-1m` | `eod_agg_1m` | Agrégats par minute |
| `eod-agg-1h` | `eod_agg_1h` | Agrégats horaires |
| `eod-agg-daily` | `eod_agg_daily` | Agrégats journaliers |
| `eod-dlt` | `eod_dlt` | `raw`, `error`, `ingest_ts` |

Les tables utilisent le moteur `MergeTree`. Les timestamps ISO sont stockés comme chaînes, conformément aux JSON du processor ; Superset les convertit en dates UTC pour les graphiques. Les agrégats sont triés par symbole et début de fenêtre, et partitionnés par mois.

Les fenêtres horaires couvrent une heure fixe en UTC, par exemple `[14:00, 15:00[`, avec une grâce de cinq minutes. Les séries minute, heure et jour sont calculées directement depuis les nouvelles cotations ; un historique existant n’est pas recalculé.

### Identifiants et réseau

| Paramètre | Valeur |
| --- | --- |
| Hôte depuis Kafka Connect et Superset | `clickhouse` |
| Port HTTP interne | `8123` |
| HTTP depuis la machine | `http://localhost:8123` |
| Base | `CLICKHOUSE_DATABASE`, `streamloom` par défaut |
| Utilisateur | `CLICKHOUSE_USER`, `streamloom` par défaut |
| Mot de passe | `CLICKHOUSE_PASSWORD` dans `.env`, généré s’il est absent |

Les services utilisent HTTP sur le réseau Docker local ; le port publié est limité à `127.0.0.1`. Les anciennes variables `CLICKHOUSE_HOST` et `CLICKHOUSE_PORT` ne sont plus utilisées. Les identifiants existants dans `.env` sont conservés et servent à créer l’utilisateur local.

Pour personnaliser la base et l’utilisateur avant le premier démarrage, copier `.env.example` vers `.env` s’il n’existe pas encore. Le nom de base doit être un identifiant SQL simple (lettres, chiffres et `_`, sans chiffre initial).

### Passer d’une ancienne configuration à Docker

```bash
make up
make topics
make connect
# Attendre que l’API Kafka Connect réponde sur localhost:8083.
make connector-update
make superset-setup
```

Pour un connecteur qui n’a jamais été enregistré, utiliser `make connector` à la place de `make connector-update`. Superset met à jour la connexion existante **StreamLoom ClickHouse** tout en conservant les graphiques et les filtres.

Les anciennes données distantes ne sont pas copiées dans le volume local. Un connecteur déjà enregistré conserve ses offsets Kafka : seuls les messages non encore consommés seront écrits dans la nouvelle base.

Le schéma est appliqué au premier démarrage d’un volume ClickHouse vide. Pour ajouter une table manquante dans un volume existant sans effacer les données :

```bash
docker compose exec -T clickhouse /docker-entrypoint-initdb.d/01-streamloom.sh
```

### Démarrer et enregistrer le connecteur

```bash
make connect
```

Une fois l’API REST disponible :

```bash
curl --fail-with-body -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @infra/kafka-connect/clickhouse-sink.json
```

Cette création est à effectuer une seule fois. Pour vérifier l’état du connecteur et de ses tâches :

```bash
curl --fail-with-body http://localhost:8083/connectors/clickhouse-sink/status
```

## Tableaux de bord Apache Superset

### Démarrer Superset

`make up` démarre Kafka, ClickHouse et Superset ensemble. Pour démarrer uniquement Superset et ses dépendances :

```bash
make superset
```

Cette commande génère les secrets locaux manquants dans `.env`, construit l’image avec le pilote ClickHouse, puis démarre ClickHouse, PostgreSQL, l’initialisation et le serveur web. Python 3 est nécessaire pour générer les secrets. Les migrations et la création du compte administrateur sont automatiques ; le mot de passe d’un compte existant n’est pas réinitialisé.

1. Attendre que `docker compose ps` indique Superset en bonne santé (`healthy`).
2. Ouvrir <http://localhost:8088>.
3. Se connecter avec `SUPERSET_ADMIN_USERNAME` (`admin` par défaut) et `SUPERSET_ADMIN_PASSWORD`, consultable dans le fichier local `.env`.
4. Créer le dashboard :

```bash
make superset-setup
```

Ouvrir ensuite <http://localhost:8088/superset/dashboard/streamloom/>.

Le serveur écoute sur l’interface locale uniquement. Cette configuration sert au développement local, sans workers de rapports programmés. PostgreSQL conserve les comptes, datasets, graphiques et connexions ; les cotations restent dans le volume ClickHouse local. Aucun service Kafka n’est nécessaire pour démarrer Superset et explorer un historique existant.

### Connexion ClickHouse et graphiques

`make superset-setup` configure la connexion vers `clickhouse:8123` en HTTP (`secure=false`) avec les identifiants locaux du fichier `.env`. Il remplace aussi l’ancienne adresse dans une connexion déjà enregistrée.

Le script [provision.py](../infra/superset/provision.py) crée :

| Dataset | Source | Graphiques |
| --- | --- | --- |
| `streamloom_minute` | `eod_agg_1m` | Clôture et volumes par minute, par symbole |
| `streamloom_heure` | `eod_agg_1h` | Clôture et volumes par heure, par symbole |
| `streamloom_jour` | `eod_agg_daily` | Clôture et volumes par jour, par symbole |
| `streamloom_derniere_seance` | Dernière journée disponible dans `eod_agg_1m` | Top 10 volumes et variations en pourcentage |

Les datasets convertissent `window_start` en timestamp UTC et exposent les champs OHLCV. La variation de séance utilise l’ouverture de la première fenêtre et la clôture de la dernière, avec un résultat nul si l’ouverture vaut zéro. Les volumes restent ceux calculés par le pipeline, y compris les répétitions de cotations.

Les graphiques temporels affichent les sept derniers jours par défaut. Le dashboard se rafraîchit toutes les 30 secondes. Dans Superset, modifier un graphique pour choisir une autre période ou ajouter un filtre sur `symbol` ; les filtres natifs de dashboard peuvent aussi être ajoutés depuis son mode édition.

La commande de préparation peut être relancée : elle crée les éléments manquants et ajoute les nouveaux graphiques au dashboard existant en conservant sa disposition et ses filtres. En cas de changement des identifiants ClickHouse, relancer `make superset`, puis `make superset-setup` pour actualiser la connexion.

### Secrets et persistance

| Variable | Usage |
| --- | --- |
| `CLICKHOUSE_PASSWORD` | Mot de passe de l’utilisateur ClickHouse local |
| `SUPERSET_SECRET_KEY` | Signature des sessions et chiffrement des identifiants de connexion |
| `SUPERSET_DB_PASSWORD` | Accès au PostgreSQL de métadonnées |
| `SUPERSET_ADMIN_USERNAME` | Nom du compte initial, `admin` par défaut |
| `SUPERSET_ADMIN_PASSWORD` | Mot de passe du compte initial |

Conserver le fichier `.env` avec les volumes `superset-db-data` et `clickhouse-data`. Changer uniquement le mot de passe PostgreSQL dans `.env` ne change pas celui d’un volume déjà initialisé. Changer uniquement le mot de passe administrateur dans `.env` ne modifie pas un compte déjà créé ; utiliser l’interface Superset pour ce changement.

Références : [configuration Superset](https://superset.apache.org/admin-docs/configuration/configuring-superset/) et [pilote ClickHouse](https://superset.apache.org/docs/databases/supported/clickhouse/).

## Arrêt et diagnostic

Arrêter les applications Go et Scala avec `Ctrl+C`, puis les services Docker :

```bash
docker compose down
```

Les volumes Kafka, ZooKeeper, ClickHouse et PostgreSQL Superset sont conservés. Éviter `docker compose down -v`, qui supprime ces données.

Pour examiner les services :

```bash
docker compose ps
docker compose logs --tail=100 kafka kafka-connect clickhouse superset-init superset
```

Si aucun agrégat n’apparaît, vérifier les messages dans `eod` et `eod-dlt`, puis attendre la fermeture des fenêtres avec un flux continu. Si ClickHouse reste vide, vérifier l’état des tâches du connecteur, les identifiants et l’existence des tables.

Si Superset ne démarre pas, vérifier les logs de `superset-init`, puis `superset-db`. Si les graphiques échouent, tester la connexion ClickHouse dans Superset, vérifier les autorisations réseau et les tables. Des graphiques vides peuvent simplement indiquer qu’aucune donnée ne correspond aux sept derniers jours.

Si `clickhouse` ne se résout pas, vérifier que Superset et Kafka Connect sont démarrés via le même projet Docker Compose. Le nom interne `clickhouse` n’est pas utilisable depuis le navigateur de la machine : utiliser `localhost:8123`.

### Erreurs de collecte Stooq

Une réponse HTTP 404 provient du serveur Stooq, avant toute publication Kafka. Le producteur transmet les symboles en minuscules dans la requête et conserve le symbole configuré dans le JSON. Si tous les symboles renvoient 404 pendant un tour de collecte, il s’arrête avec une erreur explicite pour éviter de répéter les mêmes requêtes indéfiniment. Si un seul symbole renvoie 404, les autres continuent d’être traités.

Vérifier l’URL `STOOQ_URL` (ou `-stooq-url`) et la disponibilité du service. L’URL doit retourner un CSV de cotation avec les colonnes `Date`, `Time`, `Open`, `High`, `Low`, `Close`, `Volume` ; une page HTML ou un export historique avec un autre format ne convient pas. Une source indisponible nécessite de rétablir cet accès ou de configurer un autre fournisseur avant de reprendre la collecte réelle.

`make producer up` et `make up producer` lancent les services Docker avant le producteur. Celui-ci reste actif tant que la collecte fonctionne ; lancer `make processor` dans un autre terminal.
