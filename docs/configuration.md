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

`-count` ajoute un suffixe de numérotation à chaque message lorsqu’il dépasse 1. Pour publier des cotations JSON, utiliser le mode Stooq ou `-stdin` (une ligne JSON par message, lignes vides ignorées) :

```bash
printf '%s\n' '{"symbol":"AAPL.US","date":"2026-11-12","time":"15:30:00","open":225,"high":226,"low":224,"close":225.5,"volume":1000}' \
  | go run . -stdin
```

### Kafka Streams

| Variable | Valeur par défaut |
| --- | --- |
| `BROKERS` | `localhost:9092` |
| `APP_ID` | `eod-aggregator-streams` |
| `INPUT_TOPIC` | `eod` |
| `OUTPUT_1M_TOPIC` | `eod-agg-1m` |
| `OUTPUT_1H_TOPIC` | `eod-agg-1h` |
| `OUTPUT_DAILY_TOPIC` | `eod-agg-1h` | `eod_agg_1h` | Agrégats horaires |
| `eod-agg-daily` |
| `DLT_TOPIC` | `eod-dlt` |

Ces variables doivent être définies dans le terminal qui lance l’application. Le fichier `.env` de Docker Compose n’est pas chargé automatiquement par les programmes Go et Scala.

## Stockage dans ClickHouse Cloud

### Préparer la connexion et les tables

Copier `.env.example` vers `.env` si ce fichier n’existe pas encore, puis renseigner les valeurs de l’instance :

```dotenv
CLICKHOUSE_HOST=your-instance.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=replace-me
```

Utiliser un nom d’hôte sans préfixe `https://`. Le connecteur est configuré pour TLS. Le dépôt fournit un fichier `.env.example`. Aucun script de création des tables n’est fourni : créer au préalable les tables avec des colonnes et des types compatibles avec les JSON produits.

| Topic Kafka | Table ClickHouse | Contenu |
| --- | --- | --- |
| `eod-agg-1m` | `eod_agg_1m` | Agrégats par minute |
| `eod-agg-1h` | `eod_agg_1h` | Agrégats horaires |
| `eod-agg-daily` | `eod_agg_daily` | Agrégats journaliers |
| `eod-dlt` | `eod_dlt` | `raw`, `error`, `ingest_ts` |

Le mapping est défini dans [infra/kafka-connect/clickhouse-sink.json](../infra/kafka-connect/clickhouse-sink.json). Les agrégats sont sérialisés avec des noms de champs en `snake_case`.

### Ajouter la série horaire à une installation existante

Les fenêtres horaires couvrent une heure fixe en UTC, par exemple `[14:00, 15:00[`. Une bougie `1h` regroupe directement les cotations de cette heure par symbole, avec une grâce de cinq minutes. Elle est publiée lorsque le temps du flux atteint la fin de fenêtre et la grâce ; sans nouveaux messages le temps du flux n’avance pas.

1. Dans la console SQL ClickHouse Cloud, créer la table horaire avec la même structure et le même moteur que la table minute (remplacer `DB` par le nom de la base) :

```sql
CREATE TABLE IF NOT EXISTS DB.eod_agg_1h AS DB.eod_agg_1m;
```

2. Créer le nouveau topic et mettre à jour le connecteur déjà enregistré :

```bash
make topics
make connector-update
```

3. Arrêter l’ancien processor avec `Ctrl+C`, puis le relancer avec `make processor`.
4. Reconstruire Superset et ajouter les graphiques horaires au dashboard existant :

```bash
make superset
make superset-setup
```

Cette série est alimentée par les nouvelles cotations ; l’historique existant n’est pas recalculé. Pour une nouvelle installation, créer aussi `eod_agg_1h` avant d’enregistrer le connecteur avec `make connector`.

### Démarrer et enregistrer le connecteur

```bash
docker compose up -d --build kafka-connect
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

`make up` démarre Kafka et Superset ensemble. Pour démarrer uniquement Superset et ses dépendances :

```bash
make superset
```

Cette commande génère les secrets Superset manquants dans `.env`, construit l’image avec le pilote ClickHouse, puis démarre PostgreSQL, l’initialisation et le serveur web. Python 3 est nécessaire pour générer les secrets. Les migrations et la création du compte administrateur sont automatiques ; le mot de passe d’un compte existant n’est pas réinitialisé.

1. Attendre que `docker compose ps` indique Superset en bonne santé (`healthy`).
2. Ouvrir <http://localhost:8088>.
3. Se connecter avec `SUPERSET_ADMIN_USERNAME` (`admin` par défaut) et `SUPERSET_ADMIN_PASSWORD`, consultable dans le fichier local `.env`.
4. Créer le dashboard :

```bash
make superset-setup
```

Ouvrir ensuite <http://localhost:8088/superset/dashboard/streamloom/>.

Le serveur écoute sur l’interface locale uniquement. Cette configuration sert au développement local, sans workers de rapports programmés. PostgreSQL conserve les comptes, datasets, graphiques et connexions ; les cotations restent dans ClickHouse Cloud. Aucun service Kafka n’est nécessaire pour démarrer Superset et explorer un historique existant.

### Connexion ClickHouse et graphiques

`make superset-setup` utilise les variables `CLICKHOUSE_*` du fichier `.env`. La connexion utilise HTTPS (`secure=true`, port `8443` par défaut). Autoriser l’adresse IP de sortie de la machine dans ClickHouse Cloud. Le nom de base doit contenir uniquement des lettres, chiffres et caractères `_`, sans commencer par un chiffre.

Le script [provision.py](../infra/superset/provision.py) crée :

| Dataset | Source | Graphiques |
| --- | --- | --- |
| `streamloom_minute` | `eod_agg_1m` | Clôture et volumes par minute, par symbole |
| `streamloom_heure` | `eod_agg_1h` | Clôture et volumes par heure, par symbole |
| `streamloom_jour` | `eod_agg_daily` | Clôture et volumes par jour, par symbole |
| `streamloom_derniere_seance` | Dernière journée disponible dans `eod_agg_1m` | Top 10 volumes et variations en pourcentage |

Les datasets convertissent `window_start` en timestamp UTC et exposent les champs OHLCV. La variation de séance utilise l’ouverture de la première fenêtre et la clôture de la dernière, avec un résultat nul si l’ouverture vaut zéro. Les volumes restent ceux calculés par le pipeline, y compris les répétitions de cotations.

Les graphiques temporels affichent les sept derniers jours par défaut. Le dashboard se rafraîchit toutes les 30 secondes. Dans Superset, modifier un graphique pour choisir une autre période ou ajouter un filtre sur `symbol` ; les filtres natifs de dashboard peuvent aussi être ajoutés depuis son mode édition.

La commande de préparation peut être relancée : elle crée les éléments manquants et ajoute les nouveaux graphiques au dashboard existant en conservant sa disposition et ses filtres. En cas de changement des identifiants ClickHouse, mettre aussi à jour la connexion **StreamLoom ClickHouse** dans **Settings → Database Connections** ; une relance ne remplace pas la connexion existante.

### Secrets et persistance

| Variable | Usage |
| --- | --- |
| `SUPERSET_SECRET_KEY` | Signature des sessions et chiffrement des identifiants de connexion |
| `SUPERSET_DB_PASSWORD` | Accès au PostgreSQL de métadonnées |
| `SUPERSET_ADMIN_USERNAME` | Nom du compte initial, `admin` par défaut |
| `SUPERSET_ADMIN_PASSWORD` | Mot de passe du compte initial |

Conserver le fichier `.env` avec le volume `superset-db-data`. Changer uniquement le mot de passe PostgreSQL dans `.env` ne change pas celui d’un volume déjà initialisé. Changer uniquement le mot de passe administrateur dans `.env` ne modifie pas un compte déjà créé ; utiliser l’interface Superset pour ce changement.

Références : [configuration Superset](https://superset.apache.org/admin-docs/configuration/configuring-superset/) et [pilote ClickHouse](https://superset.apache.org/docs/databases/supported/clickhouse/).

## Arrêt et diagnostic

Arrêter les applications Go et Scala avec `Ctrl+C`, puis les services Docker :

```bash
docker compose down
```

Les volumes Kafka, ZooKeeper et PostgreSQL Superset sont conservés. Éviter `docker compose down -v`, qui supprime ces données.

Pour examiner les services :

```bash
docker compose ps
docker compose logs --tail=100 kafka kafka-connect superset-init superset
```

Si aucun agrégat n’apparaît, vérifier les messages dans `eod` et `eod-dlt`, puis attendre la fermeture des fenêtres avec un flux continu. Si ClickHouse reste vide, vérifier l’état des tâches du connecteur, les identifiants et l’existence des tables.

Si Superset ne démarre pas, vérifier les logs de `superset-init`, puis `superset-db`. Si les graphiques échouent, tester la connexion ClickHouse dans Superset, vérifier les autorisations réseau et les tables. Des graphiques vides peuvent simplement indiquer qu’aucune donnée ne correspond aux sept derniers jours.

Une erreur `NameResolutionError` indique que le nom `CLICKHOUSE_HOST` ne se résout pas depuis Docker : vérifier le nom de l’instance ClickHouse Cloud et la résolution DNS avant de tester les graphiques.
