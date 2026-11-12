# language: fr
Fonctionnalité: Transformer les cotations en bougies OHLCV
  Les cotations valides sont regroupées par symbole.
  Les messages invalides sont conservés dans la DLT pour diagnostic.

  Scénario: Attendre la fermeture avant de publier une bougie minute
    Étant donné les cotations suivantes reçues par Kafka :
      | symbole | ouverture | haut | bas | cloture | volume | seconde |
      | aapl.us | 100       | 110  | 90  | 105     | 10     | 0       |
      | aapl.us | 105       | 120  | 95  | 115     | 20     | 30      |
    Alors aucun agrégat ne doit encore être publié
    Quand le temps du flux dépasse la fenêtre minute et sa grâce
    Alors la bougie "1m" pour "AAPL.US" contient :
      | ouverture | haut | bas | cloture | volume | nombre |
      | 100       | 120  | 90  | 115     | 30     | 2      |

  Scénario: Calculer une bougie journalière
    Étant donné les cotations suivantes reçues par Kafka :
      | symbole | ouverture | haut | bas | cloture | volume | seconde |
      | aapl.us | 100       | 110  | 90  | 105     | 10     | 0       |
      | aapl.us | 105       | 120  | 95  | 115     | 20     | 3600    |
    Quand le temps du flux dépasse la fenêtre journalière et sa grâce
    Alors la bougie "1d" pour "AAPL.US" contient :
      | ouverture | haut | bas | cloture | volume | nombre |
      | 100       | 120  | 90  | 115     | 30     | 2      |

  Scénario: Garder une bougie distincte pour chaque symbole
    Étant donné les cotations suivantes reçues par Kafka :
      | symbole | ouverture | haut | bas | cloture | volume | seconde |
      | aapl.us | 100       | 110  | 90  | 105     | 10     | 0       |
      | msft.us | 200       | 210  | 190 | 205     | 20     | 30      |
    Quand le temps du flux dépasse la fenêtre minute et sa grâce
    Alors les bougies minute sont séparées par symbole

  Scénario: Conserver un message illisible dans la DLT
    Quand je publie le message "hello"
    Alors la DLT contient le message original "hello" et une erreur de parsing
    Et aucun agrégat ne doit encore être publié

  Scénario: Rejeter un volume négatif
    Quand je publie une cotation avec un volume négatif
    Alors la DLT contient l'erreur "invalid_volume"
    Et aucun agrégat ne doit encore être publié

  Scénario: Calculer une bougie horaire
    Étant donné les cotations suivantes reçues par Kafka :
      | symbole | ouverture | haut | bas | cloture | volume | seconde |
      | aapl.us | 100       | 110  | 90  | 105     | 10     | 0       |
      | aapl.us | 105       | 120  | 95  | 115     | 20     | 3599    |
    Quand le temps du flux dépasse la fenêtre horaire et sa grâce
    Alors la bougie "1h" pour "AAPL.US" contient :
      | ouverture | haut | bas | cloture | volume | nombre |
      | 100       | 120  | 90  | 115     | 30     | 2      |
