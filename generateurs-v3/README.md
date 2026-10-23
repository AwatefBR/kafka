# Générateur de données — Projets Kafka ESGI

Un générateur de flux temps réel par projet, le même que celui qui tourne sur
le cluster partagé. Chaque flux mélange :

- des **données réalistes** (popularité zipfienne, sessions, paiements
  différés, géolocalisation…),
- des **données sales** comme en production : champs manquants ou `null`,
  types invalides (`"amount": "douze"`), valeurs hors bornes, enums inconnues,
  timestamps illisibles, JSON tronqué, messages vides, non-JSON, **doublons**
  et **événements en retard** (30 à 180 min),
- des **incidents périodiques** que votre pipeline doit détecter (fraude,
  surge, bot, aimbot…) : ils sont décrits dans le README de votre projet.

| Projet | Inspiration | Topics produits |
|---|---|---|
| `binge` | Netflix | `binge.playback.events`, `binge.catalog` (compacté) |
| `cartflow` | Amazon | `cartflow.orders`, `cartflow.payments`, `cartflow.stock.movements` |
| `dispatch` | Uber | `dispatch.ride.requests`, `dispatch.driver.locations`, `dispatch.trip.events` |
| `viral` | Instagram | `viral.posts`, `viral.interactions` |
| `tempo` | Spotify | `tempo.listening.events`, `tempo.tracks` (compacté) |
| `sentinel` | Stripe | `sentinel.transactions`, `sentinel.merchants` (compacté) |
| `relay` | Criteo | `relay.impressions`, `relay.clicks`, `relay.campaigns` (compacté) |
| `arena` | Riot Games | `arena.match.events`, `arena.purchases`, `arena.players` (compacté) |
| `forge` | Siemens MindSphere | `forge.readings`, `forge.production`, `forge.machines` (compacté) |

## Installation

Python 3.9 ou plus.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

```powershell
# Windows PowerShell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Générer en temps réel vers votre cluster local

Démarrez d'abord le cluster de votre projet (`docker compose up -d` dans
`projet-<nom>/` : 3 brokers sur `localhost:29092`, Kafbat UI sur
`http://localhost:8080`), puis, depuis ce dossier :

```bash
# première fois : créer les topics d'entrée du projet, puis produire
python generator.py --project arena --create-topics --replication-factor 1

# les fois suivantes
python generator.py --project arena

# aperçu sans Kafka : affiche les messages au lieu de les produire
# (le catalogue d'abord, puis le flux ; --limit arrête vite)
python generator.py --project arena --dry-run --limit 30
```

Remplacez `arena` par votre projet. Le générateur produit jusqu'à `Ctrl+C`,
vers `localhost:29092` par défaut. L'application Kafka Streams attend que les
topics d'entrée existent : lancez le générateur **avant** elle. Si les topics
existent déjà, `--create-topics` l'affiche et continue.

| Option | Défaut | Rôle |
|---|---|---|
| `--bootstrap` | `localhost:29092` | Cluster cible (ou variable `KAFKA_BOOTSTRAP`) |
| `--rate` | selon le projet (5 à 15 evt/s) | Débit d'événements par seconde |
| `--dirty-rate` | `0.07` | Proportion de messages invalides |
| `--late-rate` | `0.03` | Proportion d'événements en retard (timestamp − 30 à 180 min) |
| `--dup-rate` | `0.02` | Proportion de doublons exacts |
| `--seed` | `42` | Graine des tirages aléatoires |
| `--limit` | `0` | S'arrêter après N messages (0 = sans fin) |
| `--dry-run` | — | Afficher sur la console au lieu de produire |

Pour mettre au point une règle métier sur un **flux propre** :
`--dirty-rate 0 --late-rate 0 --dup-rate 0`. Votre application doit bien sûr
tenir le flux sale avant le rendu.

## Mode fichier : des heures de flux en quelques secondes

En temps réel, il faut attendre les incidents (plusieurs minutes, une demi-heure
pour certains). `--to-dir` écrit le flux dans des fichiers au lieu de Kafka,
avec une **horloge virtuelle** : `--minutes 180` simule trois heures de flux,
incidents compris, en quelques secondes. `rejouer.py` pousse ensuite ces
fichiers dans votre cluster :

```bash
python generator.py --project arena --to-dir data/arena --minutes 180
python rejouer.py --dossier data/arena --project arena --create-topics \
    --replication-factor 1 --bootstrap localhost:29092
```

| Option | Défaut | Rôle |
|---|---|---|
| `--to-dir` | — | Dossier de sortie (active le mode fichier) |
| `--minutes` | `60` | Durée de flux simulée |
| `--start` | il y a `--minutes` min | Début de la fenêtre simulée (ISO-8601) |

Avec le même `--seed` et le même `--start`, le jeu de données est
**identique à l'octet près** : pratique pour comparer deux versions de votre
code sur exactement la même donnée.

Ce que fait le rejeu :

- tous les topics sont fusionnés et produits **dans l'ordre chronologique**,
  avec leur clé, aussi vite que possible (`--speed 1` : temps réel,
  `--speed 60` : 60 fois plus vite ; `--dry-run` : affichage seul) ;
- un jeu généré il y a longtemps est **recalé sur l'heure courante** :
  horodatage Kafka et champ `timestamp` des messages sont décalés du même
  nombre d'heures entières. Les écarts sont conservés : les retardataires
  restent en retard, les doublons restent exacts, les messages cassés restent
  cassés à l'octet près, et vos fenêtres donnent les mêmes résultats. Sans ce
  recalage, la rétention des topics purgerait le rejeu dans les minutes qui
  suivent (`--dates-origine` le désactive) ;
- rejouer deux fois **ajoute** les messages une seconde fois. Pour repartir de
  zéro : `docker compose down -v` puis `docker compose up -d`, et pensez à
  réinitialiser votre application (`application.id` / state stores).

Format des fichiers, une ligne par message :

```json
{"key": "p-0374", "value": "{\"event_id\": \"ev-3b8f...\"}", "ts_ms": 1784155861323}
```

`key` est la clé Kafka, `value` le message **brut** (un JSON tronqué reste
tronqué), `ts_ms` l'instant d'émission, soit le timestamp que Kafka pose sur
le record. Le **temps événement** est dans le champ `timestamp` du message :
pour un retardataire, les deux diffèrent de 30 à 180 min. Pour lire les
payloads : `jq -r .value data/arena/arena.purchases.jsonl | head`.

Les fichiers pèsent des dizaines de Mo : ne les versionnez pas, régénérez-les.

## Points d'attention

- **Clés** : chaque topic est produit avec une clé métier (user, carte, SKU,
  driver, post…). Ce n'est pas toujours la clé dont vous avez besoin : vous
  devrez parfois re-partitionner (`selectKey` / `groupBy`).
- Les topics `binge.catalog`, `tempo.tracks`, `sentinel.merchants`,
  `relay.campaigns`, `arena.players` et `forge.machines` sont **compactés** et
  réémis périodiquement : parfaits pour une jointure `GlobalKTable`.
- Les événements **en retard** sont du JSON valide : ils doivent passer la
  validation, et se gérer côté fenêtrage (grace period). C'est voulu.
- **Ne produisez jamais** vers le cluster partagé : son générateur tourne déjà.
  Le vôtre, c'est pour votre cluster local.
