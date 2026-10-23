# Projet TEMPO — Analytics streaming musical

*Framework imposé : **Quarkus 3.19 + Kafka Streams***

**Implementation TMP-1 a TMP-6 :** voir [IMPLEMENTATION.md](IMPLEMENTATION.md)
pour les choix de cles, fenetres, retards, dedoublonnage, formats JSON et
commandes de demonstration. Les tests de topologie sont actifs.

## Contexte

Vous rejoignez l'équipe Data Royalties de **TEMPO**, une plateforme de
streaming musical (pensez Spotify). Chaque lecteur émet des événements
d'écoute : `START`, puis `SKIP` (abandon avant 30 s le plus souvent) ou
`COMPLETE`. Trois enjeux métier : le **skip rate** par titre (les labels
veulent savoir si une sortie floppe), le **top titres par pays**, et surtout
les **royalties** — une écoute ne rapporte 0.003 € à l'artiste que si elle est
`COMPLETE` avec `ms_played ≥ 30000`. Et l'équipe anti-fraude traque les
**fermes à streams** : des comptes qui bouclent le même titre juste au-dessus
des 30 secondes pour gonfler les revenus.

Les clients embarqués envoient des messages cassés : champs manquants, types
invalides, JSON tronqué, doublons, retardataires. **Le flux ne sera jamais
propre : c'est à votre pipeline d'être robuste.**

## Mission

Construire une application **Quarkus + Kafka Streams** qui consomme le flux
d'écoutes, écarte proprement les messages invalides, et produit skip rates,
tops par pays, compteurs de royalties et alertes fraude.

## Architecture

```
tempo.listening.events ──> [ VALIDATION ] ──> invalides ──> <grp>.tempo.dlq
                                │
                                ▼ valides
         ┌───────────────┬──────┴────────┬───────────────┐
         ▼               ▼               ▼               ▼
     skip rate       top par pays    royalties       stream farm
     (tumbling       (hopping 15/5   (COMPLETE       (≥20 COMPLETE
      10 min)         + jointure)     ≥30 s)          user+titre/h)
         │               ▲               │               │
         ▼               │               ▼               ▼
   <grp>.tempo.    tempo.tracks    <grp>.tempo.    <grp>.tempo.
   skiprate        (GlobalKTable)  royalties       alerts.fraud
                         │
                         ▼
                   <grp>.tempo.top.by-country
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`tempo.listening.events`** — clé : `user_id` — `event_type` ∈
`START | SKIP | COMPLETE`, `country` ∈ `FR | US | DE | BR | JP | GB` :

```json
{
  "event_id": "ev-3b8faa18",
  "user_id": "u-0558",
  "track_id": "trk-0042",
  "artist": "Nova Kane",
  "event_type": "COMPLETE",
  "ms_played": 187230,
  "country": "FR",
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

**`tempo.tracks`** — topic **compacté**, clé : `track_id` — la fiche titre :

```json
{
  "track_id": "trk-0042",
  "title": "Minuit Turbine",
  "artist": "Nova Kane",
  "genre": "ELECTRO",
  "duration_ms": 214000
}
```

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                     | Impact si non gérée                       |
|----------------------------------------|-----------------------------|-------------------------------------------|
| Champ requis absent                    | pas de `track_id`           | royalties non attribuables                |
| Champ à `null`                         | `"event_type": null`        | NullPointerException                      |
| Mauvais type                           | `"ms_played": "douze"`      | crash de désérialisation                  |
| Valeur hors bornes                     | `ms_played: -5000`          | royalties fausses                         |
| Enum inconnue                          | `"event_type": "COMPLETEE"` | écoute perdue                             |
| Timestamp illisible                    | `"hier a 15h"`              | fenêtrage cassé                           |
| JSON tronqué / non-JSON / message vide | `{"event_id": "ev`          | **poison pill : l'appli meurt en boucle** |
| Événement en retard                    | timestamp − 30 à 180 min    | tops faussés                              |
| Doublon exact                          | même `event_id` deux fois   | royalties payées deux fois                |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.tempo.dlq` · `<grp>.tempo.skiprate` · `<grp>.tempo.top.by-country` ·
`<grp>.tempo.royalties` · `<grp>.tempo.alerts.fraud` — constantes prêtes dans
`Topics.java`. Format des valeurs libre **mais en JSON documenté dans votre
README de rendu**.

## Backlog

> La base 8/20 = TMP-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**TMP-1 — Ingestion fiable (socle, obligatoire)**
Consommer `tempo.listening.events`, parser, valider (champs requis, types,
enums, `ms_played ≥ 0`, timestamp ISO-8601 parsable). Invalides → DLQ **avec
message original + raison**. L'application ne doit **jamais** crasher.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**TMP-2 — Skip rate par titre** → `<grp>.tempo.skiprate`
Sur **fenêtres tumbling de 10 min** : `skips / (skips + completes)` par
`track_id`. Marquer les flops : skip rate > 70 % **avec au moins 50 écoutes**
(sinon le ratio n'est pas significatif). Le générateur lance un flop toutes
les ~7 min.
*Critères : chaque `flop_release` ressort marquée ; un titre à 3 écoutes dont
3 skips n'est PAS marqué flop.*

**TMP-3 — Top titres par pays** → `<grp>.tempo.top.by-country`
Compter les écoutes (`START` exclus) par (`country`, `track_id`) sur
**fenêtres hopping 15 min / avance 5 min**, enrichir avec `title` et `artist`
via **GlobalKTable** sur `tempo.tracks`.
*Critères : sortie enrichie titre + artiste ; un `track_id` absent du
catalogue ne fait pas crasher la jointure ; choix GlobalKTable vs KTable
justifié à l'oral.*

**TMP-4 — Compteur de royalties par artiste** → `<grp>.tempo.royalties`
Cumuler `0.003 €` par écoute éligible (`COMPLETE` **et** `ms_played ≥ 30000`)
par `artist` (KTable, `aggregate`). Constantes déjà dans `TopologyProducer`.
*Critères : montants cohérents ; un doublon d'`event_id` ne paie pas deux fois
(dédoublonnage à justifier) ; un SKIP à 45 000 ms ne paie pas.*

**TMP-5 — Fraude « stream farm »** → `<grp>.tempo.alerts.fraud`
Alerte quand un couple (`user_id`, `track_id`) dépasse **20 COMPLETE par
heure** (tumbling 1 h — pour la démo, paramétrez la taille de fenêtre et
montrez-la à 5 min). Le générateur lance une ferme toutes les ~5 min
(~36 COMPLETE à 31 000 ms en 3 min).
*Critères : chaque `stream_farm` est détectée avec le user et le titre ;
silence en régime normal.*

**TMP-6 (bonus) — Tests de topologie**
Implémenter les trois tests de `TempoTopologyTest` avec
**TopologyTestDriver** (démarche vue en séance 14) : DLQ, royaltie créditée,
skip non crédité. Ajouter `kafka-streams-test-utils` en scope test.
*Critères : `mvn test` vert ; les tests pilotent le temps des fenêtres avec
des timestamps injectés, pas avec `sleep`.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Alimenter les topics d'entrée : le générateur, dans un second terminal,
#    depuis le dossier generateurs-v3/ du dépôt du cours (voir son README)
python generator.py --project tempo --create-topics --replication-factor 1

# 3. Lancer en mode dev (live reload)
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn quarkus:dev
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn quarkus:dev
```

**Important** : l'extension Quarkus attend que les topics listés dans
`quarkus.kafka-streams.topics` existent avant de démarrer la topologie :
lancez le générateur (étape 2, `--create-topics` la première fois) **avant**
l'application. Il produit en continu le flux du projet, données sales et incidents
compris (`Ctrl+C` pour l'arrêter). Son README détaille les options : débit,
flux propre pour déboguer, mode fichier qui simule 3 h de flux en quelques
secondes. Sur le cluster partagé, les topics existent déjà
(`KAFKA_BOOTSTRAP=<serveur>:9092`).

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

## Contraintes

- Java 21, Quarkus + Kafka Streams uniquement (pas de Spark/Flink), pas de
  base externe.
- `GROUPE` obligatoire : topics de sortie et `application.id`
  (`tempo-<groupe>`) préfixés — déjà câblé.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                   | Points |
|-----------------------------------------------------------|--------|
| Socle TMP-1 en production 10 min sans crash + DLQ motivée | **8**  |
| TMP-2 (skip rate + seuil de significativité)              | +2     |
| TMP-3 (top par pays + GlobalKTable)                       | +3     |
| TMP-4 (royalties + dédoublonnage)                         | +3     |
| TMP-5 (stream farm)                                       | +2     |
| TMP-6 (tests TopologyTestDriver)                          | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Implementation TMP-1

`TopologyProducer.ingest` lit les valeurs en texte brut, valide une seule fois
chaque message avec `EventValidator`, puis utilise `split` pour separer les
invalides du flux valide. Les champs `event_id`, `user_id`, `track_id`,
`artist`, `event_type`, `country`, `timestamp` sont requis et doivent etre des
chaines non vides. `ms_played` est requis, entier JSON entre 0 et
2147483647 (modele Java `Integer`). Les chaines numeriques et les decimaux
ne sont pas convertis automatiquement. Les champs supplementaires sont
acceptes. Le timestamp doit etre ISO-8601 avec fuseau, parsable par
`Instant.parse`.

La sortie `<GROUPE>.tempo.dlq` conserve la cle Kafka originale et utilise ce
format JSON (la premiere erreur rencontree est rapportee) :

```json
{"originalMessage":"{\"event_id\": \"ev","reason":"JSON invalide : ..."}
```

`originalMessage` est exactement la valeur recue, avec ses espaces et ses
sauts de ligne, ou `null` pour une valeur Kafka nulle. Un message vide, une
racine JSON autre qu'un objet ou du contenu apres l'objet sont rejetes.

Les evenements valides sont retournes sous forme de
`KStream<String, ListeningEvent>` dans `validEvents`, en conservant la cle
et le timestamp Kafka a cette etape. `Analytics.attach` branche maintenant
TMP-2 a TMP-5 : temps evenement, repartitionnement par `event_id`,
dedoublonnage, puis calculs metier. Le validateur refuse aussi les dates
anterieures a 1970 ou non representables en millisecondes Kafka pour eviter
une exception lors de l'application du temps evenement.

Verification locale sans broker : `mvn test`. `TempoIngestionTest` couvre
la DLQ de la topologie de production, les champs absents/null/mal types,
les valeurs hors bornes, les enums, les dates, les messages casses suivis
de messages valides, les doublons et les retards. Son topic `test.valid`
est ajoute seulement dans les tests pour observer le flux valide.
La tenue de 10 minutes sur le cluster partage reste a verifier.

## Conseils

- Le `Log.infof` fourni sert au premier contact : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- TMP-4 est le cœur métier : réfléchissez à la clé (`artist`) et au
  dédoublonnage AVANT l'agrégation (un state store des `event_id` vus, avec
  une politique d'expiration, est une piste).
- TMP-2 : agrégat à deux compteurs (skips, completes) dans un seul
  `aggregate`, plutôt que deux flux à joindre.
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en
  Kafka 4.0.
