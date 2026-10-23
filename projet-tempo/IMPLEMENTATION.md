# TEMPO — implementation et demonstration

## Parcours des donnees

```text
tempo.listening.events (cle initiale user_id, valeur String)
  -> EventValidator : validation stricte
     -> invalide : <GROUPE>.tempo.dlq
     -> valide : ListeningEvent
        -> EventTime : timestamp Kafka Streams = timestamp du JSON
        -> repartitionnement par event_id
        -> UniqueEvents : historique des identifiants dans un WindowStore
        -> skip rate / top par pays / royalties / fraude
tempo.tracks -> GlobalKTable du catalogue -> jointure gauche du top
```

Le `@Produces Topology` de `TopologyProducer` est demarre par Quarkus.
`Analytics` construit les quatre traitements. Les objets de sortie sont
des records Java convertis en JSON par `JsonSerdes.toJson`.

## Temps, retards et reprise

- Les fenetres utilisent le temps de l'evenement, extrait **apres** la
  validation. Un JSON casse ne peut donc pas tuer un timestamp extractor.
- Les intervalles sont `[debut, fin)`, alignes sur l'epoch UTC. Un evenement
  exactement a la fin appartient a la fenetre suivante.
- La grace vaut 180 minutes : une fenetre peut encore etre modifiee tant
  que le stream-time de sa tache n'a pas atteint sa fin plus cette grace.
  Le temps avance avec les donnees, sans minuterie murale ni `sleep`.
- Au-dela de la grace, Kafka Streams abandonne la mise a jour de la fenetre
  fermee. Ce n'est pas une erreur de validation : pas de DLQ. Les royalties,
  qui ne sont pas fenetrees, restent creditees pour une ecoute eligible unique.
- Les sorties sont des **mises a jour de snapshots**, pas des deltas et pas
  seulement des resultats finaux. Toujours garder la derniere valeur par
  cle ; ne pas sommer des snapshots successifs. Le cache peut regrouper
  plusieurs mises a jour avant publication.
- `exactly_once_v2` lie les sorties, les changelogs d'etat et les offsets
  Kafka dans les transactions. Lire les sorties avec `read_committed`.
  Cela ne remplace pas le dedoublonnage des `event_id` deja presents en entree.
- Les state stores sont persistants et journalises dans Kafka. Leur copie
  locale est dans `.tempo-state/` (non versionnee). La meme application-id
  reprend son etat ; un nouvel identifiant recree des calculs independants.

## TMP-1 — validation

Champs texte requis non vides : `event_id`, `user_id`, `track_id`, `artist`,
`event_type`, `country`, `timestamp`. `ms_played` doit etre un entier JSON
entre 0 et 2147483647. Les dates doivent etre ISO-8601 avec fuseau, a partir
de 1970 et representables en millisecondes Kafka. Les champs inconnus sont
acceptes. Un entier sous forme de chaine n'est pas converti implicitement.
La premiere erreur est rapportee.

Cle DLQ : cle Kafka d'origine (eventuellement null).

```json
{"originalMessage":"{broken","reason":"JSON invalide : ..."}
```

## Dedoublonnage commun

La cle devient `event_id`, puis un repartitionnement explicite garantit que
deux copies rejoignent la meme tache meme si leurs cles Kafka initiales
different. Un `WindowStore<String,Boolean>` conserve les identifiants vus.
L'identifiant est enregistre au stream-time courant pour que les evenements
en retard soient eux aussi dedoublonnes. La premiere copie valide gagne.
Cela protege les quatre indicateurs, pas seulement les royalties.

Retention : **168 heures de temps de flux**, configurable. L'expiration
physique des segments est geree par Kafka Streams. Une copie revenant apres
cet horizon peut compter a nouveau : augmenter la retention selon l'horizon
de rejeu et le volume attendu. Sans nouvelle donnee, le stream-time et la
retention n'avancent pas. Une retention finie evite un historique illimite.

## TMP-2 — skip rate

Cle d'agregation : `track_id`. `START` exclus. Un seul `aggregate` conserve
deux compteurs dans une fenetre tumbling de 10 minutes.
`flop` vaut vrai seulement si `skips + completes >= 50` et le ratio est
**strictement** superieur a 0.70 (comparaison entiere pour le seuil).

Cle de sortie : `<track_id>@<windowStartEpochMillis>`.

```json
{"trackId":"trk-1","windowStart":"2026-07-04T12:00:00Z","windowEnd":"2026-07-04T12:10:00Z","skips":80,"completes":20,"skipRate":0.8,"flop":true}
```

## TMP-3 — top par pays

Jointure **gauche** sur `GlobalKTable<String,String>` du catalogue.
Chaque instance possede le catalogue complet : une jointure par `track_id`
est possible meme si le flux initial est partitionne par utilisateur.
Une KTable classique demanderait de re-partitionner et de co-partitionner
les deux entrees. Le compromis GlobalKTable est une copie complete du
catalogue par instance et une jointure sur son etat courant, sans version
historique au temps de l'ecoute.

Catalogue absent, JSON casse ou tombstone : l'ecoute reste comptee,
`title` vaut null et `artist` reprend l'artiste de l'evenement. Les nouvelles
ecoutes beneficient des mises a jour du catalogue ; les anciens snapshots
ne sont pas recalcules uniquement parce qu'une fiche change.

Cle de comptage : objet `(country, trackId)` serialise en JSON pour eviter
les collisions de concatenation. Fenetres hopping de 15 minutes, avance
5 minutes. Une ecoute participe normalement a trois fenetres.

Les compteurs par titre alimentent un classement par `(country, windowStart)`.
Chaque mise a jour **remplace** le compteur precedent du titre. On conserve
tous les candidats dans le store et on publie les 10 premiers, tries par
nombre d'ecoutes decroissant puis `trackId` croissant pour les egalites.

`RankingTime` fixe le timestamp de ces mises a jour au debut de leur fenetre.
Le second store utilise un bucket de 5 minutes avec une grace augmentee de
10 minutes : sa fermeture correspond a celle de la fenetre hopping de
15 minutes. Cela evite de separer un meme classement quand les ecoutes
traversent une frontiere de 15 minutes, tout en permettant l'expiration des
anciens classements.

Cle de sortie : `<country>@<windowStartEpochMillis>`.

```json
{"country":"FR","windowStart":"2026-07-04T12:00:00Z","windowEnd":"2026-07-04T12:15:00Z","tracks":[{"trackId":"trk-1","title":"Minuit Turbine","artist":"Nova Kane","listens":100}]}
```

## TMP-4 — royalties

Seuls les `COMPLETE` avec `ms_played >= 30000` sont eligibles. Apres
dedoublonnage, cle = `artist` ; une KTable construite par `aggregate`
compte les ecoutes eligibles. Le montant est calcule par multiplication
decimale (`BigDecimal`) du compteur par 0.003, sans accumulation flottante.
Un SKIP meme a 45000 ms ne rapporte rien. Les compteurs ne sont pas fenetres.

Cle de sortie : nom de l'artiste.

```json
{"artist":"Nova Kane","eligibleStreams":100,"amountEur":0.300}
```

## TMP-5 — fraude

Cle = objet `(userId, trackId)`. On compte uniquement les `COMPLETE`
uniques dans une fenetre tumbling d'une heure. Une alerte est emise au
franchissement du seuil, au **21e COMPLETE**, une seule fois par couple et
fenetre. Le cache est desactive pour ce compteur afin de ne pas sauter le
passage par 21. Les SKIP, les autres utilisateurs et les autres titres ne
sont pas melanges. Cette regle detecte une suspicion ; elle ne retire pas
automatiquement les royalties.

Cle de sortie : `<JSON du couple>@<windowStartEpochMillis>`.

```json
{"userId":"farm-001","trackId":"trk-1","windowStart":"2026-07-04T12:00:00Z","windowEnd":"2026-07-04T13:00:00Z","completes":21}
```

## Configuration

| Variable d'environnement | Defaut | Effet |
|---|---:|---|
| `GROUPE` | `grp00` | Identite `tempo-<groupe>` et prefixe des sorties ; renseigner le vrai groupe sur le cluster partage |
| `KAFKA_BOOTSTRAP` | `localhost:29092` | Cluster Kafka |
| `TEMPO_GRACE_MINUTES` | 180 | Tolerance des fenetres aux retards |
| `TEMPO_FRAUD_WINDOW_MINUTES` | 60 | Mettre 5 pour la demonstration |
| `TEMPO_DEDUP_RETENTION_HOURS` | 168 | Historique des identifiants uniques |
| `TEMPO_TOP_LIMIT` | 10 | Nombre de titres dans chaque top |
| `TEMPO_STATE_DIR` | `./.tempo-state` | Repertoire local des stores |

La retention doit depasser la grace plus 15 minutes ; les tailles et le top
doivent etre positifs. Une configuration incoherente echoue au demarrage.
Les changelogs/repartitions utilisent un facteur de replication 3 : le
cluster fourni possede trois brokers.

## TMP-6 — tests et lancement

```bash
# Depuis projet-tempo, avec Java 21 et Maven 3.9+
mvn test
mvn package
```

`TempoTopologyTest` couvre les trois tests demandes et les seuils exacts,
le dedoublonnage, le montant decimal, les frontieres des fenetres, les trois
fenetres hopping, leur continuite, le classement, le catalogue absent ou
supprime, les retards, l'expiration des identifiants et la demo fraude 5 min.
`TempoIngestionTest` couvre la validation et la poursuite apres poison pill.
Tous injectent des timestamps ; aucun `sleep` ni broker requis.

```bash
# Terminal 1 : cluster local
docker compose up -d

# Terminal 2 : depuis ../generateurs-v3, environnement Python active
python generator.py --project tempo --create-topics --replication-factor 3

# Terminal 3 : depuis projet-tempo
GROUPE=grp07 TEMPO_FRAUD_WINDOW_MINUTES=5 mvn quarkus:dev

# Ou apres mvn package
GROUPE=grp07 TEMPO_FRAUD_WINDOW_MINUTES=5 java -jar target/quarkus-app/quarkus-run.jar
```

Pour simuler trois heures plus vite, depuis `generateurs-v3` :

```bash
python generator.py --project tempo --to-dir /tmp/tempo-data --minutes 180
python rejouer.py --dossier /tmp/tempo-data --project tempo --create-topics \
  --replication-factor 3 --bootstrap localhost:29092
```

Lire une sortie depuis un autre terminal (adapter grp07 et le topic) :

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic grp07.tempo.royalties \
  --from-beginning --isolation-level read_committed
```

Sur le cluster partage, le generateur tourne deja : lancer seulement
l'application avec le vrai `GROUPE` et l'adresse fournie. La validation de
10 minutes sur ce cluster et la defense orale restent des etapes de rendu.
