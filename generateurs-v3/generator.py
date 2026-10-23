#!/usr/bin/env python3
"""Generateur de donnees temps reel pour les projets Kafka ESGI.

Neuf scenarios inspires de cas d'usage reels :
  binge     - plateforme VOD (type Netflix)
  cartflow  - e-commerce (type Amazon)
  dispatch  - VTC / mobilite urbaine (type Uber)
  viral     - reseau social (type Instagram)
  tempo     - streaming musical (type Spotify)
  sentinel  - paiements / anti-fraude (type Stripe)
  relay     - regie publicitaire programmatique (type Criteo)
  arena     - jeu de tir competitif en ligne (type Riot Games)
  forge     - supervision d'usine / maintenance predictive

Chaque scenario produit un flux realiste PLUS des donnees sales
(champs manquants, types invalides, JSON tronque, doublons,
evenements en retard...) et des incidents scriptes que les etudiants
doivent detecter.

Trois sorties possibles :
  - Kafka (defaut)      : flux temps reel vers un cluster
  - stdout (--dry-run)  : apercu
  - fichiers (--to-dir) : jeu de donnees fige.
    Ce mode utilise une HORLOGE VIRTUELLE : --minutes 180 produit trois
    heures de flux (incidents scriptes compris) en quelques secondes.
    Les fichiers sont au format JSONL {key, value, ts_ms}, relisibles par
    rejouer.py (vers Kafka).

Exemples :
  python generator.py --project binge --dry-run --limit 20
  python generator.py --project binge --bootstrap mon-serveur:9092
  python generator.py --project sentinel --create-topics --replication-factor 3
  python generator.py --project arena --to-dir data/arena --minutes 180
"""

import argparse
import importlib
import importlib.util
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from scenarios import common

PROJECTS = ["binge", "cartflow", "dispatch", "viral", "tempo", "sentinel",
            "relay", "arena", "forge"]

TICK_SECONDS = 0.25

# ---------------------------------------------------------------------------
# Corruption : fabrique des messages "sales" comme dans la vraie vie
# ---------------------------------------------------------------------------

DIRTY_KINDS = [
    ("missing_field", 20),
    ("null_field", 15),
    ("bad_type", 15),
    ("out_of_range", 10),
    ("bad_enum", 10),
    ("bad_timestamp", 10),
    ("truncated_json", 10),
    ("empty_message", 5),
    ("not_json", 5),
]

BAD_TYPE_VALUES = ["douze", "NaN", "??", True, [1, 2, 3]]
BAD_TS_VALUES = ["hier a 15h", "12/31/2025 25:61", "0000-00-00", "bientot"]


def corrupt(value, spec, rng):
    """Retourne (payload_str, tag). Modifie une copie du dict `value`."""
    kind = common.pick_weighted(rng, DIRTY_KINDS)
    v = dict(value)

    if kind == "missing_field" and spec.get("required"):
        field = rng.choice(spec["required"])
        v.pop(field, None)
        return json.dumps(v), kind
    if kind == "null_field" and spec.get("required"):
        field = rng.choice(spec["required"])
        v[field] = None
        return json.dumps(v), kind
    if kind == "bad_type" and spec.get("numeric"):
        field = rng.choice(spec["numeric"])
        if field in v:
            v[field] = rng.choice(BAD_TYPE_VALUES)
            return json.dumps(v), kind
    if kind == "out_of_range" and spec.get("numeric"):
        field = rng.choice(spec["numeric"])
        if field in v and isinstance(v[field], (int, float)):
            v[field] = rng.choice([-1, -9999, 10 ** 9])
            return json.dumps(v), kind
    if kind == "bad_enum" and spec.get("enum"):
        field = rng.choice(list(spec["enum"].keys()))
        if field in v:
            v[field] = rng.choice(["UNKNOWN_42", "???", "test", ""])
            return json.dumps(v), kind
    if kind == "bad_timestamp" and spec.get("ts"):
        v[spec["ts"]] = rng.choice(BAD_TS_VALUES)
        return json.dumps(v), kind
    if kind == "truncated_json":
        s = json.dumps(v)
        cut = rng.randint(int(len(s) * 0.4), int(len(s) * 0.9))
        return s[:cut], kind
    if kind == "empty_message":
        return "", kind
    if kind == "not_json":
        return rng.choice([
            "PING;12,34;ERROR",
            "<event>legacy xml</event>",
            "null null null",
            "42",
        ]), kind
    # repli : champ requis supprime
    if spec.get("required"):
        v.pop(rng.choice(spec["required"]), None)
    return json.dumps(v), "missing_field"


def make_late(value, spec, rng, now):
    v = dict(value)
    ts_field = spec.get("ts", "timestamp")
    delay = rng.randint(30 * 60, 180 * 60)
    v[ts_field] = common.iso(now - timedelta(seconds=delay))
    return v


# ---------------------------------------------------------------------------
# Chaine de salissure, commune aux trois sorties
# ---------------------------------------------------------------------------

class Pipeline:
    """Applique corruption, retard et doublons a un evenement propre.

    Retourne une liste de (topic, cle, payload, tag) : en general un seul
    element, deux quand un doublon est rejoue.
    """

    def __init__(self, scenario, rng, dirty_rate, late_rate, dup_rate):
        self.scenario = scenario
        self.rng = rng
        self.dirty_rate = dirty_rate
        self.late_rate = late_rate
        self.dup_rate = dup_rate
        self.recent_valid = []

    def traiter(self, topic, key, value, now):
        rng = self.rng
        spec = self.scenario.SPEC.get(topic)
        if spec and rng.random() < self.dirty_rate:
            payload, tag = corrupt(value, spec, rng)
            return [(topic, key, payload, tag)]
        out = []
        if spec and rng.random() < self.late_rate:
            value = make_late(value, spec, rng, now)
            out.append((topic, key, json.dumps(value), "late"))
        else:
            payload = json.dumps(value)
            out.append((topic, key, payload, "valid"))
            if spec:
                self.recent_valid.append((topic, key, payload))
                if len(self.recent_valid) > 200:
                    self.recent_valid.pop(0)
        if self.recent_valid and rng.random() < self.dup_rate:
            t2, k2, p2 = rng.choice(self.recent_valid)
            out.append((t2, k2, p2, "duplicate"))
        return out


# ---------------------------------------------------------------------------
# Sortie : Kafka, stdout (dry-run) ou fichiers (--to-dir)
# ---------------------------------------------------------------------------

class KafkaSink:
    def __init__(self, bootstrap):
        from confluent_kafka import Producer
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "compression.type": "lz4",
        })

    def send(self, topic, key, payload, ts_ms=None):
        self.producer.produce(topic, key=key.encode("utf-8"),
                              value=payload.encode("utf-8"))
        self.producer.poll(0)

    def close(self):
        self.producer.flush(10)


class StdoutSink:
    def send(self, topic, key, payload, ts_ms=None):
        print("%s | %s | %s" % (topic, key, payload))

    def close(self):
        pass


class FileSink:
    """Un fichier JSONL par topic : {"key", "value", "ts_ms"} par ligne.

    Se rejoue vers Kafka avec rejouer.py.
    ts_ms est l'instant d'EMISSION (comme le timestamp que Kafka poserait) ;
    le temps evenement reste dans le champ timestamp du JSON, ce qui laisse
    les retardataires en retard.
    """

    def __init__(self, dossier):
        os.makedirs(dossier, exist_ok=True)
        self.dossier = dossier
        self.fichiers = {}
        self.compte = {}

    def send(self, topic, key, payload, ts_ms=None):
        f = self.fichiers.get(topic)
        if f is None:
            chemin = os.path.join(self.dossier, topic.replace("/", "_") + ".jsonl")
            f = open(chemin, "w", encoding="utf-8")
            self.fichiers[topic] = f
            self.compte[topic] = 0
        f.write(json.dumps({"key": key, "value": payload, "ts_ms": ts_ms},
                           ensure_ascii=True) + "\n")
        self.compte[topic] += 1

    def close(self):
        for f in self.fichiers.values():
            f.close()
        for topic in sorted(self.compte):
            print("[fichier] %-34s %7d messages" % (topic + ".jsonl",
                                                    self.compte[topic]))


def create_topics(scenario, bootstrap, replication_factor):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [
        NewTopic(name,
                 num_partitions=cfg.get("partitions", 3),
                 replication_factor=replication_factor,
                 config=cfg.get("config", {}))
        for name, cfg in scenario.TOPICS.items()
    ]
    futures = admin.create_topics(topics)
    for name, f in futures.items():
        try:
            f.result()
            print("[topics] cree : %s" % name)
        except Exception as e:
            print("[topics] %s : %s" % (name, e))


# ---------------------------------------------------------------------------
# Boucle principale
# ---------------------------------------------------------------------------

def charger_scenario(projet):
    """scenarios/<projet>_prof.py s'il est present (meme flux, incidents
    annonces sur stdout), sinon scenarios/<projet>.py."""
    for nom in ("scenarios.%s_prof" % projet, "scenarios.%s" % projet):
        if importlib.util.find_spec(nom) is not None:
            return importlib.import_module(nom)
    raise SystemExit("scenario introuvable : scenarios/%s.py" % projet)


def run(args):
    scenario = charger_scenario(args.project)
    rng = random.Random(args.seed)
    rate = args.rate if args.rate is not None else scenario.DEFAULT_RATE

    if args.to_dir:
        return run_offline(args, scenario, rng, rate)

    if args.create_topics:
        create_topics(scenario, args.bootstrap, args.replication_factor)
        if args.topics_only:
            return

    sink = StdoutSink() if args.dry_run else KafkaSink(args.bootstrap)
    state = scenario.State(rng)
    stats = {"total": 0, "valid": 0, "late": 0, "duplicate": 0}
    pipeline = Pipeline(scenario, rng, args.dirty_rate, args.late_rate,
                        args.dup_rate)

    def emit(topic, key, payload, tag):
        sink.send(topic, key, payload)
        stats["total"] += 1
        stats[tag] = stats.get(tag, 0) + 1

    def emit_catalog(now):
        for topic, key, value in scenario.catalog(rng):
            emit(topic, key, json.dumps(value), "catalog")

    print("[generator] projet=%s rate=%.1f evt/s dirty=%.0f%% late=%.0f%% dup=%.0f%% seed=%s cible=%s"
          % (args.project, rate, args.dirty_rate * 100, args.late_rate * 100,
             args.dup_rate * 100, args.seed,
             "stdout (dry-run)" if args.dry_run else args.bootstrap))

    start = time.time()
    last_catalog = 0.0
    last_stats = start
    carry = 0.0
    produced = 0

    try:
        while True:
            loop_start = time.time()
            now = datetime.now(timezone.utc)

            if loop_start - last_catalog >= scenario.CATALOG_REFRESH:
                emit_catalog(now)
                last_catalog = loop_start

            carry += rate * TICK_SECONDS
            n = int(carry)
            carry -= n

            for topic, key, value in state.tick(now, n):
                for t2, k2, p2, tag in pipeline.traiter(topic, key, value, now):
                    emit(t2, k2, p2, tag)

            produced = stats["total"]
            if args.limit and produced >= args.limit:
                break

            if not args.quiet and loop_start - last_stats >= 30:
                print("[stats] %s" % json.dumps(stats, sort_keys=True))
                last_stats = loop_start

            elapsed = time.time() - loop_start
            if elapsed < TICK_SECONDS:
                time.sleep(TICK_SECONDS - elapsed)
    except KeyboardInterrupt:
        print("\n[generator] arret demande")
    finally:
        sink.close()
        print("[stats finales] %s" % json.dumps(stats, sort_keys=True))


def run_offline(args, scenario, rng, rate):
    """Ecrit --minutes de flux dans des fichiers, horloge virtuelle.

    Meme chaine que le temps reel (memes incidents, meme salissure) mais
    sans aucune attente : 3 h de flux se simulent en quelques secondes.
    """
    # L'horloge doit etre calee AVANT de construire l'etat : les Incident
    # lisent common.monotonic() des leur creation pour programmer leur
    # premier declenchement.
    base = 1000.0
    common.caler_horloge(base)

    sink = FileSink(args.to_dir)
    state = scenario.State(rng)
    pipeline = Pipeline(scenario, rng, args.dirty_rate, args.late_rate,
                        args.dup_rate)
    stats = {"total": 0, "valid": 0, "late": 0, "duplicate": 0}

    if args.start:
        debut = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    else:
        debut = datetime.now(timezone.utc) - timedelta(minutes=args.minutes)
    if debut.tzinfo is None:
        debut = debut.replace(tzinfo=timezone.utc)

    print("[generator] projet=%s rate=%.1f evt/s duree=%d min "
          "(horloge virtuelle) seed=%s -> %s/"
          % (args.project, rate, args.minutes, args.seed, args.to_dir))
    print("[generator] fenetre simulee : %s -> %s"
          % (common.iso(debut),
             common.iso(debut + timedelta(minutes=args.minutes))))
    if scenario.__name__.endswith("_prof"):
        print("[generator] les lignes [incident] ci-dessous sont horodatees "
              "a l'heure SIMULEE.")

    def emit(topic, key, payload, tag, now):
        sink.send(topic, key, payload, int(now.timestamp() * 1000))
        stats["total"] += 1
        stats[tag] = stats.get(tag, 0) + 1

    carry = 0.0
    ticks = int(args.minutes * 60 / TICK_SECONDS)
    dernier_catalogue = -1e9
    try:
        for i in range(ticks):
            now = debut + timedelta(seconds=i * TICK_SECONDS)
            common.caler_horloge(base + i * TICK_SECONDS, now)

            if (i * TICK_SECONDS) - dernier_catalogue >= scenario.CATALOG_REFRESH:
                for topic, key, value in scenario.catalog(rng):
                    emit(topic, key, json.dumps(value), "catalog", now)
                dernier_catalogue = i * TICK_SECONDS

            carry += rate * TICK_SECONDS
            n = int(carry)
            carry -= n
            for topic, key, value in state.tick(now, n):
                for t2, k2, p2, tag in pipeline.traiter(topic, key, value, now):
                    emit(t2, k2, p2, tag, now)

            if args.limit and stats["total"] >= args.limit:
                break
    finally:
        common.caler_horloge(None)
        sink.close()
        print("[stats finales] %s" % json.dumps(stats, sort_keys=True))
        print("[generator] rejeu vers Kafka : "
              "python rejouer.py --dossier %s --bootstrap <serveur>:9092"
              % args.to_dir)


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--project", choices=PROJECTS, help="scenario a generer")
    p.add_argument("--list", action="store_true", help="liste les scenarios")
    p.add_argument("--bootstrap",
                   default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    p.add_argument("--rate", type=float, default=None,
                   help="evenements par seconde (defaut : selon scenario)")
    p.add_argument("--dirty-rate", type=float,
                   default=float(os.environ.get("DIRTY_RATE", "0.07")),
                   help="proportion de messages invalides (defaut 0.07)")
    p.add_argument("--late-rate", type=float,
                   default=float(os.environ.get("LATE_RATE", "0.03")),
                   help="proportion d'evenements en retard (defaut 0.03)")
    p.add_argument("--dup-rate", type=float,
                   default=float(os.environ.get("DUP_RATE", "0.02")),
                   help="proportion de doublons (defaut 0.02)")
    p.add_argument("--seed", type=int,
                   default=int(os.environ.get("SEED", "42")))
    p.add_argument("--limit", type=int, default=0,
                   help="s'arreter apres N messages (0 = infini)")
    p.add_argument("--dry-run", action="store_true",
                   help="afficher sur stdout au lieu de produire vers Kafka")
    p.add_argument("--to-dir", default=None, metavar="DOSSIER",
                   help="ecrire un jeu de donnees fige dans DOSSIER "
                        "(un .jsonl par topic) au lieu de produire vers "
                        "Kafka. Horloge virtuelle : instantane.")
    p.add_argument("--minutes", type=int, default=60,
                   help="avec --to-dir : duree de flux simulee (defaut 60)")
    p.add_argument("--start", default=None, metavar="ISO",
                   help="avec --to-dir : debut de la fenetre simulee "
                        "(defaut : il y a --minutes minutes). Fixer --start "
                        "et --seed rend le jeu de donnees reproductible "
                        "a l'octet pres.")
    p.add_argument("--create-topics", action="store_true",
                   help="creer les topics du scenario avant de produire")
    p.add_argument("--topics-only", action="store_true",
                   help="avec --create-topics : creer les topics puis quitter")
    p.add_argument("--replication-factor", type=int, default=3,
                   help="replication factor des topics crees (1 si broker unique)")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    if args.list or not args.project:
        print("Scenarios disponibles : %s" % ", ".join(PROJECTS))
        if not args.project:
            sys.exit(0 if args.list else 1)
        return

    run(args)


if __name__ == "__main__":
    main()
