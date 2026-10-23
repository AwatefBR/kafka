#!/usr/bin/env python3
"""Rejoue vers Kafka un jeu de donnees produit par generator.py --to-dir.

Les fichiers sont des .jsonl {key, value, ts_ms}, un par topic. Le rejeu
fusionne tous les topics par ordre chronologique et pose le ts_ms de
chaque ligne comme timestamp du record Kafka.

Par defaut, tout le jeu est d'abord translate dans le temps pour se
terminer maintenant : ts_ms ET horodatages des messages, du meme nombre
d'heures entieres. Sans ce recalage, un jeu genere il y a plus de 30 jours
serait purge par la retention des topics quelques minutes apres le rejeu.
Le temps evenement et le temps d'ingestion gardent exactement la relation
qu'ils avaient a la generation (les retardataires restent en retard) et
les fenetres restent alignees : memes resultats a chaque rejeu.
--dates-origine desactive le recalage.

Le script cree aussi les topics, a la demande :
  --create-topics         topics d'entree, declares dans scenarios/<projet>.py
  --create-output-topics  topics de sortie, listes dans sorties.json et
                          declines pour chaque groupe (<grp> substitue)
Avec --create-output-topics seul, --dossier est inutile : on cree et on sort.

Exemples :
  # aussi vite que possible (defaut) : 3 h de flux en une minute
  python rejouer.py --dossier data/arena --bootstrap localhost:29092

  # creer les topics du projet avant de pousser (broker unique : RF 1)
  python rejouer.py --dossier data/arena --project arena --create-topics \\
      --replication-factor 1

  # creer les topics de sortie des groupes, sans rejeu
  python rejouer.py --project cartflow --create-output-topics \\
      --groupes grp01,grp02,grp03

  # meme chose, groupes lus dans un fichier, sans rien envoyer au broker
  python rejouer.py --project cartflow --create-output-topics \\
      --groupes @groupes.txt --dry-run

  # en temps reel, comme si le generateur tournait (--speed 60 = 60x)
  python rejouer.py --dossier data/arena --speed 1
"""
import argparse
import glob
import heapq
import importlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

GRP = "<grp>"
SORTIES_DEFAUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "sorties.json")

# Seul format d'horodatage des jeux de donnees : ISO-8601 UTC a la
# milliseconde (common.iso). Les faux timestamps de la salissure ("hier a
# 15h", "0000-00-00", date coupee par un JSON tronque) ne matchent pas.
ISO_MS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
HEURE_MS = 3600 * 1000


def lire(chemin, topic):
    with open(chemin, encoding="utf-8") as f:
        for ligne in f:
            ligne = ligne.strip()
            if not ligne:
                continue
            d = json.loads(ligne)
            yield (int(d.get("ts_ms") or 0), topic, d.get("key"),
                   d.get("value", ""))


def bornes(fichiers):
    """(premier, dernier) ts_ms du jeu de donnees, None si inconnu.

    generator.py ecrit chaque fichier dans l'ordre chronologique : la
    premiere et la derniere ligne suffisent, sans relire 50 Mo.
    """
    debut, fin = None, None
    for chemin in fichiers:
        with open(chemin, "rb") as f:
            premiere = f.readline()
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 65536))
            lignes = [l for l in f.read().splitlines() if l.strip()]
        if not premiere.strip() or not lignes:
            continue
        a = int(json.loads(premiere).get("ts_ms") or 0)
        b = int(json.loads(lignes[-1]).get("ts_ms") or 0)
        if not a or not b:
            continue
        debut = a if debut is None else min(debut, a)
        fin = b if fin is None else max(fin, b)
    return debut, fin


def calcul_decalage(debut, fin, speed):
    """Decalage en ms qui amene la fin du jeu a maintenant.

    Arrondi a l'heure entiere inferieure : les fenetres epoch (tumbling
    10 min, hopping 15/5...) gardent leur alignement, et aucun record ne
    part dans le futur. Avec --speed > 0, la fin est visee a la fin du
    rejeu : a --speed 1, les records arrivent a leur heure.
    """
    duree_rejeu = (fin - debut) / speed if speed > 0 else 0
    cible = time.time() * 1000 + duree_rejeu
    return int((cible - fin) // HEURE_MS) * HEURE_MS


def decaler(value, decalage):
    """Translate de `decalage` ms chaque horodatage ISO du message brut.

    On travaille sur le texte, pas sur le JSON : un message sale reste sale
    a l'octet pres (meme longueur, meme troncature) et un doublon reste un
    doublon exact.
    """
    def un(m):
        try:
            dt = datetime.strptime(m.group(0), "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:      # 2026-13-45T... : faux timestamp, intact
            return m.group(0)
        dt += timedelta(milliseconds=decalage)
        return (dt.strftime("%Y-%m-%dT%H:%M:%S.")
                + "%03dZ" % (dt.microsecond // 1000))
    return ISO_MS.sub(un, value)


def minute_utc(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) \
        .strftime("%Y-%m-%d %H:%M UTC")


def lire_groupes(valeur):
    """Analyse --groupes : "grp01,grp02" ou "@fichier".

    Dans le fichier : un groupe par ligne (les virgules marchent aussi),
    tout ce qui suit un # est ignore.
    """
    if valeur.startswith("@"):
        try:
            with open(valeur[1:], encoding="utf-8") as f:
                lignes = [l.split("#", 1)[0] for l in f]
        except OSError as e:
            raise SystemExit("--groupes : %s" % e)
    else:
        lignes = [valeur]
    groupes = []
    for ligne in lignes:
        for g in ligne.replace(";", ",").split(","):
            g = g.strip()
            if g and g not in groupes:
                groupes.append(g)
    if not groupes:
        raise SystemExit("--groupes : aucun groupe dans %s" % valeur)
    return groupes


def topics_entree(projet):
    """Topics d'entree declares dans scenarios/<projet>.py."""
    scenario = importlib.import_module("scenarios.%s" % projet)
    return [(nom, cfg.get("partitions", 3), cfg.get("config", {}))
            for nom, cfg in scenario.TOPICS.items()]


def topics_sortie(chemin, projet, groupes):
    """Topics de sortie du projet, declines pour chaque groupe.

    Le JSON liste des noms modeles ou <grp> est remplace par le groupe.
    Une entree est soit une chaine (tout par defaut), soit un objet
    {"nom", "partitions", "config"} qui surcharge _defauts.
    """
    try:
        with open(chemin, encoding="utf-8") as f:
            conf = json.load(f)
    except OSError as e:
        raise SystemExit("liste des sorties : %s" % e)
    except ValueError as e:
        raise SystemExit("%s : JSON invalide (%s)" % (chemin, e))
    defauts = conf.get("_defauts", {})
    projets = conf.get("projets", {})
    if projet not in projets:
        raise SystemExit("projet '%s' absent de %s (connus : %s)"
                         % (projet, chemin,
                            ", ".join(sorted(projets)) or "aucun"))
    topics = []
    vus = set()
    for groupe in groupes:
        for entree in projets[projet]:
            if isinstance(entree, str):
                entree = {"nom": entree}
            nom = entree["nom"].replace(GRP, groupe)
            if nom in vus:
                continue        # topic sans <grp> : partage, cree une fois
            vus.add(nom)
            config = dict(defauts.get("config", {}))
            config.update(entree.get("config", {}))
            topics.append((nom,
                           entree.get("partitions",
                                      defauts.get("partitions", 3)),
                           config))
    return topics


def creer(topics, bootstrap, rf, dry_run, tag):
    """Cree les topics [(nom, partitions, config)]. Renvoie le nb d'echecs.

    « Deja present » n'est pas un echec : la commande doit pouvoir etre
    relancee (groupe ajoute, projet complete) sans rien casser.
    """
    if not topics:
        print("[%s] aucun topic a creer" % tag)
        return 0
    if dry_run:
        for nom, parts, config in topics:
            print("[%s] (dry-run) %-44s partitions=%d rf=%d %s"
                  % (tag, nom, parts, rf,
                     " ".join("%s=%s" % kv for kv in sorted(config.items()))))
        print("[%s] (dry-run) %d topics, rien envoye au broker"
              % (tag, len(topics)))
        return 0

    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([
        NewTopic(nom, num_partitions=parts, replication_factor=rf,
                 config={k: str(v) for k, v in config.items()})
        for nom, parts, config in topics])
    crees, deja, echecs = [], 0, []
    for nom, f in futures.items():
        try:
            f.result()
            crees.append(nom)
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                deja += 1
            else:
                echecs.append((nom, e.args[0].str()))
        except Exception as e:
            echecs.append((nom, e))
    for nom in crees:
        print("[%s] cree : %s" % (tag, nom))
    for nom, err in echecs:
        print("[%s] ECHEC %s : %s" % (tag, nom, err))
    print("[%s] %d crees, %d deja presents, %d echecs (rf=%d)"
          % (tag, len(crees), deja, len(echecs), rf))
    return len(echecs)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dossier",
                    help="dossier produit par generator.py --to-dir ; "
                         "inutile si on ne cree que des topics")
    ap.add_argument("--bootstrap",
                    default=os.environ.get("KAFKA_BOOTSTRAP",
                                           "localhost:29092"))
    ap.add_argument("--speed", type=float, default=0,
                    help="0 (defaut) = aussi vite que possible ; "
                         "1 = temps reel ; 60 = 60 fois plus vite")
    ap.add_argument("--project", default=None,
                    help="scenario, requis avec --create-topics "
                         "ou --create-output-topics")
    ap.add_argument("--create-topics", action="store_true",
                    help="creer les topics d'entree (scenarios/<projet>.py)")
    ap.add_argument("--create-output-topics", action="store_true",
                    help="creer les topics de sortie des groupes (--sorties)")
    ap.add_argument("--groupes", default=None,
                    help="groupes a substituer a <grp> : \"grp01,grp02\" "
                         "ou \"@fichier\"")
    ap.add_argument("--sorties", default=SORTIES_DEFAUT,
                    help="JSON des topics de sortie (defaut : %(default)s)")
    ap.add_argument("--replication-factor", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true",
                    help="afficher au lieu de creer / de produire")
    ap.add_argument("--dates-origine", action="store_true",
                    help="garder les dates d'origine au lieu de recaler le "
                         "jeu sur maintenant (purge par la retention si le "
                         "jeu a plus de 30 jours)")
    args = ap.parse_args()

    if not args.dossier and not (args.create_topics
                                 or args.create_output_topics):
        ap.error("--dossier est requis (sauf si on ne cree que des topics)")
    if args.create_topics and not args.project:
        ap.error("--create-topics exige --project")
    if args.create_output_topics and not args.project:
        ap.error("--create-output-topics exige --project")
    if args.create_output_topics and not args.groupes:
        ap.error("--create-output-topics exige --groupes")

    # Le dossier est verifie avant toute creation : mieux vaut echouer sur
    # un chemin faux que laisser 90 topics derriere soi.
    fichiers = []
    if args.dossier:
        fichiers = sorted(glob.glob(os.path.join(args.dossier, "*.jsonl")))
        if not fichiers:
            raise SystemExit("aucun .jsonl dans %s" % args.dossier)

    echecs_topics = 0
    if args.create_topics:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        echecs_topics += creer(topics_entree(args.project), args.bootstrap,
                               args.replication_factor, args.dry_run,
                               "entree")
    if args.create_output_topics:
        groupes = lire_groupes(args.groupes)
        sorties = topics_sortie(args.sorties, args.project, groupes)
        print("[sortie] %s : %d groupes (%s) -> %d topics"
              % (args.project, len(groupes), ", ".join(groupes),
                 len(sorties)))
        echecs_topics += creer(sorties, args.bootstrap,
                               args.replication_factor, args.dry_run,
                               "sortie")

    if not args.dossier:
        raise SystemExit(1 if echecs_topics else 0)

    flux = [lire(f, os.path.basename(f)[:-6]) for f in fichiers]
    print("[rejeu] %d topics : %s"
          % (len(fichiers),
             ", ".join(os.path.basename(f)[:-6] for f in fichiers)))

    decalage = 0
    debut, fin = bornes(fichiers)
    if debut is not None and not args.dates_origine:
        decalage = calcul_decalage(debut, fin, args.speed)
    if debut is not None:
        print("[rejeu] jeu de donnees : %s -> %s"
              % (minute_utc(debut), minute_utc(fin)))
    if decalage:
        heures = decalage // HEURE_MS
        print("[rejeu] dates recalees de %+d h (%+.1f j) : %s -> %s "
              "(--dates-origine pour les garder)"
              % (heures, heures / 24.0,
                 minute_utc(debut + decalage), minute_utc(fin + decalage)))

    # produce() est asynchrone : il empile en queue locale et rend la main.
    # Sans callback, aucune erreur de livraison ne remonte jamais.
    stats = {"livres": 0, "echecs": 0, "premier_echec": None}

    def livraison(err, msg):
        if err:
            stats["echecs"] += 1
            if stats["premier_echec"] is None:
                stats["premier_echec"] = "%s : %s" % (msg.topic(), err)
        else:
            stats["livres"] += 1

    if args.dry_run:
        producer = None
    else:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": args.bootstrap,
                             "linger.ms": 50, "compression.type": "lz4"})

    n = 0
    t0_flux = None
    t0_reel = time.monotonic()
    par_topic = {}
    # heapq.merge : fusion chronologique en flux, sans tout charger en memoire
    for ts_ms, topic, key, value in heapq.merge(*flux, key=lambda r: r[0]):
        if decalage:
            ts_ms += decalage
            value = decaler(value, decalage)
        if args.speed > 0:
            if t0_flux is None:
                t0_flux = ts_ms
            cible = (ts_ms - t0_flux) / 1000.0 / args.speed
            retard = cible - (time.monotonic() - t0_reel)
            if retard > 0:
                time.sleep(retard)
        if producer is None:
            print("%s | %s | %s" % (topic, key, value[:110]))
        else:
            # Contre-pression : le disque local lit bien plus vite que le
            # reseau ne livre. Quand la queue est pleine, on attend qu'elle
            # se draine au lieu de crasher sur BufferError.
            while True:
                try:
                    producer.produce(topic,
                                     key=(key or "").encode("utf-8"),
                                     value=value.encode("utf-8"),
                                     timestamp=ts_ms,
                                     on_delivery=livraison)
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)
        n += 1
        par_topic[topic] = par_topic.get(topic, 0) + 1
        if n % 20000 == 0:
            print("[rejeu] %d produits / %d livres..." % (n, stats["livres"]))

    if producer is not None:
        restant = producer.flush(300)
        if restant:
            print("[rejeu] ATTENTION : %d messages jamais livres" % restant)
    for topic in sorted(par_topic):
        print("[rejeu] %-38s %7d messages" % (topic, par_topic[topic]))
    print("[rejeu] termine : %d produits, %d livres, %d echecs en %.1f s"
          % (n, stats["livres"], stats["echecs"], time.monotonic() - t0_reel))
    if stats["premier_echec"]:
        print("[rejeu] premier echec : %s" % stats["premier_echec"])
        raise SystemExit(1)
    if echecs_topics:
        raise SystemExit(1)


if __name__ == "__main__":
    main()