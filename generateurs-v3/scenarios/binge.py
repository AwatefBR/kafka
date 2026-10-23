"""Scenario BINGE - plateforme de video a la demande (inspire Netflix).
"""
import random
from . import common

NAME = "binge"
DEFAULT_RATE = 12.0
CATALOG_REFRESH = 600

TOPIC_EVENTS = "binge.playback.events"
TOPIC_CATALOG = "binge.catalog"

TOPICS = {
    TOPIC_EVENTS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_CATALOG: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}

EVENT_TYPES = ["PLAY", "PAUSE", "HEARTBEAT", "BUFFERING", "STOP"]
DEVICES = [("TV", 4), ("MOBILE", 3), ("WEB", 2), ("CONSOLE", 1)]
REGIONS = [("EU-FR", 5), ("EU-DE", 2), ("EU-ES", 2), ("US-EAST", 3),
           ("US-WEST", 2), ("APAC", 1)]
GENRES = ["DRAME", "COMEDIE", "SF", "THRILLER", "DOCUMENTAIRE",
          "ANIMATION", "ACTION"]

TITLES = [
    "La Meute", "Zone Blanche 2100", "Pixel Wars", "Le Dernier Serveur",
    "Chronos", "Marseille Nord", "Les Recruteurs", "Apnee", "Bug Royal",
    "Odyssee 9", "La Faille", "Startup Nation", "Minuit a Belleville",
    "Overclock", "Le Protocole", "Sables Rouges", "Hexagone", "Deadline",
    "La Tour", "Silence Radio", "Neon Dynastie", "Le Grand Split",
    "Fournaise", "Kernel Panic", "Les Invisibles du Perif", "Alpha Trois",
    "Retrograde", "La Diagonale", "Point de Bascule", "Horizon Perdu",
    "Cache-Cache Mortel", "Le Refactor",
]

SPEC = {
    TOPIC_EVENTS: {
        "required": ["event_id", "event_type", "user_id", "content_id",
                     "timestamp"],
        "numeric": ["position_seconds", "bitrate_kbps"],
        "enum": {"event_type": EVENT_TYPES,
                 "device": [d for d, _ in DEVICES],
                 "region": [r for r, _ in REGIONS]},
        "ts": "timestamp",
    }
}


def _catalog_items():
    rng = random.Random(4242)
    items = []
    for i, title in enumerate(TITLES):
        items.append({
            "content_id": "c-%04d" % i,
            "title": title,
            "type": rng.choice(["FILM", "SERIE"]),
            "genre": rng.choice(GENRES),
            "duration_seconds": rng.randint(1500, 7800),
            "released_year": rng.randint(1999, 2026),
        })
    return items


def catalog(_rng):
    return [(TOPIC_CATALOG, it["content_id"], it) for it in _catalog_items()]


class State:
    def __init__(self, rng):
        self.rng = rng
        self.items = _catalog_items()
        self.weights = common.zipf_weights(len(self.items))
        self.users = ["u-%04d" % i for i in range(400)]
        self.sessions = {}
        self.storm = common.Incident("buffering_storm", 240, 45, rng)
        self.storm_target = None

    def _event(self, now, user, session, etype):
        return (TOPIC_EVENTS, user, {
            "event_id": common.new_id(self.rng, "ev"),
            "event_type": etype,
            "user_id": user,
            "content_id": session["content_id"],
            "device": session["device"],
            "region": session["region"],
            "position_seconds": session["position"],
            "bitrate_kbps": self.rng.choice([1500, 3000, 4500, 8000]),
            "timestamp": common.iso(now),
        })

    def _one(self, now):
        user = self.rng.choice(self.users)
        session = self.sessions.get(user)
        if session is None:
            idx = common.weighted_index(self.rng, self.weights)
            session = {
                "content_id": self.items[idx]["content_id"],
                "device": common.pick_weighted(self.rng, DEVICES),
                "region": common.pick_weighted(self.rng, REGIONS),
                "position": 0,
            }
            self.sessions[user] = session
            return self._event(now, user, session, "PLAY")
        r = self.rng.random()
        if r < 0.78:
            session["position"] += 30
            return self._event(now, user, session, "HEARTBEAT")
        if r < 0.83:
            return self._event(now, user, session, "PAUSE")
        if r < 0.88:
            return self._event(now, user, session, "BUFFERING")
        ev = self._event(now, user, session, "STOP")
        del self.sessions[user]
        return ev

    def tick(self, now, n):
        out = [self._one(now) for _ in range(n)]
        active, started = self.storm.active()
        if started:
            hot = self.items[self.rng.randrange(0, 5)]
            self.storm_target = (hot["content_id"], "EU-FR")
        if active and self.storm_target:
            cid, region = self.storm_target
            user = self.rng.choice(self.users)
            out.append((TOPIC_EVENTS, user, {
                "event_id": common.new_id(self.rng, "ev"),
                "event_type": "BUFFERING",
                "user_id": user,
                "content_id": cid,
                "device": "TV",
                "region": region,
                "position_seconds": self.rng.randint(0, 3600),
                "bitrate_kbps": 800,
                "timestamp": common.iso(now),
            }))
        return out
