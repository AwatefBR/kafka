"""Scenario TEMPO - streaming musical (inspire Spotify).

"""
import random
from . import common

NAME = "tempo"
DEFAULT_RATE = 12.0
CATALOG_REFRESH = 600

TOPIC_EVENTS = "tempo.listening.events"
TOPIC_TRACKS = "tempo.tracks"

TOPICS = {
    TOPIC_EVENTS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_TRACKS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}

EVENT_TYPES = ["START", "SKIP", "COMPLETE"]
COUNTRIES = [("FR", 4), ("US", 2), ("DE", 2), ("BR", 1), ("JP", 1), ("GB", 1)]
GENRES = ["POP", "RAP", "ROCK", "ELECTRO", "JAZZ", "VARIETE"]

ARTISTS = [
    "Nova Kane", "Les Circuits", "MC Terminal", "Aurore B", "Kilomaitre",
    "DJ Sarcelle", "Velours Noir", "La Grande Ourse", "Pixel Sonore",
    "Marnie", "Octet", "Bruits Blancs", "Cap Horn", "Yona", "Le Syndicat",
    "Fauve Polaire", "Nuit Grave", "Sismo", "Belladone", "Rase Campagne",
]
WORDS = ["Minuit", "Fusee", "Orage", "Boulevard", "Reflet", "Turbine",
         "Eclipse", "Papier", "Frontiere", "Vertige", "Ceramique", "Delta"]

SPEC = {
    TOPIC_EVENTS: {
        "required": ["event_id", "user_id", "track_id", "event_type",
                     "timestamp"],
        "numeric": ["ms_played"],
        "enum": {"event_type": EVENT_TYPES,
                 "country": [c for c, _ in COUNTRIES]},
        "ts": "timestamp",
    }
}


def _tracks():
    rng = random.Random(9090)
    tracks = []
    for i in range(120):
        artist = rng.choice(ARTISTS)
        title = "%s %s" % (rng.choice(WORDS), rng.choice(WORDS))
        tracks.append({
            "track_id": "trk-%04d" % i,
            "title": title,
            "artist": artist,
            "genre": rng.choice(GENRES),
            "duration_ms": rng.randint(120000, 300000),
        })
    return tracks


def catalog(_rng):
    return [(TOPIC_TRACKS, t["track_id"], t) for t in _tracks()]


class State:
    def __init__(self, rng):
        self.rng = rng
        self.tracks = _tracks()
        self.weights = common.zipf_weights(len(self.tracks))
        self.users = ["u-%04d" % i for i in range(600)]
        self.farm = common.Incident("stream_farm", 300, 180, rng)
        self.farm_user = None
        self.farm_track = None
        self.farm_gate = 0
        self.flop = common.Incident("flop_release", 420, 120, rng)
        self.flop_track = None

    def _listen(self, now, user=None, track=None, force=None):
        user = user or self.rng.choice(self.users)
        track = track or self.tracks[common.weighted_index(self.rng, self.weights)]
        country = common.pick_weighted(self.rng, COUNTRIES)
        r = self.rng.random()
        if force == "COMPLETE" or (force is None and r < 0.55):
            etype = "COMPLETE"
            ms = self.rng.randint(int(track["duration_ms"] * 0.85),
                                  track["duration_ms"])
        elif force == "SKIP" or (force is None and r < 0.80):
            etype = "SKIP"
            ms = self.rng.randint(1000, 25000)
        else:
            etype = "START"
            ms = 0
        return (TOPIC_EVENTS, user, {
            "event_id": common.new_id(self.rng, "ev"),
            "user_id": user,
            "track_id": track["track_id"],
            "artist": track["artist"],
            "event_type": etype,
            "ms_played": ms,
            "country": country,
            "timestamp": common.iso(now),
        })

    def tick(self, now, n):
        out = [self._listen(now) for _ in range(n)]

        active, started = self.farm.active()
        if started:
            self.farm_user = "farm-%03d" % self.rng.randint(0, 999)
            self.farm_track = self.tracks[self.rng.randrange(60, 120)]
        if active and self.farm_user:
            self.farm_gate += 1
            if self.farm_gate % 20 == 0:  # ~1 ecoute / 5 s
                ev = self._listen(now, user=self.farm_user,
                                  track=self.farm_track, force="COMPLETE")
                ev[2]["ms_played"] = 31000
                out.append(ev)

        active, started = self.flop.active()
        if started:
            self.flop_track = self.tracks[self.rng.randrange(60, 120)]
        if active and self.flop_track:
            force = "SKIP" if self.rng.random() < 0.85 else "COMPLETE"
            out.append(self._listen(now, track=self.flop_track, force=force))
        return out
