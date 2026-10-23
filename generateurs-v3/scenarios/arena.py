"""Scenario ARENA - jeu de tir competitif en ligne (inspire Riot Games).
"""
import random

from . import common

NAME = "arena"
DEFAULT_RATE = 15.0
CATALOG_REFRESH = 600

TOPIC_EVENTS = "arena.match.events"
TOPIC_PURCHASES = "arena.purchases"
TOPIC_PLAYERS = "arena.players"

TOPICS = {
    TOPIC_EVENTS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_PURCHASES: {"partitions": 3, "config": {"retention.ms": "2592000000"}},
    TOPIC_PLAYERS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}

EVENT_TYPES = ["MATCH_START", "KILL", "DEATH", "MATCH_END"]
WEAPONS = ["Vandale", "Spectre", "Fantome", "Operateur", "Judge", "Marshal",
           "Bulldog", "Guardian", "Sheriff", "Classic", "Odin", "Ares"]
MAPS = ["Ascension", "Havre", "Scission", "Lotus", "Abysse", "Brise"]
MODES = [("CLASSE", 5), ("PARTIE_RAPIDE", 3), ("MATCH_A_MORT", 2)]
REGIONS = [("EU-OUEST", 5), ("EU-EST", 2), ("NA", 4), ("BR", 2), ("KR", 2),
           ("APAC", 2)]
RANKS = ["FER", "BRONZE", "ARGENT", "OR", "PLATINE", "DIAMANT", "IMMORTEL"]
ITEM_TYPES = [("SKIN", 6), ("MONNAIE", 3), ("PASSE_COMBAT", 2), ("LOT", 1)]
PRIX = [4.99, 9.99, 12.99, 19.99, 24.99, 49.99, 99.99]

PSEUDO_A = ["Nova", "Vex", "Kilo", "Ombre", "Zen", "Rakan", "Pyro", "Nyx",
            "Drake", "Sable", "Onyx", "Kraft", "Volt", "Mirage", "Faucon",
            "Bruit", "Sismo", "Delta", "Orage", "Turbo"]
PSEUDO_B = ["Kane", "Storm", "X", "Sniper", "Blade", "Wolf", "Ghost", "Fury",
            "Zero", "Prime", "Ace", "Rex"]

# --- Regles de session -----------------------------------------------------
SESSION_GAP_MINUTES = 5      # 5 min sans evenement = le joueur a quitte
MARATHON_MINUTES = 12        # session au-dela de laquelle on alerte (demo)

# --- Seuils de triche ------------------------------------------------------
HEADSHOT_MAX_RATIO = 0.80
HEADSHOT_MIN_KILLS = 50

NB_JOUEURS = 600
NB_SMURFS = 12               # ~2 % des comptes, absents du catalogue
MATCHS_SIMULTANES = 90

SPEC = {
    TOPIC_EVENTS: {
        "required": ["event_id", "match_id", "player_id", "event_type",
                     "timestamp"],
        "numeric": ["reaction_ms"],
        "enum": {"event_type": EVENT_TYPES, "map": MAPS,
                 "mode": [m for m, _ in MODES]},
        "ts": "timestamp",
    },
    TOPIC_PURCHASES: {
        "required": ["purchase_id", "player_id", "item_id", "timestamp"],
        "numeric": ["amount_eur"],
        "enum": {"item_type": [i for i, _ in ITEM_TYPES]},
        "ts": "timestamp",
    },
}


def _joueurs():
    rng = random.Random(3131)
    out = []
    for i in range(NB_JOUEURS):
        out.append({
            "player_id": "p-%04d" % i,
            "pseudo": "%s%s%d" % (rng.choice(PSEUDO_A), rng.choice(PSEUDO_B),
                                  rng.randint(2, 99)),
            "region": common.pick_weighted(rng, REGIONS),
            "rank": rng.choice(RANKS),
            "level": rng.randint(1, 400),
        })
    return out


def catalog(_rng):
    return [(TOPIC_PLAYERS, j["player_id"], j) for j in _joueurs()]


class State:
    def __init__(self, rng):
        self.rng = rng
        self.joueurs = _joueurs()
        self.poids = common.zipf_weights(len(self.joueurs))  # baleines
        # les smurfs jouent mais ne sont pas au catalogue
        self.ids = [j["player_id"] for j in self.joueurs] \
            + ["smurf-%04d" % i for i in range(NB_SMURFS)]
        self.actifs = {}
        self.libre_a = {}
        self.achat_gate = 0
        self.aimbot = common.Incident("aimbot", 300, 150, rng)
        self.aimbot_joueur = None
        self.aimbot_match = None
        self.aimbot_gate = 0
        self.marathon = common.Incident("marathon", 1800, 1500, rng)
        self.marathon_joueur = None
        self.marathon_match = None
        self.marathon_gate = 0

    def _match(self, t):
        return {
            "match_id": common.new_id(self.rng, "m"),
            "map": self.rng.choice(MAPS),
            "mode": common.pick_weighted(self.rng, MODES),
            "fin": t + self.rng.uniform(180, 360),   # 3 a 6 min
        }

    def _event(self, now, pid, m, etype=None, headshot=None, reaction=None):
        if etype is None:
            etype = "KILL" if self.rng.random() < 0.45 else "DEATH"
        if etype == "KILL":
            if headshot is None:
                headshot = self.rng.random() < 0.22
            if reaction is None:
                reaction = self.rng.randint(180, 400)
        else:
            headshot = False if headshot is None else headshot
            reaction = 0 if reaction is None else reaction
        return (TOPIC_EVENTS, pid, {
            "event_id": common.new_id(self.rng, "ev"),
            "match_id": m["match_id"],
            "player_id": pid,
            "event_type": etype,
            "weapon": self.rng.choice(WEAPONS),
            "headshot": headshot,
            "reaction_ms": reaction,
            "map": m["map"],
            "mode": m["mode"],
            "timestamp": common.iso(now),
        })

    def tick(self, now, n):
        out = []
        t = common.monotonic()

        # --- fin des matchs : le joueur part en pause (> trou de session)
        for pid in [p for p, m in self.actifs.items() if t >= m["fin"]]:
            m = self.actifs.pop(pid)
            out.append(self._event(now, pid, m, "MATCH_END"))
            self.libre_a[pid] = t + self.rng.uniform(420, 1200)  # 7 a 20 min

        # --- nouveaux matchs
        while len(self.actifs) < MATCHS_SIMULTANES:
            libres = [p for p in self.ids
                      if p not in self.actifs and self.libre_a.get(p, 0.0) <= t]
            if not libres:
                break
            pid = self.rng.choice(libres)
            m = self._match(t)
            self.actifs[pid] = m
            out.append(self._event(now, pid, m, "MATCH_START"))

        # --- action de jeu
        en_jeu = list(self.actifs)
        for _ in range(n):
            if not en_jeu:
                break
            pid = self.rng.choice(en_jeu)
            out.append(self._event(now, pid, self.actifs[pid]))

        # --- achats (loi de puissance : quelques baleines)
        self.achat_gate += 1
        if self.achat_gate % 12 == 0:            # ~1 achat / 3 s
            j = self.joueurs[common.weighted_index(self.rng, self.poids)]
            out.append((TOPIC_PURCHASES, j["player_id"], {
                "purchase_id": common.new_id(self.rng, "pur"),
                "player_id": j["player_id"],
                "item_id": "itm-%04d" % self.rng.randrange(0, 300),
                "item_type": common.pick_weighted(self.rng, ITEM_TYPES),
                "amount_eur": self.rng.choice(PRIX),
                "timestamp": common.iso(now),
            }))

        # --- incident aimbot
        active, started = self.aimbot.active()
        if started:
            self.aimbot_joueur = "aim-%03d" % self.rng.randint(0, 999)
            self.aimbot_match = self._match(t)
        if active and self.aimbot_joueur:
            self.aimbot_gate += 1
            if self.aimbot_gate % 4 == 0:        # ~1 KILL / s
                out.append(self._event(
                    now, self.aimbot_joueur, self.aimbot_match, "KILL",
                    headshot=self.rng.random() < 0.97,
                    reaction=self.rng.randint(60, 110)))

        # --- incident marathon
        active, started = self.marathon.active()
        if started:
            self.marathon_joueur = "mara-%03d" % self.rng.randint(0, 999)
            self.marathon_match = self._match(t)
        if active and self.marathon_joueur:
            if t >= self.marathon_match["fin"]:
                self.marathon_match = self._match(t)
            self.marathon_gate += 1
            if self.marathon_gate % 32 == 0:     # ~1 evenement / 8 s
                out.append(self._event(now, self.marathon_joueur,
                                       self.marathon_match))
        return out
