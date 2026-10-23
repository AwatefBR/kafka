"""Scenario FORGE - supervision d'usine / maintenance predictive
(inspire des plateformes type Siemens MindSphere).

"""
import random

from . import common

NAME = "forge"
DEFAULT_RATE = 8.0            # pieces produites par seconde
CATALOG_REFRESH = 600

TOPIC_READINGS = "forge.readings"
TOPIC_PRODUCTION = "forge.production"
TOPIC_MACHINES = "forge.machines"

TOPICS = {
    TOPIC_READINGS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_PRODUCTION: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_MACHINES: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}

ATELIERS = [("EMBOUTISSAGE", 4), ("USINAGE", 5), ("ASSEMBLAGE", 4),
            ("PEINTURE", 2), ("CONTROLE", 2)]
MODELES = ["HX-200", "HX-400", "TR-90", "TR-120", "KM-7", "KM-9", "ZL-3"]
PRODUITS = ["ref-%03d" % i for i in range(40)]

NB_MACHINES = 60
NB_NEUVES = 4                 # machines hors catalogue
RELEVES_PAR_TICK = 1          # tour de role : 60 machines / 4 ticks/s = 15 s

REBUT_NORMAL = 0.03
REBUT_DERIVE = 0.65
REBUT_MAX_RATIO = 0.15        # au-dela, la machine est en derive
REBUT_MIN_PIECES = 50         # en dessous, le taux n'est pas significatif
SILENCE_MINUTES = 8           # sans releve au-dela, la machine est muette

SPEC = {
    TOPIC_READINGS: {
        "required": ["reading_id", "machine_id", "timestamp"],
        "numeric": ["temperature_c", "vibration_mm_s", "rpm"],
        "enum": {},
        "ts": "timestamp",
    },
    TOPIC_PRODUCTION: {
        "required": ["unit_id", "machine_id", "product_id", "timestamp"],
        "numeric": ["cycle_time_ms", "unit_value_eur"],
        "enum": {},
        "ts": "timestamp",
    },
}


def _machines():
    rng = random.Random(6060)
    out = []
    for i in range(NB_MACHINES):
        atelier = common.pick_weighted(rng, ATELIERS)
        out.append({
            "machine_id": "mac-%03d" % i,
            "label": "%s %02d" % (rng.choice(MODELES), rng.randint(1, 40)),
            "atelier": atelier,
            "ligne": "%s-L%d" % (atelier[:3], rng.randint(1, 4)),
            "modele": rng.choice(MODELES),
        })
    return out


def catalog(_rng):
    return [(TOPIC_MACHINES, m["machine_id"], m) for m in _machines()]


class State:
    def __init__(self, rng):
        self.rng = rng
        self.machines = _machines()
        # machines neuves : elles produisent mais ne sont pas au catalogue
        self.parc = self.machines + [
            {"machine_id": "new-%04d" % i, "atelier": None}
            for i in range(NB_NEUVES)]
        self.poids = common.zipf_weights(len(self.parc))
        self.curseur = 0
        self.derive = common.Incident("derive_outil", 300, 150, rng)
        self.derive_machine = None
        self.derive_gate = 0
        self.panne = common.Incident("panne_passerelle", 1200, 900, rng)
        self.muettes = set()

    def _releve(self, now, m):
        return (TOPIC_READINGS, m["machine_id"], {
            "reading_id": common.new_id(self.rng, "rd"),
            "machine_id": m["machine_id"],
            "temperature_c": round(self.rng.uniform(38.0, 74.0), 1),
            "vibration_mm_s": round(self.rng.uniform(0.4, 4.5), 2),
            "rpm": self.rng.randint(600, 3200),
            "timestamp": common.iso(now),
        })

    def _piece(self, now, m=None, rebut=None):
        if m is None:
            m = self.parc[common.weighted_index(self.rng, self.poids)]
        if rebut is None:
            rebut = self.rng.random() < REBUT_NORMAL
        return (TOPIC_PRODUCTION, m["machine_id"], {
            "unit_id": common.new_id(self.rng, "u"),
            "machine_id": m["machine_id"],
            "product_id": self.rng.choice(PRODUITS),
            "scrap": rebut,
            "cycle_time_ms": self.rng.randint(4000, 45000),
            "unit_value_eur": round(self.rng.uniform(1.5, 240.0), 2),
            "timestamp": common.iso(now),
        })

    def tick(self, now, n):
        out = []

        # --- incident passerelle : on decide AVANT le tour de role
        active, started = self.panne.active()
        if started:
            choisies = self.rng.sample(self.machines, 3)
            self.muettes = {m["machine_id"] for m in choisies}
        if not active and self.muettes:
            self.muettes = set()

        # --- releves capteurs : tour de role, ~1 machine toutes les 15 s
        for _ in range(RELEVES_PAR_TICK):
            m = self.machines[self.curseur % len(self.machines)]
            self.curseur += 1
            if m["machine_id"] in self.muettes:
                continue          # passerelle en panne : silence radio
            out.append(self._releve(now, m))

        # --- production
        for _ in range(n):
            out.append(self._piece(now))

        # --- incident derive d'outil
        active, started = self.derive.active()
        if started:
            # une machine de la longue traine : sinon sa production normale
            # noierait le rebut sous le seuil
            self.derive_machine = self.machines[self.rng.randrange(40, 60)]
        if active and self.derive_machine:
            self.derive_gate += 1
            if self.derive_gate % 4 == 0:      # ~1 piece / s
                out.append(self._piece(
                    now, m=self.derive_machine,
                    rebut=self.rng.random() < REBUT_DERIVE))
        return out
