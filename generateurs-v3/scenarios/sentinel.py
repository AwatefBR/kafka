"""Scenario SENTINEL - paiements par carte / anti-fraude (inspire Stripe).

"""
import random
from . import common

NAME = "sentinel"
DEFAULT_RATE = 10.0
CATALOG_REFRESH = 600

TOPIC_TX = "sentinel.transactions"
TOPIC_MERCHANTS = "sentinel.merchants"

TOPICS = {
    TOPIC_TX: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_MERCHANTS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}

CITIES = {
    "Paris": (48.8566, 2.3522), "Lyon": (45.7640, 4.8357),
    "Marseille": (43.2965, 5.3698), "Bordeaux": (44.8378, -0.5792),
    "Lille": (50.6292, 3.0573), "Berlin": (52.5200, 13.4050),
    "Madrid": (40.4168, -3.7038), "Milan": (45.4642, 9.1900),
    "London": (51.5074, -0.1278), "New York": (40.7128, -74.0060),
    "Amsterdam": (52.3676, 4.9041), "Bruxelles": (50.8503, 4.3517),
}
MCC = {
    "RESTAURANT": (8, 60), "SUPERMARCHE": (15, 120), "ESSENCE": (30, 90),
    "ELECTRONIQUE": (50, 900), "VOYAGE": (80, 700), "MODE": (20, 250),
    "VTC": (8, 45), "STREAMING": (5, 20),
}
MERCHANT_NAMES = [
    "Chez Momo", "SuperFrais", "Volt Station", "TechCave", "AirVoyage",
    "ModeExpress", "RideNow", "StreamBox", "La Cantine", "PrixMini",
    "GigaWatt", "PixelStore", "SunTrip", "DressCode", "GoCab", "PlayFlux",
    "Bistrot 21", "MaxiCourses", "FuelUp", "ByteShop",
]
CURRENCIES = ["EUR", "USD", "GBP"]
STATUSES = ["APPROVED", "DECLINED"]

SPEC = {
    TOPIC_TX: {
        "required": ["tx_id", "card_id", "merchant_id", "amount",
                     "currency", "timestamp"],
        "numeric": ["amount", "lat", "lon"],
        "enum": {"currency": CURRENCIES, "status": STATUSES},
        "ts": "timestamp",
    }
}


def _merchants():
    rng = random.Random(5150)
    cities = list(CITIES.keys())
    merchants = []
    for i in range(40):
        cat = rng.choice(list(MCC.keys()))
        city = rng.choice(cities)
        merchants.append({
            "merchant_id": "mch-%03d" % i,
            "name": "%s %d" % (rng.choice(MERCHANT_NAMES), rng.randint(1, 99)),
            "category": cat,
            "city": city,
        })
    return merchants


def catalog(_rng):
    return [(TOPIC_MERCHANTS, m["merchant_id"], m) for m in _merchants()]


class State:
    def __init__(self, rng):
        self.rng = rng
        self.merchants = _merchants()
        self.weights = common.zipf_weights(len(self.merchants), s=0.9)
        # profil par carte : panier moyen stable pour rendre big_ticket detectable
        self.cards = {}
        for i in range(500):
            self.cards["card-%04d" % i] = round(rng.uniform(12, 80), 2)
        self.card_ids = list(self.cards.keys())
        self.velocity = common.Incident("velocity_attack", 300, 45, rng)
        self.velocity_card = None
        self.velocity_left = 0
        self.velocity_gate = 0
        self.travel = common.Incident("impossible_travel", 360, 1, rng)
        self.travel_pending = None
        self.travel_fire = 0.0
        self.big = common.Incident("big_ticket", 240, 1, rng)
        self.outage = common.Incident("merchant_outage", 480, 90, rng)
        self.outage_merchant = None

    def _tx(self, now, card=None, merchant=None, amount=None, city=None,
            status=None):
        card = card or self.rng.choice(self.card_ids)
        merchant = merchant or self.merchants[
            common.weighted_index(self.rng, self.weights)]
        if amount is None:
            base = self.cards.get(card, 30.0)
            lo, hi = MCC[merchant["category"]]
            amount = round(max(1.0, self.rng.gauss(base, base * 0.35)
                               + self.rng.uniform(0, (lo + hi) * 0.05)), 2)
        city = city or merchant["city"]
        lat, lon = CITIES[city]
        if status is None:
            status = "APPROVED" if self.rng.random() < 0.94 else "DECLINED"
        return (TOPIC_TX, card, {
            "tx_id": common.new_id(self.rng, "tx"),
            "card_id": card,
            "merchant_id": merchant["merchant_id"],
            "merchant_name": merchant["name"],
            "category": merchant["category"],
            "amount": amount,
            "currency": "EUR" if city in ("Paris", "Lyon", "Marseille",
                                          "Bordeaux", "Lille", "Berlin",
                                          "Madrid", "Milan", "Amsterdam",
                                          "Bruxelles")
                        else ("USD" if city == "New York" else "GBP"),
            "city": city,
            "lat": lat,
            "lon": lon,
            "status": status,
            "timestamp": common.iso(now),
        })

    def tick(self, now, n):
        out = [self._tx(now) for _ in range(n)]
        t = common.monotonic()

        active, started = self.velocity.active()
        if started:
            self.velocity_card = "card-v%03d" % self.rng.randint(0, 999)
            self.velocity_left = 6
            self.velocity_gate = 0
        if active and self.velocity_left > 0:
            self.velocity_gate += 1
            if self.velocity_gate % 28 == 0:  # ~1 tx / 7 s
                self.velocity_left -= 1
                out.append(self._tx(now, card=self.velocity_card,
                                    amount=round(self.rng.uniform(15, 90), 2)))

        _, started = self.travel.active()
        if started:
            card = "card-t%03d" % self.rng.randint(0, 999)
            out.append(self._tx(now, card=card, city="Paris"))
            self.travel_pending = card
            self.travel_fire = t + self.rng.uniform(180, 260)
        if self.travel_pending and t >= self.travel_fire:
            out.append(self._tx(now, card=self.travel_pending,
                                city="New York"))
            self.travel_pending = None

        _, started = self.big.active()
        if started:
            card = self.rng.choice(self.card_ids)
            amount = round(self.cards[card] * self.rng.uniform(18, 25), 2)
            out.append(self._tx(now, card=card, amount=amount))

        active, started = self.outage.active()
        if started:
            self.outage_merchant = self.merchants[self.rng.randrange(0, 6)]
        if active and self.outage_merchant and self.rng.random() < 0.5:
            status = "DECLINED" if self.rng.random() < 0.65 else "APPROVED"
            out.append(self._tx(now, merchant=self.outage_merchant,
                                status=status))
        return out
