"""Scenario CARTFLOW - e-commerce (inspire Amazon).
"""
import random
from . import common

NAME = "cartflow"
DEFAULT_RATE = 5.0
CATALOG_REFRESH = 10 ** 9  # pas de topic compacte pour ce scenario

TOPIC_ORDERS = "cartflow.orders"
TOPIC_PAYMENTS = "cartflow.payments"
TOPIC_STOCK = "cartflow.stock.movements"

TOPICS = {
    TOPIC_ORDERS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_PAYMENTS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_STOCK: {"partitions": 3, "config": {"retention.ms": "2592000000"}},
}

CATEGORIES = {
    "HIGH-TECH": (49, 1500), "MAISON": (15, 400), "MODE": (9, 180),
    "SPORT": (12, 350), "BEAUTE": (5, 90), "JARDIN": (20, 600),
    "JOUETS": (8, 120), "EPICERIE": (2, 40),
}
ADJ = ["Pro", "Max", "Lite", "Ultra", "Eco", "Family", "Nomad", "Prime",
       "Flex", "Zen"]
NOUNS = ["Casque", "Lampe", "Sac", "Clavier", "Tapis", "Batterie", "Drone",
         "Cafetiere", "Enceinte", "Montre"]
CITIES = ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux", "Nantes",
          "Toulouse", "Strasbourg", "Rennes", "Nice"]
WAREHOUSES = ["ORLY-1", "LILLE-2", "LYON-3"]

SPEC = {
    TOPIC_ORDERS: {
        "required": ["order_id", "user_id", "card_id", "items", "total",
                     "timestamp"],
        "numeric": ["total"],
        "enum": {"currency": ["EUR"]},
        "ts": "timestamp",
    },
    TOPIC_PAYMENTS: {
        "required": ["payment_id", "order_id", "card_id", "amount",
                     "timestamp"],
        "numeric": ["amount"],
        "enum": {"status": ["AUTHORIZED", "DECLINED"]},
        "ts": "timestamp",
    },
    TOPIC_STOCK: {
        "required": ["sku", "delta", "warehouse", "timestamp"],
        "numeric": ["delta"],
        "enum": {"warehouse": WAREHOUSES},
        "ts": "timestamp",
    },
}


def _products():
    rng = random.Random(777)
    prods = []
    i = 0
    for cat, (lo, hi) in CATEGORIES.items():
        for _ in range(10):
            prods.append({
                "sku": "sku-%04d" % i,
                "product": "%s %s %d" % (rng.choice(NOUNS), rng.choice(ADJ),
                                         rng.randint(2, 99)),
                "category": cat,
                "unit_price": round(rng.uniform(lo, hi), 2),
            })
            i += 1
    return prods


def catalog(_rng):
    return []


class State:
    def __init__(self, rng):
        self.rng = rng
        self.products = _products()
        self.weights = common.zipf_weights(len(self.products))
        self.users = ["u-%04d" % i for i in range(400)]
        self.cards = ["card-%04d" % i for i in range(300)]
        self.future = common.FutureQueue()
        self.fraud = common.Incident("fraud_card", 300, 90, rng)
        self.fraud_card = None
        self.fraud_left = 0
        self.fraud_next = 0.0
        self.stockrun = common.Incident("stock_run", 420, 60, rng)
        self.stockrun_sku = None
        self.restock_next = 0.0

    def _order(self, now, user=None, card=None, city=None):
        user = user or self.rng.choice(self.users)
        card = card or self.rng.choice(self.cards)
        city = city or self.rng.choice(CITIES)
        n_items = self.rng.randint(1, 4)
        items, total, moves = [], 0.0, []
        for _ in range(n_items):
            p = self.products[common.weighted_index(self.rng, self.weights)]
            qty = self.rng.randint(1, 3)
            items.append({"sku": p["sku"], "product": p["product"],
                          "category": p["category"], "qty": qty,
                          "unit_price": p["unit_price"]})
            total += qty * p["unit_price"]
            moves.append((p["sku"], -qty))
        order_id = common.new_id(self.rng, "ord")
        order = (TOPIC_ORDERS, card, {
            "order_id": order_id,
            "user_id": user,
            "card_id": card,
            "items": items,
            "total": round(total, 2),
            "currency": "EUR",
            "shipping_city": city,
            "timestamp": common.iso(now),
        })
        if self.rng.random() < 0.88:
            delay = self.rng.uniform(5, 480)
            status = "AUTHORIZED" if self.rng.random() < 0.92 else "DECLINED"
            self.future.push(common.monotonic() + delay,
                             (order_id, card, round(total, 2), status))
        out = [order]
        for sku, delta in moves:
            out.append((TOPIC_STOCK, sku, {
                "sku": sku, "delta": delta,
                "warehouse": self.rng.choice(WAREHOUSES),
                "timestamp": common.iso(now),
            }))
        return out

    def tick(self, now, n):
        out = []
        t = common.monotonic()

        for _ in range(n):
            out.extend(self._order(now))

        for order_id, card, amount, status in self.future.pop_due(t):
            out.append((TOPIC_PAYMENTS, card, {
                "payment_id": common.new_id(self.rng, "pay"),
                "order_id": order_id,
                "card_id": card,
                "amount": amount,
                "status": status,
                "timestamp": common.iso(now),
            }))

        if t >= self.restock_next:
            self.restock_next = t + 8
            p = self.rng.choice(self.products)
            out.append((TOPIC_STOCK, p["sku"], {
                "sku": p["sku"], "delta": self.rng.randint(20, 60),
                "warehouse": self.rng.choice(WAREHOUSES),
                "timestamp": common.iso(now),
            }))

        active, started = self.fraud.active()
        if started:
            self.fraud_card = "card-9%03d" % self.rng.randint(0, 999)
            self.fraud_left = self.rng.randint(4, 5)
            self.fraud_next = t
        if active and self.fraud_left > 0 and t >= self.fraud_next:
            self.fraud_next = t + self.rng.uniform(10, 22)
            self.fraud_left -= 1
            out.extend(self._order(now, user="u-fraude",
                                   card=self.fraud_card,
                                   city=self.rng.choice(CITIES)))

        active, started = self.stockrun.active()
        if started:
            self.stockrun_sku = self.products[self.rng.randrange(0, 8)]["sku"]
        if active and self.stockrun_sku:
            out.append((TOPIC_STOCK, self.stockrun_sku, {
                "sku": self.stockrun_sku, "delta": -self.rng.randint(10, 25),
                "warehouse": "ORLY-1",
                "timestamp": common.iso(now),
            }))
        return out
