"""Scenario RELAY - regie publicitaire programmatique (inspire Criteo).
"""
import random

from . import common

NAME = "relay"
DEFAULT_RATE = 15.0
CATALOG_REFRESH = 600

TOPIC_IMPRESSIONS = "relay.impressions"
TOPIC_CLICKS = "relay.clicks"
TOPIC_CAMPAIGNS = "relay.campaigns"

TOPICS = {
    TOPIC_IMPRESSIONS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_CLICKS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_CAMPAIGNS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}

DEVICES = ["TV", "MOBILE", "WEB", "CONSOLE"]
GEOS = [("FR", 4), ("US", 2), ("DE", 2), ("BR", 1), ("JP", 1), ("GB", 1)]
VERTICALS = ["MODE", "HIGH-TECH", "VOYAGE", "AUTO", "BANQUE", "TELECOM"]

ADVERTISERS = [
    "Nomad Store", "Cap Horn Voyages", "Velours Noir", "Octet Mobile",
    "Banque Sarcelle", "Pixel Auto", "La Grande Ourse", "Delta Assurances",
    "Ceramique & Co", "Turbine Sport", "Belladone Beaute", "Sismo Telecom",
]
CAMPAIGN_OPS = ["Soldes", "Rentree", "Black Week", "Printemps", "Lancement",
                "Retargeting", "Notoriete", "Trafic", "Noel", "Ete"]
CAMPAIGN_WORDS = ["Nomade", "Prime", "Eclipse", "Boulevard", "Vertige",
                  "Frontiere", "Reflet", "Orage", "Minuit", "Delta"]

# Campagnes servies par le bidder mais absentes du catalogue :
# elles doivent survivre a la jointure (leftJoin), pas disparaitre.
ORPHAN_CAMPAIGNS = ["cmp-0900", "cmp-0901", "cmp-0902"]

VIEWABLE_RATE = 0.72        # taux normal de bannieres visibles
BLIND_VIEWABLE_RATE = 0.10  # taux pendant l'incident blind_site
CLICK_RATE = 0.025          # part des impressions qui recoivent un clic

SPEC = {
    TOPIC_IMPRESSIONS: {
        "required": ["impression_id", "user_id", "campaign_id", "site_id",
                     "timestamp"],
        "numeric": ["paid_price_eur"],
        "enum": {"device": DEVICES, "geo": [g for g, _ in GEOS]},
        "ts": "timestamp",
    },
    TOPIC_CLICKS: {
        "required": ["click_id", "impression_id", "user_id", "campaign_id",
                     "timestamp"],
        "numeric": [],
        "enum": {"device": DEVICES, "geo": [g for g, _ in GEOS]},
        "ts": "timestamp",
    },
}


def _campaigns():
    rng = random.Random(5150)
    camps = []
    for i in range(40):
        camps.append({
            "campaign_id": "cmp-%04d" % i,
            "name": "%s %s" % (rng.choice(CAMPAIGN_OPS),
                               rng.choice(CAMPAIGN_WORDS)),
            "advertiser": rng.choice(ADVERTISERS),
            "vertical": rng.choice(VERTICALS),
            "daily_budget_eur": float(rng.choice([50, 120, 250, 500, 1000])),
            "cpc_max_eur": round(rng.uniform(0.08, 0.90), 2),
        })
    return camps


def catalog(_rng):
    return [(TOPIC_CAMPAIGNS, c["campaign_id"], c) for c in _campaigns()]


class State:
    def __init__(self, rng):
        self.rng = rng
        self.campaigns = _campaigns()
        self.camp_weights = common.zipf_weights(len(self.campaigns))
        self.sites = ["site-%03d" % i for i in range(20)]
        self.site_weights = common.zipf_weights(len(self.sites))
        self.users = ["u-%04d" % i for i in range(800)]
        self.future = common.FutureQueue()
        self.bot = common.Incident("click_bot", 300, 150, rng)
        self.bot_user = None
        self.bot_gate = 0
        self.blind = common.Incident("blind_site", 420, 120, rng)
        self.blind_site = None

    def _impression(self, now, user=None, site=None, viewable=None,
                    click_delay=None):
        user = user or self.rng.choice(self.users)
        if site is None:
            site = self.sites[common.weighted_index(self.rng,
                                                    self.site_weights)]
        if self.rng.random() < 0.02:
            campaign_id = self.rng.choice(ORPHAN_CAMPAIGNS)
        else:
            campaign_id = self.campaigns[common.weighted_index(
                self.rng, self.camp_weights)]["campaign_id"]
        if viewable is None:
            viewable = self.rng.random() < VIEWABLE_RATE
        device = self.rng.choice(DEVICES)
        geo = common.pick_weighted(self.rng, GEOS)
        impression_id = common.new_id(self.rng, "imp")

        if click_delay is None and self.rng.random() < CLICK_RATE:
            click_delay = self.rng.uniform(3, 90)
        if click_delay is not None:
            self.future.push(common.monotonic() + click_delay,
                             (impression_id, user, campaign_id, device, geo))

        return (TOPIC_IMPRESSIONS, user, {
            "impression_id": impression_id,
            "user_id": user,
            "campaign_id": campaign_id,
            "creative_id": "crea-%04d" % self.rng.randrange(0, 200),
            "site_id": site,
            "device": device,
            "geo": geo,
            "viewable": viewable,
            "paid_price_eur": round(self.rng.uniform(0.0005, 0.006), 5),
            "timestamp": common.iso(now),
        })

    def tick(self, now, n):
        out = [self._impression(now) for _ in range(n)]
        t = common.monotonic()

        for impression_id, user, campaign_id, device, geo \
                in self.future.pop_due(t):
            out.append((TOPIC_CLICKS, user, {
                "click_id": common.new_id(self.rng, "clk"),
                "impression_id": impression_id,
                "user_id": user,
                "campaign_id": campaign_id,
                "device": device,
                "geo": geo,
                "timestamp": common.iso(now),
            }))

        active, started = self.bot.active()
        if started:
            self.bot_user = "bot-%03d" % self.rng.randint(0, 999)
        if active and self.bot_user:
            self.bot_gate += 1
            if self.bot_gate % 6 == 0:  # ~1 impression / 1.5 s
                out.append(self._impression(
                    now, user=self.bot_user,
                    click_delay=self.rng.uniform(0.1, 0.6)))

        active, started = self.blind.active()
        if started:
            self.blind_site = self.sites[self.rng.randrange(12, 20)]
        if active and self.blind_site:
            out.append(self._impression(
                now, site=self.blind_site,
                viewable=self.rng.random() < BLIND_VIEWABLE_RATE))
        return out
