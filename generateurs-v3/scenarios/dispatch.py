"""Scenario DISPATCH - VTC / mobilite urbaine (inspire Uber).

"""
from . import common

NAME = "dispatch"
DEFAULT_RATE = 12.0
CATALOG_REFRESH = 10 ** 9

TOPIC_REQUESTS = "dispatch.ride.requests"
TOPIC_LOCATIONS = "dispatch.driver.locations"
TOPIC_TRIPS = "dispatch.trip.events"

TOPICS = {
    TOPIC_REQUESTS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_LOCATIONS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
    TOPIC_TRIPS: {"partitions": 3, "config": {"retention.ms": "2592000000"}},
}

ZONES = [
    ("Z-CHATELET", 48.8586, 2.3470), ("Z-MONTMARTRE", 48.8867, 2.3431),
    ("Z-BASTILLE", 48.8532, 2.3692), ("Z-BERCY", 48.8388, 2.3826),
    ("Z-DEFENSE", 48.8917, 2.2419), ("Z-ETOILE", 48.8738, 2.2950),
    ("Z-MONTPARNASSE", 48.8421, 2.3219), ("Z-BELLEVILLE", 48.8720, 2.3765),
    ("Z-ODEON", 48.8520, 2.3396), ("Z-PIGALLE", 48.8820, 2.3370),
    ("Z-NATION", 48.8483, 2.3962), ("Z-AUTEUIL", 48.8470, 2.2600),
]
ZONE_IDS = [z for z, _, _ in ZONES]
STATUSES = ["AVAILABLE", "BUSY", "OFFLINE"]

SPEC = {
    TOPIC_REQUESTS: {
        "required": ["request_id", "user_id", "pickup", "dropoff",
                     "timestamp"],
        "numeric": [],
        "enum": {},
        "ts": "timestamp",
    },
    TOPIC_LOCATIONS: {
        "required": ["driver_id", "lat", "lon", "zone", "status",
                     "timestamp"],
        "numeric": ["lat", "lon"],
        "enum": {"status": STATUSES, "zone": ZONE_IDS},
        "ts": "timestamp",
    },
    TOPIC_TRIPS: {
        "required": ["trip_id", "driver_id", "user_id", "event",
                     "timestamp"],
        "numeric": ["distance_km", "duration_seconds"],
        "enum": {"event": ["START", "END"]},
        "ts": "timestamp",
    },
}


def catalog(_rng):
    return []


class State:
    def __init__(self, rng):
        self.rng = rng
        self.users = ["u-%04d" % i for i in range(500)]
        self.drivers = {}
        for i in range(80):
            self.drivers["drv-%03d" % i] = {
                "zone": rng.randrange(len(ZONES)),
                "status": rng.choice(["AVAILABLE", "AVAILABLE", "BUSY"]),
            }
        self.driver_ids = list(self.drivers.keys())
        self.zone_weights = common.zipf_weights(len(ZONES), s=0.8)
        self.future = common.FutureQueue()
        self.surge = common.Incident("surge_bercy", 360, 60, rng)
        self.ghost = common.Incident("ghost_trip", 300, 1, rng)
        self.ping_cursor = 0

    def _point(self, zone_idx):
        _, lat, lon = ZONES[zone_idx]
        return {
            "lat": round(lat + self.rng.uniform(-0.006, 0.006), 6),
            "lon": round(lon + self.rng.uniform(-0.008, 0.008), 6),
            "zone": ZONES[zone_idx][0],
        }

    def _request(self, now, zone_idx=None):
        if zone_idx is None:
            zone_idx = common.weighted_index(self.rng, self.zone_weights)
        user = self.rng.choice(self.users)
        req = (TOPIC_REQUESTS, user, {
            "request_id": common.new_id(self.rng, "req"),
            "user_id": user,
            "pickup": self._point(zone_idx),
            "dropoff": self._point(self.rng.randrange(len(ZONES))),
            "timestamp": common.iso(now),
        })
        # 60 % des demandes deviennent une course
        if self.rng.random() < 0.6:
            driver = self.rng.choice(self.driver_ids)
            trip_id = common.new_id(self.rng, "trip")
            t = common.monotonic()
            self.future.push(t + self.rng.uniform(5, 20),
                             ("START", trip_id, driver, user, None, None))
            duration = self.rng.randint(300, 2100)
            speed_kmh = self.rng.uniform(14, 38)
            distance = round(speed_kmh * duration / 3600.0, 2)
            self.future.push(t + 25 + duration * 0.03,  # flux accelere pour la demo
                             ("END", trip_id, driver, user, distance, duration))
        return req

    def _trip_event(self, now, kind, trip_id, driver, user, distance, duration):
        value = {
            "trip_id": trip_id,
            "driver_id": driver,
            "user_id": user,
            "event": kind,
            "timestamp": common.iso(now),
        }
        if kind == "END":
            value["distance_km"] = distance
            value["duration_seconds"] = duration
        return (TOPIC_TRIPS, trip_id, value)

    def tick(self, now, n):
        out = []
        t = common.monotonic()

        # ~70 % des messages = pings chauffeurs, ~30 % = demandes
        for _ in range(n):
            if self.rng.random() < 0.72:
                drv = self.driver_ids[self.ping_cursor % len(self.driver_ids)]
                self.ping_cursor += 1
                d = self.drivers[drv]
                if self.rng.random() < 0.08:
                    d["zone"] = self.rng.randrange(len(ZONES))
                if self.rng.random() < 0.05:
                    d["status"] = common.pick_weighted(
                        self.rng, [("AVAILABLE", 6), ("BUSY", 3), ("OFFLINE", 1)])
                p = self._point(d["zone"])
                out.append((TOPIC_LOCATIONS, drv, {
                    "driver_id": drv,
                    "lat": p["lat"], "lon": p["lon"], "zone": p["zone"],
                    "status": d["status"],
                    "timestamp": common.iso(now),
                }))
            else:
                out.append(self._request(now))

        for item in self.future.pop_due(t):
            out.append(self._trip_event(now, *item))

        active, _ = self.surge.active()
        if active:
            bercy = ZONE_IDS.index("Z-BERCY")
            out.append(self._request(now, zone_idx=bercy))

        _, started = self.ghost.active()
        if started:
            trip_id = common.new_id(self.rng, "trip")
            driver = self.rng.choice(self.driver_ids)
            user = self.rng.choice(self.users)
            if self.rng.random() < 0.5:
                distance, duration = 900.0, 1200      # 2700 km/h
            else:
                distance, duration = 12.0, 21600      # 6 h de course
            out.append(self._trip_event(now, "END", trip_id, driver, user,
                                        distance, duration))
        return out
