"""Scenario VIRAL - reseau social (inspire Instagram / Meta).

"""
from . import common

NAME = "viral"
DEFAULT_RATE = 15.0
CATALOG_REFRESH = 10 ** 9

TOPIC_POSTS = "viral.posts"
TOPIC_INTERACTIONS = "viral.interactions"

TOPICS = {
    TOPIC_POSTS: {"partitions": 3, "config": {"retention.ms": "2592000000"}},
    TOPIC_INTERACTIONS: {"partitions": 6, "config": {"retention.ms": "2592000000"}},
}

INTERACTION_TYPES = [("VIEW", 70), ("LIKE", 20), ("COMMENT", 6), ("SHARE", 4)]
LANGS = [("fr", 6), ("en", 3), ("es", 1)]

HASHTAGS = [
    "recette", "fitness", "paris", "voyage", "streetfood", "gaming",
    "makeup", "moto", "chatons", "brocante", "running", "photo",
    "randonnee", "sneakers", "jardinage", "concert", "vanlife", "deco",
    "foot", "manga", "crypto", "vinyle", "surf", "patisserie", "tattoo",
    "cinema", "velo", "kpop", "bricolage", "astro", "escalade", "friperie",
    "openspace", "montagne", "festival", "coding", "plage", "fromage",
    "marathon", "aquarelle",
]

TEXTS = [
    "Nouvelle journee, nouveau projet",
    "Vous en pensez quoi ?",
    "Je ne m'attendais pas a ca...",
    "Best moment de la semaine",
    "Petit avant / apres",
    "On lache rien",
    "Le resultat apres 3 mois",
    "Tuto express en story",
    "Grosse annonce bientot",
    "Merci pour les 10k",
]

SPEC = {
    TOPIC_POSTS: {
        "required": ["post_id", "user_id", "text", "timestamp"],
        "numeric": [],
        "enum": {"lang": [l for l, _ in LANGS]},
        "ts": "timestamp",
    },
    TOPIC_INTERACTIONS: {
        "required": ["interaction_id", "post_id", "user_id", "type",
                     "timestamp"],
        "numeric": [],
        "enum": {"type": [t for t, _ in INTERACTION_TYPES]},
        "ts": "timestamp",
    },
}


def catalog(_rng):
    return []


class State:
    def __init__(self, rng):
        self.rng = rng
        self.authors = ["auteur-%03d" % i for i in range(200)]
        self.users = ["u-%04d" % i for i in range(800)]
        self.recent_posts = []
        self.hot_tag = None
        self.hot_until = 0.0
        self.trend = common.Incident("trending_tag", 120, 120, rng)
        self.viral = common.Incident("viral_post", 300, 90, rng)
        self.viral_post = None
        self.bots = common.Incident("bot_wave", 240, 120, rng)
        self.bot_user = None

    def _tags(self):
        n = self.rng.randint(1, 3)
        tags = [self.rng.choice(HASHTAGS) for _ in range(n)]
        if self.hot_tag and self.rng.random() < 0.6:
            tags.append(self.hot_tag)
        return tags

    def _post(self, now):
        author = self.rng.choice(self.authors)
        post_id = common.new_id(self.rng, "post")
        tags = self._tags()
        text = "%s %s" % (self.rng.choice(TEXTS),
                          " ".join("#" + t for t in tags))
        self.recent_posts.append(post_id)
        if len(self.recent_posts) > 300:
            self.recent_posts.pop(0)
        return (TOPIC_POSTS, post_id, {
            "post_id": post_id,
            "user_id": author,
            "text": text,
            "lang": common.pick_weighted(self.rng, LANGS),
            "timestamp": common.iso(now),
        })

    def _interaction(self, now, post_id=None, user=None, itype=None):
        if post_id is None:
            if not self.recent_posts:
                return None
            # biais de popularite vers les posts recents
            idx = min(len(self.recent_posts) - 1,
                      int(abs(self.rng.gauss(0, 0.25)) * len(self.recent_posts)))
            post_id = self.recent_posts[-1 - idx]
        user = user or self.rng.choice(self.users)
        itype = itype or common.pick_weighted(self.rng, INTERACTION_TYPES)
        return (TOPIC_INTERACTIONS, post_id, {
            "interaction_id": common.new_id(self.rng, "int"),
            "post_id": post_id,
            "user_id": user,
            "type": itype,
            "timestamp": common.iso(now),
        })

    def tick(self, now, n):
        out = []
        for _ in range(n):
            if self.rng.random() < 0.06 or not self.recent_posts:
                out.append(self._post(now))
            else:
                ev = self._interaction(now)
                if ev:
                    out.append(ev)

        _, started = self.trend.active()
        if started:
            self.hot_tag = self.rng.choice(HASHTAGS)

        active, started = self.viral.active()
        if started and self.recent_posts:
            self.viral_post = self.recent_posts[-1]
        if active and self.viral_post:
            ev = self._interaction(now, post_id=self.viral_post)
            if ev:
                out.append(ev)

        active, started = self.bots.active()
        if started:
            self.bot_user = "bot-%03d" % self.rng.randint(0, 999)
        if active and self.bot_user and self.recent_posts and self.rng.random() < 0.25:
            ev = self._interaction(now, user=self.bot_user, itype="LIKE")
            if ev:
                out.append(ev)
        return out
