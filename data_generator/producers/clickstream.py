import uuid
import random
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty", "Food", "Toys"]
DEVICES = [("mobile", 0.60), ("desktop", 0.30), ("tablet", 0.10)]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "CA", "AU", "BR", "JP", "KR",
             "MX", "IT", "ES", "NL", "SG", "AE", "ZA", "NG", "AR", "PL"]
REFERRERS = ["google.com", "facebook.com", "instagram.com", "twitter.com",
             "direct", "email", "youtube.com", "tiktok.com"]

FUNNEL = ["page_view", "product_view", "add_to_cart", "checkout_start", "checkout_complete"]


def _pick_device():
    r = random.random()
    cumulative = 0
    for device, weight in DEVICES:
        cumulative += weight
        if r < cumulative:
            return device
    return "mobile"


class ClickstreamProducer:
    def __init__(self, user_pool: list, product_pool: list):
        self.users = user_pool
        self.products = product_pool

    def generate_event(self, user_id: str = None, session_id: str = None,
                       event_type: str = None) -> dict:
        uid = user_id or random.choice(self.users)["id"]
        sid = session_id or str(uuid.uuid4())
        product = random.choice(self.products)
        et = event_type or random.choice(FUNNEL[:3])
        device = _pick_device()
        country = random.choice(COUNTRIES)

        return {
            "event_id": str(uuid.uuid4()),
            "user_id": uid,
            "session_id": sid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": et,
            "page_url": f"https://retailpulse.com/{product['category'].lower()}/{product['id']}",
            "product_id": product["id"] if et in ("product_view", "add_to_cart") else None,
            "product_category": product["category"],
            "search_query": fake.word() if et == "search" else None,
            "device_type": device,
            "browser": random.choice(BROWSERS),
            "country": country,
            "city": fake.city(),
            "referrer": random.choice(REFERRERS),
        }

    def generate_session(self) -> list[dict]:
        user = random.choice(self.users)
        session_id = str(uuid.uuid4())
        events = []

        events.append(self.generate_event(user["id"], session_id, "page_view"))

        for _ in range(random.randint(1, 4)):
            events.append(self.generate_event(user["id"], session_id, "product_view"))

        if random.random() < 0.40:
            events.append(self.generate_event(user["id"], session_id, "add_to_cart"))
            if random.random() < 0.60:
                events.append(self.generate_event(user["id"], session_id, "checkout_start"))
                if random.random() < 0.70:
                    events.append(self.generate_event(user["id"], session_id, "checkout_complete"))

        return events

    def produce(self, producer, topic: str = "retail.clickstream", n: int = 5):
        import json
        sent = 0
        for _ in range(n):
            for event in self.generate_session():
                producer.send(topic, value=json.dumps(event).encode())
                sent += 1
        return sent
