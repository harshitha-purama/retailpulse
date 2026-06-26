import uuid
import random
import json
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
SHIPPING_METHODS = ["standard", "express", "overnight"]
STATUSES = ["order_created", "order_shipped", "order_delivered", "order_cancelled", "order_returned"]
COUPONS = ["SAVE10", "SUMMER20", "VIP15", "FLASH25", None, None, None, None]


class OrderProducer:
    def __init__(self, user_pool: list, product_pool: list):
        self.users = user_pool
        self.products = product_pool

    def _build_items(self) -> tuple[list, float]:
        n_items = random.choices([1, 2, 3, 4, 5], weights=[0.50, 0.25, 0.13, 0.07, 0.05])[0]
        items = []
        subtotal = 0.0
        for _ in range(n_items):
            p = random.choice(self.products)
            qty = random.randint(1, 3)
            price = round(p["base_price"] * random.uniform(0.85, 1.15), 2)
            discount = round(random.choice([0, 0, 0, 5, 10, 15, 20]) / 100, 2)
            items.append({
                "product_id": p["id"],
                "product_name": p["name"],
                "category": p["category"],
                "quantity": qty,
                "unit_price": price,
                "discount_pct": discount,
            })
            subtotal += price * qty * (1 - discount)
        return items, round(subtotal, 2)

    def generate_order(self, user_id: str = None) -> dict:
        uid = user_id or random.choice(self.users)["id"]
        items, subtotal = self._build_items()
        coupon = random.choice(COUPONS)
        discount_amount = round(subtotal * 0.10, 2) if coupon else 0.0
        tax = round((subtotal - discount_amount) * 0.08, 2)
        total = round(subtotal - discount_amount + tax, 2)
        country = fake.country_code()

        event_type = "order_created"
        r = random.random()
        if r < 0.05:
            event_type = "order_returned"
        elif r < 0.20:
            event_type = "order_cancelled"
        elif r < 0.55:
            event_type = "order_delivered"
        elif r < 0.75:
            event_type = "order_shipped"

        return {
            "order_id": str(uuid.uuid4()),
            "user_id": uid,
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "items": items,
            "subtotal": subtotal,
            "discount_amount": discount_amount,
            "tax_amount": tax,
            "total_amount": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_method": random.choice(SHIPPING_METHODS),
            "shipping_address": {
                "country": country,
                "city": fake.city(),
                "zip": fake.postcode(),
            },
            "coupon_code": coupon,
            "status": event_type.replace("order_", ""),
        }

    def produce(self, producer, topic: str = "retail.orders", n: int = 5):
        sent = 0
        for _ in range(n):
            order = self.generate_order()
            producer.send(topic, value=json.dumps(order).encode())
            sent += 1
        return sent
