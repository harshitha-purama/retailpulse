import uuid
import random
import json
from datetime import datetime, timezone

WAREHOUSES = ["WH001", "WH002", "WH003", "WH004", "WH005"]
SUPPLIERS = [f"SUP{i:03d}" for i in range(1, 21)]
EVENT_TYPES = ["restock", "sale", "adjustment", "damage", "return"]


class InventoryProducer:
    def __init__(self, product_pool: list):
        self.products = product_pool
        self.stock = {
            (p["id"], wh): random.randint(50, 500)
            for p in product_pool
            for wh in WAREHOUSES
        }

    def generate_event(self) -> dict:
        product = random.choice(self.products)
        warehouse = random.choice(WAREHOUSES)
        key = (product["id"], warehouse)
        qty_before = self.stock.get(key, 100)

        r = random.random()
        if r < 0.35:
            event_type = "sale"
            qty_change = -random.randint(1, 5)
        elif r < 0.55:
            event_type = "restock"
            qty_change = random.randint(50, 300)
        elif r < 0.70:
            event_type = "return"
            qty_change = random.randint(1, 3)
        elif r < 0.85:
            event_type = "adjustment"
            qty_change = random.randint(-10, 10)
        else:
            event_type = "damage"
            qty_change = -random.randint(1, 10)

        qty_after = max(0, qty_before + qty_change)
        self.stock[key] = qty_after

        return {
            "inventory_id": str(uuid.uuid4()),
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "sku": f"SKU-{product['id'][:8].upper()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "quantity_change": qty_change,
            "quantity_before": qty_before,
            "quantity_after": qty_after,
            "warehouse_id": warehouse,
            "supplier_id": random.choice(SUPPLIERS) if event_type == "restock" else None,
            "unit_cost": round(product.get("cost_price", 10.0), 2),
        }

    def produce(self, producer, topic: str = "retail.inventory", n: int = 3):
        sent = 0
        for _ in range(n):
            event = self.generate_event()
            producer.send(topic, value=json.dumps(event).encode())
            sent += 1
        return sent
