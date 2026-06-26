import os
import time
import json
import logging
import random
import uuid
from datetime import datetime, timezone

import psycopg2
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from producers.clickstream import ClickstreamProducer
from producers.orders import OrderProducer
from producers.inventory import InventoryProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
fake = Faker()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PG_DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port=5432 dbname=retailpulse "
    f"user={os.getenv('POSTGRES_USER', 'retailpulse')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'retailpulse123')}"
)

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty", "Food", "Toys"]
TIERS = ["bronze", "silver", "gold", "platinum"]


def wait_for_kafka(max_retries=30, delay=5) -> KafkaProducer:
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                acks="all",
                retries=5,
                max_in_flight_requests_per_connection=1,
            )
            log.info("Kafka connected.")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready (attempt {attempt+1}/{max_retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries.")


def seed_postgres() -> tuple[list, list]:
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE,
            name TEXT,
            signup_date DATE,
            country TEXT,
            tier TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            subcategory TEXT,
            brand TEXT,
            base_price NUMERIC(10,2),
            cost_price NUMERIC(10,2),
            supplier_id TEXT
        )
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        log.info("Seeding 500 users...")
        users_data = []
        for _ in range(500):
            u = {
                "id": str(uuid.uuid4()),
                "email": fake.unique.email(),
                "name": fake.name(),
                "signup_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
                "country": fake.country_code(),
                "tier": random.choice(TIERS),
            }
            users_data.append(u)
            cur.execute(
                "INSERT INTO users VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (u["id"], u["email"], u["name"], u["signup_date"], u["country"], u["tier"])
            )
        conn.commit()
    else:
        cur.execute("SELECT id, email, name, signup_date, country, tier FROM users")
        users_data = [{"id": r[0], "email": r[1], "name": r[2], "signup_date": str(r[3]),
                       "country": r[4], "tier": r[5]} for r in cur.fetchall()]

    cur.execute("SELECT COUNT(*) FROM products")
    if cur.fetchone()[0] == 0:
        log.info("Seeding 200 products...")
        products_data = []
        for cat in CATEGORIES:
            for i in range(25):
                p = {
                    "id": str(uuid.uuid4()),
                    "name": f"{fake.word().title()} {cat[:-1] if cat.endswith('s') else cat} {i+1}",
                    "category": cat,
                    "subcategory": fake.word().title(),
                    "brand": fake.company(),
                    "base_price": round(random.uniform(9.99, 499.99), 2),
                    "cost_price": round(random.uniform(5.0, 200.0), 2),
                    "supplier_id": f"SUP{random.randint(1,20):03d}",
                }
                products_data.append(p)
                cur.execute(
                    "INSERT INTO products VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (p["id"], p["name"], p["category"], p["subcategory"],
                     p["brand"], p["base_price"], p["cost_price"], p["supplier_id"])
                )
        conn.commit()
    else:
        cur.execute("SELECT id, name, category, subcategory, brand, base_price, cost_price, supplier_id FROM products")
        products_data = [
            {"id": r[0], "name": r[1], "category": r[2], "subcategory": r[3],
             "brand": r[4], "base_price": float(r[5]), "cost_price": float(r[6]), "supplier_id": r[7]}
            for r in cur.fetchall()
        ]

    cur.close()
    conn.close()
    log.info(f"Loaded {len(users_data)} users, {len(products_data)} products.")
    return users_data, products_data


def main():
    producer = wait_for_kafka()
    users, products = seed_postgres()

    click_p = ClickstreamProducer(users, products)
    order_p = OrderProducer(users, products)
    inv_p = InventoryProducer(products)

    stats = {"clickstream": 0, "orders": 0, "inventory": 0}
    last_log = time.time()
    tick = 0

    log.info("Starting event generation loop...")
    while True:
        # Clickstream: every tick (0.1s)
        n = click_p.produce(producer, n=random.randint(1, 3))
        stats["clickstream"] += n

        if tick % 20 == 0:  # every ~2s
            n = order_p.produce(producer, n=random.randint(1, 2))
            stats["orders"] += n

        if tick % 50 == 0:  # every ~5s
            n = inv_p.produce(producer, n=1)
            stats["inventory"] += n

        producer.flush()
        tick += 1

        if time.time() - last_log > 60:
            log.info(f"Stats (last 60s): {stats}")
            stats = {"clickstream": 0, "orders": 0, "inventory": 0}
            last_log = time.time()

        time.sleep(0.1)


if __name__ == "__main__":
    main()
