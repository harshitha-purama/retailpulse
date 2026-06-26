"""Batch job: Bronze orders (Delta Lake) -> silver.transactions (PostgreSQL)."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, current_date, size

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
PG_URL = "jdbc:postgresql://postgres:5432/retailpulse"
PG_PROPS = {"user": "retailpulse", "password": "retailpulse123", "driver": "org.postgresql.Driver"}


def create_spark():
    return (
        SparkSession.builder
        .appName("silver_orders")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def ensure_silver_table(spark):
    spark.read.jdbc(PG_URL, "(SELECT 1) t", properties=PG_PROPS)  # connection check
    conn_str = f"postgresql://retailpulse:retailpulse123@postgres:5432/retailpulse"
    # Create silver schema + table via JDBC execute
    import psycopg2
    conn = psycopg2.connect(
        host="postgres", dbname="retailpulse", user="retailpulse", password="retailpulse123"
    )
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.transactions (
            order_id TEXT PRIMARY KEY,
            user_id TEXT,
            timestamp TIMESTAMPTZ,
            order_date DATE,
            total_amount NUMERIC(12,2),
            item_count INT,
            primary_category TEXT,
            payment_method TEXT,
            status TEXT,
            country TEXT,
            city TEXT,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def run():
    spark = create_spark()
    ensure_silver_table(spark)

    df = spark.read.format("delta").load("s3a://bronze/orders/")

    transactions = (
        df.select(
            col("order_id"),
            col("user_id"),
            to_timestamp(col("timestamp")).alias("timestamp"),
            col("timestamp").cast("date").alias("order_date"),
            col("total_amount"),
            size(col("items")).alias("item_count"),
            col("items")[0]["category"].alias("primary_category"),
            col("payment_method"),
            col("status"),
            col("shipping_address.country").alias("country"),
            col("shipping_address.city").alias("city"),
        )
        .dropDuplicates(["order_id"])
    )

    (
        transactions.write
        .jdbc(PG_URL, "silver.transactions", mode="append", properties={
            **PG_PROPS,
            "batchsize": "10000",
        })
    )

    count = transactions.count()
    spark.stop()
    print(f"silver_orders: wrote {count} rows to silver.transactions")


if __name__ == "__main__":
    run()
