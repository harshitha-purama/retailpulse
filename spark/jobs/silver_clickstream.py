"""Batch job: Bronze clickstream (Delta Lake) -> silver.sessions (PostgreSQL)."""
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, count, sum as spark_sum,
    to_timestamp, when
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
PG_URL = "jdbc:postgresql://postgres:5432/retailpulse"
PG_PROPS = {"user": "retailpulse", "password": "retailpulse123", "driver": "org.postgresql.Driver"}


def create_spark():
    return (
        SparkSession.builder
        .appName("silver_clickstream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def ensure_silver_table():
    conn = psycopg2.connect(
        host="postgres", dbname="retailpulse", user="retailpulse", password="retailpulse123"
    )
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT,
            start_time TIMESTAMPTZ,
            end_time TIMESTAMPTZ,
            duration_seconds BIGINT,
            page_views BIGINT,
            products_viewed BIGINT,
            add_to_cart_count BIGINT,
            converted BOOLEAN,
            device_type TEXT,
            country TEXT,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def run():
    spark = create_spark()
    ensure_silver_table()

    df = spark.read.format("delta").load("s3a://bronze/clickstream/")

    sessions = (
        df.groupBy("session_id", "user_id", "device_type", "country")
        .agg(
            spark_min(to_timestamp("timestamp")).alias("start_time"),
            spark_max(to_timestamp("timestamp")).alias("end_time"),
            count("*").alias("page_views"),
            count(when(col("event_type") == "product_view", 1)).alias("products_viewed"),
            count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
            (count(when(col("event_type") == "checkout_complete", 1)) > 0).alias("converted"),
        )
        .withColumn(
            "duration_seconds",
            (col("end_time").cast("long") - col("start_time").cast("long"))
        )
        .dropDuplicates(["session_id"])
    )

    (
        sessions.write
        .jdbc(PG_URL, "silver.sessions", mode="append", properties={
            **PG_PROPS,
            "batchsize": "10000",
        })
    )

    count_val = sessions.count()
    spark.stop()
    print(f"silver_clickstream: wrote {count_val} rows to silver.sessions")


if __name__ == "__main__":
    run()
