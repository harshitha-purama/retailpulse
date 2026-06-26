"""Batch job: Bronze inventory (Delta Lake) -> silver.inventory_snapshots (PostgreSQL)."""
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, last, count, max as spark_max

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
PG_URL = "jdbc:postgresql://postgres:5432/retailpulse"
PG_PROPS = {"user": "retailpulse", "password": "retailpulse123", "driver": "org.postgresql.Driver"}


def create_spark():
    return (
        SparkSession.builder
        .appName("silver_inventory")
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
        CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
            product_id TEXT,
            warehouse_id TEXT,
            snapshot_date DATE,
            quantity BIGINT,
            event_count BIGINT,
            ingested_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (product_id, warehouse_id, snapshot_date)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def run():
    spark = create_spark()
    ensure_silver_table()

    df = (
        spark.read.format("delta").load("s3a://bronze/inventory/")
        .withColumn("event_date", to_date(to_timestamp("timestamp")))
    )

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    window = Window.partitionBy("product_id", "warehouse_id", "event_date").orderBy(desc("timestamp"))

    snapshots = (
        df.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .groupBy("product_id", "warehouse_id", "event_date")
        .agg(
            spark_max("quantity_after").alias("quantity"),
            count("*").alias("event_count"),
        )
        .withColumnRenamed("event_date", "snapshot_date")
    )

    (
        snapshots.write
        .jdbc(PG_URL, "silver.inventory_snapshots", mode="append", properties={
            **PG_PROPS,
            "batchsize": "5000",
        })
    )

    count_val = snapshots.count()
    spark.stop()
    print(f"silver_inventory: wrote {count_val} rows to silver.inventory_snapshots")


if __name__ == "__main__":
    run()
