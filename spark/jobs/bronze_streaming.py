"""
bronze_streaming.py
-------------------
PySpark Structured Streaming job that ingests from all three Kafka topics
(retail.clickstream, retail.orders, retail.inventory) and writes raw data
to MinIO (S3-compatible) in Delta Lake format under the bronze layer.

Partitions each stream by ingestion_date and uses 30-second micro-batches.
"""

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    from_json,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bronze_streaming")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "kafka:9092"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BRONZE_BASE = "s3a://bronze"
CHECKPOINT_BASE = f"{BRONZE_BASE}/_checkpoints"

TOPIC_CLICKSTREAM = "retail.clickstream"
TOPIC_ORDERS = "retail.orders"
TOPIC_INVENTORY = "retail.inventory"

# ---------------------------------------------------------------------------
# Kafka message schemas
# ---------------------------------------------------------------------------

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),       # page_view, product_view, add_to_cart, checkout_complete, etc.
        StructField("page_url", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("device_type", StringType(), True),      # mobile, desktop, tablet
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

ORDER_ITEM_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("status", StringType(), True),           # pending, confirmed, shipped, delivered, cancelled
        StructField("total_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),   # credit_card, paypal, crypto, etc.
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("items", ArrayType(ORDER_ITEM_SCHEMA), True),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("event_type", StringType(), True),       # restock, sale, adjustment, transfer
        StructField("quantity", IntegerType(), True),
        StructField("previous_quantity", IntegerType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("supplier_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Spark session factory
# ---------------------------------------------------------------------------


def create_spark_session() -> SparkSession:
    """Build a SparkSession configured for Delta Lake, Kafka, and MinIO S3A."""
    logger.info("Creating SparkSession ...")

    spark = (
        SparkSession.builder.appName("RetailPulse-Bronze-Streaming")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Delta streaming settings
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Streaming reliability
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Shuffle partitions – keep small for streaming micro-batches
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created successfully.")
    return spark


# ---------------------------------------------------------------------------
# Stream builders
# ---------------------------------------------------------------------------


def build_kafka_stream(spark: SparkSession, topic: str):
    """Return a raw Kafka DataFrame for the given topic."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", f"retailpulse-bronze-{topic.replace('.', '-')}")
        .load()
    )


def parse_and_enrich(raw_df, schema: StructType, topic_label: str):
    """
    Parse the JSON value from a Kafka message and add ingestion metadata columns.
    Malformed records are silently dropped via PERMISSIVE mode (null struct).
    """
    parsed = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    )

    # Flatten the parsed struct; keep kafka_timestamp for auditability
    field_cols = [col(f"data.{f.name}").alias(f.name) for f in schema.fields]

    enriched = parsed.select(
        *field_cols,
        col("kafka_timestamp"),
        current_date().alias("ingestion_date"),
        current_timestamp().alias("ingestion_timestamp"),
    )

    # Drop rows where the primary key field is null (indicates parse failure)
    pk_field = schema.fields[0].name
    enriched = enriched.filter(col(pk_field).isNotNull())

    logger.info("Built enriched stream for topic '%s'.", topic_label)
    return enriched


# ---------------------------------------------------------------------------
# Stream writers
# ---------------------------------------------------------------------------


def write_stream(df, output_path: str, checkpoint_path: str, label: str):
    """Write a streaming DataFrame to Delta Lake on MinIO."""
    logger.info(
        "Starting stream writer for '%s' -> %s (checkpoint: %s)",
        label,
        output_path,
        checkpoint_path,
    )
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("ingestion_date")
        .trigger(processingTime="30 seconds")
        .queryName(f"bronze_{label}")
        .start()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    spark = create_spark_session()

    # ---- Clickstream -------------------------------------------------------
    logger.info("Setting up clickstream stream ...")
    raw_clickstream = build_kafka_stream(spark, TOPIC_CLICKSTREAM)
    enriched_clickstream = parse_and_enrich(raw_clickstream, CLICKSTREAM_SCHEMA, "clickstream")
    query_clickstream = write_stream(
        enriched_clickstream,
        output_path=f"{BRONZE_BASE}/clickstream",
        checkpoint_path=f"{CHECKPOINT_BASE}/clickstream",
        label="clickstream",
    )

    # ---- Orders ------------------------------------------------------------
    logger.info("Setting up orders stream ...")
    raw_orders = build_kafka_stream(spark, TOPIC_ORDERS)
    enriched_orders = parse_and_enrich(raw_orders, ORDERS_SCHEMA, "orders")
    query_orders = write_stream(
        enriched_orders,
        output_path=f"{BRONZE_BASE}/orders",
        checkpoint_path=f"{CHECKPOINT_BASE}/orders",
        label="orders",
    )

    # ---- Inventory ---------------------------------------------------------
    logger.info("Setting up inventory stream ...")
    raw_inventory = build_kafka_stream(spark, TOPIC_INVENTORY)
    enriched_inventory = parse_and_enrich(raw_inventory, INVENTORY_SCHEMA, "inventory")
    query_inventory = write_stream(
        enriched_inventory,
        output_path=f"{BRONZE_BASE}/inventory",
        checkpoint_path=f"{CHECKPOINT_BASE}/inventory",
        label="inventory",
    )

    # ---- Wait for all queries ----------------------------------------------
    queries = [query_clickstream, query_orders, query_inventory]
    logger.info("All three streaming queries started. Awaiting termination ...")

    try:
        # Block until any query terminates (or fails)
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Interrupt received – stopping streaming queries gracefully ...")
        for q in queries:
            if q.isActive:
                q.stop()
    finally:
        logger.info("Streaming queries stopped. Shutting down SparkSession.")
        spark.stop()


if __name__ == "__main__":
    main()
