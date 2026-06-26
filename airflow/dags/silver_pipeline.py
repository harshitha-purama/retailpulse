from datetime import datetime, timedelta
import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.1.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.592,"
    "org.postgresql:postgresql:42.7.1"
)
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

default_args = {
    "owner": "retailpulse",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def check_bronze_data(**ctx):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
    )
    for prefix in ("orders/", "clickstream/", "inventory/"):
        resp = s3.list_objects_v2(Bucket="bronze", Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            raise ValueError(f"No data found in bronze/{prefix}")
    print("Bronze data check passed.")


def data_quality_silver(**ctx):
    conn = psycopg2.connect(
        host="postgres", dbname="retailpulse", user="retailpulse", password="retailpulse123"
    )
    cur = conn.cursor()
    checks = [
        ("silver.transactions null order_id",
         "SELECT COUNT(*) FROM silver.transactions WHERE order_id IS NULL"),
        ("silver.transactions negative total",
         "SELECT COUNT(*) FROM silver.transactions WHERE total_amount < 0"),
        ("silver.sessions null session_id",
         "SELECT COUNT(*) FROM silver.sessions WHERE session_id IS NULL"),
    ]
    failed = []
    for name, sql in checks:
        cur.execute(sql)
        bad_rows = cur.fetchone()[0]
        if bad_rows > 0:
            failed.append(f"{name}: {bad_rows} bad rows")
    cur.close()
    conn.close()
    if failed:
        raise ValueError("Data quality FAILED:\n" + "\n".join(failed))
    print("Silver data quality checks passed.")


with DAG(
    "silver_pipeline",
    default_args=default_args,
    description="Spark batch jobs: Bronze -> Silver (PostgreSQL)",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "spark", "bronze"],
) as dag:

    check_bronze = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data,
    )

    silver_orders = SparkSubmitOperator(
        task_id="silver_orders",
        conn_id="spark_default",
        application="/opt/spark/jobs/silver_orders.py",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        executor_memory="2g",
        driver_memory="1g",
    )

    silver_clickstream = SparkSubmitOperator(
        task_id="silver_clickstream",
        conn_id="spark_default",
        application="/opt/spark/jobs/silver_clickstream.py",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        executor_memory="2g",
        driver_memory="1g",
    )

    silver_inventory = SparkSubmitOperator(
        task_id="silver_inventory",
        conn_id="spark_default",
        application="/opt/spark/jobs/silver_inventory.py",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        executor_memory="1g",
        driver_memory="512m",
    )

    dq_silver = PythonOperator(
        task_id="data_quality_silver",
        python_callable=data_quality_silver,
    )

    check_bronze >> [silver_orders, silver_clickstream, silver_inventory] >> dq_silver
