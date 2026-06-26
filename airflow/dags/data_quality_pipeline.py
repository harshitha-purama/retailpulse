from datetime import datetime, timedelta, timezone
import psycopg2
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

PG_CONN = dict(host="postgres", dbname="retailpulse", user="retailpulse", password="retailpulse123")

default_args = {
    "owner": "retailpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def bronze_freshness_check(**ctx):
    s3 = boto3.client(
        "s3", endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin123"
    )
    from datetime import timezone
    now = datetime.now(timezone.utc)
    for prefix in ("orders/", "clickstream/", "inventory/"):
        resp = s3.list_objects_v2(Bucket="bronze", Prefix=prefix)
        objects = resp.get("Contents", [])
        if not objects:
            raise ValueError(f"No objects in bronze/{prefix}")
        latest_mod = max(o["LastModified"] for o in objects)
        age_hours = (now - latest_mod).total_seconds() / 3600
        if age_hours > 2:
            raise ValueError(f"bronze/{prefix} is stale: {age_hours:.1f}h old")
        print(f"bronze/{prefix}: OK ({age_hours:.1f}h old)")


def silver_null_check(**ctx):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    checks = {
        "transactions.order_id": "SELECT COUNT(*) FROM silver.transactions WHERE order_id IS NULL",
        "transactions.user_id": "SELECT COUNT(*) FROM silver.transactions WHERE user_id IS NULL",
        "transactions.total_amount": "SELECT COUNT(*) FROM silver.transactions WHERE total_amount IS NULL",
        "sessions.session_id": "SELECT COUNT(*) FROM silver.sessions WHERE session_id IS NULL",
    }
    failed = []
    for col, sql in checks.items():
        cur.execute(sql)
        nulls = cur.fetchone()[0]
        if nulls > 0:
            failed.append(f"{col}: {nulls} nulls")
    cur.close()
    conn.close()
    if failed:
        raise ValueError("Null checks FAILED: " + ", ".join(failed))
    print("Null checks passed.")


def silver_duplicate_check(**ctx):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id, COUNT(*) c FROM silver.transactions
            GROUP BY order_id HAVING COUNT(*) > 1
        ) dups
    """)
    dupes = cur.fetchone()[0]
    cur.close()
    conn.close()
    if dupes > 0:
        raise ValueError(f"Found {dupes} duplicate order_ids in silver.transactions")
    print("Duplicate checks passed.")


def gold_kpi_sanity(**ctx):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    issues = []

    cur.execute("SELECT COUNT(*) FROM gold.agg_daily_revenue WHERE gross_revenue < 0")
    if cur.fetchone()[0] > 0:
        issues.append("Negative gross_revenue in agg_daily_revenue")

    cur.execute("SELECT COUNT(*) FROM gold.agg_funnel WHERE overall_conversion_rate > 100")
    if cur.fetchone()[0] > 0:
        issues.append("Conversion rate > 100% in agg_funnel")

    cur.execute("SELECT COUNT(*) FROM gold.agg_rfm_scores WHERE r_score NOT BETWEEN 1 AND 5")
    if cur.fetchone()[0] > 0:
        issues.append("Invalid RFM scores")

    cur.close()
    conn.close()
    if issues:
        raise ValueError("Gold KPI sanity FAILED: " + "; ".join(issues))
    print("Gold KPI sanity checks passed.")


def generate_dq_report(**ctx):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    stats = {}
    for table in ["silver.transactions", "silver.sessions", "gold.fct_orders",
                  "gold.dim_customers", "gold.agg_rfm_scores"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        stats[table] = cur.fetchone()[0]
    cur.close()
    conn.close()
    print("=== Data Quality Report ===")
    for table, cnt in stats.items():
        print(f"  {table}: {cnt:,} rows")
    print("All checks PASSED.")


with DAG(
    "data_quality_pipeline",
    default_args=default_args,
    description="Daily comprehensive data quality checks across all layers",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data_quality", "monitoring"],
) as dag:

    t1 = PythonOperator(task_id="bronze_freshness_check", python_callable=bronze_freshness_check)
    t2 = PythonOperator(task_id="silver_null_check", python_callable=silver_null_check)
    t3 = PythonOperator(task_id="silver_duplicate_check", python_callable=silver_duplicate_check)
    t4 = PythonOperator(task_id="gold_kpi_sanity", python_callable=gold_kpi_sanity)
    t5 = PythonOperator(task_id="generate_dq_report", python_callable=generate_dq_report)

    t1 >> [t2, t3] >> t4 >> t5
