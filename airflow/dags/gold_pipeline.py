import subprocess
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

DBT_DIR = "/opt/airflow/dbt/retailpulse"

default_args = {
    "owner": "retailpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def data_quality_gold(**ctx):
    conn = psycopg2.connect(
        host="postgres", dbname="retailpulse", user="retailpulse", password="retailpulse123"
    )
    cur = conn.cursor()
    checks = [
        ("gold.agg_daily_revenue rows", "SELECT COUNT(*) FROM gold.agg_daily_revenue", 1),
        ("gold.agg_rfm_scores rows", "SELECT COUNT(*) FROM gold.agg_rfm_scores", 1),
        ("gold negative revenue",
         "SELECT COUNT(*) FROM gold.agg_daily_revenue WHERE net_revenue < 0", 0),
    ]
    failed = []
    for name, sql, expected_min in checks:
        cur.execute(sql)
        val = cur.fetchone()[0]
        if val < expected_min:
            failed.append(f"{name}: got {val}, expected >= {expected_min}")
    cur.close()
    conn.close()
    if failed:
        raise ValueError("Gold DQ FAILED:\n" + "\n".join(failed))
    print("Gold data quality checks passed.")


def notify_success(**ctx):
    conn = psycopg2.connect(
        host="postgres", dbname="retailpulse", user="retailpulse", password="retailpulse123"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold.fct_orders")
    orders = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM gold.dim_customers")
    customers = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"Gold pipeline complete. Orders: {orders}, Customers: {customers}")


with DAG(
    "gold_pipeline",
    default_args=default_args,
    description="dbt transformations: Silver -> Gold (business aggregations)",
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "dbt"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_pipeline",
        external_task_id=None,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_DIR}",
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && dbt run --select silver --profiles-dir {DBT_DIR}",
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && dbt run --select gold --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    dq_gold = PythonOperator(
        task_id="data_quality_gold",
        python_callable=data_quality_gold,
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    wait_for_silver >> dbt_deps >> dbt_run_silver >> dbt_run_gold >> dbt_test >> dq_gold >> notify
