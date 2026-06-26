"""Model drift monitoring using Evidently AI."""
import os
import logging
import psycopg2
import pandas as pd
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, ClassificationPreset
from evidently.metrics import DatasetDriftMetric, ColumnDriftMetric

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PG_CONN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname="retailpulse",
    user=os.getenv("POSTGRES_USER", "retailpulse"),
    password=os.getenv("POSTGRES_PASSWORD", "retailpulse123"),
)

FEATURE_COLS = [
    "days_since_last_order", "total_orders", "total_revenue", "avg_order_value",
    "cancellation_rate", "r_score", "f_score", "m_score", "rfm_total",
    "avg_session_duration", "avg_products_viewed", "total_sessions_30d",
]


def load_features() -> pd.DataFrame:
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql(
        f"SELECT {', '.join(FEATURE_COLS)}, label FROM gold.agg_churn_features",
        conn
    )
    conn.close()
    return df.fillna(0)


def run_drift_report(output_path: str = "/tmp/drift_report.html"):
    df = load_features()
    if len(df) < 20:
        log.warning("Not enough data for drift detection")
        return

    split = len(df) // 2
    reference = df.iloc[:split]
    current = df.iloc[split:]

    report = Report(metrics=[
        DatasetDriftMetric(),
        ColumnDriftMetric(column_name="days_since_last_order"),
        ColumnDriftMetric(column_name="total_orders"),
        ColumnDriftMetric(column_name="rfm_total"),
    ])
    report.run(reference_data=reference, current_data=current)
    report.save_html(output_path)

    result = report.as_dict()
    drift_detected = result["metrics"][0]["result"]["dataset_drift"]
    drift_share = result["metrics"][0]["result"]["share_of_drifted_columns"]

    log.info(f"Drift detected: {drift_detected}, share of drifted columns: {drift_share:.2%}")

    if drift_detected:
        log.warning("ALERT: Significant data drift detected — consider retraining models")

    return {"drift_detected": drift_detected, "drift_share": drift_share}


if __name__ == "__main__":
    result = run_drift_report()
    print(result)
