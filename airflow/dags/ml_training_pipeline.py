from datetime import datetime, timedelta
import os
import psycopg2
import pandas as pd
import mlflow
import mlflow.sklearn
from airflow import DAG
from airflow.operators.python import PythonOperator

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
PG_CONN = dict(host="postgres", dbname="retailpulse", user="retailpulse", password="retailpulse123")

default_args = {
    "owner": "retailpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def check_data_freshness(**ctx):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(order_date) FROM gold.fct_orders
    """)
    latest = cur.fetchone()[0]
    cur.close()
    conn.close()
    if latest is None:
        raise ValueError("No data in gold.fct_orders")
    print(f"Latest order date: {latest}")


def train_churn_model(**ctx):
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score, f1_score
    from sklearn.preprocessing import LabelEncoder
    import numpy as np

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("churn_prediction")

    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql("""
        SELECT days_since_last_order, total_orders, total_revenue, avg_order_value,
               cancellation_rate, r_score, f_score, m_score, rfm_total,
               avg_session_duration, avg_products_viewed, total_sessions_30d, label
        FROM gold.agg_churn_features
        WHERE label IS NOT NULL
    """, conn)
    conn.close()

    if df.empty or len(df) < 50:
        print("Not enough data to train churn model. Skipping.")
        return

    df = df.fillna(0)
    X = df.drop("label", axis=1)
    y = df["label"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run(run_name=f"churn_{datetime.now().strftime('%Y%m%d_%H%M')}"):
        params = {"n_estimators": 100, "max_depth": 5, "learning_rate": 0.1, "random_state": 42}
        model = GradientBoostingClassifier(**params)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_prob)
        f1 = f1_score(y_test, y_pred)

        mlflow.log_params(params)
        mlflow.log_metric("roc_auc", auc)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("train_size", len(X_train))
        mlflow.sklearn.log_model(model, "churn_model", registered_model_name="churn_prediction")

        print(f"Churn model: AUC={auc:.4f}, F1={f1:.4f}")


def train_demand_model(**ctx):
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, r2_score
    import numpy as np

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("demand_forecasting")

    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql("""
        SELECT order_date, total_orders, gross_revenue, avg_order_value,
               new_customers, cancellation_rate_pct
        FROM gold.agg_daily_revenue
        ORDER BY order_date
    """, conn)
    conn.close()

    if df.empty or len(df) < 30:
        print("Not enough data for demand model. Skipping.")
        return

    df["day_of_week"] = pd.to_datetime(df["order_date"]).dt.dayofweek
    df["month"] = pd.to_datetime(df["order_date"]).dt.month
    df["lag_7"] = df["total_orders"].shift(7).fillna(0)
    df["lag_14"] = df["total_orders"].shift(14).fillna(0)
    df = df.dropna()

    features = ["day_of_week", "month", "lag_7", "lag_14", "avg_order_value", "cancellation_rate_pct"]
    X = df[features]
    y = df["total_orders"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=False)

    with mlflow.start_run(run_name=f"demand_{datetime.now().strftime('%Y%m%d_%H%M')}"):
        params = {"n_estimators": 100, "max_depth": 4, "learning_rate": 0.05}
        model = GradientBoostingRegressor(**params)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        mlflow.log_params(params)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2_score", r2)
        mlflow.sklearn.log_model(model, "demand_model", registered_model_name="demand_forecasting")

        print(f"Demand model: MAE={mae:.2f}, R2={r2:.4f}")


def evaluate_and_promote(**ctx):
    mlflow.set_tracking_uri(MLFLOW_URI)
    client = mlflow.tracking.MlflowClient()

    for model_name in ["churn_prediction", "demand_forecasting"]:
        try:
            latest = client.get_latest_versions(model_name, stages=["None"])
            if latest:
                version = latest[0].version
                client.transition_model_version_stage(
                    name=model_name, version=version, stage="Production",
                    archive_existing_versions=True
                )
                print(f"Promoted {model_name} v{version} to Production")
        except Exception as e:
            print(f"Could not promote {model_name}: {e}")


with DAG(
    "ml_training_pipeline",
    default_args=default_args,
    description="Daily ML model retraining pipeline",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "mlflow", "training"],
) as dag:

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    train_churn = PythonOperator(
        task_id="train_churn_model",
        python_callable=train_churn_model,
    )

    train_demand = PythonOperator(
        task_id="train_demand_model",
        python_callable=train_demand_model,
    )

    promote = PythonOperator(
        task_id="evaluate_and_promote",
        python_callable=evaluate_and_promote,
    )

    check_freshness >> [train_churn, train_demand] >> promote
