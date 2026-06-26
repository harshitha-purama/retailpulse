"""FastAPI model serving endpoint for RetailPulse ML models."""
import os
import logging
from typing import Optional
from datetime import datetime

import psycopg2
import pandas as pd
import mlflow
import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
PG_CONN = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname="retailpulse",
    user=os.getenv("POSTGRES_USER", "retailpulse"),
    password=os.getenv("POSTGRES_PASSWORD", "retailpulse123"),
)

mlflow.set_tracking_uri(MLFLOW_URI)

app = FastAPI(
    title="RetailPulse ML API",
    description="Churn prediction, demand forecasting, and customer segmentation endpoints",
    version="1.0.0",
)

_model_cache: dict = {}


def _load_model(name: str, stage: str = "Production"):
    cache_key = f"{name}@{stage}"
    if cache_key not in _model_cache:
        try:
            model = mlflow.pyfunc.load_model(f"models:/{name}/{stage}")
            _model_cache[cache_key] = model
            log.info(f"Loaded {cache_key} from MLflow")
        except Exception as e:
            log.warning(f"Could not load {cache_key}: {e}")
            _model_cache[cache_key] = None
    return _model_cache[cache_key]


# --- Request / Response schemas ---

class ChurnRequest(BaseModel):
    user_id: str

class ChurnResponse(BaseModel):
    user_id: str
    churn_probability: float
    is_at_risk: bool
    rfm_segment: Optional[str]
    days_since_last_order: Optional[float]

class DemandRequest(BaseModel):
    days_ahead: int = 7

class DemandResponse(BaseModel):
    forecast_date: str
    predicted_orders: float

class SegmentResponse(BaseModel):
    user_id: str
    rfm_segment: str
    r_score: int
    f_score: int
    m_score: int
    monetary: float


# --- Endpoints ---

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.post("/predict/churn", response_model=ChurnResponse)
def predict_churn(req: ChurnRequest):
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql(f"""
        SELECT days_since_last_order, total_orders, total_revenue, avg_order_value,
               cancellation_rate, r_score, f_score, m_score, rfm_total,
               avg_session_duration, avg_products_viewed, total_sessions_30d,
               rfm_segment
        FROM gold.agg_churn_features
        WHERE user_id = %s
    """, conn, params=(req.user_id,))
    conn.close()

    if df.empty:
        raise HTTPException(status_code=404, detail=f"User {req.user_id} not found")

    rfm_segment = df["rfm_segment"].iloc[0]
    days_since = float(df["days_since_last_order"].iloc[0])
    features = df.drop(columns=["rfm_segment"])

    model = _load_model("churn_prediction")
    if model is None:
        churn_prob = float(days_since > 90) * 0.85
    else:
        churn_prob = float(model.predict(features.fillna(0))[0])

    return ChurnResponse(
        user_id=req.user_id,
        churn_probability=round(churn_prob, 4),
        is_at_risk=churn_prob > 0.60,
        rfm_segment=rfm_segment,
        days_since_last_order=days_since,
    )


@app.get("/predict/demand", response_model=list[DemandResponse])
def predict_demand(days_ahead: int = 7):
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql("""
        SELECT order_date, total_orders, avg_order_value, cancellation_rate_pct
        FROM gold.agg_daily_revenue
        ORDER BY order_date DESC LIMIT 30
    """, conn)
    conn.close()

    if df.empty:
        raise HTTPException(status_code=404, detail="No demand data available")

    df["day_of_week"] = pd.to_datetime(df["order_date"]).dt.dayofweek
    df["month"] = pd.to_datetime(df["order_date"]).dt.month
    df["lag_7"] = df["total_orders"].shift(7).fillna(df["total_orders"].mean())
    df["lag_14"] = df["total_orders"].shift(14).fillna(df["total_orders"].mean())

    features = ["day_of_week", "month", "lag_7", "lag_14", "avg_order_value", "cancellation_rate_pct"]
    baseline_row = df[features].iloc[0].fillna(0)

    model = _load_model("demand_forecasting")
    forecasts = []
    for i in range(1, days_ahead + 1):
        from datetime import timedelta
        fd = (datetime.utcnow() + timedelta(days=i)).date()
        row = baseline_row.copy()
        row["day_of_week"] = fd.weekday()
        row["month"] = fd.month

        if model:
            pred = float(model.predict(pd.DataFrame([row]))[0])
        else:
            pred = float(df["total_orders"].mean() * (1 + (fd.weekday() in (5, 6)) * 0.3))

        forecasts.append(DemandResponse(
            forecast_date=fd.isoformat(),
            predicted_orders=round(max(0, pred), 1),
        ))
    return forecasts


@app.get("/segment/{user_id}", response_model=SegmentResponse)
def get_segment(user_id: str):
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql("""
        SELECT user_id, rfm_segment, r_score, f_score, m_score, monetary
        FROM gold.agg_rfm_scores
        WHERE user_id = %s
    """, conn, params=(user_id,))
    conn.close()

    if df.empty:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found in RFM scores")

    row = df.iloc[0]
    return SegmentResponse(
        user_id=row["user_id"],
        rfm_segment=row["rfm_segment"],
        r_score=int(row["r_score"]),
        f_score=int(row["f_score"]),
        m_score=int(row["m_score"]),
        monetary=float(row["monetary"]),
    )


@app.get("/customers/at-risk", response_model=list[ChurnResponse])
def get_at_risk_customers(limit: int = 20):
    conn = psycopg2.connect(**PG_CONN)
    df = pd.read_sql(f"""
        SELECT f.user_id, f.days_since_last_order, f.rfm_segment,
               f.days_since_last_order, f.total_orders
        FROM gold.agg_churn_features f
        JOIN gold.dim_customers c ON f.user_id = c.user_id
        WHERE c.is_churned = TRUE
        ORDER BY f.days_since_last_order DESC
        LIMIT %s
    """, conn, params=(limit,))
    conn.close()

    return [
        ChurnResponse(
            user_id=row["user_id"],
            churn_probability=0.90,
            is_at_risk=True,
            rfm_segment=row.get("rfm_segment"),
            days_since_last_order=float(row["days_since_last_order"]),
        )
        for _, row in df.iterrows()
    ]
