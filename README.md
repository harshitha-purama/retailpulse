# RetailPulse — End-to-End Retail Intelligence Platform

> A production-grade **Data Engineering + MLOps + Business Analytics** platform built entirely on Docker.  
> One command to start. Full lakehouse architecture. Real-time streaming. ML-powered predictions. Executive dashboards.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         RETAILPULSE DATA PLATFORM                               │
│                                                                                 │
│  [Python Data Generator]                                                        │
│        │  clickstream / orders / inventory events                               │
│        ▼                                                                        │
│  ┌─────────────┐     ┌──────────────────────────────────────────────────┐      │
│  │  Apache     │────▶│  Spark Structured Streaming                      │      │
│  │  Kafka      │     │  Bronze Layer → MinIO (Delta Lake)               │      │
│  │  3 topics   │     └────────────────────┬─────────────────────────────┘      │
│  └─────────────┘                          │                                     │
│                                           ▼                                     │
│                              ┌────────────────────────┐                         │
│                              │  Apache Airflow        │                         │
│                              │  Hourly Batch Pipeline │                         │
│                              └────────────┬───────────┘                         │
│                                           │  Spark batch jobs                   │
│                                           ▼                                     │
│                              ┌────────────────────────┐                         │
│                              │  Silver Layer          │                         │
│                              │  PostgreSQL            │                         │
│                              │  • transactions        │                         │
│                              │  • sessions            │                         │
│                              │  • inventory_snapshots │                         │
│                              └────────────┬───────────┘                         │
│                                           │  dbt transformations                │
│                                           ▼                                     │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                        GOLD LAYER (dbt models)                        │     │
│  │  dim_customers  │  dim_products  │  fct_orders  │  agg_daily_revenue  │     │
│  │  agg_rfm_scores │  agg_cohort_retention         │  agg_funnel         │     │
│  │  agg_churn_features (ML feature table)                                │     │
│  └────────┬───────────────────────────┬────────────────────────┬─────────┘     │
│           │                           │                        │               │
│           ▼                           ▼                        ▼               │
│  ┌──────────────┐           ┌─────────────────┐    ┌──────────────────────┐   │
│  │  Apache      │           │  MLflow +        │    │  Trino Query Engine  │   │
│  │  Superset    │           │  FastAPI Serving  │    │  + JupyterLab        │   │
│  │  Dashboards  │           │  /predict/churn  │    │  BA Notebooks        │   │
│  │  (BI layer)  │           │  /predict/demand │    │  (Cohort/RFM/A-B)    │   │
│  └──────────────┘           └─────────────────┘    └──────────────────────┘   │
│                                                                                 │
│  [Prometheus + Grafana]  [Great Expectations DQ]  [Evidently AI Drift]          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Event Streaming | **Apache Kafka** | Real-time clickstream, orders, inventory |
| Stream Processing | **Apache Spark Structured Streaming** | Kafka → Bronze Delta Lake |
| Object Storage | **MinIO** (S3-compatible) | Bronze Delta Lake storage |
| Orchestration | **Apache Airflow** | Hourly/daily pipeline scheduling |
| Transformation | **dbt** (dbt-core + dbt-postgres) | Silver/Gold layer SQL models |
| Warehouse | **PostgreSQL** | Silver + Gold analytical tables |
| Query Engine | **Trino** | Federated SQL over PostgreSQL + MinIO |
| BI / Dashboards | **Apache Superset** | Executive KPI dashboards |
| Notebooks | **JupyterLab** | BA analytics (cohort, RFM, A/B, ML) |
| ML Tracking | **MLflow** | Experiment tracking + model registry |
| Model Serving | **FastAPI** | REST endpoints for churn / demand / segments |
| Drift Monitoring | **Evidently AI** | Feature & model drift detection |
| Data Quality | **Great Expectations** | Schema and rule-based DQ gates |
| Observability | **Prometheus + Grafana** | Pipeline health + business KPI dashboards |
| Infrastructure | **Docker Compose** | Full stack on a single machine |
| CI/CD | **GitHub Actions** | Lint, test, Docker build on every push |
<img width="980" height="641" alt="image" src="https://github.com/user-attachments/assets/d30cadfd-1095-4f9f-8228-68580a6a797c" />


---

## Data Model

### Medallion Architecture

```
BRONZE  →  Raw Delta Lake partitioned by date (MinIO / S3)
SILVER  →  Cleaned, deduplicated PostgreSQL tables
GOLD    →  Business aggregations materialized by dbt
```

### Gold Layer Models

| Model | Description |
|---|---|
| `dim_customers` | Customer dimension: CLV, churn flag, tier, order history |
| `dim_products` | Product dimension: ABC classification, margin, revenue |
| `fct_orders` | Order fact table enriched with customer and category dims |
| `agg_daily_revenue` | Daily revenue, orders, new customers, cancellation rate per category |
| `agg_rfm_scores` | Recency / Frequency / Monetary scores + named segments per customer |
| `agg_cohort_retention` | 12-month cohort retention matrix |
| `agg_funnel` | Daily conversion funnel (session → cart → purchase) |
| `agg_churn_features` | Feature table for the churn prediction ML model |

---

## Business Analytics (Amity BA Coursework Integration)

Five Jupyter notebooks covering every major BA skill:

| Notebook | Skills Demonstrated |
|---|---|
| `01_cohort_analysis.ipynb` | Cohort retention heatmaps, customer lifecycle analysis |
| `02_rfm_segmentation.ipynb` | RFM scoring, K-Means clustering, segment marketing actions |
| `03_ab_test_analysis.ipynb` | Hypothesis testing (chi-squared), statistical significance, p-values |
| `04_demand_forecasting.ipynb` | Time-series features, cross-validation, gradient boosting regressor |
| `05_churn_model.ipynb` | Binary classification, ROC curves, MLflow model logging |

---

## ML Models

| Model | Algorithm | Endpoint |
|---|---|---|
| Churn Prediction | Gradient Boosting Classifier | `POST /predict/churn` |
| Demand Forecasting | Gradient Boosting Regressor | `GET /predict/demand?days_ahead=7` |
| Customer Segmentation | RFM + K-Means | `GET /segment/{user_id}` |

---

## Quick Start

### Prerequisites
- Docker Desktop (≥ 4.x) with ≥ 8 GB RAM allocated
- Docker Compose v2

### 1. Clone and start

```bash
git clone https://github.com/your-username/retailpulse.git
cd retailpulse
docker compose up -d
```

Wait ~2 minutes for all services to initialise.

### 2. Verify services

```bash
docker compose ps
```

### 3. Access UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MLflow | http://localhost:5000 | — |
| Superset | http://localhost:8088 | admin / admin |
| JupyterLab | http://localhost:8888 | token: `retailpulse` |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| MinIO | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master | http://localhost:8090 | — |
| FastAPI Docs | http://localhost:8000/docs | — |
| Trino | http://localhost:8085 | — |

### 4. Start the streaming pipeline

```bash
# Submit Spark Structured Streaming job (runs continuously)
docker compose exec spark-master bash /opt/spark/jobs/submit_streaming.sh
```

### 5. Trigger the batch pipeline manually

In Airflow UI → DAGs → `silver_pipeline` → Trigger

### 6. Run dbt manually

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt/retailpulse && dbt run"
```

### 7. Train ML models

In Airflow UI → DAGs → `ml_training_pipeline` → Trigger

---

## Project Structure

```
retailpulse/
├── docker-compose.yml          # All 17 services
├── .env                        # Environment variables
├── Makefile                    # make up / down / dbt-run / train-models
│
├── data_generator/             # Python Kafka producers
│   ├── generator.py            # Main loop (clickstream + orders + inventory)
│   └── producers/              # ClickstreamProducer, OrderProducer, InventoryProducer
│
├── spark/jobs/                 # PySpark jobs
│   ├── bronze_streaming.py     # Kafka → Delta Lake (streaming)
│   ├── silver_orders.py        # Bronze → silver.transactions (batch)
│   ├── silver_clickstream.py   # Bronze → silver.sessions (batch)
│   └── silver_inventory.py     # Bronze → silver.inventory_snapshots (batch)
│
├── airflow/dags/               # Airflow DAGs
│   ├── silver_pipeline.py      # Hourly: Spark batch → Silver
│   ├── gold_pipeline.py        # Hourly+30m: dbt → Gold
│   ├── ml_training_pipeline.py # Daily 2am: model retraining
│   └── data_quality_pipeline.py # Daily 6am: DQ checks all layers
│
├── dbt/retailpulse/
│   ├── models/silver/          # Staging views (stg_users, stg_products, etc.)
│   └── models/gold/            # Business aggregations (dim, fct, agg_*)
│
├── mlops/serving/main.py       # FastAPI: /predict/churn, /predict/demand, /segment
├── monitoring/                 # Prometheus config + Grafana dashboards
├── notebooks/                  # 5 BA Jupyter notebooks
└── .github/workflows/ci.yml    # GitHub Actions CI/CD
```

---
## Results 
<img width="2400" height="900" alt="image" src="https://github.com/user-attachments/assets/6b4bf895-a567-4698-9082-4ee45d47e07b" />

<img width="2100" height="1050" alt="image" src="https://github.com/user-attachments/assets/c3b5c212-d31c-446e-aeaa-db4292a96240" />
 I built a 12-month cohort retention matrix on top of a medallion architecture. The data flows from Kafka through Spark into PostgreSQL, dbt builds the aggregations, and this heatmap visualizes customer lifecycle behavior. The June 2025 cohort shows 66% Month-1 retention which tells us early engagement campaigns are working
 <img width="2100" height="750" alt="image" src="https://github.com/user-attachments/assets/e124bc41-3e9c-4ec6-b7ef-f1e2fd10dbf2" />
 ran an A/B test splitting sessions by session ID parity. The new checkout flow showed a 43% lift in conversion rate, statistically significant at p=0.019. This means we can confidently ship the new flow.
<img width="2400" height="900" alt="image" src="https://github.com/user-attachments/assets/b67b622d-9063-4cc1-99d1-5c6c6c42d855" />
 This is a Gradient Boosting demand forecast model. It's trained on 366 days of transaction data from the Gold layer. The rolling 7-day average is the strongest lag feature, and the model achieves reasonable accuracy on a time-series cross-validation split — meaning no data leakage
 <img width="2100" height="900" alt="image" src="https://github.com/user-attachments/assets/0b297b8b-b14d-403e-947b-4a6f7e4951f6" />
<img width="695" height="403" alt="image" src="https://github.com/user-attachments/assets/fb39a90e-79ff-4e35-9006-394b36939dc7" />
  The Random Forest churn model achieves 0.99 AUC. The most important feature is days since last order — customers inactive for 90+ days are flagged for win-back campaigns. This model is tracked in MLflow and can be served via the FastAPI endpoint.
 

## License

MIT — build on it, break it, learn from it.
