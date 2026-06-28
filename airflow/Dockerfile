FROM apache/airflow:2.8.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    psycopg2-binary==2.9.9 \
    dbt-postgres==1.7.0 \
    scikit-learn==1.3.2 \
    pandas==2.0.3 \
    mlflow==2.9.2 \
    boto3==1.34.0
