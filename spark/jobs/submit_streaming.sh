#!/bin/bash
# Submit the Spark Structured Streaming job that ingests Kafka -> Bronze Delta Lake

SPARK_HOME=/opt/spark
PACKAGES="io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.592,org.postgresql:postgresql:42.7.1"

$SPARK_HOME/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "$PACKAGES" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  /opt/spark/jobs/bronze_streaming.py
