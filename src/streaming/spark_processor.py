"""
Production Apache Spark Structured Streaming Pipeline for Real-Time Fake Job Detection.
Implements the distributed streaming architecture described in the research paper:
Kafka Ingestion -> Schema Parsing -> Broadcast ML Inference -> Micro-Batch Database Persistence.
"""

import sys
import json
import logging
from pathlib import Path

# Add project root to sys.path
BASE_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_PROJECT_DIR))

from src.config import (
    SPARK_APP_NAME, SPARK_MASTER, KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_JOB_POSTINGS, MICRO_BATCH_INTERVAL_SECONDS,
    SPARK_CHECKPOINT_DIR, MODELS_DIR
)
from src.storage.db import DatabaseStorage
from src.ml.feature_extractor import clean_text

logger = logging.getLogger("SparkStreamingProcessor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def run_spark_streaming():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, from_json, udf, struct
        from pyspark.sql.types import (
            StringType, IntegerType, StructType, StructField, DoubleType, BooleanType
        )
    except ImportError:
        logger.error("PySpark is not installed in current environment. Use native_stream_engine.py for native async execution or install pyspark with Java 17.")
        return

    import joblib

    print(f"Initializing Apache Spark Session ({SPARK_APP_NAME})...")
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", SPARK_CHECKPOINT_DIR) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Load trained models & broadcast
    model_path = MODELS_DIR / "streamguard_calibrated_lr.pkl"
    extractor_path = MODELS_DIR / "multimodal_extractor.pkl"

    if not model_path.exists() or not extractor_path.exists():
        logger.error("Model checkpoints not found. Run 'python -m src.ml.train' first.")
        return

    print("Loading model and vectorizer checkpoints...")
    model = joblib.load(model_path)
    extractor = joblib.load(extractor_path)

    broadcast_model = spark.sparkContext.broadcast(model)
    broadcast_extractor = spark.sparkContext.broadcast(extractor)
    db_storage = DatabaseStorage()

    # Define schema matching incoming Kafka JSON
    job_schema = StructType([
        StructField("job_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("company_profile", StringType(), True),
        StructField("description", StringType(), True),
        StructField("requirements", StringType(), True),
        StructField("benefits", StringType(), True),
        StructField("location", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary_range", StringType(), True),
        StructField("telecommuting", IntegerType(), True),
        StructField("has_company_logo", IntegerType(), True),
        StructField("has_questions", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Connect to Kafka Stream
    print(f"Subscribing to Kafka stream: {KAFKA_BOOTSTRAP_SERVERS} [{KAFKA_TOPIC_JOB_POSTINGS}]")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_JOB_POSTINGS) \
        .option("startingOffsets", "latest") \
        .load()

    # Deserialize JSON value
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), job_schema).alias("data")) \
        .select("data.*")

    # Define Spark foreachBatch sink
    def process_micro_batch(batch_df, batch_id):
        records = batch_df.collect()
        if not records:
            return

        batch_dicts = [r.asDict() for r in records]
        import pandas as pd
        import time

        batch_pandas = pd.DataFrame(batch_dicts)
        t0 = time.time()
        X_batch = broadcast_extractor.value.transform(batch_pandas)
        probs = broadcast_model.value.predict_proba(X_batch)[:, 1]
        latency = (time.time() - t0) * 1000 / len(records)

        storage_records = []
        fraud_count = 0
        for i, r in enumerate(batch_dicts):
            prob = float(probs[i])
            is_fraud = prob >= 0.40
            if is_fraud: fraud_count += 1
            risk = "CRITICAL" if prob >= 0.70 else ("HIGH" if prob >= 0.40 else ("MEDIUM" if prob >= 0.20 else "LOW"))

            storage_records.append({
                "job_id": str(r.get("job_id")),
                "title": r.get("title", ""),
                "company_name": r.get("department", "Not Specified"),
                "location": r.get("location", ""),
                "prediction": "FRAUDULENT" if is_fraud else "LEGITIMATE",
                "confidence_score": prob,
                "risk_level": risk,
                "explanation": f"Spark Streaming inference: Fraud probability {prob*100:.1f}%.",
                "token_attributions": [],
                "forensic_flags": [],
                "latency_ms": latency,
                "timestamp": r.get("timestamp"),
                "is_alert": is_fraud
            })

        db_storage.save_microbatch(storage_records)
        db_storage.log_microbatch_metric(
            batch_id=str(batch_id),
            batch_size=len(records),
            fraud_count=fraud_count,
            fraud_rate=round(fraud_count / len(records) * 100, 2),
            avg_latency_ms=round(latency, 2)
        )
        logger.info(f"[Spark Batch {batch_id}] Processed {len(records)} jobs. Frauds: {fraud_count} ({fraud_count/len(records)*100:.1f}%) | Latency: {latency:.2f}ms/job")

    # Start Micro-Batch Processing Loop
    query = df_parsed.writeStream \
        .foreachBatch(process_micro_batch) \
        .trigger(processingTime=f"{MICRO_BATCH_INTERVAL_SECONDS} seconds") \
        .start()

    logger.info(f"Spark Structured Streaming pipeline running (Trigger: {MICRO_BATCH_INTERVAL_SECONDS}s). Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    run_spark_streaming()
