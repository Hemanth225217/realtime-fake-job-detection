import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "fake_job_postings.csv"
PROCESSED_DATA_PATH = DATA_DIR / "processed" / "cleaned_jobs.parquet"
MODELS_DIR = BASE_DIR / "models" / "checkpoints"
EVAL_DIR = BASE_DIR / "models" / "evaluations"
STATIC_DIR = BASE_DIR / "dashboard"

# Ensure directories exist
for p in [DATA_DIR, DATA_DIR / "raw", DATA_DIR / "processed", MODELS_DIR, EVAL_DIR, STATIC_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_JOB_POSTINGS = os.getenv("KAFKA_TOPIC_JOB_POSTINGS", "job_postings")
KAFKA_TOPIC_FRAUD_ALERTS = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud_alerts")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-detection-engine")

# Spark Configuration
SPARK_APP_NAME = "RealTimeFakeJobDetector"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
MICRO_BATCH_INTERVAL_SECONDS = int(os.getenv("MICRO_BATCH_INTERVAL_SECONDS", "5"))
SPARK_CHECKPOINT_DIR = str(BASE_DIR / "data" / "spark_checkpoints")

# Database Configuration (PostgreSQL with automatic SQLite Fallback)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
SQLITE_DB_PATH = BASE_DIR / "data" / "fraud_detection.db"

# API & Streaming
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "5000"))
SIMULATED_STREAM_TPS = float(os.getenv("SIMULATED_STREAM_TPS", "5.0")) # transactions per second

# Model Thresholds
FRAUD_PROBABILITY_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.40")) # Tuned for optimal F1 on imbalanced data
HIGH_RISK_THRESHOLD = 0.70
MEDIUM_RISK_THRESHOLD = 0.40
