import os
import sys
import time
import json
import random
import logging
from pathlib import Path

BASE_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_PROJECT_DIR))

import numpy as np
import pandas as pd
from typing import Dict, Any, Generator, Optional
from src.config import RAW_DATA_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_JOB_POSTINGS, SIMULATED_STREAM_TPS

logger = logging.getLogger("KafkaProducer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

try:
    from kafka import KafkaProducer as RealKafkaProducer
    KAFKA_LIB_AVAILABLE = True
except ImportError:
    KAFKA_LIB_AVAILABLE = False


class JobStreamProducer:
    """
    Simulates high-velocity real-time job posting streams.
    Can publish directly to Apache Kafka or emit to an in-memory queue.
    """
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC_JOB_POSTINGS):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.kafka_producer = None
        self.is_connected = False
        self._init_kafka()

    def _init_kafka(self):
        if KAFKA_LIB_AVAILABLE and os.getenv("ENABLE_KAFKA", "false").lower() in ("true", "1"):
            try:
                self.kafka_producer = RealKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    request_timeout_ms=500,
                    max_block_ms=500
                )
                self.is_connected = True
                logger.info(f"Connected to Apache Kafka cluster at {self.bootstrap_servers}, topic: {self.topic}")
                return
            except Exception as e:
                logger.warning(f"Kafka cluster unreachable ({e}). Running in standalone streaming mode.")
        self.is_connected = False
        logger.info("Operating in standalone high-throughput streaming mode.")

    def get_job_generator(self, cycle: bool = True, fraud_boost_ratio: float = 0.20) -> Generator[Dict[str, Any], None, None]:
        """
        Yields job postings continuously from the dataset.
        fraud_boost_ratio increases fraud frequency slightly for demonstration purposes.
        """
        if not os.path.exists(RAW_DATA_PATH):
            raise FileNotFoundError(f"Dataset not found at {RAW_DATA_PATH}")

        df = pd.read_csv(RAW_DATA_PATH)
        real_df = df[df["fraudulent"] == 0]
        fake_df = df[df["fraudulent"] == 1]

        while True:
            # Decide whether to pick fake or real based on ratio
            if random.random() < fraud_boost_ratio and len(fake_df) > 0:
                row = fake_df.sample(1).iloc[0].to_dict()
            else:
                row = real_df.sample(1).iloc[0].to_dict()

            # Clean NaNs to None for JSON serialization
            cleaned = {}
            for k, v in row.items():
                if pd.isna(v):
                    cleaned[k] = None
                elif isinstance(v, (np.int64, np.int32)):
                    cleaned[k] = int(v)
                elif isinstance(v, (np.float64, np.float32)):
                    cleaned[k] = float(v)
                else:
                    cleaned[k] = v

            cleaned["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            cleaned["job_id"] = str(cleaned.get("job_id", random.randint(100000, 999999)))
            yield cleaned

            if not cycle:
                break

    def stream_to_kafka(self, max_messages: Optional[int] = None, tps: float = SIMULATED_STREAM_TPS):
        """Streams messages continuously to Kafka or terminal/log."""
        logger.info(f"Starting job stream generation at {tps} records/sec...")
        delay = 1.0 / max(0.1, tps)
        count = 0

        for job in self.get_job_generator(cycle=True):
            if self.is_connected and self.kafka_producer:
                self.kafka_producer.send(self.topic, value=job)
            count += 1
            if count % 25 == 0:
                logger.info(f"Emitted {count} streaming job postings to topic '{self.topic}'.")
            time.sleep(delay)
            if max_messages and count >= max_messages:
                break


if __name__ == "__main__":
    producer = JobStreamProducer()
    producer.stream_to_kafka(max_messages=50, tps=5.0)
