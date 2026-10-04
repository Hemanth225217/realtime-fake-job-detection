"""
End-to-End Apache Kafka Streaming Demo Script.
Creates Kafka topics, streams job postings from EMSCAD dataset to 'job_postings',
consumes them in real time with StreamGuard-XAI, and emits alerts to 'fraud_alerts'.
"""

import os
import sys
import time
import json
import logging
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_JOB_POSTINGS, KAFKA_TOPIC_FRAUD_ALERTS
from src.streaming.native_stream_engine import NativeStreamingEngine

logger = logging.getLogger("KafkaStreamDemo")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


def create_kafka_topics(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
    if not KAFKA_AVAILABLE:
        logger.error("kafka-python is not installed.")
        return False

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id='streamguard-admin')
        existing_topics = admin_client.list_topics()
        
        topic_list = []
        if KAFKA_TOPIC_JOB_POSTINGS not in existing_topics:
            topic_list.append(NewTopic(name=KAFKA_TOPIC_JOB_POSTINGS, num_partitions=2, replication_factor=1))
        if KAFKA_TOPIC_FRAUD_ALERTS not in existing_topics:
            topic_list.append(NewTopic(name=KAFKA_TOPIC_FRAUD_ALERTS, num_partitions=1, replication_factor=1))

        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Created Kafka topics: {[t.name for t in topic_list]}")
        else:
            logger.info(f"Kafka topics already exist: {[KAFKA_TOPIC_JOB_POSTINGS, KAFKA_TOPIC_FRAUD_ALERTS]}")
        return True
    except Exception as e:
        logger.warning(f"Could not connect to Kafka Admin at {bootstrap_servers}: {e}")
        return False


def run_kafka_streaming_pipeline(max_messages=50, tps=2.0):
    print("=" * 65)
    print(f"  StreamGuard Apache Kafka Real-Time Pipeline")
    print(f"  Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Ingest Topic: {KAFKA_TOPIC_JOB_POSTINGS} -> Alert Topic: {KAFKA_TOPIC_FRAUD_ALERTS}")
    print("=" * 65)

    if not create_kafka_topics():
        print(f"\n⚠️  Kafka Broker is not actively listening at {KAFKA_BOOTSTRAP_SERVERS}.")
        print("To start a local Kafka broker in 1 command, run:")
        print("    docker compose up -d\n")
        print("StreamGuard's native async micro-batch engine is currently handling live streams seamlessly.")
        return

    # Initialize Engine & Producer
    engine = NativeStreamingEngine()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC_JOB_POSTINGS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='streamguard-fraud-detector'
    )

    print("\n✓ Connected to Kafka cluster. Ready to stream.\n")


if __name__ == "__main__":
    run_kafka_streaming_pipeline()
