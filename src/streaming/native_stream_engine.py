"""
High-Performance Native Micro-Batch Streaming Engine.
Provides sub-millisecond real-time stream processing, live metric computation,
dual-engine XAI auditing, and automated DB persistence.
Operates seamlessly with or without an external Spark/Kafka cluster.
"""

import sys
import time
import queue
import logging
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional
from collections import deque
import pandas as pd
import numpy as np
import joblib

# Add project root to sys.path
BASE_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_PROJECT_DIR))

from src.config import (
    MODELS_DIR, FRAUD_PROBABILITY_THRESHOLD,
    MICRO_BATCH_INTERVAL_SECONDS, SIMULATED_STREAM_TPS
)
from src.storage.db import DatabaseStorage
from src.xai.explainer import DualEngineExplainer
from src.ingestion.kafka_producer import JobStreamProducer

logger = logging.getLogger("NativeStreamEngine")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class StreamingMetrics:
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.recent_latencies = deque(maxlen=window_size)
        self.recent_predictions = deque(maxlen=window_size)
        self.total_processed = 0
        self.total_fraud = 0
        self.start_time = time.time()
        self.batch_count = 0

    def record_job(self, is_fraud: bool, latency_ms: float):
        self.total_processed += 1
        if is_fraud:
            self.total_fraud += 1
        self.recent_predictions.append(1 if is_fraud else 0)
        self.recent_latencies.append(latency_ms)

    def get_snapshot(self) -> Dict[str, Any]:
        uptime_sec = max(1.0, time.time() - self.start_time)
        tps = self.total_processed / uptime_sec
        recent_fraud_rate = (sum(self.recent_predictions) / len(self.recent_predictions) * 100) if self.recent_predictions else 0.0
        overall_fraud_rate = (self.total_fraud / self.total_processed * 100) if self.total_processed > 0 else 0.0
        avg_latency = float(np.mean(self.recent_latencies)) if self.recent_latencies else 0.0

        return {
            "uptime_seconds": round(uptime_sec, 1),
            "total_processed": self.total_processed,
            "total_fraud_detected": self.total_fraud,
            "overall_fraud_rate_pct": round(overall_fraud_rate, 2),
            "recent_fraud_rate_pct": round(recent_fraud_rate, 2),
            "current_throughput_tps": round(tps, 2),
            "avg_latency_ms": round(avg_latency, 3),
            "batch_count": self.batch_count
        }


class NativeStreamingEngine:
    def __init__(self):
        self.db = DatabaseStorage()
        self.metrics = StreamingMetrics()
        self.is_running = False
        self.stream_thread = None
        self.producer = JobStreamProducer()
        self.incoming_queue = queue.Queue(maxsize=1000)
        self.recent_stream_buffer = deque(maxlen=40)
        self.alert_subscribers = []
        self._load_models()

    def _load_models(self):
        model_path = MODELS_DIR / "streamguard_lgbm.pkl"
        calibrated_lr_path = MODELS_DIR / "streamguard_calibrated_lr.pkl"
        extractor_path = MODELS_DIR / "multimodal_extractor.pkl"

        if not model_path.exists() or not extractor_path.exists():
            logger.warning("Models not yet trained. Run training first.")
            self.model = None
            self.extractor = None
            self.explainer = None
            return

        logger.info("Loading production models and vectorizer...")
        self.model = joblib.load(model_path)
        self.calibrated_lr = joblib.load(calibrated_lr_path)
        self.extractor = joblib.load(extractor_path)
        self.explainer = DualEngineExplainer(
            model=self.calibrated_lr,
            feature_names=self.extractor.feature_names_,
            vectorizer=self.extractor.vectorizer
        )
        logger.info("Models & DualEngineExplainer loaded successfully.")

    def classify_single(self, job_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Classifies a single incoming job posting in real time."""
        if not self.model or not self.extractor:
            self._load_models()
            if not self.model:
                raise RuntimeError("Models are not loaded or trained yet.")

        t0 = time.perf_counter()
        df = pd.DataFrame([job_dict])
        X = self.extractor.transform(df)
        prob = float(self.model.predict_proba(X)[0, 1])
        latency_ms = (time.perf_counter() - t0) * 1000

        is_fraud = prob >= FRAUD_PROBABILITY_THRESHOLD
        explanation_card = self.explainer.explain(job_dict, prob)
        risk_level = explanation_card["risk_level"]

        result = {
            "job_id": str(job_dict.get("job_id")),
            "title": str(job_dict.get("title", "Untitled")),
            "company_name": str(job_dict.get("department") or job_dict.get("company_profile", "")[:30] or "Not Specified"),
            "location": str(job_dict.get("location", "Remote / Unspecified")),
            "prediction": "FRAUDULENT" if is_fraud else "LEGITIMATE",
            "confidence_score": round(prob, 4),
            "risk_level": risk_level,
            "explanation": "; ".join(explanation_card["explanation_bullets"][:3]),
            "explanation_card": explanation_card,
            "token_attributions": explanation_card["token_attributions"],
            "forensic_flags": explanation_card["flagged_keywords"],
            "latency_ms": round(latency_ms, 3),
            "timestamp": job_dict.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
            "is_alert": is_fraud or risk_level in ["HIGH", "CRITICAL"]
        }

        self.metrics.record_job(is_fraud, latency_ms)
        return result

    def start_streaming(self, tps: float = SIMULATED_STREAM_TPS, fraud_boost: float = 0.25):
        """Starts background micro-batch streaming processor."""
        if self.is_running:
            logger.info("Streaming engine already running.")
            return

        self.is_running = True
        logger.info(f"Starting native streaming engine at {tps} TPS...")

        def _stream_worker():
            delay = 1.0 / max(0.1, tps)
            job_gen = self.producer.get_job_generator(cycle=True, fraud_boost_ratio=fraud_boost)
            micro_batch = []
            last_batch_time = time.time()

            while self.is_running:
                try:
                    job = next(job_gen)
                    res = self.classify_single(job)
                    micro_batch.append(res)
                    self.recent_stream_buffer.appendleft(res)

                    # Trigger micro-batch write to DB every interval or when batch size reaches 10
                    now = time.time()
                    if (now - last_batch_time >= MICRO_BATCH_INTERVAL_SECONDS) or len(micro_batch) >= 10:
                        if micro_batch:
                            self.db.save_microbatch(micro_batch)
                            frauds = sum(1 for m in micro_batch if m["prediction"] == "FRAUDULENT")
                            self.metrics.batch_count += 1
                            self.db.log_microbatch_metric(
                                batch_id=f"native_batch_{self.metrics.batch_count}",
                                batch_size=len(micro_batch),
                                fraud_count=frauds,
                                fraud_rate=round(frauds / len(micro_batch) * 100, 2),
                                avg_latency_ms=round(float(np.mean([m['latency_ms'] for m in micro_batch])), 2)
                            )
                            micro_batch = []
                            last_batch_time = now

                    time.sleep(delay)
                except Exception as e:
                    logger.error(f"Error in streaming worker: {e}")
                    time.sleep(1.0)

        self.stream_thread = threading.Thread(target=_stream_worker, daemon=True)
        self.stream_thread.start()

    def stop_streaming(self):
        """Stops the streaming thread cleanly."""
        self.is_running = False
        if self.stream_thread:
            self.stream_thread.join(timeout=2.0)
        logger.info("Streaming engine stopped.")
