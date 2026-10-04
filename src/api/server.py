"""
Production REST API and Real-Time Dashboard Server for StreamGuard Fake Job Detection.
Provides endpoints for high-throughput inference, live stream control, metrics telemetry,
and glass-box explainability visualization.
"""

import os
import sys
import time
import json
import logging
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory, render_template_string

# Add project root to sys.path
BASE_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_PROJECT_DIR))

from src.config import API_HOST, API_PORT, EVAL_DIR, STATIC_DIR
from src.streaming.native_stream_engine import NativeStreamingEngine

logger = logging.getLogger("APIServer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

app = Flask(__name__, static_folder=str(STATIC_DIR))
engine = None

def get_engine():
    global engine
    if engine is None:
        engine = NativeStreamingEngine()
    return engine


@app.route("/")
def index():
    """Serves the Real-Time Mission Control Dashboard."""
    dashboard_file = STATIC_DIR / "index.html"
    if dashboard_file.exists():
        with open(dashboard_file, "r", encoding="utf-8") as f:
            return f.read()
    return "<h3>StreamGuard Real-Time Fake Job Detection API is running. Dashboard file not found.</h3>"


@app.route("/api/health", methods=["GET"])
def health():
    eng = get_engine()
    return jsonify({
        "status": "healthy",
        "service": "StreamGuard Real-Time Fake Job Detection Platform",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "database_backend": eng.db.backend,
        "models_loaded": eng.model is not None,
        "streaming_active": eng.is_running
    })


@app.route("/api/predict", methods=["POST"])
def predict():
    """
    Evaluates a single job posting payload.
    Expected JSON keys: title, company_profile, description, requirements, benefits, location, etc.
    """
    data = request.get_json(force=True)
    if not data:
        return jsonify({"error": "Empty payload"}), 400

    try:
        eng = get_engine()
        result = eng.classify_single(data)
        # Also persist to database
        eng.db.save_prediction(result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/stream/start", methods=["POST"])
def start_stream():
    """Starts continuous background job stream simulation."""
    params = request.get_json(silent=True) or {}
    tps = float(params.get("tps", 5.0))
    fraud_boost = float(params.get("fraud_boost", 0.25))

    eng = get_engine()
    eng.start_streaming(tps=tps, fraud_boost=fraud_boost)
    return jsonify({
        "status": "started",
        "tps": tps,
        "fraud_boost_ratio": fraud_boost,
        "message": f"Real-time stream initiated at {tps} jobs/sec"
    })


@app.route("/api/stream/stop", methods=["POST"])
def stop_stream():
    """Stops background streaming simulation."""
    eng = get_engine()
    eng.stop_streaming()
    return jsonify({
        "status": "stopped",
        "message": "Real-time stream stopped"
    })


@app.route("/api/stream/metrics", methods=["GET"])
def stream_metrics():
    """Returns real-time streaming telemetry and database totals."""
    eng = get_engine()
    metrics = eng.metrics.get_snapshot()
    db_summary = eng.db.get_system_summary()
    return jsonify({
        "realtime_telemetry": metrics,
        "database_totals": db_summary,
        "is_streaming": eng.is_running
    })


@app.route("/api/stream/recent", methods=["GET"])
def stream_recent():
    """Returns the latest evaluated streaming jobs for UI ticker."""
    eng = get_engine()
    limit = int(request.args.get("limit", 25))
    recent_jobs = list(eng.recent_stream_buffer)[:limit]
    return jsonify(recent_jobs)


@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    """Returns high/critical risk alerts from the database."""
    eng = get_engine()
    limit = int(request.args.get("limit", 50))
    alerts = eng.db.get_alerts(limit=limit)
    return jsonify(alerts)


@app.route("/api/benchmark", methods=["GET"])
def get_benchmark():
    """Returns benchmark comparison between baseline and proposed models."""
    eval_file = EVAL_DIR / "benchmark_results.json"
    if eval_file.exists():
        with open(eval_file, "r") as f:
            return jsonify(json.load(f))
    return jsonify({"error": "Benchmark results not yet generated. Run training."}), 404


def run_api_server():
    print(f"Starting StreamGuard REST API on http://{API_HOST}:{API_PORT}...")
    app.run(host=API_HOST, port=API_PORT, debug=False, threaded=True)


if __name__ == "__main__":
    run_api_server()
