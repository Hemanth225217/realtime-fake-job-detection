import os
import json
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from src.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD, SQLITE_DB_PATH
)

logger = logging.getLogger("StorageEngine")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

try:
    import psycopg2
    from psycopg2.extras import execute_values
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


class DatabaseStorage:
    """
    Enterprise-grade Dual Storage Engine.
    Tries PostgreSQL first; falls back gracefully to SQLite if PostgreSQL credentials
    or server are unreachable. Ensures zero-downtime micro-batch writes.
    """
    def __init__(self):
        self.backend = "sqlite"
        self.pg_conn_params = {
            "host": POSTGRES_HOST,
            "port": POSTGRES_PORT,
            "dbname": POSTGRES_DB,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD
        }
        self._test_connection()
        self._init_schema()

    def _test_connection(self):
        if PSYCOPG2_AVAILABLE and self.pg_conn_params["password"]:
            try:
                conn = psycopg2.connect(**self.pg_conn_params, connect_timeout=3)
                conn.close()
                self.backend = "postgresql"
                logger.info(f"Connected successfully to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
                return
            except Exception as e:
                logger.warning(f"PostgreSQL connection failed ({e}). Falling back to robust SQLite storage.")
        self.backend = "sqlite"
        logger.info(f"Operating with local SQLite storage at {SQLITE_DB_PATH}")

    def _get_connection(self):
        if self.backend == "postgresql":
            return psycopg2.connect(**self.pg_conn_params)
        else:
            conn = sqlite3.connect(str(SQLITE_DB_PATH), timeout=15)
            conn.row_factory = sqlite3.Row
            return conn

    def _init_schema(self):
        schema_sql = """
        CREATE TABLE IF NOT EXISTS job_predictions (
            job_id VARCHAR(64) PRIMARY KEY,
            title TEXT,
            company_name TEXT,
            location TEXT,
            prediction VARCHAR(20) NOT NULL,
            confidence_score REAL NOT NULL,
            risk_level VARCHAR(20) NOT NULL,
            explanation TEXT,
            token_attributions TEXT,
            forensic_flags TEXT,
            latency_ms REAL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_alert BOOLEAN DEFAULT FALSE
        );

        CREATE INDEX IF NOT EXISTS idx_pred_timestamp ON job_predictions(timestamp);
        CREATE INDEX IF NOT EXISTS idx_pred_risk ON job_predictions(risk_level);
        CREATE INDEX IF NOT EXISTS idx_pred_alert ON job_predictions(is_alert);

        CREATE TABLE IF NOT EXISTS stream_metrics_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id VARCHAR(64),
            batch_size INTEGER,
            fraud_count INTEGER,
            fraud_rate REAL,
            avg_latency_ms REAL,
            batch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        if self.backend == "postgresql":
            pg_schema_sql = schema_sql.replace("AUTOINCREMENT", "SERIAL")
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(pg_schema_sql)
                conn.commit()
        else:
            with self._get_connection() as conn:
                conn.executescript(schema_sql)
                conn.commit()

    def save_prediction(self, record: Dict[str, Any]) -> bool:
        """Saves a single job classification record."""
        return self.save_microbatch([record])

    def save_microbatch(self, records: List[Dict[str, Any]]) -> bool:
        """Saves a micro-batch of classification records transactionally."""
        if not records:
            return True

        if self.backend == "postgresql":
            insert_query = """
            INSERT INTO job_predictions (
                job_id, title, company_name, location, prediction, confidence_score,
                risk_level, explanation, token_attributions, forensic_flags, latency_ms, timestamp, is_alert
            ) VALUES %s
            ON CONFLICT (job_id) DO UPDATE SET
                prediction = EXCLUDED.prediction,
                confidence_score = EXCLUDED.confidence_score,
                risk_level = EXCLUDED.risk_level,
                explanation = EXCLUDED.explanation,
                token_attributions = EXCLUDED.token_attributions,
                forensic_flags = EXCLUDED.forensic_flags,
                latency_ms = EXCLUDED.latency_ms,
                is_alert = EXCLUDED.is_alert;
            """
            values = []
            for r in records:
                values.append((
                    str(r.get("job_id")),
                    r.get("title", ""),
                    r.get("company_name", ""),
                    r.get("location", ""),
                    r.get("prediction", "UNKNOWN"),
                    float(r.get("confidence_score", 0.0)),
                    r.get("risk_level", "LOW"),
                    r.get("explanation", ""),
                    json.dumps(r.get("token_attributions", [])),
                    json.dumps(r.get("forensic_flags", [])),
                    float(r.get("latency_ms", 0.0)),
                    r.get("timestamp", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
                    bool(r.get("is_alert", False))
                ))
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, insert_query, values)
                conn.commit()
            return True
        else:
            insert_query = """
            INSERT INTO job_predictions (
                job_id, title, company_name, location, prediction, confidence_score,
                risk_level, explanation, token_attributions, forensic_flags, latency_ms, timestamp, is_alert
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                prediction = excluded.prediction,
                confidence_score = excluded.confidence_score,
                risk_level = excluded.risk_level,
                explanation = excluded.explanation,
                token_attributions = excluded.token_attributions,
                forensic_flags = excluded.forensic_flags,
                latency_ms = excluded.latency_ms,
                is_alert = excluded.is_alert;
            """
            rows = []
            for r in records:
                rows.append((
                    str(r.get("job_id")),
                    r.get("title", ""),
                    r.get("company_name", ""),
                    r.get("location", ""),
                    r.get("prediction", "UNKNOWN"),
                    float(r.get("confidence_score", 0.0)),
                    r.get("risk_level", "LOW"),
                    r.get("explanation", ""),
                    json.dumps(r.get("token_attributions", [])),
                    json.dumps(r.get("forensic_flags", [])),
                    float(r.get("latency_ms", 0.0)),
                    r.get("timestamp", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")),
                    1 if r.get("is_alert", False) else 0
                ))
            with self._get_connection() as conn:
                conn.executemany(insert_query, rows)
                conn.commit()
            return True

    def log_microbatch_metric(self, batch_id: str, batch_size: int, fraud_count: int,
                              fraud_rate: float, avg_latency_ms: float):
        sql = """
        INSERT INTO stream_metrics_log (batch_id, batch_size, fraud_count, fraud_rate, avg_latency_ms)
        VALUES (?, ?, ?, ?, ?)
        """
        if self.backend == "postgresql":
            sql = sql.replace("?", "%s")
        with self._get_connection() as conn:
            with conn.cursor() if self.backend == "postgresql" else conn as cur:
                cur.execute(sql, (batch_id, batch_size, fraud_count, fraud_rate, avg_latency_ms))
            conn.commit()

    def get_recent_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM job_predictions ORDER BY timestamp DESC LIMIT {limit}"
        with self._get_connection() as conn:
            if self.backend == "postgresql":
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cols = [desc[0] for desc in cur.description]
                    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            else:
                cur = conn.cursor()
                cur.execute(sql)
                rows = [dict(row) for row in cur.fetchall()]
        
        for r in rows:
            if isinstance(r.get("token_attributions"), str):
                try: r["token_attributions"] = json.loads(r["token_attributions"])
                except: pass
            if isinstance(r.get("forensic_flags"), str):
                try: r["forensic_flags"] = json.loads(r["forensic_flags"])
                except: pass
        return rows

    def get_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM job_predictions WHERE is_alert = TRUE OR risk_level IN ('HIGH', 'CRITICAL') ORDER BY timestamp DESC LIMIT {limit}"
        if self.backend == "sqlite":
            sql = sql.replace("TRUE", "1")
        with self._get_connection() as conn:
            if self.backend == "postgresql":
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cols = [desc[0] for desc in cur.description]
                    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            else:
                cur = conn.cursor()
                cur.execute(sql)
                rows = [dict(row) for row in cur.fetchall()]
        
        for r in rows:
            if isinstance(r.get("token_attributions"), str):
                try: r["token_attributions"] = json.loads(r["token_attributions"])
                except: pass
            if isinstance(r.get("forensic_flags"), str):
                try: r["forensic_flags"] = json.loads(r["forensic_flags"])
                except: pass
        return rows

    def get_system_summary(self) -> Dict[str, Any]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*), SUM(CASE WHEN prediction = 'FRAUDULENT' THEN 1 ELSE 0 END), AVG(latency_ms) FROM job_predictions")
            row = cur.fetchone()
            total = row[0] or 0
            frauds = row[1] or 0
            avg_lat = row[2] or 0.0
            return {
                "total_jobs_processed": total,
                "fraudulent_detected": frauds,
                "legitimate_verified": total - frauds,
                "fraud_rate_pct": round((frauds / total * 100), 2) if total > 0 else 0.0,
                "avg_inference_latency_ms": round(avg_lat, 2),
                "database_backend": self.backend
            }
