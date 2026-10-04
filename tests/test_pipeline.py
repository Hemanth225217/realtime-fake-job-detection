import sys
from pathlib import Path

# Add project root to sys.path
BASE_PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(BASE_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_PROJECT_DIR))

from src.storage.db import DatabaseStorage
from src.streaming.native_stream_engine import NativeStreamingEngine
from src.ml.feature_extractor import clean_text, extract_forensic_signals

def test_text_cleaner():
    raw = "<p>Urgent Hiring! Contact hr@gmail.com or visit http://example.com.</p>"
    cleaned = clean_text(raw)
    assert "urgent hiring" in cleaned
    assert "<p>" not in cleaned
    assert "url_link" in cleaned

def test_forensic_signals_detection():
    job = {
        "title": "Urgent Data Entry Clerk",
        "company_profile": "",
        "description": "We will wire transfer funds for home office equipment. Text me on Telegram @recruiter_job.",
        "requirements": "None",
        "has_company_logo": 0,
        "has_questions": 0,
        "telecommuting": 1
    }
    signals = extract_forensic_signals(job)
    assert signals["payment_scam_flags"] > 0
    assert signals["off_platform_flags"] > 0
    assert signals["missing_company_profile"] == 1
    assert signals["forensic_risk_score"] > 40

def test_database_storage_crud():
    db = DatabaseStorage()
    record = {
        "job_id": "UNIT_TEST_999",
        "title": "Test Engineer",
        "company_name": "Test Org",
        "location": "Remote",
        "prediction": "LEGITIMATE",
        "confidence_score": 0.05,
        "risk_level": "LOW",
        "explanation": "Valid test record",
        "token_attributions": [],
        "forensic_flags": [],
        "latency_ms": 1.2,
        "is_alert": False
    }
    assert db.save_prediction(record) is True
    recent = db.get_recent_jobs(limit=5)
    assert len(recent) > 0

def test_streaming_engine_classification():
    engine = NativeStreamingEngine()
    test_scam = {
        "job_id": "UNIT_SCAM_001",
        "title": "Work from home data entry - Cashier check",
        "company_profile": "",
        "description": "Earn $5000 weekly. We will send cashier's check to cover software setup. Msg Telegram @fast_hire.",
        "requirements": "",
        "has_company_logo": 0,
        "has_questions": 0
    }
    res = engine.classify_single(test_scam)
    assert res["prediction"] == "FRAUDULENT"
    assert res["risk_level"] in ["HIGH", "CRITICAL"]
    assert "explanation_card" in res
    assert len(res["explanation_card"]["explanation_bullets"]) > 0

if __name__ == "__main__":
    print("Running unit tests...")
    test_text_cleaner()
    print("✓ test_text_cleaner passed")
    test_forensic_signals_detection()
    print("✓ test_forensic_signals_detection passed")
    test_database_storage_crud()
    print("✓ test_database_storage_crud passed")
    test_streaming_engine_classification()
    print("✓ test_streaming_engine_classification passed")
    print("All unit tests passed successfully!")
