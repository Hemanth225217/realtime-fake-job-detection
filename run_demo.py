"""
StreamGuard Platform Launch Script.
Initializes the background streaming engine, database, and starts the API & Dashboard server.
"""

import sys
import os
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.api.server import app, get_engine
from src.config import API_HOST, API_PORT

def main():
    print("=" * 65)
    print("  StreamGuard-XAI: Real-Time Fake Job Detection Platform")
    print(f"  Dashboard & REST API running on: http://{API_HOST}:{API_PORT}")
    print("=" * 65)
    
    # Pre-initialize streaming engine
    eng = get_engine()
    print(f"✓ Engine initialized (Database backend: {eng.db.backend.upper()})")
    print(f"✓ Models loaded: {eng.model is not None}")
    print(f"✓ Explainer ready: {eng.explainer is not None}")
    print("\nPress Ctrl+C to stop the server.\n")

    app.run(host=API_HOST, port=API_PORT, debug=False, threaded=True)

if __name__ == "__main__":
    main()
