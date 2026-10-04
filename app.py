"""
StreamGuard Native Desktop Application Launcher.
Ensures backend Python server is running silently, then launches the dedicated desktop app window.
Zero manual terminal commands required.
"""

import os
import sys
import time
import threading
import subprocess
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config import API_HOST, API_PORT


def is_server_running():
    try:
        req = urllib.request.Request(f"http://{API_HOST}:{API_PORT}/api/health")
        with urllib.request.urlopen(req, timeout=1.2) as resp:
            return resp.status == 200
    except:
        return False


def start_backend():
    from src.api.server import app, get_engine
    # Pre-warm models and explainer
    _ = get_engine()
    app.run(host=API_HOST, port=API_PORT, debug=False, threaded=True)


def main():
    backend_started_here = False

    # 1. Start backend server silently if not already running
    if not is_server_running():
        server_thread = threading.Thread(target=start_backend, daemon=True)
        server_thread.start()
        backend_started_here = True

        for _ in range(40):
            if is_server_running():
                break
            time.sleep(0.2)

    target_url = f"http://{API_HOST}:{API_PORT}"

    # 2. Launch dedicated standalone native app window
    edge_candidates = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"
    ]

    launched = False
    for edge_exe in edge_candidates:
        if os.path.exists(edge_exe):
            proc = subprocess.Popen([
                edge_exe,
                f"--app={target_url}",
                "--window-size=1340,880",
                "--app-id=streamguard-xai"
            ])
            launched = True
            break

    if not launched:
        import webbrowser
        webbrowser.open(target_url)

    # 3. If this process spawned the server thread, keep running while user has app open
    if backend_started_here:
        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            pass


if __name__ == "__main__":
    main()
