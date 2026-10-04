"""
StreamGuard Desktop Application Launcher.
Wraps the StreamGuard XAI platform in a standalone native desktop window.
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
        with urllib.request.urlopen(f"http://{API_HOST}:{API_PORT}/api/health", timeout=1.5) as resp:
            return resp.status == 200
    except:
        return False


def start_backend_server():
    from src.api.server import app, get_engine
    eng = get_engine()
    app.run(host=API_HOST, port=API_PORT, debug=False, threaded=True)


def launch_native_app():
    # 1. Ensure backend is running
    if not is_server_running():
        print("Starting StreamGuard backend server in background thread...")
        server_thread = threading.Thread(target=start_backend_server, daemon=True)
        server_thread.start()

        # Wait for server ready
        for _ in range(30):
            if is_server_running():
                break
            time.sleep(0.3)

    target_url = f"http://{API_HOST}:{API_PORT}"
    print(f"Backend ready at {target_url}. Launching native Desktop App window...")

    # 2. Try launching with pywebview
    try:
        import webview
        icon_path = str(BASE_DIR / "streamguard.ico")
        
        window = webview.create_window(
            title="StreamGuard AI — Real-Time Fake Job Detection & XAI Platform",
            url=target_url,
            width=1320,
            height=880,
            resizable=True,
            min_size=(1024, 720),
            text_select=True
        )
        webview.start(debug=False)
        return
    except Exception as e:
        print(f"pywebview notice ({e}). Launching via native Windows App mode...")

    # 3. Fallback to Windows Edge App Mode (Dedicated window without address bar or browser tabs)
    try:
        edge_cmd = [
            "cmd.exe", "/c", "start", "msedge",
            f"--app={target_url}",
            f"--window-size=1320,880",
            f"--app-id=streamguard-xai"
        ]
        subprocess.run(edge_cmd, check=True)
    except Exception as e:
        import webbrowser
        webbrowser.open(target_url)


if __name__ == "__main__":
    launch_native_app()
