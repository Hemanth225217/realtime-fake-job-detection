@echo off
title StreamGuard AI Desktop App
cd /d "%~dp0"
start "" /b python run_demo.py
timeout /t 1 /nobreak >nul
start msedge --app=http://localhost:5000 --window-size=1340,880
