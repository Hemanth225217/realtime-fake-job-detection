Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "C:\Users\Samsung Laptop\realtime-fake-job-detection"

' Start backend server silently if not already up
WshShell.Run "python run_demo.py", 0, False

' Pause briefly to ensure port binding
WScript.Sleep 1000

' Launch dedicated standalone App window without browser chrome
WshShell.Run "cmd.exe /c start msedge --app=http://localhost:5000 --window-size=1340,880", 0, False
