Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "C:\Users\Samsung Laptop\realtime-fake-job-detection"
WshShell.Run "python app.py", 0, False
