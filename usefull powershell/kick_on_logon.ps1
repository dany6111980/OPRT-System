Start-ScheduledTask -TaskName "OPRT Hourly Run (Logged-In)"
Start-Sleep 2
# Optional: show last 60 lines so you know it fired
Get-ChildItem C:\OPRT\logs\run_*.log |
  Sort-Object LastWriteTime -Desc |
  Select-Object -First 1 |
  Get-Content -Tail 60 |
  Out-String | Write-Output
