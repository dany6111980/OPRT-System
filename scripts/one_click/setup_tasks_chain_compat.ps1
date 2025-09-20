# Creates/updates two Windows Scheduled Tasks that call the chain scripts.
param(
  [string]$Root="C:\OPRT",
  [string]$Py="python.exe"  # used inside the chain scripts
)

$ErrorActionPreference="Stop"

# PowerShell host command to run the chains
$pw = "$PSHOME\powershell.exe"
$hourlyCmd  = '"{0}" -NoProfile -ExecutionPolicy Bypass -File "{1}\scripts\oprt_hourly_chain.ps1" -Root "{1}"' -f $pw, $Root
$nightlyCmd = '"{0}" -NoProfile -ExecutionPolicy Bypass -File "{1}\scripts\oprt_nightly_chain.ps1" -Root "{1}"' -f $pw, $Root

# Start times (next hour)
$now    = Get-Date
$hStart = (Get-Date -Minute 2 -Second 0).AddHours(1)   # HH:02 → after candle close
$hTime  = $hStart.ToString("HH:mm")

# 1) Hourly chain — every hour at HH:02
schtasks /Create /TN "OPRT Hourly Chain" /TR $hourlyCmd /SC HOURLY /MO 1 /ST $hTime /RL HIGHEST /F | Out-Null

# 2) Nightly chain — 23:55 daily
schtasks /Create /TN "OPRT Nightly Chain" /TR $nightlyCmd /SC DAILY /ST 23:55 /RL HIGHEST /F | Out-Null

Write-Host "Tasks created/updated:" -ForegroundColor Cyan
schtasks /Query /FO LIST | Select-String "OPRT Hourly Chain|OPRT Nightly Chain" | ForEach-Object { $_.Line }
