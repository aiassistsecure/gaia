param(
  [ValidateSet("exists","status","down")][string]$Action,
  [int]$Port,
  [string]$HearthBinary
)
$ErrorActionPreference = "Stop"

# Git Bash PIDs are not Win32 PIDs. Match only this Hearth binary and port.
$binary = [IO.Path]::GetFullPath($HearthBinary)
$processes = @(Get-CimInstance Win32_Process)
$gateway = @($processes | Where-Object {
  $_.Name -ieq "hearth.exe" -and
  $_.ExecutablePath -ieq $binary -and
  $_.CommandLine -match "(?i)\s+up\s" -and
  $_.CommandLine -match ("(?i)--port\s+" + $Port + "(?:\s|$)")
})
$keepers = @($processes | Where-Object {
  $_.Name -ieq "bash.exe" -and
  $_.CommandLine -like "*keeper*" -and
  $_.CommandLine -like "*hearth.pid*" -and
  $_.CommandLine -like "*--port $Port*"
})
if ($Action -eq "exists") {
  if ($gateway.Count -gt 0 -or $keepers.Count -gt 0) { exit 0 }
  exit 1
}
if ($Action -eq "status") {
  Write-Output ("Windows: {0} gateway(s), {1} keeper(s) on port {2}" -f $gateway.Count,$keepers.Count,$Port)
  foreach ($p in $gateway) { Write-Output ("  hearth.exe PID {0}" -f $p.ProcessId) }
  foreach ($p in $keepers) { Write-Output ("  keeper bash.exe PID {0}" -f $p.ProcessId) }
  exit 0
}
# Snapshot only llama-server children of our gateway(s). Never kill unrelated Bash.
$gatewayIds = @($gateway | ForEach-Object { $_.ProcessId })
$workers = @($processes | Where-Object {
  $_.Name -ieq "llama-server.exe" -and $gatewayIds -contains $_.ParentProcessId
})
foreach ($p in $keepers) {
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Milliseconds 250
foreach ($p in $gateway) {
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
foreach ($p in $workers) {
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
Write-Output ("Windows: stopped {0} keeper(s), {1} gateway(s), {2} worker(s)" -f $keepers.Count,$gateway.Count,$workers.Count)
Write-Warning "Windows termination is forced; an unloaded event may not be recorded. Run 'hearth verify'."
