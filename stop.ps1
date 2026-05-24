#Requires -Version 5.1
# stop.ps1 — Shut down the Zeitgeist pipeline

$dir = $PSScriptRoot
$pidsFile = Join-Path $dir ".pids.json"

if (Test-Path $pidsFile) {
    $pidMap = Get-Content $pidsFile | ConvertFrom-Json
    foreach ($prop in $pidMap.PSObject.Properties) {
        $name = $prop.Name
        $id   = [int]$prop.Value
        # taskkill /T kills the cmd wrapper and its python/streamlit child
        $result = taskkill /T /F /PID $id 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  Stopped $name (PID $id)"
        } else {
            Write-Host "  $name (PID $id) already gone"
        }
    }
    Remove-Item $pidsFile
} else {
    Write-Host "No .pids.json found — pipeline may not be running."
}

Write-Host ""
Write-Host "Stopping Docker..."
docker compose -f "$dir\docker-compose.yml" down 2>&1 | Out-Null
Write-Host "Done."
