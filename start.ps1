#Requires -Version 5.1
# start.ps1 — Launch the full Zeitgeist pipeline silently

$dir = $PSScriptRoot
$logsDir = Join-Path $dir "logs"
New-Item -ItemType Directory -Force -Path $logsDir | Out-Null

# ── DOCKER ────────────────────────────────────────────────────
Write-Host "Starting Docker infrastructure..."
# Tear down first to clear any stale Zookeeper ephemeral nodes from a previous run
docker compose -f "$dir\docker-compose.yml" down -v > "$logsDir\docker.log" 2>&1
docker compose -f "$dir\docker-compose.yml" up -d >> "$logsDir\docker.log" 2>&1

Write-Host -NoNewline "Waiting for Kafka to be ready"
$deadline = (Get-Date).AddSeconds(120)
$kafkaReady = $false
while ((Get-Date) -lt $deadline) {
    # Wait for container health, then verify Kafka actually responds to topic list
    $health = docker inspect --format "{{.State.Health.Status}}" zeitgeist-kafka 2>$null
    if ($health -eq "healthy") {
        $topics = docker exec zeitgeist-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>$null
        if ($LASTEXITCODE -eq 0) { $kafkaReady = $true; break }
    }
    Write-Host -NoNewline "."
    Start-Sleep -Seconds 2
}
Write-Host ""

if (-not $kafkaReady) {
    Write-Error "Kafka did not become ready within 120s. Check: docker compose ps"
    exit 1
}
Write-Host "Kafka is ready."

Write-Host "Creating Kafka topics..."
$topics = @("raw.reddit", "raw.youtube", "raw.news", "processed.signals")
foreach ($topic in $topics) {
    docker exec zeitgeist-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1 2>&1 | Out-Null
}
Write-Host "Topics ready."
Write-Host ""

# ── PYTHON PROCESSES ──────────────────────────────────────────
$services = @(
    @{ Name = "reddit";    Exe = "python"; Args = "producers/reddit_producer.py" },
    @{ Name = "youtube";   Exe = "python"; Args = "producers/youtube_producer.py" },
    @{ Name = "news";      Exe = "python"; Args = "producers/news_producer.py" },
    @{ Name = "flink";     Exe = "python"; Args = "flink/sentiment_pipeline.py" },
    @{ Name = "scorer";    Exe = "python"; Args = "llm_service/sentiment_scorer.py" },
    @{ Name = "dashboard"; Exe = "streamlit"; Args = "run dashboard/app.py" }
)

$pidMap = @{}
foreach ($svc in $services) {
    $log = Join-Path $logsDir "$($svc.Name).log"
    # cmd /c merges stderr into stdout so all output lands in one log file
    $proc = Start-Process cmd `
        -ArgumentList "/c", "$($svc.Exe) $($svc.Args) >> `"$log`" 2>&1" `
        -WorkingDirectory $dir `
        -WindowStyle Hidden `
        -PassThru
    $pidMap[$svc.Name] = $proc.Id
    Write-Host "  [$($svc.Name)] started  (PID $($proc.Id))  ->  logs\$($svc.Name).log"
}

$pidMap | ConvertTo-Json | Set-Content (Join-Path $dir ".pids.json") -Encoding utf8

Write-Host ""
Write-Host "Zeitgeist is running."
Write-Host "  Dashboard : http://localhost:8501"
Write-Host "  Kafdrop   : http://localhost:9000"
Write-Host "  Logs      : $logsDir"
Write-Host "  Stop      : .\stop.ps1"
