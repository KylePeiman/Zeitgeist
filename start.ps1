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

# ── LLAMA.CPP ─────────────────────────────────────────────────
$pidMap     = @{}
$llamaModel = if ($env:LLAMA_MODEL_PATH) { $env:LLAMA_MODEL_PATH } else { Join-Path $dir "Qwen2.5-1.5B-Instruct-Q4_K_M.gguf" }
$defaultExe = "C:\Users\Kyle\Desktop\GitHub\llama.cpp\build\bin\Release\llama-server.exe"
$llamaExe   = if ($env:LLAMA_SERVER_EXE) { $env:LLAMA_SERVER_EXE } elseif (Get-Command llama-server -ErrorAction SilentlyContinue) { "llama-server" } else { $defaultExe }

if (Test-Path $llamaModel) {
    Write-Host "Starting llama.cpp server..."
    $llamaProc = Start-Process `
        -FilePath $llamaExe `
        -ArgumentList "-m", $llamaModel, "--port", "8080", "-ngl", "99", "-t", "12" `
        -RedirectStandardOutput (Join-Path $logsDir "llama.log") `
        -RedirectStandardError  (Join-Path $logsDir "llama.err") `
        -WindowStyle Hidden `
        -PassThru
    $pidMap["llama"] = $llamaProc.Id

    Write-Host -NoNewline "Waiting for model to load"
    $llamaDeadline = (Get-Date).AddSeconds(300)
    $llamaReady = $false
    while ((Get-Date) -lt $llamaDeadline) {
        try {
            $r = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
            if ($r.StatusCode -eq 200 -and $r.Content -match '"ok"') { $llamaReady = $true; break }
        } catch {}
        Write-Host -NoNewline "."
        Start-Sleep -Seconds 3
    }
    Write-Host ""
    if ($llamaReady) {
        # Check if GPU is actually being used
        $errLog = Join-Path $logsDir "llama.err"
        if ((Test-Path $errLog) -and (Select-String -Path $errLog -Pattern "no usable GPU" -Quiet)) {
            Write-Warning "llama.cpp running on CPU only (no CUDA build). Inference will be slow (~30s/req)."
            Write-Warning "For GPU speed: rebuild llama.cpp with -DGGML_CUDA=ON"
        } else {
            Write-Host "llama.cpp is ready (GPU)."
        }
    } else {
        Write-Warning "llama.cpp did not become ready within 300s - scorer will use VADER fallback"
    }
} else {
    Write-Warning "Model not found at: $llamaModel - scorer will use VADER fallback"
}
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
