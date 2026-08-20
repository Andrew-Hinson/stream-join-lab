$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root
$LabEnv = Join-Path $Root ".lab.env"
$WaitSecs = 600
$PortCap = 100

function Invoke-Compose {
  param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)
  if (Test-Path $LabEnv) {
    & docker compose --env-file $LabEnv @Args
  } else {
    & docker compose @Args
  }
}

Write-Host ""
Write-Host "Clean start: existing Lab volumes will be destroyed."
Write-Host "First run is about 20 minutes (image pulls + Maven). Later runs are faster."
Write-Host "Match facts and Grafana open when Ready (at least one match_facts row), or after 10 minutes."
Write-Host ""

$ready = $false
for ($i = 1; $i -le 30; $i++) {
  docker info 2>$null | Out-Null
  if ($LASTEXITCODE -eq 0) { $ready = $true; break }
  Write-Host "Waiting for Docker Desktop... ($i/30)"
  Start-Sleep -Seconds 2
}
if (-not $ready) {
  Write-Error "Docker is not running. Open Docker Desktop and re-run .\up.cmd."
}

docker compose version 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
  Write-Error "Need Docker Compose v2 (docker compose). Install Docker Desktop."
}

function Test-DockerDesktop {
  $os = docker info --format '{{.OperatingSystem}}' 2>$null
  return ($os -match 'Desktop')
}

function Write-RamHint {
  param([string]$Stream = "Output")
  $msg = if (Test-DockerDesktop) {
    "Docker Desktop → Settings → Resources → Memory."
  } else {
    "Give this Docker daemon more RAM, or close other containers."
  }
  if ($Stream -eq "Error") { Write-Error $msg } else { Write-Host $msg }
}

$ramBytes = [int64](docker info --format '{{.MemTotal}}')
$ramGiB = [math]::Round($ramBytes / 1GB, 1)
$failBytes = 6L * 1GB
$warnBytes = 8L * 1GB
if ($ramBytes -lt $failBytes) {
  Write-Host "Docker has $ramGiB GiB. Need at least 6 GiB (8 GiB is comfortable)."
  if (Test-DockerDesktop) {
    Write-Error "Docker Desktop → Settings → Resources → Memory."
  } else {
    Write-Error "Give this Docker daemon more RAM, or close other containers."
  }
}
if ($ramBytes -lt $warnBytes) {
  Write-Host "Docker has $ramGiB GiB — tight. Raise the slider to 8 GiB+."
  if (Test-DockerDesktop) {
    Write-Host "Docker Desktop → Settings → Resources → Memory."
  } else {
    Write-Host "Give this Docker daemon more RAM, or close other containers."
  }
}

function Test-PortFree([int]$Port) {
  try {
    $client = New-Object System.Net.Sockets.TcpClient
    $iar = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
    $ok = $iar.AsyncWaitHandle.WaitOne(250, $false)
    if ($ok -and $client.Connected) {
      $client.Close()
      return $false
    }
    $client.Close()
    return $true
  } catch {
    return $true
  }
}

function Select-Port([string]$Name, [int]$Start) {
  for ($p = $Start; $p -le ($Start + $PortCap); $p++) {
    if (Test-PortFree $p) { return $p }
  }
  throw "No free port for $Name in $Start-$($Start + $PortCap). Something is using $Start."
}

Write-Host "Wiping Lab volumes..."
try { Invoke-Compose down -v | Out-Null } catch { }

$superset = Select-Port SUPERSET 8088
$grafana = Select-Port GRAFANA 3000
$flink = Select-Port FLINK 8081
@"
SUPERSET_PORT=$superset
GRAFANA_PORT=$grafana
FLINK_PORT=$flink
"@ | Set-Content -Path $LabEnv -Encoding ascii

Write-Host "Starting Lab (Superset $superset, Grafana $grafana, Flink $flink)..."
Invoke-Compose up --build -d
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

function Get-FactCount {
  $out = & docker compose --env-file $LabEnv exec -T trino trino --output-format CSV_UNQUOTED --execute "SELECT COUNT(*) FROM iceberg.demo.match_facts" 2>$null
  $n = ($out | Out-String).Trim()
  if ($n -match '^\d+$') { return [int]$n }
  return 0
}

function Test-HttpOk([string]$Url) {
  try {
    Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2 | Out-Null
    return $true
  } catch {
    return $false
  }
}

Write-Host "Waiting until match_facts has a row (Ready), up to 10 minutes..."
$deadline = (Get-Date).AddSeconds($WaitSecs)
$n = 0
while ((Get-Date) -lt $deadline) {
  $n = Get-FactCount
  if ($n -ge 1 -and (Test-HttpOk "http://127.0.0.1:${superset}/health") -and (Test-HttpOk "http://127.0.0.1:${grafana}/api/health")) {
    Write-Host "Ready: $n match_facts row(s)."
    break
  }
  Start-Sleep -Seconds 5
  $n = Get-FactCount
}
if ($n -lt 1) {
  Write-Host "Still filling: no match_facts rows after 10 minutes. Opening UIs anyway."
}

$factsUrl = "http://127.0.0.1:${superset}/superset/dashboard/match-facts/"
$grafanaUrl = "http://127.0.0.1:${grafana}/d/pipeline-health"
$flinkUrl = "http://127.0.0.1:${flink}"
Write-Host ""
Write-Host "========================================"
Write-Host " Match facts  $factsUrl"
Write-Host " Grafana      $grafanaUrl"
Write-Host " Flink        $flinkUrl"
Write-Host "========================================"
Write-Host ""

try { Start-Process $factsUrl } catch { }
try { Start-Process $grafanaUrl } catch { }
