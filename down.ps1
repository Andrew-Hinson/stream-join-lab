$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root
$LabEnv = Join-Path $Root ".lab.env"

if (Test-Path $LabEnv) {
  & docker compose --env-file $LabEnv down
} else {
  & docker compose down
}
