$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

Write-Step "Initialisation de la base Airflow et de l'utilisateur admin"
docker compose up airflow-init
if ($LASTEXITCODE -ne 0) {
    throw "Impossible d'initialiser Airflow."
}

Write-Step "Demarrage de PostgreSQL, Airflow scheduler et webserver"
docker compose up -d postgres airflow-postgres airflow-webserver airflow-scheduler
if ($LASTEXITCODE -ne 0) {
    throw "Impossible de demarrer les services Airflow."
}

Write-Host ""
Write-Host "Airflow est disponible sur http://localhost:8080" -ForegroundColor Green
Write-Host "Identifiants par defaut: admin / admin" -ForegroundColor Yellow
