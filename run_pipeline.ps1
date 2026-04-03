$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$envFile = Join-Path $projectRoot ".env"
$envExampleFile = Join-Path $projectRoot ".env.example"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Assert-FileExists {
    param(
        [string]$Path,
        [string]$Message
    )

    if (-not (Test-Path $Path)) {
        throw $Message
    }
}

function Invoke-Step {
    param(
        [string]$Title,
        [string[]]$Command
    )

    Write-Step $Title
    & $Command[0] $Command[1..($Command.Length - 1)]
    if ($LASTEXITCODE -ne 0) {
        throw "Etape en echec: $Title"
    }
}

Assert-FileExists $pythonExe "Environnement virtuel introuvable: .venv\Scripts\python.exe"

if (-not (Test-Path $envFile)) {
    Assert-FileExists $envExampleFile "Fichier .env.example introuvable."
    Copy-Item $envExampleFile $envFile
    Write-Host ".env cree automatiquement depuis .env.example" -ForegroundColor Yellow
}

$dockerAvailable = $null -ne (Get-Command docker -ErrorAction SilentlyContinue)
if ($dockerAvailable) {
    Write-Step "Build des services Docker"
    docker compose build
    if ($LASTEXITCODE -ne 0) {
        throw "Impossible de builder les services Docker."
    }

    Write-Step "Demarrage de PostgreSQL avec Docker"
    docker compose up -d
    if ($LASTEXITCODE -ne 0) {
        throw "Impossible de demarrer PostgreSQL avec Docker."
    }
} else {
    Write-Host "Docker non detecte, je suppose que PostgreSQL est deja lance." -ForegroundColor Yellow
}

$env:PYSPARK_SUBMIT_ARGS = '--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED" --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED" pyspark-shell'

Invoke-Step "Recuperation des donnees API" @($pythonExe, "src/get_api.py")
Invoke-Step "Scraping de la meteo" @($pythonExe, "src/scrape_weather.py")
Invoke-Step "EDA" @($pythonExe, "notebook.py")
Invoke-Step "Traitement Spark" @($pythonExe, "src/traitement.py")
Invoke-Step "Chargement PostgreSQL" @($pythonExe, "src/load_postgres.py")

$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = "false"
Invoke-Step "Lancement du dashboard Streamlit" @(
    $pythonExe,
    "-m",
    "streamlit",
    "run",
    "dashboard/app.py",
    "--browser.gatherUsageStats",
    "false"
)
