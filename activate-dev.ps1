# PowerShell script to activate virtual environment and load .env variables
# Generated for music-tracker

# Check if .env file exists and load environment variables
$envFile = ".env"
if (Test-Path $envFile) {
Write-Host "Loading environment variables from .env file..." -ForegroundColor Green

Get-Content $envFile | ForEach-Object {
$line = $_.Trim()

# Skip empty lines and comments
if ($line -and !$line.StartsWith("#")) {
# Split on first '=' to handle values that might contain '='
$keyValue = $line -split '=', 2
if ($keyValue.Length -eq 2) {
$key = $keyValue[0].Trim()
$value = $keyValue[1].Trim()

# Remove quotes if present
if ($value.StartsWith('"') -and $value.EndsWith('"')) {
$value = $value.Substring(1, $value.Length - 2)
}
if ($value.StartsWith("'") -and $value.EndsWith("'")) {
$value = $value.Substring(1, $value.Length - 2)
}

# Set the environment variable
[Environment]::SetEnvironmentVariable($key, $value, "Process")

# Mask values for keys containing 'ENV_SECRET'
$displayValue = if ($key -like "*ENV_SECRET*") { "***" } else { $value }
Write-Host " $key = $displayValue" -ForegroundColor Gray
}
}
}
Write-Host "Environment variables loaded." -ForegroundColor Green
} else {
Write-Host "No .env file found. Create one from .env.example if needed." -ForegroundColor Yellow
}

# Check for and deactivate any existing virtual environment
if ($env:VIRTUAL_ENV) {
Write-Host "Deactivating existing virtual environment: $($env:VIRTUAL_ENV)" -ForegroundColor Yellow

# Check if deactivate function exists (from a previous activation)
if (Test-Path function:deactivate) {
deactivate
Write-Host "Previous virtual environment deactivated." -ForegroundColor Green
} else {
# Fallback: try to find and run deactivate script from the current venv
$currentVenvDeactivate = Join-Path $env:VIRTUAL_ENV "Scripts\deactivate.bat"
if (Test-Path $currentVenvDeactivate) {
& $currentVenvDeactivate
Write-Host "Previous virtual environment deactivated." -ForegroundColor Green
} else {
Write-Host "Could not find deactivate script for current environment." -ForegroundColor Yellow
}
}
}

# Activate the virtual environment
$venvActivate = ".venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
Write-Host "Activating virtual environment..." -ForegroundColor Green
& $venvActivate

# Show environment info
Write-Host ""
Write-Host "=== music-tracker DEVELOPMENT ENVIRONMENT ===" -ForegroundColor Cyan
Write-Host "Project: music-tracker" -ForegroundColor White
Write-Host "Client: Music Tracker" -ForegroundColor White
Write-Host "Python: $(python --version)" -ForegroundColor White
Write-Host "Virtual Environment: Activated" -ForegroundColor Green
Write-Host ""
Write-Host "Quick Commands:" -ForegroundColor Yellow
Write-Host " dbt deps - Install dbt packages" -ForegroundColor Gray
Write-Host " dbt debug - Test dbt connection" -ForegroundColor Gray
Write-Host " dbt run - Run dbt models" -ForegroundColor Gray
Write-Host " dbt test - Run dbt tests" -ForegroundColor Gray
Write-Host ""
Write-Host "SQL Formatting:" -ForegroundColor Yellow
Write-Host " sqlfluff format dbt/models/ - Format SQL (T-SQL dialect)" -ForegroundColor Gray
Write-Host ""
} else {
Write-Host "Virtual environment not found at .venv\Scripts\Activate.ps1" -ForegroundColor Red
Write-Host "Run 'uv venv' to create the virtual environment first." -ForegroundColor Yellow
}

# Change to project directory if not already there
$currentDir = Get-Location
$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ($currentDir -ne $projectDir) {
Set-Location $projectDir
}
