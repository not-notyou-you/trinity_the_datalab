# PROMPT 4 VERIFICATION - PowerShell Wrapper
# Run: .\test_prompt4.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "PROMPT 4 VERIFICATION - STARTING" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Ensure venv is activated
if ($env:VIRTUAL_ENV) {
    Write-Host "[OK] Virtual environment active: $env:VIRTUAL_ENV" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Virtual environment NOT active" -ForegroundColor Red
    Write-Host "Run: .\venv\Scripts\Activate.ps1" -ForegroundColor Yellow
    exit 1
}

# Check Python version
$pythonVersion = python --version 2>&1
Write-Host "[OK] Python: $pythonVersion" -ForegroundColor Green

# Run Python verification script
Write-Host "`nRunning verification tests..." -ForegroundColor Cyan
Write-Host ""

python tests/verify_prompt4.py

$exitCode = $LASTEXITCODE

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan

if ($exitCode -eq 0) {
    Write-Host "RESULT: SUCCESS" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Review output above" -ForegroundColor Gray
    Write-Host "  2. Run Prompt 5: Frontend wizard update" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host "RESULT: FAILED" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Check errors above and fix before continuing" -ForegroundColor Yellow
    Write-Host ""
}

exit $exitCode