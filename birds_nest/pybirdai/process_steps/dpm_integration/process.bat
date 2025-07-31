@echo off
setlocal enabledelayedexpansion

REM Check if database file is provided
if "%~1"=="" (
    echo Usage: %0 ^<database.accdb^>
    echo.
    echo This script exports all tables from an Access .accdb database to CSV files
    echo Requires Microsoft Access or Access Runtime to be installed
    exit /b 1
)

set "DATABASE=%~1"

REM Check if database file exists
if not exist "%DATABASE%" (
    echo Error: Database file '%DATABASE%' not found
    exit /b 1
)

REM Get full path to database
for %%F in ("%DATABASE%") do set "FULL_PATH=%%~fF"

REM Create target folder if it doesn't exist
if not exist "target" mkdir target

echo Exporting Access database: %DATABASE%
echo.

REM Detect if we should use 32-bit or 64-bit PowerShell
REM Access.Application COM object requires matching architecture
set "PS_PATH=powershell"

REM Check if 64-bit PowerShell exists and use it
if exist "%WINDIR%\sysnative\WindowsPowerShell\v1.0\powershell.exe" (
    REM Running from 32-bit context, use 64-bit PowerShell
    set "PS_PATH=%WINDIR%\sysnative\WindowsPowerShell\v1.0\powershell.exe"
    echo Using 64-bit PowerShell from sysnative
) else if exist "%WINDIR%\System32\WindowsPowerShell\v1.0\powershell.exe" (
    REM Use System32 PowerShell (will be 64-bit on 64-bit OS)
    set "PS_PATH=%WINDIR%\System32\WindowsPowerShell\v1.0\powershell.exe"
    echo Using PowerShell from System32
)

REM Call PowerShell script to do the actual export
%PS_PATH% -ExecutionPolicy Bypass -Command ^
"$ErrorActionPreference = 'Stop'; ^
try { ^
    $access = New-Object -ComObject Access.Application; ^
    $access.Visible = $false; ^
    Write-Host 'Opening database...'; ^
    $db = $access.OpenCurrentDatabase('%FULL_PATH%'); ^
    $tables = $access.CurrentDb().TableDefs; ^
    $tableCount = 0; ^
    $exportedCount = 0; ^
    foreach ($table in $tables) { ^
        if ($table.Name -notlike 'MSys*' -and $table.Name -notlike '~*') { ^
            $tableCount++; ^
            $tableName = $table.Name; ^
            $csvPath = Join-Path (Get-Location) \"target\$tableName.csv\"; ^
            Write-Host \"Exporting table: $tableName\"; ^
            try { ^
                $access.DoCmd.TransferText(2, $null, $tableName, $csvPath, $true); ^
                Write-Host \"Successfully exported $tableName to target\$tableName.csv\"; ^
                $exportedCount++; ^
            } catch { ^
                Write-Host \"Error exporting table $($tableName): $($_.Exception.Message)\" -ForegroundColor Red; ^
            } ^
        } ^
    }; ^
    Write-Host \"\"; ^
    Write-Host \"Export Summary:\"; ^
    Write-Host \"- Total tables found: $tableCount\"; ^
    Write-Host \"- Successfully exported: $exportedCount\"; ^
    $access.CloseCurrentDatabase(); ^
    $access.Quit(); ^
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($access); ^
    [System.GC]::Collect(); ^
    [System.GC]::WaitForPendingFinalizers(); ^
    Write-Host \"Export complete\"; ^
} catch { ^
    Write-Host \"Error: $($_.Exception.Message)\" -ForegroundColor Red; ^
    if ($access) { ^
        try { ^
            $access.Quit(); ^
            [System.Runtime.Interopservices.Marshal]::ReleaseComObject($access); ^
        } catch {} ^
    }; ^
    exit 1; ^
}"

if errorlevel 1 (
    echo.
    echo Export failed. Make sure:
    echo - Microsoft Access or Access Runtime is installed
    echo - The database file is not corrupted
    echo - The database is not password protected
    echo - No other application is using the database
    exit /b 1
)

exit /b 0
