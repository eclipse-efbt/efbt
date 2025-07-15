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

REM Call PowerShell script to do the actual export
powershell -ExecutionPolicy Bypass -Command "& {
    try {
        # Create Access Application object
        $access = New-Object -ComObject Access.Application
        $access.Visible = $false

        # Open database
        Write-Host 'Opening database...'
        $db = $access.OpenCurrentDatabase('%FULL_PATH%')

        # Get all tables
        $tables = $access.CurrentDb().TableDefs
        $tableCount = 0
        $exportedCount = 0

        foreach ($table in $tables) {
            # Skip system tables (names starting with MSys)
            if ($table.Name -notlike 'MSys*' -and $table.Name -notlike '~*') {
                $tableCount++
                $tableName = $table.Name
                $csvPath = Join-Path (Get-Location) \"target\$tableName.csv\"

                Write-Host \"Exporting table: $tableName\"

                try {
                    # Export table to CSV
                    $access.DoCmd.TransferText(
                        2,          # acExportDelim
                        $null,      # SpecificationName
                        $tableName, # TableName
                        $csvPath,   # FileName
                        $true       # HasFieldNames
                    )
                    Write-Host \"Successfully exported $tableName to target\$tableName.csv\"
                    $exportedCount++
                } catch {
                    Write-Host \"Error exporting table $tableName : $($_.Exception.Message)\" -ForegroundColor Red
                }
            }
        }

        Write-Host \"\"
        Write-Host \"Export Summary:\"
        Write-Host \"- Total tables found: $tableCount\"
        Write-Host \"- Successfully exported: $exportedCount\"

        # Close database and quit Access
        $access.CloseCurrentDatabase()
        $access.Quit()

        # Release COM objects
        [System.Runtime.Interopservices.Marshal]::ReleaseComObject($access) | Out-Null
        [System.GC]::Collect()
        [System.GC]::WaitForPendingFinalizers()

        Write-Host \"Export complete\"
        exit 0

    } catch {
        Write-Host \"Error: $($_.Exception.Message)\" -ForegroundColor Red

        # Try to clean up if Access object exists
        if ($access) {
            try {
                $access.Quit()
                [System.Runtime.Interopservices.Marshal]::ReleaseComObject($access) | Out-Null
            } catch {}
        }

        exit 1
    }
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
