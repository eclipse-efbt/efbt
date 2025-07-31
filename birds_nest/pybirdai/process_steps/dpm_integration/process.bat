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

REM Use PowerShell Core (pwsh.exe) which should be available in PATH
set "PS_PATH=pwsh.exe"
echo Using PowerShell Core (pwsh.exe) from PATH

REM Create temporary PowerShell script file
set "TEMP_PS_FILE=%TEMP%\access_export_%RANDOM%.ps1"

REM Write PowerShell script to temporary file
echo $ErrorActionPreference = "Stop" > "%TEMP_PS_FILE%"
echo try { >> "%TEMP_PS_FILE%"
echo     Write-Host "Using ADODB/DAO method for Access database export..." >> "%TEMP_PS_FILE%"
echo     $connectionString = "Provider=Microsoft.ACE.OLEDB.16.0;Data Source=%FULL_PATH%;" >> "%TEMP_PS_FILE%"
echo     $connection = New-Object -ComObject ADODB.Connection >> "%TEMP_PS_FILE%"
echo     $connection.Open($connectionString) >> "%TEMP_PS_FILE%"
echo     Write-Host "Connected to database successfully" >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo     $recordset = New-Object -ComObject ADODB.Recordset >> "%TEMP_PS_FILE%"
echo     $recordset.Open("SELECT Name FROM MSysObjects WHERE Type=1 AND Flags=0", $connection) >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo     $tables = @() >> "%TEMP_PS_FILE%"
echo     while (-not $recordset.EOF) { >> "%TEMP_PS_FILE%"
echo         $tableName = $recordset.Fields.Item("Name").Value >> "%TEMP_PS_FILE%"
echo         if ($tableName -notlike "MSys*" -and $tableName -notlike "~*") { >> "%TEMP_PS_FILE%"
echo             $tables += $tableName >> "%TEMP_PS_FILE%"
echo         } >> "%TEMP_PS_FILE%"
echo         $recordset.MoveNext() >> "%TEMP_PS_FILE%"
echo     } >> "%TEMP_PS_FILE%"
echo     $recordset.Close() >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo     Write-Host "Found $($tables.Count) tables to export" >> "%TEMP_PS_FILE%"
echo     $exportedCount = 0 >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo     foreach ($tableName in $tables) { >> "%TEMP_PS_FILE%"
echo         try { >> "%TEMP_PS_FILE%"
echo             Write-Host "Exporting table: $tableName" >> "%TEMP_PS_FILE%"
echo             $query = "SELECT * FROM [$tableName]" >> "%TEMP_PS_FILE%"
echo             $rs = New-Object -ComObject ADODB.Recordset >> "%TEMP_PS_FILE%"
echo             $rs.Open($query, $connection) >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo             $csvPath = Join-Path (Get-Location) "target\$tableName.csv" >> "%TEMP_PS_FILE%"
echo             $stream = New-Object -ComObject ADODB.Stream >> "%TEMP_PS_FILE%"
echo             $stream.Open() >> "%TEMP_PS_FILE%"
echo             $stream.Type = 2 >> "%TEMP_PS_FILE%"
echo             $stream.Charset = "utf-8" >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo             $headers = @() >> "%TEMP_PS_FILE%"
echo             for ($i = 0; $i -lt $rs.Fields.Count; $i++) { >> "%TEMP_PS_FILE%"
echo                 $headers += $rs.Fields.Item($i).Name >> "%TEMP_PS_FILE%"
echo             } >> "%TEMP_PS_FILE%"
echo             $stream.WriteText(($headers -join ",") + "`r`n") >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo             while (-not $rs.EOF) { >> "%TEMP_PS_FILE%"
echo                 $row = @() >> "%TEMP_PS_FILE%"
echo                 for ($i = 0; $i -lt $rs.Fields.Count; $i++) { >> "%TEMP_PS_FILE%"
echo                     $value = $rs.Fields.Item($i).Value >> "%TEMP_PS_FILE%"
echo                     if ($null -eq $value) { $value = "" } >> "%TEMP_PS_FILE%"
echo                     $value = $value.ToString().Replace('"', '""') >> "%TEMP_PS_FILE%"
echo                     if ($value.Contains(",") -or $value.Contains("`r") -or $value.Contains("`n") -or $value.Contains('"')) { $value = """$value""" } >> "%TEMP_PS_FILE%"
echo                     $row += $value >> "%TEMP_PS_FILE%"
echo                 } >> "%TEMP_PS_FILE%"
echo                 $stream.WriteText(($row -join ",") + "`r`n") >> "%TEMP_PS_FILE%"
echo                 $rs.MoveNext() >> "%TEMP_PS_FILE%"
echo             } >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo             $stream.SaveToFile($csvPath, 2) >> "%TEMP_PS_FILE%"
echo             $stream.Close() >> "%TEMP_PS_FILE%"
echo             $rs.Close() >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo             Write-Host "Successfully exported $tableName to target\$tableName.csv" >> "%TEMP_PS_FILE%"
echo             $exportedCount++ >> "%TEMP_PS_FILE%"
echo         } catch { >> "%TEMP_PS_FILE%"
echo             Write-Host "Error exporting table $($tableName): $($_.Exception.Message)" -ForegroundColor Red >> "%TEMP_PS_FILE%"
echo         } >> "%TEMP_PS_FILE%"
echo     } >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo     Write-Host "" >> "%TEMP_PS_FILE%"
echo     Write-Host "Export Summary:" >> "%TEMP_PS_FILE%"
echo     Write-Host "- Total tables found: $($tables.Count)" >> "%TEMP_PS_FILE%"
echo     Write-Host "- Successfully exported: $exportedCount" >> "%TEMP_PS_FILE%"
echo. >> "%TEMP_PS_FILE%"
echo     $connection.Close() >> "%TEMP_PS_FILE%"
echo     Write-Host "Export complete" >> "%TEMP_PS_FILE%"
echo } catch { >> "%TEMP_PS_FILE%"
echo     Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red >> "%TEMP_PS_FILE%"
echo     if ($connection) { >> "%TEMP_PS_FILE%"
echo         try { >> "%TEMP_PS_FILE%"
echo             $connection.Close() >> "%TEMP_PS_FILE%"
echo         } catch {} >> "%TEMP_PS_FILE%"
echo     } >> "%TEMP_PS_FILE%"
echo     exit 1 >> "%TEMP_PS_FILE%"
echo } >> "%TEMP_PS_FILE%"

REM Execute the PowerShell script
%PS_PATH% -ExecutionPolicy Bypass -File "%TEMP_PS_FILE%"

REM Clean up temporary file
del "%TEMP_PS_FILE%" 2>nul

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