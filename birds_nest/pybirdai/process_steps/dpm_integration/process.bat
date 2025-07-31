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

REM Detect PowerShell availability - try PowerShell Core first, then Windows PowerShell

REM First try PowerShell Core (pwsh) which is commonly available in CI environments
where /q pwsh 2>nul
if %errorlevel% equ 0 (
    set "PS_PATH=pwsh"
    echo Using PowerShell Core (pwsh)
    goto :PowerShellFound
)

REM Check for Windows PowerShell in different locations
if exist "%WINDIR%\System32\WindowsPowerShell\v1.0\powershell.exe" (
    set "PS_PATH=%WINDIR%\System32\WindowsPowerShell\v1.0\powershell.exe"
    echo Using Windows PowerShell from System32
    goto :PowerShellFound
)

if exist "%WINDIR%\sysnative\WindowsPowerShell\v1.0\powershell.exe" (
    REM Only exists when running from 32-bit process on 64-bit OS
    set "PS_PATH=%WINDIR%\sysnative\WindowsPowerShell\v1.0\powershell.exe"
    echo Using 64-bit PowerShell from sysnative
    goto :PowerShellFound
)

REM Try PowerShell from PATH as last resort
where /q powershell 2>nul
if %errorlevel% equ 0 (
    set "PS_PATH=powershell"
    echo Using PowerShell from PATH
    goto :PowerShellFound
)

REM No PowerShell found
echo Error: Neither PowerShell Core (pwsh) nor Windows PowerShell could be found
echo Please ensure PowerShell is installed and available in PATH
exit /b 1

:PowerShellFound

REM Call PowerShell script to do the actual export
%PS_PATH% -ExecutionPolicy Bypass -Command ^
"$ErrorActionPreference = 'Stop'; ^
try { ^
    Write-Host 'Using ADODB/DAO method for Access database export...'; ^
    $connectionString = 'Provider=Microsoft.ACE.OLEDB.16.0;Data Source=%FULL_PATH%;'; ^
    $connection = New-Object -ComObject ADODB.Connection; ^
    $connection.Open($connectionString); ^
    Write-Host 'Connected to database successfully'; ^
    ^
    $recordset = New-Object -ComObject ADODB.Recordset; ^
    $recordset.Open('SELECT Name FROM MSysObjects WHERE Type=1 AND Flags=0', $connection); ^
    ^
    $tables = @(); ^
    while (-not $recordset.EOF) { ^
        $tableName = $recordset.Fields.Item('Name').Value; ^
        if ($tableName -notlike 'MSys*' -and $tableName -notlike '~*') { ^
            $tables += $tableName; ^
        } ^
        $recordset.MoveNext(); ^
    } ^
    $recordset.Close(); ^
    ^
    Write-Host \"Found $($tables.Count) tables to export\"; ^
    $exportedCount = 0; ^
    ^
    foreach ($tableName in $tables) { ^
        try { ^
            Write-Host \"Exporting table: $tableName\"; ^
            $query = \"SELECT * FROM [$tableName]\"; ^
            $rs = New-Object -ComObject ADODB.Recordset; ^
            $rs.Open($query, $connection); ^
            ^
            $csvPath = Join-Path (Get-Location) \"target\$tableName.csv\"; ^
            $stream = New-Object -ComObject ADODB.Stream; ^
            $stream.Open(); ^
            $stream.Type = 2; ^
            $stream.Charset = 'utf-8'; ^
            ^
            $headers = @(); ^
            for ($i = 0; $i -lt $rs.Fields.Count; $i++) { ^
                $headers += $rs.Fields.Item($i).Name; ^
            } ^
            $stream.WriteText(($headers -join ',') + \"`r`n\"); ^
            ^
            while (-not $rs.EOF) { ^
                $row = @(); ^
                for ($i = 0; $i -lt $rs.Fields.Count; $i++) { ^
                    $value = $rs.Fields.Item($i).Value; ^
                    if ($null -eq $value) { $value = ''; } ^
                    $value = $value.ToString().Replace('\"', '\"\"'); ^
                    if ($value.Contains(',') -or $value.Contains(\"`r\") -or $value.Contains(\"`n\") -or $value.Contains('\"')) { $value = \"\"\"$value\"\"\"; } ^
                    $row += $value; ^
                } ^
                $stream.WriteText(($row -join ',') + \"`r`n\"); ^
                $rs.MoveNext(); ^
            } ^
            ^
            $stream.SaveToFile($csvPath, 2); ^
            $stream.Close(); ^
            $rs.Close(); ^
            ^
            Write-Host \"Successfully exported $tableName to target\$tableName.csv\"; ^
            $exportedCount++; ^
        } catch { ^
            Write-Host \"Error exporting table $($tableName): $($_.Exception.Message)\" -ForegroundColor Red; ^
        } ^
    } ^
    ^
    Write-Host \"\"; ^
    Write-Host \"Export Summary:\"; ^
    Write-Host \"- Total tables found: $($tables.Count)\"; ^
    Write-Host \"- Successfully exported: $exportedCount\"; ^
    ^
    $connection.Close(); ^
    Write-Host \"Export complete\"; ^
} catch { ^
    Write-Host \"Error: $($_.Exception.Message)\" -ForegroundColor Red; ^
    if ($connection) { ^
        try { ^
            $connection.Close(); ^
        } catch {} ^
    } ^
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
