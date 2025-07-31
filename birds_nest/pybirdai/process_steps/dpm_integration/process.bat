@echo off
setlocal enabledelayedexpansion

REM Check if database file is provided
if "%~1"=="" (
    echo Usage: %0 ^<database.accdb^>
    exit /b 1
)

set "DATABASE=%~1"

REM Check if database file exists
if not exist "%DATABASE%" (
    echo Error: Database file '%DATABASE%' not found
    exit /b 1
)

REM Create target folder if it doesn't exist
if not exist "target" mkdir target

REM Use PowerShell to extract tables using ACE OLEDB provider
powershell -Command "& {
    $dbPath = '%DATABASE%'
    $targetDir = 'target'

    try {
        # Connection string for Access database
        $connectionString = 'Provider=Microsoft.ACE.OLEDB.12.0;Data Source=' + $dbPath
        $connection = New-Object System.Data.OleDb.OleDbConnection($connectionString)
        $connection.Open()

        # Get table names
        $tables = $connection.GetSchema('Tables') | Where-Object {$_.TABLE_TYPE -eq 'TABLE'}

        if ($tables.Count -eq 0) {
            Write-Host 'No tables found in database'
            exit 0
        }

        foreach ($table in $tables) {
            $tableName = $table.TABLE_NAME
            Write-Host \"Exporting table: $tableName\"

            try {
                $query = \"SELECT * FROM [$tableName]\"
                $command = New-Object System.Data.OleDb.OleDbCommand($query, $connection)
                $adapter = New-Object System.Data.OleDb.OleDbDataAdapter($command)
                $dataSet = New-Object System.Data.DataSet
                $adapter.Fill($dataSet)

                $csvPath = \"$targetDir\\$tableName.csv\"
                $dataSet.Tables[0] | Export-Csv -Path $csvPath -NoTypeInformation
                Write-Host \"Successfully exported $tableName to $csvPath\"
            }
            catch {
                Write-Host \"Error exporting table: $tableName - $($_.Exception.Message)\"
            }
        }

        $connection.Close()
        Write-Host 'Export complete'
    }
    catch {
        Write-Host \"Error: $($_.Exception.Message)\"
        exit 1
    }
}"
