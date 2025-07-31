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

REM Define required tables for mapping functions
set "REQUIRED_TABLES=ReportingFramework,Domain,Member,Dimension,TemplateGroup,TemplateGroupTemplate,TaxonomyTableVersion,Taxonomy,Table,TableVersion,Axis,AxisOrdinate,TableCell,CellPosition,DataPointVersion,ContextDefinition,Hierarchy,HierarchyNode,OpenMemberRestriction,OrdinateCategorisation"

REM Use PowerShell to extract only required tables using ACE OLEDB provider
powershell -Command "$dbPath = '%DATABASE%'; $targetDir = 'target'; $requiredTables = '%REQUIRED_TABLES%'.Split(','); try { $connectionString = 'Provider=Microsoft.ACE.OLEDB.16.0;Data Source=' + $dbPath; $connection = New-Object System.Data.OleDb.OleDbConnection($connectionString); $connection.Open(); $allTables = $connection.GetSchema('Tables') | Where-Object {$_.TABLE_TYPE -eq 'TABLE'}; if ($allTables.Count -eq 0) { Write-Host 'No tables found in database'; exit 0 }; $exportedCount = 0; foreach ($table in $allTables) { $tableName = $table.TABLE_NAME; if ($requiredTables -contains $tableName) { Write-Host \"Exporting required table: $tableName\"; try { $query = \"SELECT * FROM [$tableName]\"; $command = New-Object System.Data.OleDb.OleDbCommand($query, $connection); $adapter = New-Object System.Data.OleDb.OleDbDataAdapter($command); $dataSet = New-Object System.Data.DataSet; $adapter.Fill($dataSet); $csvPath = \"$targetDir\\$tableName.csv\"; $dataSet.Tables[0] | Export-Csv -Path $csvPath -NoTypeInformation; Write-Host \"Successfully exported $tableName to $csvPath\"; $exportedCount++ } catch { Write-Host \"Error exporting table: $tableName - $($_.Exception.Message)\" } } else { Write-Host \"Skipping table: $tableName (not required for mapping functions)\" } }; $connection.Close(); Write-Host \"Export complete - exported $exportedCount required tables\" } catch { Write-Host \"Error: $($_.Exception.Message)\"; exit 1 }"
