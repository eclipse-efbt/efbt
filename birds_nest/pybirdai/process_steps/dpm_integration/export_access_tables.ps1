param(
    [Parameter(Mandatory=$true)]
    [string]$DatabasePath
)

try {
    # Create Access Application object
    $access = New-Object -ComObject Access.Application
    $access.Visible = $false

    # Open database
    Write-Host 'Opening database...'
    $db = $access.OpenCurrentDatabase($DatabasePath)

    # Get all tables
    $tables = $access.CurrentDb().TableDefs
    $tableCount = 0
    $exportedCount = 0

    foreach ($table in $tables) {
        # Skip system tables (names starting with MSys)
        if ($table.Name -notlike 'MSys*' -and $table.Name -notlike '~*') {
            $tableCount++
            $tableName = $table.Name
            $csvPath = Join-Path (Get-Location) "target\$tableName.csv"

            Write-Host "Exporting table: $tableName"

            try {
                # Export table to CSV
                $access.DoCmd.TransferText(
                    2,          # acExportDelim
                    $null,      # SpecificationName
                    $tableName, # TableName
                    $csvPath,   # FileName
                    $true       # HasFieldNames
                )
                Write-Host "Successfully exported $tableName to target\$tableName.csv"
                $exportedCount++
            } catch {
                Write-Host "Error exporting table $tableName : $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    }

    Write-Host ""
    Write-Host "Export Summary:"
    Write-Host "- Total tables found: $tableCount"
    Write-Host "- Successfully exported: $exportedCount"

    # Close database and quit Access
    $access.CloseCurrentDatabase()
    $access.Quit()

    # Release COM objects
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($access) | Out-Null
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()

    Write-Host "Export complete"
    exit 0

} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red

    # Try to clean up if Access object exists
    if ($access) {
        try {
            $access.Quit()
            [System.Runtime.Interopservices.Marshal]::ReleaseComObject($access) | Out-Null
        } catch {}
    }

    exit 1
}