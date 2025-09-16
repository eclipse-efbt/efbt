$OUTPUT_FILE = "ruff_check_results.txt"

"Running ruff checks on PyBIRD AI codebase..." | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8
"Generated on: $(Get-Date)" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
"=========================================" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
"" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append

# Function to run ruff check and capture output
function Run-RuffCheck {
    param($path)
    "Checking: $path" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    "-------------------" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    try {
        uv run ruff check $path 2>&1 | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    } catch {
        $_.Exception.Message | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    }
    "" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
}

# Run all the ruff checks
Run-RuffCheck "pybirdai/utils/"
Run-RuffCheck "pybirdai/entry_points/"
Run-RuffCheck "pybirdai/process_steps/"

"=========================================" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
"End of ruff check results" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append

Write-Host "Ruff checks completed. Results saved to: $OUTPUT_FILE"
