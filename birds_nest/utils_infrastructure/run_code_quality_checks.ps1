$OUTPUT_FILE = "code_quality_results.txt"

"Running code quality checks on PyBIRD AI codebase..." | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8
"=========================================" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
"" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append

# Function to run black formatting check
function Run-BlackCheck {
    param($path)

    "Checking formatting with Black: $path" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    "-----------------------------------" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    try {
        uv run black --check --diff $path 2>&1 | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    }
    catch {
        "Error running black on $path" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    }
    "" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
}

# Function to run pylint check
function Run-PylintCheck {
    param($path)

    "Running pylint on: $path" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    "-----------------------------------" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    try {
        uv run pylint $path 2>&1 | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    }
    catch {
        "Error running pylint on $path" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
    }
    "" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
}

# Run black formatting checks
"=== BLACK FORMATTING CHECKS ===" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
Run-BlackCheck "pybirdai/utils/"
Run-BlackCheck "pybirdai/entry_points/"
Run-BlackCheck "pybirdai/process_steps/"

"" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
"=== PYLINT CHECKS ===" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append

# Run pylint checks
Run-PylintCheck "pybirdai/utils/"
Run-PylintCheck "pybirdai/entry_points/"
Run-PylintCheck "pybirdai/process_steps/"

"End of code quality check results" | Out-File -FilePath $OUTPUT_FILE -Encoding UTF8 -Append
Write-Host "Results saved to: $OUTPUT_FILE"