#!/bin/bash

# Check licenses for Python dependencies using Eclipse Dash
# Usage: ./check_licenses.sh

echo "=== Python Dependency License Check ==="
echo "Analyzing dependencies from requirements.txt..."
echo

# Count total dependencies
TOTAL_DEPS=$(grep -c "^[^#]" ../requirements.txt 2>/dev/null || echo 0)
echo "Total dependencies found: $TOTAL_DEPS"
echo

# Parse requirements.txt and format for Eclipse Dash
python3 prep_dependencies.py
java -jar org.eclipse.dash.licenses-1.1.1-20250909.055027-577.jar new_req.txt -summary DEPENDENCIES - "$@" 2>/dev/null
java -jar org.eclipse.dash.licenses-1.1.1-20250909.055027-577.jar new_req.txt -review -repo https://github.com/eclipse-efbt/efbt -token cYz2vt_f9AHEyM4C1r5Q -project technology.efbt


echo
echo "=== License Analysis Complete ==="
echo "Results saved to: DEPENDENCIES"

# Show summary of license types
if [ -f "DEPENDENCIES" ]; then
    echo
    echo "License Summary:"
    echo "================"

    # Count approved vs restricted
    APPROVED=$(grep -c ", approved," DEPENDENCIES 2>/dev/null || echo 0)
    RESTRICTED=$(grep -c ", restricted," DEPENDENCIES 2>/dev/null || echo 0)

    echo "Approved licenses: $APPROVED"
    echo "Restricted licenses: $RESTRICTED"

    if [ $RESTRICTED -gt 0 ]; then
        echo
        echo "Packages requiring review:"
        grep ", restricted," DEPENDENCIES | cut -d',' -f1 | sed 's/^/  - /'
    fi
fi
