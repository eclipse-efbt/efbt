#!/bin/bash

# Check if database file is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <database.mdb>"
    exit 1
fi

DATABASE="$1"

# Check if database file exists
if [ ! -f "$DATABASE" ]; then
    echo "Error: Database file '$DATABASE' not found"
    exit 1
fi

# Check if mdb-tools is installed
if ! command -v mdb-tables &> /dev/null; then
    echo "Error: mdb-tools is not installed"
    exit 1
fi

# Create target folder if it doesn't exist
mkdir -p target

# Get list of tables
TABLES=$(mdb-tables -1 "$DATABASE")

if [ -z "$TABLES" ]; then
    echo "No tables found in database"
    exit 0
fi

# Export each table to CSV
for table in $TABLES; do
    echo "Exporting table: $table"
    mdb-export "$DATABASE" "$table" > "target/${table}.csv"
    if [ $? -eq 0 ]; then
        echo "Successfully exported $table to target/${table}.csv"
    else
        echo "Error exporting table: $table"
    fi
done

echo "Export complete"
