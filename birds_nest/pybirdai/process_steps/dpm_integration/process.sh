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

# Define required tables for mapping functions
REQUIRED_TABLES="ReportingFramework Domain Member Dimension TemplateGroup TemplateGroupTemplate TaxonomyTableVersion Taxonomy Table TableVersion Axis AxisOrdinate TableCell CellPosition DataPointVersion ContextDefinition Hierarchy HierarchyNode OpenMemberRestriction OrdinateCategorisation"

# Get list of all tables
ALL_TABLES=$(mdb-tables -1 "$DATABASE")

if [ -z "$ALL_TABLES" ]; then
    echo "No tables found in database"
    exit 0
fi

# Export only required tables to CSV
exported_count=0
for table in $ALL_TABLES; do
    # Check if current table is in the required tables list
    if echo "$REQUIRED_TABLES" | grep -q "\b$table\b"; then
        echo "Exporting required table: $table"
        mdb-export "$DATABASE" "$table" > "target/${table}.csv"
        if [ $? -eq 0 ]; then
            echo "Successfully exported $table to target/${table}.csv"
            exported_count=$((exported_count + 1))
        else
            echo "Error exporting table: $table"
        fi
    else
        echo "Skipping table: $table (not required for mapping functions)"
    fi
done

echo "Export complete - exported $exported_count required tables"
