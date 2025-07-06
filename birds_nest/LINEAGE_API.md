# Comprehensive Lineage API Documentation

## Overview

The Comprehensive Lineage API provides complete access to all AORTA (Advanced Object-Relational Tracking Architecture) lineage information for executed trails in PyBIRD AI. This API extracts the complete data lineage graph including tables, columns, rows, values, and all their relationships.

## Endpoints

### 1. Trail Summary Endpoint

**URL:** `/pybirdai/api/trail/<trail_id>/summary/`  
**Method:** `GET`  
**Description:** Returns lightweight summary statistics for a trail

#### Response Structure
```json
{
  "trail": {
    "id": 17,
    "name": "DataPoint_F_05_01_REF_FINREP_3_0_152589_REF_20250706_112701",
    "created_at": "2025-07-06T11:27:01.156527+00:00"
  },
  "summary": {
    "database_tables": 11,
    "derived_tables": 10,
    "total_rows": 57,
    "database_rows": 54,
    "derived_rows": 3,
    "column_values": 728,
    "evaluated_functions": 278,
    "has_lineage_data": true
  }
}
```

### 2. Complete Lineage Endpoint

**URL:** `/pybirdai/api/trail/<trail_id>/complete-lineage/`  
**Method:** `GET`  
**Description:** Returns complete lineage information for a trail in JSON format

#### Response Structure
```json
{
  "trail": {
    "id": 17,
    "name": "DataPoint_F_05_01_REF_FINREP_3_0_152589_REF_20250706_112701",
    "created_at": "2025-07-06T11:27:01.156527+00:00",
    "execution_context": {},
    "metadata_trail_id": 17
  },
  "metadata_trail": {
    "id": 17
  },
  "database_tables": [
    {
      "id": 168,
      "name": "BLNC_SHT_RCGNSD_FNNCL_ASST_INSTRMNT_IFRS",
      "fields": [
        {
          "id": 2619,
          "name": "test_id",
          "table_id": 168
        }
        // ... more fields
      ]
    }
    // ... more tables
  ],
  "derived_tables": [
    {
      "id": 178,
      "name": "F_05_01_REF_FINREP_3_0",
      "table_creation_function_id": 178,
      "functions": [
        {
          "id": 2794,
          "name": "calc_r0010_c0010",
          "table_id": 178,
          "function_text_id": 2794,
          "function_text": "def calc_r0010_c0010(...):\n    # function implementation",
          "function_language": "python"
        }
        // ... more functions
      ]
    }
    // ... more derived tables
  ],
  "populated_database_tables": [
    {
      "id": 384,
      "table_id": 168,
      "table_name": "BLNC_SHT_RCGNSD_FNNCL_ASST_INSTRMNT_IFRS",
      "trail_id": 17,
      "rows": [
        {
          "id": 5371,
          "row_identifier": "1",
          "populated_table_id": 384,
          "values": [
            {
              "id": 54199,
              "value": 1.0,
              "string_value": null,
              "column_id": 2619,
              "column_name": "test_id",
              "row_id": 5371
            }
            // ... more values
          ]
        }
        // ... more rows
      ]
    }
    // ... more populated tables
  ],
  "evaluated_derived_tables": [
    {
      "id": 394,
      "table_id": 178,
      "table_name": "F_05_01_REF_FINREP_3_0",
      "trail_id": 17,
      "rows": [
        {
          "id": 5425,
          "row_identifier": "1",
          "populated_table_id": 394,
          "evaluated_functions": [
            {
              "id": 55204,
              "value": 123456.0,
              "string_value": null,
              "function_id": 2794,
              "function_name": "calc_r0010_c0010",
              "row_id": 5425
            }
            // ... more evaluated functions
          ]
        }
        // ... more rows
      ]
    }
    // ... more evaluated tables
  ],
  "lineage_relationships": {
    "function_column_references": [
      {
        "id": 456,
        "function_id": 2794,
        "function_name": "calc_r0010_c0010",
        "referenced_object_type": "databasefield",
        "referenced_object_id": 2619
      }
      // ... more column references
    ],
    "derived_row_source_references": [
      {
        "id": 789,
        "derived_row_id": 5425,
        "source_object_type": "databaserow",
        "source_object_id": 5371
      }
      // ... more row references
    ],
    "evaluated_function_source_values": [
      {
        "id": 1011,
        "evaluated_function_id": 55204,
        "source_object_type": "databasecolumnvalue",
        "source_object_id": 54199
      }
      // ... more value references
    ],
    "table_creation_source_tables": [
      {
        "id": 1213,
        "table_creation_function_id": 178,
        "table_creation_function_name": "create_F_05_01_REF_FINREP_3_0",
        "source_object_type": "databasetable",
        "source_object_id": 168
      }
      // ... more table references
    ],
    "table_creation_function_columns": [
      {
        "id": 1415,
        "table_creation_function_id": 178,
        "table_creation_function_name": "create_F_05_01_REF_FINREP_3_0",
        "referenced_object_type": "databasefield",
        "referenced_object_id": 2619,
        "reference_text": "BLNC_SHT_RCGNSD_FNNCL_ASST_INSTRMNT_IFRS.test_id"
      }
      // ... more column references
    ]
  },
  "metadata": {
    "table_references": [
      {
        "id": 1617,
        "table_content_type": "DatabaseTable",
        "table_id": 168
      }
      // ... more metadata references
    ],
    "generation_timestamp": "2025-07-06T11:55:42.123456+00:00",
    "total_counts": {
      "database_tables": 11,
      "derived_tables": 10,
      "populated_database_tables": 11,
      "evaluated_derived_tables": 10,
      "total_database_rows": 54,
      "total_derived_rows": 3,
      "total_column_values": 728,
      "total_evaluated_functions": 278,
      "function_column_references": 0,
      "derived_row_source_references": 0,
      "evaluated_function_source_values": 236,
      "table_creation_source_tables": 0,
      "table_creation_function_columns": 0
    }
  }
}
```

## Data Model Explanation

### Core Trail Structure
- **`trail`**: Top-level execution container with metadata
- **`metadata_trail`**: Schema/metadata definitions container

### Table Models
- **`database_tables`**: Source data tables with field definitions
- **`derived_tables`**: Computed tables with function definitions
- **`populated_database_tables`**: Database table instances with actual data
- **`evaluated_derived_tables`**: Derived table instances with computed results

### Data Structure
- **`rows`**: Individual data rows within populated tables
- **`values`**: Actual data values in database rows
- **`evaluated_functions`**: Computed values in derived rows

### Lineage Relationships
- **`function_column_references`**: Which columns each function depends on
- **`derived_row_source_references`**: Which source rows contributed to derived rows
- **`evaluated_function_source_values`**: Which source values contributed to computed values
- **`table_creation_source_tables`**: Which source tables were used to create derived tables
- **`table_creation_function_columns`**: Which columns are referenced in table creation

## Usage Examples

### Python
```python
import requests
import json

# Get trail summary
response = requests.get('http://localhost:8000/pybirdai/api/trail/17/summary/')
summary = response.json()
print(f"Trail has {summary['summary']['total_rows']} rows")

# Get complete lineage
response = requests.get('http://localhost:8000/pybirdai/api/trail/17/complete-lineage/')
lineage = response.json()

# Access database tables
for table in lineage['database_tables']:
    print(f"Table: {table['name']} with {len(table['fields'])} fields")

# Access lineage relationships
for ref in lineage['lineage_relationships']['evaluated_function_source_values']:
    print(f"Function {ref['evaluated_function_id']} uses source {ref['source_object_id']}")
```

### JavaScript
```javascript
// Get trail summary
fetch('/pybirdai/api/trail/17/summary/')
  .then(response => response.json())
  .then(data => {
    console.log(`Trail: ${data.trail.name}`);
    console.log(`Total rows: ${data.summary.total_rows}`);
  });

// Get complete lineage
fetch('/pybirdai/api/trail/17/complete-lineage/')
  .then(response => response.json())
  .then(data => {
    console.log('Database tables:', data.database_tables.length);
    console.log('Derived tables:', data.derived_tables.length);
    console.log('Value lineage refs:', data.lineage_relationships.evaluated_function_source_values.length);
  });
```

### curl
```bash
# Get summary
curl "http://localhost:8000/pybirdai/api/trail/17/summary/" | jq '.summary'

# Get complete lineage (save to file)
curl "http://localhost:8000/pybirdai/api/trail/17/complete-lineage/" > trail_lineage.json

# Extract specific information
curl "http://localhost:8000/pybirdai/api/trail/17/complete-lineage/" | jq '.metadata.total_counts'
```

## Error Handling

Both endpoints return appropriate HTTP status codes:

- **200**: Success with JSON data
- **404**: Trail not found
- **500**: Server error with error details

Error response format:
```json
{
  "error": "Error description",
  "trail_id": 17,
  "trail_name": "Trail name if available",
  "error_type": "specific_error_type"
}
```

## Performance Notes

- **Summary endpoint**: Lightweight, fast response
- **Complete lineage endpoint**: Comprehensive but may be large for complex trails
- **Caching**: Consider caching responses for frequently accessed trails
- **Pagination**: For very large trails, consider implementing pagination

## Use Cases

### 1. **Data Lineage Visualization**
Use the complete lineage data to build interactive lineage graphs showing how data flows through the system.

### 2. **Impact Analysis**
Determine which downstream calculations are affected when source data changes.

### 3. **Audit and Compliance**
Provide complete audit trails showing how regulatory reports are calculated.

### 4. **Debugging and Quality Assurance**
Trace the source of specific values in reports back to their original data sources.

### 5. **Performance Analysis**
Analyze which functions and data sources are most frequently used.

### 6. **Data Governance**
Track data usage patterns and dependencies across the organization.

## UI Integration

### Trail Lineage Viewer Integration
The JSON API endpoints are integrated into the Trail Lineage Viewer interface with convenient export options:

**Location**: Right panel "JSON API Exports" section

**Features**:
- **View**: Opens JSON in new browser tab for inspection
- **Download**: Downloads JSON file to local system
- **Copy**: Copies JSON to clipboard for pasting elsewhere

### Trail List Integration
The trail list page includes dropdown menus for quick JSON access:

**Location**: Trail list table, "JSON" dropdown button

**Options**:
- View Summary JSON (opens in new tab)
- View Complete Lineage JSON (opens in new tab)
- Download Summary (direct file download)
- Download Complete (direct file download)

### Navigation Paths
1. **Main Navigation**: Home → Trails → Select Trail → JSON Export buttons
2. **Direct Access**: Trails list → JSON dropdown → Select export type
3. **URL Access**: Direct API endpoint access via browser or tools

## Implementation Details

The API is implemented in `/pybirdai/lineage_api.py` with the following functions:
- `get_trail_complete_lineage(request, trail_id)`
- `get_trail_lineage_summary(request, trail_id)`

URL patterns are defined in `/pybirdai/urls.py`:
- `path('api/trail/<int:trail_id>/complete-lineage/', ...)`
- `path('api/trail/<int:trail_id>/summary/', ...)`

UI integration files:
- `/pybirdai/templates/pybirdai/lineage_viewer.html` - Export buttons in viewer
- `/pybirdai/templates/pybirdai/trail_list.html` - Dropdown menus in list

The API uses Django's ORM with optimized queries including `select_related` and `prefetch_related` for efficient data retrieval.

JavaScript functions provide:
- Asynchronous JSON fetching with loading indicators
- Browser download functionality via Blob API
- Clipboard integration with fallback for older browsers
- Error handling with user feedback