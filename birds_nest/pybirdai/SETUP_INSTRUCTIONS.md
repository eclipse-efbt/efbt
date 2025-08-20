# Setup Instructions for Enhanced Lineage Tracking

## 1. Database Migration

After deploying the code changes, run the following Django commands to create the database tables for the new models:

```bash
cd birds_nest
python manage.py makemigrations pybirdai
python manage.py migrate
```

This will create the following new tables:
- `CalculationUsedRow` - tracks which rows were used in calculations
- `CalculationUsedField` - tracks which fields were accessed in calculations

## 2. New API Endpoints

The following new endpoints are now available:

### Get Filtered Lineage
```
GET /pybirdai/api/trail/<trail_id>/filtered-lineage/
```

Query parameters:
- `calculation_name` (optional): Filter to a specific calculation cell
- `include_unused` (optional): Set to 'true' to include all data (default is filtered)

Examples:
```
# Get only data used in a specific calculation
GET /pybirdai/api/trail/123/filtered-lineage/?calculation_name=Cell_F_01_01_REF_FINREP_3_0_45749_REF

# Get filtered data for all calculations
GET /pybirdai/api/trail/123/filtered-lineage/

# Get all data (unfiltered)
GET /pybirdai/api/trail/123/filtered-lineage/?include_unused=true
```

### Get Calculation Summary
```
GET /pybirdai/api/trail/<trail_id>/calculation-summary/
```

Returns a summary of all calculations with counts of used rows and fields.

Example:
```
GET /pybirdai/api/trail/123/calculation-summary/
```

## 3. Testing the Enhanced Lineage Tracking

### Step 1: Enable Lineage Tracking
Make sure lineage tracking is enabled in your context configuration.

### Step 2: Execute a Data Point
Run a data point calculation that uses report cells (like those in `report_cells.py`).

### Step 3: Check the New Endpoints
After execution, you should be able to access the new endpoints and see:
- Only rows that passed the filters in each calculation
- Only fields that were accessed during the calculations
- Lineage relationships between the filtered data

## 4. Implementing Enhanced Cells

### Option A: Modify Existing Cells
Follow the pattern in `/pybirdai/process_steps/filter_code/example_enhanced_cell.py` to add tracking to existing report cells.

### Option B: Use Enhancement Utilities
Use the utilities in `/pybirdai/process_steps/filter_code/cell_enhancement_utils.py` to automatically convert existing cells.

### Option C: Use the Enhanced Base Class
For new cells, inherit from `EnhancedCellBase` in `/pybirdai/process_steps/filter_code/enhanced_report_cells.py`.

## 5. Verification

To verify the system is working:

1. Execute a datapoint
2. Check that you get a trail ID
3. Access the calculation summary: `GET /pybirdai/api/trail/<trail_id>/calculation-summary/`
4. You should see a list of calculations with row and field counts
5. Access the filtered lineage: `GET /pybirdai/api/trail/<trail_id>/filtered-lineage/`
6. The response should contain only the data that was actually used

## 6. Benefits You'll See

- **Smaller JSON responses**: Only relevant data is included
- **Better debugging**: Clear visibility into which data contributed to results
- **Audit trails**: Exact tracking of data usage for compliance
- **Performance**: Reduced data transfer and processing

## 7. Troubleshooting

If tracking isn't working:
1. Ensure lineage tracking is enabled in orchestration
2. Check that report cells are setting the calculation context
3. Verify that the `@lineage` decorator is being used
4. Check Django logs for any errors in the tracking methods