# Debugging Lineage Tracking Issues

## Problem
The filtered lineage is only showing 1 table and 1 row when it should show 3-4 tables and multiple derived tables.

## Investigation Steps

### Step 1: Check Unfiltered Lineage
First, let's see what the complete (unfiltered) lineage looks like:

```
GET /pybirdai/api/trail/14/complete-lineage/
```

This will show us what data *should* be available.

### Step 2: Check Calculation Summary
```
GET /pybirdai/api/trail/14/calculation-summary/
```

This will show us what calculations were tracked.

### Step 3: Compare Filtered vs Unfiltered
```
# Unfiltered - should show everything
GET /pybirdai/api/trail/14/filtered-lineage/?include_unused=true

# Filtered - currently showing too little
GET /pybirdai/api/trail/14/filtered-lineage/
```

## Hypothesis
The issue is likely that:

1. **Missing Table Tracking**: The automatic wrapper is only tracking the final report cell results, not the intermediate table processing
2. **Missing Derived Table Tracking**: The system isn't recognizing that some of the intermediate processing creates derived tables
3. **Incomplete Field Tracking**: We're only tracking fields accessed in the final filtering step, not fields used in the data preparation

## Expected vs Actual

### Expected for F_05_01_REF_FINREP_3_0:
- Multiple database tables (probably INSTRMNT, INSTRMNT_RL, PRTY, etc.)
- Derived tables for the union operations (F_05_01_REF_FINREP_3_0_UnionTable, etc.)
- Multiple rows from each table
- Field usage across all the intermediate processing

### Actual:
- Only 1 database table (INSTRMNT_ENTTY_RL_ASSGNMNT)
- No derived tables
- Only 1 row
- Limited field usage

## Root Cause Analysis

The automatic tracking wrapper is applied at the wrong level. It's only seeing the final filtering in the report cell, but the real data processing happens in:

1. **Table Initialization**: When tables like `F_05_01_REF_FINREP_3_0_Table` are created and populated
2. **Union Operations**: When `calc_F_05_01_REF_FINREP_3_0_UnionItems()` combines data from multiple source tables
3. **Field Access**: When each union item accesses fields from its base objects

## Solution Approach

We need to track usage at multiple levels:

1. **Table Level**: Track when tables are initialized and populated
2. **Union Level**: Track when union operations combine data from multiple sources  
3. **Field Level**: Track when fields are accessed during the union and filtering operations
4. **Row Level**: Track both source rows and derived rows

## Implementation Fix

The tracking needs to happen in the orchestration during table initialization and data processing, not just in the final report cell filtering.