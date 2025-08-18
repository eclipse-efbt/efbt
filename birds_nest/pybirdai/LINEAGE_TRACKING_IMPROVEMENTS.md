# Lineage Tracking Improvements

This document describes the improvements made to the lineage tracking system to show only the subset of fields and rows that were actually used in calculations.

## Overview

The enhanced lineage tracking system now captures:
1. **Used Fields**: Only the fields that were accessed during calculations
2. **Used Rows**: Only the rows that passed filters and contributed to results
3. **Calculation Context**: Which calculation used which rows and fields

## Key Components

### 1. New Database Models

Two new models have been added to track usage:

```python
class CalculationUsedRow(models.Model):
    """Track which rows were actually used in a calculation (passed filters)"""
    trail = models.ForeignKey('Trail', related_name='calculation_used_rows', on_delete=models.CASCADE)
    calculation_name = models.CharField(max_length=255)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    used_row = GenericForeignKey('content_type', 'object_id')

class CalculationUsedField(models.Model):
    """Track which fields were actually accessed during a calculation"""
    trail = models.ForeignKey('Trail', related_name='calculation_used_fields', on_delete=models.CASCADE)
    calculation_name = models.CharField(max_length=255)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    used_field = GenericForeignKey('content_type', 'object_id')
```

### 2. Enhanced Orchestration Methods

The `OrchestrationWithLineage` class now includes:

```python
def track_calculation_used_row(self, calculation_name, row):
    """Track that a specific row was used in a calculation"""

def track_calculation_used_field(self, calculation_name, field_name, row=None):
    """Track that a specific field was accessed during a calculation"""

def get_calculation_used_rows(self, calculation_name):
    """Get all rows that were used in a specific calculation"""

def get_calculation_used_fields(self, calculation_name):
    """Get all fields that were accessed during a specific calculation"""
```

### 3. Enhanced @lineage Decorator

The `@lineage` decorator now automatically tracks field usage when a calculation context is set.

### 4. Enhanced Lineage API

New API endpoints provide filtered lineage information:

#### Get Filtered Lineage
```
GET /api/trail/<trail_id>/filtered-lineage/
```

Query parameters:
- `calculation_name`: Filter to a specific calculation
- `include_unused`: If 'true', include all data (default filters to only used data)

Returns JSON with:
- Only rows that passed filters
- Only fields that were accessed
- Lineage relationships for the filtered data

#### Get Calculation Summary
```
GET /api/trail/<trail_id>/calculation-summary/
```

Returns a summary of all calculations with counts of used rows and fields.

## Implementation Guide

### Method 1: Enhance Existing Cell Classes

1. Import required modules:
```python
from pybirdai.annotations.decorators import lineage, _lineage_context
```

2. Add tracking in `calc_referenced_items`:
```python
def calc_referenced_items(self):
    items = self.F_01_01_REF_FINREP_3_0_Table.F_01_01_REF_FINREP_3_0s
    
    # Set calculation context
    orchestration = _lineage_context.get('orchestration')
    if orchestration:
        orchestration.current_calculation = self.__class__.__name__
    
    for item in items:
        filter_passed = True
        
        # Track field access when checking filters
        try:
            value = item.ACCNTNG_CLSSFCTN()
            if orchestration and hasattr(orchestration, 'track_calculation_used_field'):
                orchestration.track_calculation_used_field(
                    self.__class__.__name__, 'ACCNTNG_CLSSFCTN', item)
            
            if value == '9':
                pass
            else:
                filter_passed = False
        except:
            filter_passed = False
        
        # ... repeat for other filters ...
        
        # Track row if it passed all filters
        if filter_passed:
            if orchestration and hasattr(orchestration, 'track_calculation_used_row'):
                orchestration.track_calculation_used_row(
                    self.__class__.__name__, item)
            self.F_01_01_REF_FINREP_3_0s.append(item)
```

### Method 2: Use the Enhanced Base Class

```python
from pybirdai.process_steps.filter_code.enhanced_report_cells import EnhancedCellBase

class Cell_F_01_01_REF_FINREP_3_0_45749_REF(EnhancedCellBase):
    # ... your existing code ...
```

### Method 3: Use the Cell Enhancement Utilities

```python
from pybirdai.process_steps.filter_code.cell_enhancement_utils import (
    enhance_existing_cell_file
)

# Convert an entire file
enhance_existing_cell_file(
    'report_cells.py',
    'enhanced_report_cells.py'
)
```

## Running Migrations

After adding the new models, run Django migrations:

```bash
python manage.py makemigrations
python manage.py migrate
```

## Example Usage

1. Execute a datapoint with lineage tracking enabled
2. Access the filtered lineage:
   ```
   http://localhost:8000/pybirdai/api/trail/123/filtered-lineage/?calculation_name=Cell_F_01_01_REF_FINREP_3_0_45749_REF
   ```

3. The response will include only:
   - Tables that contain used data
   - Rows that passed filters
   - Fields that were accessed
   - Lineage relationships between the filtered data

## Benefits

1. **Reduced JSON Size**: Lineage exports are much smaller, containing only relevant data
2. **Better Understanding**: Clear visibility into which data actually contributed to results
3. **Debugging**: Easier to trace why specific values were calculated
4. **Compliance**: Better audit trail showing exactly which data was used

## Backward Compatibility

The original lineage API endpoints remain unchanged. The new filtering is opt-in through:
- New API endpoints
- Query parameter `include_unused=true` to get all data
- Existing cells continue to work without modification