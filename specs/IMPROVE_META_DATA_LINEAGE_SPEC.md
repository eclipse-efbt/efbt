# Metadata Lineage Improvement Specification

## Overview

This specification describes improvements to the metadata lineage system in PyBIRD AI, specifically transitioning from a union-based approach to a direct filtering approach for report cells. The goal is to optimize performance and simplify the data processing pipeline by eliminating unnecessary union operations.

## Current Architecture

### 1. Report Cell Filtering
- **Location**: `efbt/birds_nest/pybirdai/process_steps/filter_code/report_cells.py`
- **Current behavior**: Report cells create filters on output layer tables
- **Generation**: Created by `efbt/birds_nest/pybirdai/process_steps/pybird/create_executable_filters.py`

### 2. Output Layer Tables
- **Location**: `efbt/birds_nest/pybirdai/process_steps/filter_code/output_tables.py`
- **Current behavior**: Output layers are created by unioning multiple product-specific classes
- **Generation**: Created by `efbt/birds_nest/pybirdai/process_steps/pybird/create_python_django_transformations.py`

### 3. Product-Specific Classes (Slice Classes)
- **Location**: `efbt/birds_nest/pybirdai/process_steps/filter_code/F_05_01_REF_FINREP_3_0_logic.py`
- **Examples**: 
  - `F_05_01_REF_FINREP_3_0_Other_loans_Table`
  - `F_05_01_REF_FINREP_3_0_Credit_card_debt_Table`
- **Function**: Each class contains a `calc_` function that gathers input tables for specific products
- **Also known as**: "Product-specific joins" or "slice classes"
- **Generation**: Created by `efbt/birds_nest/pybirdai/process_steps/pybird/create_python_django_transformations.py`

### 4. Union Tables
- **Example**: `F_05_01_REF_FINREP_3_0_UnionTable`
- **Current behavior**: Combines multiple product-specific classes into a single output layer

## Problem Statement

The current approach creates unnecessary overhead by:
1. Creating union tables that combine all product-specific classes
2. Filtering on the union table instead of directly on relevant product classes
3. Processing data for all products even when only specific products are needed

## Proposed Solution

### Direct Filtering Approach

Instead of filtering on union tables, report cells should filter directly on the relevant product-specific classes. The mapping is determined by the `TYP_INSTRMNT` filter value in each report cell.

### Implementation Strategy

#### 1. Product Mapping
- Use the `TYP_INSTRMNT` member from report cell combination items
- Map to product-specific classes using the configuration file:
  - **File**: `efbt/birds_nest/resources/joins_configuration/join_for_product_to_reference_category_FINREP_REF.csv`
  - **Mapping**: `TYP_INSTRMNT` → `Main Category` column → `slice name` column
  - **Example**: `TYP_INSTRMNT_114` maps to:
    - 'Other loans' slice
    - 'Non Negotiable bonds' slice

#### 2. Filter Creation Process
When creating filters in `create_executable_filters.py`:
1. Examine combination items for each report cell
2. Identify the `TYP_INSTRMNT` original member
3. For hierarchical members, find all leaf nodes
4. Look up corresponding product-specific classes for the original member using the mapping file
5. Generate filters that target these specific classes instead of the union table

#### 3. Code Generation Changes

**A. Modify `create_executable_filters.py`:**
- Add logic to identify relevant product-specific classes based on `TYP_INSTRMNT`
- Generate imports for only the needed product-specific classes
- Create filter methods that operate directly on these classes

**B. Modify `create_python_django_transformations.py`:**
- Continue generating product-specific classes as before
- ✅ **IMPLEMENTED**: Skip generation of `output_tables.py` and union classes entirely
- ✅ **IMPLEMENTED**: Product-specific classes remain standalone and queryable
- ✅ **IMPLEMENTED**: Eliminated `UnionItem` and `UnionTable` class generation

## Benefits

1. **Performance**: Eliminate unnecessary union operations
2. **Efficiency**: Process only relevant data for each report cell
3. **Clarity**: Direct relationship between report requirements and data sources
4. **Maintainability**: Simpler code with fewer intermediary layers
5. **Reduced Code Generation**: No longer generates `output_tables.py` or union classes
6. **Memory Efficiency**: Eliminates creation of union items that wrap product data

## Technical Considerations

### Hierarchy Processing
- When processing combination items, ensure proper expansion of hierarchical members to leaf nodes
- Maintain existing hierarchy traversal logic from the current implementation

### Multiple Product Mapping
- Some `TYP_INSTRMNT` values map to multiple products
- Report cells must handle filtering across multiple product-specific classes when necessary
- Consider using Django's Q objects for OR conditions across multiple tables


## Example Implementation

### Current Approach (Simplified)
```python
# In report_cells.py
class ReportCellFilter:
    def filter_F_05_01(self):
        # Filters on union table
        return F_05_01_REF_FINREP_3_0_UnionTable.objects.filter(
            TYP_INSTRMNT='114',
            # other filters...
        )
```

### Proposed Approach (Simplified)
```python
# In report_cells.py
class ReportCellFilter:
    def filter_F_05_01(self):
        # Direct filtering on relevant product classes
        typ_instrmnt = '114'
        # Lookup shows this maps to 'Other loans' and 'Non Negotiable bonds'
        
        other_loans = F_05_01_REF_FINREP_3_0_Other_loans_Table.objects.filter(
            # relevant filters...
        )
        
        non_negotiable_bonds = F_05_01_REF_FINREP_3_0_Non_Negotiable_bonds_Table.objects.filter(
            # relevant filters...
        )
        
        # Combine results as needed
        return other_loans.union(non_negotiable_bonds)
```

## Implementation Status

✅ **COMPLETED**: All implementation tasks have been successfully completed:

1. ✅ Analyzed existing report cell definitions and `TYP_INSTRMNT` usage patterns
2. ✅ Created comprehensive mapping between `TYP_INSTRMNT` values and product classes via CSV configuration
3. ✅ Implemented complete solution with direct product-specific filtering
4. ✅ Eliminated generation of `output_tables.py` and all union-related classes
5. ✅ Updated filter generation to include `TYP_INSTRMNT` filters for completeness
6. ✅ Verified implementation with comprehensive test suite
7. ✅ Fixed lineage decorators to reference product-specific classes instead of output layers
8. ✅ Fixed metric_value function to work correctly with filtered items from product-specific classes

## Final Architecture

The system now:
- Maps `TYP_INSTRMNT` values directly to product-specific classes using CSV configuration
- Generates report cells that filter directly on relevant product classes
- Includes redundant `TYP_INSTRMNT` filters for completeness and consistency
- Eliminates all union table overhead and intermediary wrapper classes
- Assumes all report cells have `TYP_INSTRMNT` values (no fallback needed)
- **Generates correct lineage dependencies** pointing to actual product classes (e.g., `Other_loans.CRRYNG_AMNT`) instead of output layers (e.g., `F_01_01_REF_FINREP_3_0.CRRYNG_AMNT`)