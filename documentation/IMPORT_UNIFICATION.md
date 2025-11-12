# Import Module Unification - Technical Documentation

## Overview

The import functionality for SDD model data has been unified into a single module structure that supports multiple dataset types (FINREP, ANCRDT, DPM) through configuration-based routing.

**Previous Architecture:**
- `website_to_sddmodel/import_func/` - FINREP/DPM imports
- `ancrdt_transformation/import_func/` - ANCRDT imports (duplicate structure)

**Unified Architecture:**
- `website_to_sddmodel/import_func/` - All dataset types with routing via `DatasetConfig`

## Key Components

### 1. DatasetConfig Class

**Location:** `birds_nest/pybirdai/process_steps/website_to_sddmodel/import_func/config.py`

Configuration class that controls dataset-specific import behavior:

```python
from pybirdai.process_steps.website_to_sddmodel.import_func import DatasetConfig

# FINREP configuration (default)
config = DatasetConfig(dataset_type="finrep")

# ANCRDT configuration
config = DatasetConfig(dataset_type="ancrdt", file_directory="ancrdt_csv")

# DPM configuration
config = DatasetConfig(dataset_type="dpm", file_directory="technical_export")
```

**Configuration Properties:**

| Property | FINREP | ANCRDT | DPM |
|----------|--------|--------|-----|
| `bypass_ecb_filter` | False | True | False |
| `includes_cubes` | False | True | False |
| `includes_subdomains` | False | True | False |
| `use_csv_copy` | False | False | True |
| `column_index_class_name` | standard | ancrdt | standard |

**Methods:**

- `get_column_indexes()` - Returns appropriate ColumnIndexes class for dataset type
- `should_include_entity(maintenance_agency, is_reference_data)` - Filtering logic for entities

### 2. Unified Orchestrator

**Location:** `birds_nest/pybirdai/process_steps/website_to_sddmodel/import_func/import_report_templates_from_sdd.py`

Main entry point for importing report templates with dataset routing:

```python
from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_report_templates_from_sdd
)

# Import FINREP data
import_report_templates_from_sdd(
    sdd_context,
    dataset_type="finrep",
    file_dir="technical_export"
)

# Import ANCRDT data
import_report_templates_from_sdd(
    sdd_context,
    dataset_type="ancrdt",
    file_dir="ancrdt_csv"
)

# Import DPM data (backward compatible)
import_report_templates_from_sdd(
    sdd_context,
    dpm=True  # Maps to dataset_type="dpm"
)
```

**Function Signature:**
```python
def import_report_templates_from_sdd(
    sdd_context,
    dataset_type="finrep",  # "finrep", "ancrdt", or "dpm"
    file_dir=None,          # Auto-set based on dataset_type if None
    dpm=False               # Backward compatibility flag
):
```

**Import Flow:**

1. Create `DatasetConfig` based on `dataset_type`
2. Import basic entities (agencies, frameworks, domains, members, variables)
3. **If `config.includes_subdomains`:** Import subdomains and subdomain enumerations
4. **If `config.includes_cubes`:** Import cube structures, items, and cubes
5. Import rendering entities (tables, axes, cells)
6. **If `config.use_csv_copy`:** Use optimized CSV COPY imports
7. **Else:** Use standard bulk_create imports

## ANCRDT-Specific Imports

The following imports are now part of the unified module:

### Subdomains
```python
from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_subdomains,
    import_subdomain_enumerations
)

import_subdomains(base_path, sdd_context)
import_subdomain_enumerations(base_path, sdd_context)
```

**Files:**
- `import_subdomains.py` - Imports SUBDOMAIN entities
- `import_subdomain_enumerations.py` - Imports SUBDOMAIN_ENUMERATION entities

### Cube Structures
```python
from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_cube_structures,
    import_cube_structure_items,
    import_cubes
)

import_cube_structures(base_path, sdd_context)
import_cube_structure_items(base_path, sdd_context)
import_cubes(base_path, sdd_context)
```

**Files:**
- `import_cube_structures.py` - Imports CUBE_STRUCTURE entities
- `import_cube_structure_items.py` - Imports CUBE_STRUCTURE_ITEM entities with error handling
- `import_cubes.py` - Imports CUBE entities with framework assignment

## Migration Guide

### From ancrdt_transformation/import_func

**Old Code:**
```python
from pybirdai.process_steps.ancrdt_transformation.import_func import (
    import_cubes,
    import_cube_structures
)

import_cubes(base_path, context)
import_cube_structures(base_path, context)
```

**New Code:**
```python
from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_cubes,
    import_cube_structures
)

import_cubes(base_path, context)
import_cube_structures(base_path, context)
```

**Orchestrator Migration:**

**Old Code:**
```python
from pybirdai.process_steps.ancrdt_transformation.import_website_to_sdd_model_django_ancrdt import (
    ImportWebsiteToSDDModel
)

importer = ImportWebsiteToSDDModel()
importer.import_report_templates_from_sdd(sdd_context, "ancrdt_csv", True)
```

**New Code:**
```python
from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_report_templates_from_sdd
)

import_report_templates_from_sdd(
    sdd_context,
    dataset_type="ancrdt",
    file_dir="ancrdt_csv"
)
```

### Updated ANCRDT Importer

**Location:** `birds_nest/pybirdai/process_steps/ancrdt_transformation/ancrdt_importer.py`

The ANCRDT importer now uses the unified module:

```python
from pybirdai.process_steps.website_to_sddmodel.import_func.import_report_templates_from_sdd import (
    import_report_templates_from_sdd
)

# Temporarily override file_directory to point to results instead of resources
original_file_directory = sdd_context.file_directory
sdd_context.file_directory = os.path.join(base_dir, 'results')

# Use unified import with dataset_type="ancrdt"
import_report_templates_from_sdd(
    sdd_context,
    dataset_type="ancrdt",
    file_dir="ancrdt_csv"
)

sdd_context.file_directory = original_file_directory
```

## Benefits of Unification

### 1. Single Source of Truth
- One module structure instead of two duplicate structures
- Shared utility functions, lookups, and warning handlers
- Consistent error handling across all dataset types

### 2. Easier Maintenance
- Bug fixes apply to all dataset types automatically
- No need to sync changes between two modules
- Clear separation of dataset-specific vs shared logic

### 3. Extensibility
- Adding new dataset types is straightforward (e.g., "corep", "ale")
- New dataset types just need configuration in `DatasetConfig`
- No need to duplicate entire module structure

### 4. Consistent API
- Same function signatures across all dataset types
- Predictable behavior and error handling
- Clear documentation and usage patterns

### 5. Performance Optimization
- `use_csv_copy` flag enables optimized imports for large datasets (DPM)
- Configuration-based optimization without code duplication
- Easy to add dataset-specific optimizations

## File Structure

```
birds_nest/pybirdai/process_steps/website_to_sddmodel/import_func/
├── __init__.py                              # Exports all functions
├── config.py                                # DatasetConfig class
│
├── import_report_templates_from_sdd.py      # Main orchestrator
├── import_semantic_integrations_from_sdd.py # Mapping orchestrator
├── import_hierarchies_from_sdd.py           # Hierarchy orchestrator
│
├── import_maintenance_agencies.py           # Basic entities
├── import_frameworks.py
├── import_domains.py
├── import_members.py
├── import_variables.py
│
├── import_member_hierarchies.py             # Hierarchies
├── import_parent_members_with_children.py
├── import_member_hierarchy_nodes.py
│
├── import_report_tables.py                  # Report templates
├── import_axis.py
├── import_axis_ordinates.py
├── import_table_cells.py
├── import_table_cells_csv_copy.py
├── import_ordinate_items.py
├── import_ordinate_items_csv_copy.py
├── import_cell_positions.py
├── import_cell_positions_csv_copy.py
│
├── import_variable_mappings.py              # Mappings
├── import_variable_mapping_items.py
├── import_member_mappings.py
├── import_member_mapping_items.py
├── import_mapping_definitions.py
├── import_mapping_to_cubes.py
│
├── import_cubes.py                          # ANCRDT-specific
├── import_cube_structures.py
├── import_cube_structure_items.py
├── import_subdomains.py
├── import_subdomain_enumerations.py
│
├── utilities.py                             # Utilities
├── lookups.py
├── warning_writers.py
├── database_helpers.py
└── csv_copy_importer.py
```

## Testing

### Unit Testing Individual Imports

```python
import os
from django.conf import settings
from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_cubes,
    import_cube_structures,
    DatasetConfig
)

# Test ANCRDT cube import
base_path = os.path.join(settings.BASE_DIR, 'results/ancrdt_csv')
config = DatasetConfig(dataset_type="ancrdt")

import_cube_structures(base_path, sdd_context)
import_cubes(base_path, sdd_context)

# Verify cubes were imported
from pybirdai.bird_meta_data_model import CUBE
cubes = CUBE.objects.filter(framework_id__framework_id__icontains='ANCRDT')
assert cubes.count() > 0, "ANCRDT cubes not imported"
```

### Integration Testing

```python
from pybirdai.process_steps.website_to_sddmodel.import_func import (
    import_report_templates_from_sdd
)

# Test full ANCRDT import workflow
import_report_templates_from_sdd(
    sdd_context,
    dataset_type="ancrdt",
    file_dir="ancrdt_csv"
)

# Verify all entities were imported
from pybirdai.bird_meta_data_model import (
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
    SUBDOMAIN, SUBDOMAIN_ENUMERATION
)

assert CUBE.objects.filter(framework_id__framework_id__icontains='ANCRDT').exists()
assert CUBE_STRUCTURE.objects.exists()
assert CUBE_STRUCTURE_ITEM.objects.exists()
assert SUBDOMAIN.objects.exists()
assert SUBDOMAIN_ENUMERATION.objects.exists()
```

## Adding New Dataset Types

To add support for a new dataset type (e.g., "corep"):

### 1. Update DatasetConfig

**File:** `config.py`

```python
class DatasetConfig:
    FINREP = "finrep"
    ANCRDT = "ancrdt"
    DPM = "dpm"
    COREP = "corep"  # Add new constant

    def __init__(self, dataset_type="finrep", file_directory="technical_export"):
        self.dataset_type = dataset_type.lower()
        self.file_directory = file_directory

        # Configure behavior based on dataset type
        if self.dataset_type == self.COREP:
            self.bypass_ecb_filter = False
            self.includes_cubes = False
            self.includes_subdomains = False
            self.use_csv_copy = True
            self.column_index_class_name = "standard"
        # ... existing configurations
```

### 2. Create Dataset-Specific Imports (if needed)

If the dataset requires unique import logic, create new import functions:

```python
# import_corep_specific.py
def import_corep_specific_entity(base_path, sdd_context):
    """Import COREP-specific entities."""
    # Implementation
```

### 3. Update Orchestrator

**File:** `import_report_templates_from_sdd.py`

```python
def import_report_templates_from_sdd(sdd_context, dataset_type="finrep", file_dir=None, dpm=False):
    config = DatasetConfig(dataset_type=dataset_type, file_directory=file_dir)

    # Import basic entities (always needed)
    import_maintenance_agencies(sdd_context)
    import_frameworks(sdd_context)
    import_domains(sdd_context, False, config)
    import_members(sdd_context, False, config)
    import_variables(sdd_context, False, config)

    # Import COREP-specific entities if applicable
    if config.dataset_type == "corep":
        import_corep_specific_entity(base_path, sdd_context)

    # ... rest of imports
```

### 4. Update __init__.py Exports

```python
# COREP-specific import functions
from .import_corep_specific import import_corep_specific_entity

__all__ = [
    # ... existing exports
    'import_corep_specific_entity',
]
```

## Troubleshooting

### Issue: Import fails with "No module named 'pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt'"

**Solution:** Ensure ANCRDT ColumnIndexes is accessible:
```python
# In config.py get_column_indexes() method
if self.column_index_class_name == "ancrdt":
    from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
```

### Issue: Entities not being imported

**Solution:** Check configuration flags:
```python
config = DatasetConfig(dataset_type="ancrdt")
print(f"Includes cubes: {config.includes_cubes}")
print(f"Includes subdomains: {config.includes_subdomains}")
print(f"Bypass ECB filter: {config.bypass_ecb_filter}")
```

### Issue: Cube framework assignment missing

**Solution:** Ensure `import_cubes.py` reads framework_id from CSV:
```python
framework_id_str = row[ColumnIndexes().cube_framework_index]
if framework_id_str:
    try:
        cube.framework_id = FRAMEWORK.objects.get(framework_id=framework_id_str)
    except FRAMEWORK.DoesNotExist:
        logger.warning(f"FRAMEWORK not found: {framework_id_str}")
```

## Backward Compatibility

### ancrdt_transformation/import_func

The old ANCRDT import module structure remains in place for backward compatibility:
- `birds_nest/pybirdai/process_steps/ancrdt_transformation/import_func/`

However, **new code should use the unified module** in `website_to_sddmodel/import_func/`.

### Deprecation Path

**Phase 1 (Current):** Both modules exist, new code uses unified module
**Phase 2 (Future):** Add deprecation warnings to old module
**Phase 3 (Future):** Remove old module after migration period

## References

- **REFACTORING_SUMMARY.md** - Details of ANCRDT import refactoring
- **config.py** - DatasetConfig class implementation
- **import_report_templates_from_sdd.py** - Unified orchestrator
- **ancrdt_importer.py** - ANCRDT entry point using unified module

## Contributors

- **Neil Mackenzie** - Original FINREP import implementation
- **Benjamin Arfa** - ANCRDT imports, modular refactoring, unification

---

**Status:** Production-ready, fully tested with FINREP and ANCRDT datasets

**Last Updated:** 2025-01-10
