# Clone Mode Testing Documentation

## Overview

Clone mode allows you to save and restore the database state at any point in the workflow. This is useful for:
- Sharing work progress between team members
- Creating checkpoints before making changes
- Testing workflow steps in isolation
- Disaster recovery

## Commands

### Save Clone State
```bash
python manage.py save_clone_state --local-only [--force]
```

Options:
- `--local-only`: Export to local directory only (no GitHub push)
- `--force`: Allow export even after code generation steps
- `--output-dir`: Custom output directory
- `--config`: Path to config file (default: clone_mode_config.json)

### Load Clone State
```bash
python manage.py load_clone_state --local-path <path> [--force] [--skip-cleanup]
```

Options:
- `--local-path`: Path to local export directory
- `--force`: Allow import even if metadata shows code generation was completed
- `--skip-cleanup`: Don't delete existing data before import
- `--verify-only`: Only verify, don't actually import

## Testing Matrix

### Main Workflow

| Step | Name | Description | Testable |
|------|------|-------------|----------|
| 1 | SMCubes Core Creation | Database setup + data import | Yes |
| 2 | SMCubes Transformation Rules | Hierarchy conversion | Yes |
| 3 | Python Transformation Rules | Code generation | Yes* |
| 4 | Full Execution with Test Suite | Run tests | Yes* |

*Steps 3-4 require `--force` flag as they involve code generation.

### DPM Workflow

| Step | Name | Description | Testable |
|------|------|-------------|----------|
| 1 | Extract DPM Metadata | Download and extract EBA DPM data | Yes |
| 2 | Process & Import Tables | Ordinate explosion + DB import | Yes |
| 3 | Create Output Layers | Create cube structures | Yes |
| 4 | Create Transformation Rules | Generate joins metadata | Yes* |
| 5 | Generate Python Code | Create executable code | Yes* |
| 6 | Execute DPM Tests | Run test suite | Yes* |

*Steps 4-6 require `--force` flag.

### ANCRDT Workflow

| Step | Name | Description | Testable |
|------|------|-------------|----------|
| 0 | Initial Setup | Automode database setup | Yes |
| 1 | Data Import | Import input model + technical export | Yes |
| 2 | Hierarchy Conversion | Convert LDM to SDD hierarchies | Yes |
| 3 | Joins Metadata | Create joins metadata | Yes |
| 4 | Code Generation | Generate executable joins | Yes* |
| 5 | Execution | Run transformations | Yes* |

*Steps 4-5 require `--force` flag.

## Running Tests

### Automated Tests

```bash
# Run all clone mode tests
python -m pytest pybirdai/tests/clone_mode/test_clone_mode.py -v

# Run specific workflow tests
python -m pytest pybirdai/tests/clone_mode/test_clone_mode.py::TestDPMWorkflow -v
python -m pytest pybirdai/tests/clone_mode/test_clone_mode.py::TestMainWorkflow -v
python -m pytest pybirdai/tests/clone_mode/test_clone_mode.py::TestANCRDTWorkflow -v

# Run with shell script
bash pybirdai/tests/clone_mode/run_clone_mode_tests.sh
```

### Manual Testing

1. **Run workflow to desired step**:
   ```bash
   # Example: Run DPM Step 1 and 2
   python -c "
   import django, os
   os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
   django.setup()
   from pybirdai.entry_points.import_dpm_data import RunImportDPMData
   RunImportDPMData.run_import_phase_a(frameworks=['FINREP'])
   RunImportDPMData.run_import_phase_b()
   "
   ```

2. **Save clone state**:
   ```bash
   python manage.py save_clone_state --local-only --force
   ```

3. **Verify export**:
   ```bash
   ls -la results/clone_export/database_export/
   cat results/clone_export/database_export/process_metadata.json
   ```

4. **Clear database**:
   ```bash
   rm db.sqlite3
   python manage.py migrate --run-syncdb
   ```

5. **Load clone state**:
   ```bash
   python manage.py load_clone_state \
       --local-path results/clone_export/database_export \
       --force --skip-cleanup
   ```

6. **Verify data integrity**:
   ```bash
   python -c "
   import django, os
   os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
   django.setup()
   from pybirdai.models.bird_meta_data_model import *
   print(f'TABLE: {TABLE.objects.count()}')
   print(f'VARIABLE: {VARIABLE.objects.count()}')
   print(f'MEMBER: {MEMBER.objects.count()}')
   "
   ```

## Expected Data Counts by Workflow

### DPM FINREP (Steps 1-2)

| Model | Expected Count |
|-------|---------------|
| MAINTENANCE_AGENCY | 1 |
| FRAMEWORK | 1 |
| DOMAIN | 126 |
| VARIABLE | 2,558 |
| MEMBER | 10,640 |
| TABLE | 122 |
| AXIS | 249 |
| AXIS_ORDINATE | 3,450 |
| ORDINATE_ITEM | 11,625 |
| TABLE_CELL | 18,107 |
| CELL_POSITION | 36,562 |
| MEMBER_HIERARCHY | 887 |
| MEMBER_HIERARCHY_NODE | 13,964 |

### DPM FINREP (Step 3 - Output Layers)

Additional models populated:
| Model | Expected Count |
|-------|---------------|
| CUBE | ~122 (one per table) |
| CUBE_STRUCTURE | ~122 |
| CUBE_STRUCTURE_ITEM | varies |

## Troubleshooting

### Common Issues

1. **"operation_type" field error**
   - Fixed in commit e693e6ad
   - DPM/AnaCredit workflow models don't have this field

2. **"framework_id_id" column error**
   - Fixed: Export now uses `field.column` for correct DB column names

3. **Import shows 0 records**
   - Fixed: Removed debug condition that blocked "bird_*" files

4. **Workflow states not restored**
   - Ensure WorkflowSession exists for DPM/AnaCredit workflows

### Debug Mode

Enable verbose logging:
```python
import logging
logging.getLogger('pybirdai.utils.clone_mode').setLevel(logging.DEBUG)
```

## File Locations

- **Export directory**: `results/clone_export/database_export/`
- **ZIP package**: `results/clone_export/clone_state_export.zip`
- **Process metadata**: `results/clone_export/database_export/process_metadata.json`
- **Import results**: `import_results/ordered_import_results_*.json`
