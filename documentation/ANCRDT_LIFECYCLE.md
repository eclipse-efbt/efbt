# ANCRDT Code Generation Lifecycle

## Overview

The ANCRDT (Analytical Credit Datasets) code generation system now follows a structured **3-stage lifecycle**: **Generate → Edit → Deploy**.

This lifecycle ensures that:
- Generated code can be manually edited without being overwritten
- Edited versions are preserved when code is regenerated
- Production execution uses stable, reviewed code
- Clear separation between staging (editable) and production (executable) code

## Architecture

```
┌─────────────────────────────────────┐
│  STAGE 1: GENERATION                │
│  results/generated_python_joins/    │
│  ├── ANCRDT_INSTRMNT_C_1_logic.py  │  ← Editable working copy
│  ├── ANCRDT_INSTRMNT_C_1_logic.py.generated  │  ← Base reference
│  └── ancrdt_output_tables.py       │
└─────────────────────────────────────┘
            ↓ Edit in Web UI
┌─────────────────────────────────────┐
│  STAGE 2: EDIT                      │
│  Web Code Editor                    │
│  • Syntax validation                │
│  • Manual modifications             │
│  • Backup creation                  │
└─────────────────────────────────────┘
            ↓ Deploy to Production
┌─────────────────────────────────────┐
│  STAGE 3: DEPLOY                    │
│  pybirdai/process_steps/filter_code/│
│  ├── ANCRDT_INSTRMNT_C_1_logic.py  │  ← Executable code
│  └── ancrdt_output_tables.py       │  ← Production ready
└─────────────────────────────────────┘
            ↓ Execute
┌─────────────────────────────────────┐
│  EXECUTION                          │
│  run_generated_joins.py             │
│  • Imports from filter_code first   │
│  • Falls back to generated_python_joins │
└─────────────────────────────────────┘
```

## File Types and Purpose

### Working Files (Staging Area)
Location: `results/generated_python_joins/`

1. **`.py` files** - Editable working copies
   - Modified through web UI
   - Preserved during regeneration if edited
   - Source for deployment to production

2. **`.py.generated` files** - Base references
   - Created automatically after generation
   - Used to detect manual edits
   - Regenerated with each code generation

3. **`.py.backup` files** - Safety backups
   - Created before each save
   - Single-level backup (overwrites previous)

### Production Files
Location: `pybirdai/process_steps/filter_code/`

- Deployed versions of ANCRDT code
- Used for actual execution
- Match FINREP pattern (all executable code in filter_code)

## Workflow

### 1. Initial Generation

```bash
# Code is generated with metadata header
python manage.py run_create_executable_joins_ancrdt
```

Creates:
- `ANCRDT_INSTRMNT_C_1_logic.py` (working copy)
- `ANCRDT_INSTRMNT_C_1_logic.py.generated` (base reference)
- `ancrdt_output_tables.py` + `.generated`

### 2. Edit Code

Via Web UI: `/pybirdai/ancrdt-workflow/step-3-review/`

- Edit code in browser
- Syntax validation on save
- Automatic backup creation
- Modifications tracked in database

### 3. Deploy to Production

**Option A: Manual Deploy via API**
```bash
POST /pybirdai/code-sync/deploy/
{
    "file_name": "ANCRDT_INSTRMNT_C_1_logic.py",
    "create_backup": true
}
```

**Option B: Save and Deploy in One Step**
```bash
POST /pybirdai/code-sync/save-and-deploy/
{
    "file_name": "ANCRDT_INSTRMNT_C_1_logic.py",
    "code_content": "... edited python code ...",
    "create_backup": true
}
```

**Option C: Deploy All Files**
```bash
POST /pybirdai/code-sync/deploy-all/
{
    "create_backup": true
}
```

### 4. Execute

```bash
# Execution automatically uses production code
python birds_nest/pybirdai/standalone/run_generated_joins.py --framework ancrdt
```

The system:
1. First tries to import from `filter_code/` (production)
2. Falls back to `generated_python_joins/` (staging) if not deployed

## Regeneration Behavior

When you regenerate code:

**If file has NO edits** (matches `.generated`):
- ✅ File is regenerated
- ✅ New `.generated` base created
- ✅ Working copy updated

**If file HAS edits** (differs from `.generated`):
- ✅ Existing file preserved
- ✅ Generation skipped
- ⚠️ Warning logged: "manual edits detected"

To force regeneration:
1. Delete the `.py` file (preserves manual changes elsewhere if needed)
2. Or delete both `.py` and `.generated` for fresh start

## API Endpoints

### Sync Operations

**Deploy Single File**
```
POST /pybirdai/code-sync/deploy/
Body: {"file_name": "...", "create_backup": true}
```

**Deploy All ANCRDT Files**
```
POST /pybirdai/code-sync/deploy-all/
Body: {"create_backup": true}
```

**Save and Deploy (Combined)**
```
POST /pybirdai/code-sync/save-and-deploy/
Body: {"file_name": "...", "code_content": "...", "create_backup": true}
```

### Status Checks

**Get Sync Status (All Files)**
```
GET /pybirdai/code-sync/status/
```

**Get Sync Status (Single File)**
```
GET /pybirdai/code-sync/status/<file_name>/
```

**Check Manual Edits**
```
GET /pybirdai/code-sync/check-edits/<file_name>/
```

**Get Diff Summary**
```
GET /pybirdai/code-sync/diff/<file_name>/
```

## Status Indicators

When viewing code in the web UI, you'll see:

- **[Generated]** - File is freshly generated, no edits
- **[Edited]** - File has manual modifications
- **[Synced]** - Staging and production versions match
- **[Out of Sync]** - Staging has changes not deployed to production

## Python API

You can also use the sync manager directly in Python:

```python
from pybirdai.utils.code_sync import CodeSyncManager

# Initialize manager
sync_manager = CodeSyncManager()

# Sync single file
result = sync_manager.sync_file('ANCRDT_INSTRMNT_C_1_logic.py')

# Sync all ANCRDT files
results = sync_manager.sync_all_ancrdt_files()

# Check sync status
status = sync_manager.get_sync_status()

# Check if file has manual edits
has_edits = sync_manager.has_manual_edits('ANCRDT_INSTRMNT_C_1_logic.py')

# Check if staging and production are synced
is_synced = sync_manager.is_synced('ANCRDT_INSTRMNT_C_1_logic.py')
```

## Best Practices

1. **Always review generated code** before deploying to production
2. **Test in staging** (`generated_python_joins`) before deploying
3. **Deploy regularly** to keep production up-to-date
4. **Check sync status** before executing to avoid running outdated code
5. **Keep edits minimal** - major changes may indicate metadata issues

## Troubleshooting

### Problem: Code regenerates over my edits

**Solution:** This shouldn't happen with the new lifecycle. If it does:
1. Check if `.generated` file exists
2. Verify your edits are in the `.py` file (not `.generated`)
3. Check generator logs for "manual edits detected" message

### Problem: Execution uses old code

**Solution:** Check sync status and deploy:
```bash
GET /pybirdai/code-sync/status/
POST /pybirdai/code-sync/deploy-all/
```

### Problem: Can't find ANCRDT code during execution

**Solution:** The execution system tries both locations:
1. First: `pybirdai/process_steps/filter_code/` (production)
2. Then: `results/generated_python_joins/` (staging)

Check both locations exist and have valid Python files.

### Problem: Syntax errors after editing

**Solution:** Use the web UI editor which validates syntax before saving.
Or manually validate:
```bash
python -m py_compile results/generated_python_joins/ANCRDT_INSTRMNT_C_1_logic.py
```

## Migration from Old System

If you have existing ANCRDT code without `.generated` files:

1. **First Generation Run:**
   - Existing `.py` files will be preserved
   - New `.generated` files created as base references
   - No code will be overwritten

2. **Sync to Production:**
   ```bash
   POST /pybirdai/code-sync/deploy-all/
   ```

3. **Verify Execution:**
   ```bash
   python birds_nest/pybirdai/standalone/run_generated_joins.py --framework ancrdt
   ```

## Related Files

### Core Implementation
- `pybirdai/utils/code_sync.py` - Sync manager utility
- `pybirdai/process_steps/ancrdt_transformation/create_python_django_transformations_ancrdt.py` - Code generator with lifecycle
- `pybirdai/views/execution_code_editor_views.py` - Web UI and API endpoints
- `pybirdai/standalone/run_generated_joins.py` - Execution with fallback support

### Web Interface
- `pybirdai/views/ancrdt_workflow_views.py` - ANCRDT workflow with sync status
- `pybirdai/templates/pybirdai/ancrdt_workflow/step_3_execution_code.html` - Code editor UI
- `pybirdai/templates/pybirdai/ancrdt_workflow/step_3_review.html` - Review page with sync status

### Configuration
- `pybirdai/urls.py` - URL routing for sync endpoints

## Future Enhancements

Potential improvements to consider:

- **Git Integration:** Automatic commits of generated and edited versions
- **Diff Viewer:** Visual comparison between staging and production
- **Version History:** Multi-level backups with timestamps
- **Rollback Feature:** Easy revert to previous versions
- **Conflict Resolution:** Merge tool for conflicting edits
- **Automated Tests:** Run tests before deployment
- **Deployment Hooks:** Custom scripts on deploy events

## Summary

The ANCRDT lifecycle provides:
✅ Edit preservation during regeneration
✅ Clear staging → production workflow
✅ Backward compatibility with old system
✅ Web UI for easy management
✅ API for automation
✅ Safe deployment with backups

This system brings ANCRDT code management up to the same standard as FINREP, with all production code unified in `filter_code/` directory.
