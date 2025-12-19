# Pipeline-Isolated Metadata Architecture Plan

## Core Concept

- **Setup phase**: Fetch all input artifacts from GitHub (everything except `database_export`)
- **Overlay structure**: Base folders + pipeline-specific overlays (`_ancrdt`, `_dpm`)
- **PR phase**: Push `database_export` + `joins_configuration` to pipeline-specific repos

---

## Phase 1: Resource Folder Restructure

**New folder structure:**

```
resources/
├── bird/                        # Base BIRD model (shared)
├── il/                          # SQL Developer IL (shared)
├── ldm/                         # SQL Developer LDM (shared)
├── derivation_files/            # Base derivation rules (shared)
│
├── joins_configuration/         # Base join configs (main/BIRD pipeline)
├── joins_configuration_ancrdt/  # ANCRDT pipeline joins
├── joins_configuration_dpm/     # DPM pipeline joins (FINREP, COREP, etc.)
│
├── database_export/             # Main/BIRD pipeline exports
├── database_export_ancrdt/      # ANCRDT pipeline exports
├── database_export_dpm/         # DPM pipeline exports
│
└── backups/{pipeline}/{timestamp}/  # Local backups
```

---

## Phase 2: Modify Artifact Fetching

**File: `pybirdai/utils/github_file_fetcher.py`**

### 2.1 Update `fetch_database_export_files()`

- **SKIP** fetching `database_export/` from source repo
- Continue fetching: `bird/`, `admin/`, `technical_export/`

### 2.2 Add `fetch_joins_configuration(pipeline)`

- Fetch base `joins_configuration/`
- Fetch pipeline overlay `joins_configuration_{pipeline}/` if exists
- Merge: overlay takes precedence over base

### 2.3 Add `fetch_all_setup_artifacts()`

Fetches everything needed for setup:
- SQL Developer CSVs (`il/`, `ldm/`)
- Derivation rules (`derivation_files/`)
- Test fixtures
- Report templates
- Join configurations (per pipeline)

**Excludes**: `database_export*/` folders (these are outputs, not inputs)

---

## Phase 3: Pipeline-Specific Repository Service

**New file: `pybirdai/services/pipeline_repo_service.py`**

```python
class PipelineRepoService:
    PIPELINES = ['main', 'ancrdt', 'dpm']
    REPO_NAMING = "pybird-{pipeline}"  # pybird-main, pybird-ancrdt, pybird-dpm

    def get_repo_url(pipeline: str) -> str
    def get_joins_path(pipeline: str) -> str  # joins_configuration_{pipeline}
    def get_export_path(pipeline: str) -> str  # database_export_{pipeline}
```

### Pipeline Repo Structure

Example for `pybird-dpm`:

```
pybird-dpm/
├── database_export/
│   ├── cube.csv
│   ├── member.csv
│   └── ... (framework-filtered SMCubes CSVs)
├── joins_configuration/
│   ├── cube_link.csv
│   ├── member_link.csv
│   └── ... (pipeline-specific join rules)
└── metadata.json  # pipeline version, frameworks, last updated
```

---

## Phase 4: PR Workflow (database_export + joins_config)

**Modify: `pybirdai/views/workflow/github.py`**

### 4.1 Update `export_database_to_github()`

- Detect current pipeline from session/context
- Export to pipeline-specific paths:
  - `database_export_{pipeline}/` → repo `database_export/`
  - `joins_configuration_{pipeline}/` → repo `joins_configuration/`
- Create PR with pipeline name in branch: `{pipeline}-update-{timestamp}`

### 4.2 Add pipeline selection to clone mode modal

- User selects pipeline before export
- Shows which files will be included

---

## Phase 5: Local Backup System

**New file: `pybirdai/services/backup_service.py`**

```python
class BackupService:
    def create_backup(pipeline: str, step: int) -> str:
        # Creates: resources/backups/{pipeline}/{timestamp}_step{N}/
        # Backs up: database_export_{pipeline}/ + joins_configuration_{pipeline}/

    def list_backups(pipeline: str) -> list
    def restore_backup(pipeline: str, backup_id: str) -> bool
```

### Auto-backup triggers

- Before DPM Step 1 (if existing data in `database_export_dpm/`)
- Before DPM Step 2 (if new table selection)
- Before ANCRDT import (if existing data)

---

## Phase 6: Pipeline Context Integration

**Modify: `pybirdai/context/sdd_context_django.py`**

### 6.1 Add `current_pipeline` property

### 6.2 Auto-detect pipeline from `current_frameworks`

| Frameworks | Pipeline |
|------------|----------|
| `ANCRDT` | `ancrdt` |
| `FINREP`, `COREP`, `AE`, etc. | `dpm` |
| `BIRD` only | `main` |

### 6.3 Use pipeline to resolve paths

- `get_joins_configuration_path()` → returns pipeline-specific path
- `get_database_export_path()` → returns pipeline-specific path

---

## Files to Create/Modify

| Action | File |
|--------|------|
| Create | `pybirdai/services/pipeline_repo_service.py` |
| Create | `pybirdai/services/backup_service.py` |
| Create | `pybirdai/config/pipeline_repos.json` |
| Modify | `pybirdai/utils/github_file_fetcher.py` |
| Modify | `pybirdai/context/sdd_context_django.py` |
| Modify | `pybirdai/views/workflow/github.py` |
| Modify | `pybirdai/templates/pybirdai/workflow/clone_mode_modal.html` |

---

## Summary

| Phase | Fetch | PR |
|-------|-------|-----|
| Setup | Everything except `database_export*/` | - |
| Workflow | Join configs (pipeline-specific) | - |
| Export | - | `database_export` + `joins_configuration` |

**3 Pipelines**: `main`, `ancrdt`, `dpm` - each with isolated exports and join configs.

---

## Implementation Order

1. **Phase 1**: Create folder structure (manual or script)
2. **Phase 3**: Create `PipelineRepoService` (foundation for other phases)
3. **Phase 6**: Add pipeline context integration
4. **Phase 2**: Modify artifact fetching to use pipeline paths
5. **Phase 5**: Add backup service
6. **Phase 4**: Update PR workflow with pipeline selection
