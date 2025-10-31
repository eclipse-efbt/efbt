# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Eclipse Free BIRD Tools (PyBIRD AI) - A Django web application for processing Banks' Integrated Reporting Dictionary (BIRD) data. This system converts financial regulatory data models (LDM/EIL/ELDM) into structured data definitions (SDD) and generates executable transformations for regulatory reporting (primarily FINREP).

**Key URLs:**
- Wiki: https://github.com/eclipse/efbt/wiki
- Getting Started: https://github.com/eclipse-efbt/efbt/wiki/Getting-Started-with-PyBIRD-AI

## Development Commands

**Navigate to project directory:**
```bash
cd birds_nest
```

**Install dependencies:**
```bash
pip install --upgrade pip
python -m pip install django==5.1.3
python -m pip install pyecore==0.15.1
python -m pip install pytest==8.3.4
python -m pip install pytest-xdist==3.6.1
python -m pip install ruff==0.9.7
```

**Run Django development server:**
```bash
python manage.py runserver
```

**Alternative: Run with startup script (auto-restart on failure):**
```bash
cd birds_nest
bash startup.sh
```

**Database operations:**
```bash
python manage.py makemigrations
python manage.py migrate
```

**Run tests:**
```bash
python pybirdai/utils/run_tests.py --uv "False" --config-file "tests/configuration_file_tests.json"
```

**Django management commands:**
```bash
python manage.py complete_automode_setup  # Complete automode database setup
```

## Development Guidelines

### Views Development
- **Model Association:** Each view file should be linked to specific Django models
- **DO NOT add new views to `core_views.py`** - It is already large (~4,695 lines) and should not grow further
- **Create specialized view files** for new functionality areas instead
- **Follow the pattern:** Views linked to model domains (e.g., lineage views → lineage models, workflow views → workflow models)
- **Harmonization in progress:** The project is actively reorganizing views to maintain clean separation of concerns

### Code Organization Principles
- **Separation of Concerns:** Keep views, APIs, and business logic properly separated
- **Model-Centric Design:** Group functionality around Django models
- **Avoid Monolithic Files:** Split large modules into focused, maintainable components

## Architecture

### Core Structure
- **`birds_nest/`** - Django project root
- **`pybirdai/`** - Main Django app containing all BIRD processing logic
- **`resources/`** - Input data files (CSV imports, technical exports, etc.)
- **`results/`** - Generated outputs (Python code, HTML reports, database configs)

### Key Components

**Views (`pybirdai/views/`):**
7 specialized view modules organized by functionality and linked to specific models:
- **`core_views.py`** (~4,695 lines) - Core CRUD operations, data import/export, CSV template system
  - **Linked Models:** `VARIABLE`, `MEMBER`, `CUBE`, `MAPPING_DEFINITION`, `VARIABLE_MAPPING`, `MEMBER_MAPPING`, `COMBINATION`, etc.
  - **Key Functions:** Entry point orchestration, CRUD for BIRD metadata, file uploads, CSV export/import
- **`workflow_views.py`** (~4,244 lines) - Workflow orchestration, automode execution, task management
  - **Linked Models:** `WorkflowSession`, `WorkflowTaskExecution`, `DPMProcessExecution`, `AnaCreditProcessExecution`
  - **Key Functions:** Workflow dashboard, automode execution, database setup, GitHub integration
- **`lineage_views.py`** (~680 lines) - Data lineage visualization and trail management
  - **Linked Models:** `Trail`, `MetaDataTrail`, `DatabaseTable`, `DerivedTable`, `DatabaseRow`, `DatabaseColumnValue`
  - **Key Functions:** Trail viewer, lineage data API, node details
- **`bpmn_metadata_lineage_views.py`** (~362 lines) - BPMN-based metadata lineage for datapoints
  - **Linked Models:** `UserTask`, `ServiceTask`, `SubProcess`, `SequenceFlow`, `WorkflowModule`
  - **Key Functions:** BPMN lineage viewer, process datapoint lineage, lineage graph API
- **`aorta_views.py`** (~274 lines) - REST API for trail management
  - **Linked Models:** `Trail`, `DatabaseTable`, `DerivedTable` (lineage models)
  - **Key Functions:** Trail list/detail views, table data retrieval, function details
- **`report_views.py`** (~143 lines) - Report generation and CSV validation reports
  - **Linked Models:** None (filesystem-based CSV reports)
  - **Key Functions:** Mapping/hierarchy warnings, review pages, template discovery
- **`ancrdt_transformation_views.py`** (~100 lines) - ANCRDT (AnaCredit) transformation workflow
  - **Linked Models:** Uses BIRD metadata models through entry points
  - **Key Functions:** 4-step ANCRDT workflow (fetch, import, create joins, execute)

**API (`pybirdai/api/`):**
3 REST API modules providing programmatic access:
- **`lineage_api.py`** - Complete lineage export API (comprehensive JSON export)
- **`enhanced_lineage_api.py`** - Filtered lineage with calculation tracking
- **`workflow_api.py`** - GitHub integration services, configuration management

**Entry Points (`pybirdai/entry_points/`):**
28 modules orchestrating the complete BIRD data processing pipeline:
1. **Database Setup:** `automode_database_setup.py`, `create_django_models.py`, `delete_bird_metadata_database.py`
2. **Data Import:** `import_input_model.py`, `upload_technical_export_files.py`, `upload_sqldev_eil_files.py`, `import_dpm_data.py`
3. **Hierarchy Processing:** `convert_ldm_to_sdd_hierarchies.py`, `create_joins_metadata.py`
4. **Code Generation:** `create_filters.py`, `create_executable_joins.py`, `run_create_executable_filters.py`
5. **Execution:** `execute_datapoint.py`, `metadata_lineage_processor.py`
6. **Template System:** `template_mapping_definition.py` (CSV-based mapping import/export)
7. **DPM Integration:** `dpm_output_layer_creation.py`

**Models (`pybirdai/models/`):**
5 model files with 106+ Django models:
- **`bird_meta_data_model.py`** (~33 models) - Core BIRD metadata (VARIABLE, MEMBER, CUBE, MAPPING_DEFINITION, etc.)
- **`lineage_model.py`** (~27 models) - Execution lineage tracking (Trail, DatabaseTable, DatabaseRow, CalculationUsedRow, etc.)
- **`workflow_model.py`** (~6 models) - Workflow configuration (WorkflowSession, WorkflowTaskExecution, DPMProcessExecution)
- **`bpmn_lite_models.py`** (~25 models) - BPMN visualization (UserTask, ServiceTask, SubProcess, SequenceFlow)
- **`requirements_text_models.py`** (~15 models) - Requirements management

**Process Steps (`pybirdai/process_steps/`):**
19 subdirectories containing lower-level processing modules:
- **Core:** `automode/`, `input_model/`, `hierarchy_conversion/`, `joins_meta_data/`, `generate_etl/`, `pybird/`
- **Integration:** `sqldeveloper_import/`, `upload_files/`, `website_to_sddmodel/`, `dpm_integration/`, `ancrdt_transformation/`
- **Filter/Report:** `filter_code/` (FINREP filter logic with automatic tracking), `report_filters/nrolc/` (Non-Reference Output Layer Creator)
- **Advanced:** `template_mapping_definition/` (business-friendly CSV mapping system), `metadata_lineage/`, `mapping_join_metadata_eil_ldm/`

**Context System (`context/`):**
Configuration and file management using `SDDContext` and `Context` objects

### Data Processing Workflow
1. **Setup** → Create database models and migrate
2. **Import** → Load LDM/EIL/ELDM models and technical exports
3. **Transform** → Convert hierarchies, create join metadata
4. **Generate** → Create executable Python filters and joins
5. **Execute** → Run data point calculations

### File Organization
- **Input:** CSV files in `resources/` (ldm/, technical_export/, joins_configuration/)
- **Output:** Generated Python code in `results/generated_python_*/`
- **Configuration:** Django settings, URL patterns, template system

## Django App Structure

**Main URLs:** 140+ endpoints in `pybirdai/urls.py` covering:
- Automode setup and step-by-step workflows
- Data import/export operations
- Metadata editing (mappings, cubes, variables)
- Report generation and visualization
- File upload handling
- Lineage visualization and trail management
- BPMN metadata lineage
- Workflow orchestration and task management

**Views Architecture:**
The application uses a modular views architecture with 7 specialized view modules (see Architecture section above):
- `core_views.py` - Core CRUD operations and data import/export
- `workflow_views.py` - Workflow orchestration and automode execution
- `lineage_views.py` - Data lineage visualization
- `bpmn_metadata_lineage_views.py` - BPMN-based lineage
- `aorta_views.py` - REST API for trail management
- `report_views.py` - Report generation
- `ancrdt_transformation_views.py` - ANCRDT workflow

**API Endpoints:**
REST APIs for programmatic access (see API section in Architecture):
- Complete lineage export (`lineage_api.py`)
- Filtered lineage with calculation tracking (`enhanced_lineage_api.py`)
- GitHub integration and configuration services (`workflow_api.py`)

**Templates:** Located in `pybirdai/templates/` with extensive UI for data management, organized by functionality:
- `pybirdai/workflow/` - Workflow dashboard, DPM review, ANCRDT review
- Main templates for data editing, mapping management, lineage visualization

The system supports both automated ("automode") and manual step-by-step processing of regulatory banking data according to BIRD/FINREP standards.