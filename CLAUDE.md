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

## Architecture

### Core Structure
- **`birds_nest/`** - Django project root
- **`pybirdai/`** - Main Django app containing all BIRD processing logic
- **`resources/`** - Input data files (CSV imports, technical exports, etc.)
- **`results/`** - Generated outputs (Python code, HTML reports, database configs)

### Key Components

**Entry Points (`pybirdai/entry_points/`):**
22 modules orchestrating the complete BIRD data processing pipeline:
1. **Database Setup:** `automode_database_setup.py`, `create_django_models.py`
2. **Data Import:** `import_input_model.py`, `upload_technical_export_files.py`
3. **Hierarchy Processing:** `convert_ldm_to_sdd_hierarchies.py`, `create_joins_metadata.py`
4. **Code Generation:** `create_filters.py`, `create_executable_joins.py`
5. **Execution:** `execute_datapoint.py`

**Models (`bird_meta_data_model.py`):**
Django models representing BIRD metadata structures (variables, members, cubes, mappings, transformations)

**Process Steps (`process_steps/`):**
Lower-level processing modules organized by function (automode, hierarchy conversion, joins metadata, etc.)

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

**Key Views:**
- Web interface for BIRD data processing workflows
- REST endpoints for AJAX operations
- Report rendering with HTML templates
- CSV data visualization

**Templates:** Located in `pybirdai/templates/` with extensive UI for data management

The system supports both automated ("automode") and manual step-by-step processing of regulatory banking data according to BIRD/FINREP standards.

## DPM Workflow

The DPM (Data Point Model) workflow processes EBA DPM data for regulatory reporting frameworks (FINREP, COREP, etc.). The workflow consists of 6 sequential steps:

### Step 1: Extract DPM Metadata
**Entry Point:** `pybirdai/entry_points/import_dpm_data.py::run_import_phase_a()`

Downloads DPM database from EBA website and extracts lightweight metadata:
- Frameworks (user-selected: FINREP, COREP, AE, FP, etc.)
- Domains, Metrics, Members, Dimensions/Variables
- Hierarchies and Hierarchy Nodes
- **Tables** (writes to `results/technical_export/table.csv`)

This step stops before the expensive ordinate explosion operation. Output CSVs are written to `results/technical_export/` for user review.

### Step 2: Process & Import Selected Tables
**Entry Point:** `pybirdai/entry_points/import_dpm_data.py::run_import_phase_b()`

When clicking "Do" on Step 2, a modal appears allowing table selection by:
- Framework (FINREP, COREP, etc.)
- Search by table code/name
- Saved presets for common selections

After selection confirmation:
1. Reloads metadata CSVs from Step 1
2. Filters tables based on user selection
3. Runs ordinate explosion for selected tables only (THE EXPENSIVE OPERATION)
4. Maps axes, ordinates, cells, and positions
5. Imports everything into Django database
6. Creates Float subdomain for MTRC variable

**This step combines table processing with database import.**

### Step 3: Create Output Layers
**Entry Point:** `pybirdai/entry_points/dpm_output_layer_creation.py`

Creates cube structures from DPM tables:
- Generates CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM models
- Can process specific framework/version/table or batch process multiple tables
- Creates cube links and combinations for transformations

### Step 4: Create Transformation Rules
**Entry Points:**
- `create_filters.py::run_create_filters()`
- `create_joins_metadata.py::run_create_joins_meta_data()`

Generates metadata for data transformations:
- Creates filter rules for output layer cubes
- Generates joins metadata (CUBE_LINK, MEMBER_LINK, etc.)
- Stores transformation logic in database models

### Step 5: Generate Python Code
**Entry Points:**
- `run_create_executable_filters.py::run_create_executable_filters_from_db()`
- `create_executable_joins.py::run_create_executable_joins()`

Generates executable Python code from metadata:
- Creates filter Python files in `pybirdai/process_steps/filter_code/F_*.py`
- Creates join Python files in `pybirdai/process_steps/join_code/J_*.py`
- Code is dynamically generated based on cube structures

### Step 6: Execute DPM Tests
**Entry Point:** `pybirdai/utils/datapoint_test_run/run_tests.py`

Runs regulatory template tests:
- Looks for test suite in `tests/dpm/`
- Executes test configuration from `configuration_file_tests.json`
- Validates data point calculations against expected results
- Generates test reports

### Workflow Access
All DPM workflow steps are accessible via the main dashboard at `/pybirdai/workflow/dashboard/`:
- Click "Do" to execute a step
- Click "Review" to view step results
- Step 3 includes a cube structure viewer for inspecting output layers

### Key Features
- **Table filtering:** Only process the tables you need (Step 2 modal)
- **Preset support:** Save and reuse common table selections
- **Re-executable Step 2:** Run Step 2 multiple times with different table selections
- **Framework selection:** Choose which frameworks to process (FINREP, COREP, etc.)