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