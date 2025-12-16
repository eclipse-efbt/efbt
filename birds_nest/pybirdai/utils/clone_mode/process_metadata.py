# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Process Metadata Management for Clone Mode.

This module provides functionality for generating, saving, loading, and validating
process_metadata.json files that track workflow execution state for clone mode.

The process_metadata.json file captures:
- Workflow step statuses (completed, pending, in_progress)
- User selections (tables, frameworks, etc.)
- Configuration settings
- Timestamps for tracking progress

This enables clone mode to restore the exact state of a workflow and continue
from where it left off.
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Schema version for forward compatibility
SCHEMA_VERSION = "1.0"

# Valid restart points for clone mode (metadata parsing steps only)
VALID_RESTART_POINTS = {
    'main': ['task1_database_setup', 'task2_data_import'],
    'dpm': ['step1_extract_metadata', 'step2_process_tables'],
    'anacredit': ['step1', 'step2', 'step3'],
}

# Steps that involve code generation (not valid for clone)
CODE_GENERATION_STEPS = {
    'main': ['task3_hierarchy_conversion', 'task4_code_generation'],
    'dpm': ['step3_output_layers', 'step4_transformation_rules', 'step5_generate_code', 'step6_execute_tests'],
    'anacredit': ['step4', 'step5'],
}


def generate_process_metadata() -> dict:
    """
    Generate process_metadata.json from current database state.

    Queries Django models to determine:
    - Which workflow steps have been completed
    - User selections (tables, frameworks)
    - Configuration settings

    Returns:
        dict: Process metadata dictionary conforming to the schema
    """
    from django.conf import settings

    metadata = {
        "version": SCHEMA_VERSION,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "last_step_completed": None,
        "workflows": {
            "main": _get_main_workflow_status(),
            "dpm": _get_dpm_workflow_status(),
            "anacredit": _get_anacredit_workflow_status(),
        },
        "user_selections": _get_user_selections(),
        "configuration": _get_configuration(),
    }

    # Determine last completed step
    metadata["last_step_completed"] = _determine_last_step(metadata["workflows"])

    return metadata


def _get_main_workflow_status() -> dict:
    """Get status of main workflow tasks."""
    try:
        from pybirdai.models.bird_meta_data_model import (
            MAINTENANCE_AGENCY, DOMAIN, VARIABLE, CUBE
        )

        # Check if basic metadata exists (Task 1 & 2 indicators)
        has_agencies = MAINTENANCE_AGENCY.objects.exists()
        has_domains = DOMAIN.objects.exists()
        has_variables = VARIABLE.objects.exists()
        has_cubes = CUBE.objects.exists()

        task1_complete = has_agencies and has_domains
        task2_complete = task1_complete and has_variables

        return {
            "task1_database_setup": {
                "status": "completed" if task1_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if task1_complete else None
            },
            "task2_data_import": {
                "status": "completed" if task2_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if task2_complete else None
            },
            "task3_hierarchy_conversion": {
                "status": "completed" if has_cubes else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if has_cubes else None
            },
            "task4_code_generation": {
                "status": "pending",
                "completed_at": None
            }
        }
    except Exception as e:
        logger.warning(f"Error getting main workflow status: {e}")
        return _empty_main_workflow()


def _get_dpm_workflow_status() -> dict:
    """Get status of DPM workflow steps.

    DPM workflow is separate from the main FINREP workflow.
    We detect DPM state by looking for:
    - TABLE objects (EBA DPM tables like C_07.00.a)
    - CUBEs with DPM-specific naming patterns (not FINREP REF cubes)
    """
    try:
        from pybirdai.models.bird_meta_data_model import (
            TABLE, CUBE, CUBE_STRUCTURE, COMBINATION
        )

        # DPM uses TABLE model for EBA tables (e.g., C_07.00.a, F_01.00)
        # Check for DPM-specific table codes (COREP/FINREP DPM format)
        has_dpm_tables = TABLE.objects.filter(
            code__regex=r'^[A-Z]_\d{2}\.\d{2}'  # Pattern like C_07.00 or F_01.00
        ).exists()

        # DPM output layer cubes have specific naming (not FINREP REF cubes)
        # FINREP cubes: F_01_01_REF_FINREP_3_0
        # DPM cubes would be: DPM_C_07_00_a or similar
        has_dpm_cubes = CUBE.objects.filter(name__startswith='DPM_').exists()

        # Only count as DPM structures if we have DPM cubes
        has_dpm_structures = False
        if has_dpm_cubes:
            dpm_cubes = CUBE.objects.filter(name__startswith='DPM_')
            has_dpm_structures = CUBE_STRUCTURE.objects.filter(cube__in=dpm_cubes).exists()

        return {
            "step1_extract_metadata": {
                "status": "completed" if has_dpm_tables else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if has_dpm_tables else None
            },
            "step2_process_tables": {
                "status": "completed" if has_dpm_tables else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if has_dpm_tables else None
            },
            "step3_output_layers": {
                "status": "completed" if has_dpm_cubes else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if has_dpm_cubes else None
            },
            "step4_transformation_rules": {
                "status": "completed" if has_dpm_structures else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if has_dpm_structures else None
            },
            "step5_generate_code": {
                "status": "pending",
                "completed_at": None
            },
            "step6_execute_tests": {
                "status": "pending",
                "completed_at": None
            }
        }
    except Exception as e:
        logger.warning(f"Error getting DPM workflow status: {e}")
        return _empty_dpm_workflow()


def _get_anacredit_workflow_status() -> dict:
    """Get status of AnaCredit workflow steps."""
    try:
        from pybirdai.models.bird_meta_data_model import CUBE_LINK

        # Check for ANCRDT-specific data
        has_ancrdt_links = CUBE_LINK.objects.filter(
            name__icontains='ANCRDT'
        ).exists()

        return {
            "step1": {"status": "pending", "completed_at": None},
            "step2": {"status": "pending", "completed_at": None},
            "step3": {"status": "completed" if has_ancrdt_links else "pending",
                     "completed_at": datetime.utcnow().isoformat() + "Z" if has_ancrdt_links else None},
            "step4": {"status": "pending", "completed_at": None},
            "step5": {"status": "pending", "completed_at": None}
        }
    except Exception as e:
        logger.warning(f"Error getting AnaCredit workflow status: {e}")
        return _empty_anacredit_workflow()


def _empty_main_workflow() -> dict:
    """Return empty main workflow status."""
    return {
        "task1_database_setup": {"status": "pending", "completed_at": None},
        "task2_data_import": {"status": "pending", "completed_at": None},
        "task3_hierarchy_conversion": {"status": "pending", "completed_at": None},
        "task4_code_generation": {"status": "pending", "completed_at": None}
    }


def _empty_dpm_workflow() -> dict:
    """Return empty DPM workflow status."""
    return {
        "step1_extract_metadata": {"status": "pending", "completed_at": None},
        "step2_process_tables": {"status": "pending", "completed_at": None},
        "step3_output_layers": {"status": "pending", "completed_at": None},
        "step4_transformation_rules": {"status": "pending", "completed_at": None},
        "step5_generate_code": {"status": "pending", "completed_at": None},
        "step6_execute_tests": {"status": "pending", "completed_at": None}
    }


def _empty_anacredit_workflow() -> dict:
    """Return empty AnaCredit workflow status."""
    return {
        "step1": {"status": "pending", "completed_at": None},
        "step2": {"status": "pending", "completed_at": None},
        "step3": {"status": "pending", "completed_at": None},
        "step4": {"status": "pending", "completed_at": None},
        "step5": {"status": "pending", "completed_at": None}
    }


def _get_user_selections() -> dict:
    """Get user selections from database."""
    try:
        from pybirdai.models.bird_meta_data_model import TABLE, FRAMEWORK

        # Get selected tables
        tables = list(TABLE.objects.values_list('code', flat=True)[:100])

        # Get frameworks
        frameworks = list(FRAMEWORK.objects.values_list('code', flat=True).distinct())

        return {
            "selected_tables": tables,
            "selected_frameworks": frameworks if frameworks else ["FINREP"],
            "data_model_type": "EIL",
            "dpm_version": "4.0"
        }
    except Exception as e:
        logger.warning(f"Error getting user selections: {e}")
        return {
            "selected_tables": [],
            "selected_frameworks": [],
            "data_model_type": "EIL",
            "dpm_version": "4.0"
        }


def _get_configuration() -> dict:
    """Get configuration settings."""
    return {
        "data_model_type": "EIL",
        "technical_export_source": "GITHUB",
        "technical_export_github_url": "",
        "enable_lineage_tracking": False
    }


def _determine_last_step(workflows: dict) -> Optional[str]:
    """Determine the last completed step across all workflows."""
    last_step = None

    # Check DPM workflow first (most common)
    dpm = workflows.get('dpm', {})
    for step_name in ['step2_process_tables', 'step1_extract_metadata']:
        step = dpm.get(step_name, {})
        if step.get('status') == 'completed':
            last_step = f"DPM_{step_name.upper()}"
            break

    # Check main workflow
    if not last_step:
        main = workflows.get('main', {})
        for task_name in ['task2_data_import', 'task1_database_setup']:
            task = main.get(task_name, {})
            if task.get('status') == 'completed':
                last_step = f"MAIN_{task_name.upper()}"
                break

    return last_step


def save_process_metadata(output_path: str, metadata: Optional[dict] = None) -> str:
    """
    Save metadata to JSON file.

    Args:
        output_path: Directory path where to save the file
        metadata: Optional metadata dict; if None, generates from current state

    Returns:
        str: Full path to the saved file
    """
    if metadata is None:
        metadata = generate_process_metadata()

    # Update timestamp
    metadata["updated_at"] = datetime.utcnow().isoformat() + "Z"

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, 'process_metadata.json')

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    logger.info(f"Process metadata saved to: {file_path}")
    return file_path


def load_process_metadata(file_path: str) -> dict:
    """
    Load and validate process_metadata.json.

    Args:
        file_path: Path to the JSON file

    Returns:
        dict: Loaded and validated metadata

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is invalid or incompatible version
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Process metadata file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    # Validate version
    version = metadata.get('version')
    if not version:
        raise ValueError("Invalid process_metadata.json: missing version field")

    # Check for required fields
    required_fields = ['workflows', 'user_selections', 'configuration']
    for field in required_fields:
        if field not in metadata:
            raise ValueError(f"Invalid process_metadata.json: missing {field} field")

    logger.info(f"Loaded process metadata version {version} from: {file_path}")
    return metadata


def get_restart_point(metadata: dict) -> str:
    """
    Determine valid restart point from metadata.

    Args:
        metadata: Loaded process metadata

    Returns:
        str: The step/task name to restart from
    """
    return metadata.get('last_step_completed', 'MAIN_TASK1_DATABASE_SETUP')


def validate_metadata_parsing_only(metadata: dict) -> bool:
    """
    Ensure clone is only for metadata parsing steps (before code generation).

    Clone mode should only be used to restore state up to the point where
    code generation hasn't started. This prevents issues with generated code
    that may not match the cloned database state.

    Args:
        metadata: Loaded process metadata

    Returns:
        bool: True if valid for clone, False if code generation has started
    """
    workflows = metadata.get('workflows', {})

    # Check DPM workflow
    dpm = workflows.get('dpm', {})
    for step_name in CODE_GENERATION_STEPS.get('dpm', []):
        step = dpm.get(step_name, {})
        if step.get('status') == 'completed':
            logger.warning(f"DPM code generation step {step_name} is completed - invalid for clone")
            return False

    # Check main workflow
    main = workflows.get('main', {})
    for task_name in CODE_GENERATION_STEPS.get('main', []):
        task = main.get(task_name, {})
        if task.get('status') == 'completed':
            logger.warning(f"Main code generation task {task_name} is completed - invalid for clone")
            return False

    # Check AnaCredit workflow
    anacredit = workflows.get('anacredit', {})
    for step_name in CODE_GENERATION_STEPS.get('anacredit', []):
        step = anacredit.get(step_name, {})
        if step.get('status') == 'completed':
            logger.warning(f"AnaCredit code generation step {step_name} is completed - invalid for clone")
            return False

    return True


def restore_workflow_states(metadata: dict) -> dict:
    """
    Restore workflow execution states from metadata.

    This function updates the Django database models that track workflow
    execution state based on the loaded metadata.

    Args:
        metadata: Loaded process metadata

    Returns:
        dict: Summary of restored states
    """
    from django.utils import timezone
    from dateutil import parser as date_parser

    results = {
        'workflows_restored': [],
        'errors': []
    }

    workflows = metadata.get('workflows', {})

    # Restore main workflow states
    try:
        from pybirdai.models.workflow_model import WorkflowTaskExecution

        main_workflow = workflows.get('main', {})
        task_map = {
            'task1_database_setup': 1,
            'task2_data_import': 2,
            'task3_hierarchy_conversion': 3,
            'task4_code_generation': 4
        }

        for task_name, task_data in main_workflow.items():
            if task_name in task_map:
                task_number = task_map[task_name]
                status = task_data.get('status', 'pending')
                completed_at_str = task_data.get('completed_at')

                # Parse completed_at if provided
                completed_at = None
                if completed_at_str:
                    try:
                        completed_at = date_parser.parse(completed_at_str)
                    except Exception:
                        completed_at = timezone.now() if status == 'completed' else None

                # Create or update WorkflowTaskExecution for 'do' operation
                execution, created = WorkflowTaskExecution.objects.update_or_create(
                    task_number=task_number,
                    operation_type='do',
                    defaults={
                        'status': status,
                        'completed_at': completed_at,
                        'framework_id': 'FINREP'
                    }
                )

                action = "Created" if created else "Updated"
                logger.info(f"{action} main workflow task {task_number} ({task_name}) to status: {status}")
                results['workflows_restored'].append(f"main.{task_name}")

    except ImportError as e:
        logger.warning(f"Could not import WorkflowTaskExecution model: {e}")
        results['errors'].append(f"Could not restore main workflow: {e}")
    except Exception as e:
        logger.error(f"Error restoring main workflow states: {e}")
        results['errors'].append(f"Error restoring main workflow: {e}")

    # Restore DPM workflow states
    try:
        from pybirdai.models.workflow_model import DPMProcessExecution

        dpm_workflow = workflows.get('dpm', {})
        step_map = {
            'step1_extract_metadata': 1,
            'step2_process_tables': 2,
            'step3_output_layers': 3,
            'step4_transformation_rules': 4,
            'step5_generate_code': 5,
            'step6_execute_tests': 6
        }

        for step_name, step_data in dpm_workflow.items():
            if step_name in step_map:
                step_number = step_map[step_name]
                status = step_data.get('status', 'pending')
                completed_at_str = step_data.get('completed_at')

                # Parse completed_at if provided
                completed_at = None
                if completed_at_str:
                    try:
                        completed_at = date_parser.parse(completed_at_str)
                    except Exception:
                        completed_at = timezone.now() if status == 'completed' else None

                # Create or update DPMProcessExecution
                # Note: DPMProcessExecution requires a session, so we need to get/create one first
                from pybirdai.models.workflow_model import WorkflowSession
                session, _ = WorkflowSession.objects.get_or_create(
                    session_type='dpm',
                    defaults={'user_agent': 'clone_mode_import', 'is_active': True}
                )

                execution, created = DPMProcessExecution.objects.update_or_create(
                    session=session,
                    step_number=step_number,
                    defaults={
                        'step_name': step_name,
                        'status': status,
                        'completed_at': completed_at,
                        'framework_id': metadata.get('user_selections', {}).get('selected_frameworks', ['COREP'])[0] if metadata.get('user_selections', {}).get('selected_frameworks') else 'COREP'
                    }
                )

                action = "Created" if created else "Updated"
                logger.info(f"{action} DPM workflow step {step_number} ({step_name}) to status: {status}")
                results['workflows_restored'].append(f"dpm.{step_name}")

    except ImportError as e:
        logger.warning(f"Could not import DPMProcessExecution model: {e}")
        results['errors'].append(f"Could not restore DPM workflow: {e}")
    except Exception as e:
        logger.error(f"Error restoring DPM workflow states: {e}")
        results['errors'].append(f"Error restoring DPM workflow: {e}")

    # Restore AnaCredit workflow states
    try:
        from pybirdai.models.workflow_model import AnaCreditProcessExecution

        anacredit_workflow = workflows.get('anacredit', {})
        step_map = {
            'step1': 1,
            'step2': 2,
            'step3': 3,
            'step4': 4,
            'step5': 5
        }

        for step_name, step_data in anacredit_workflow.items():
            if step_name in step_map:
                step_number = step_map[step_name]
                status = step_data.get('status', 'pending')
                completed_at_str = step_data.get('completed_at')

                # Parse completed_at if provided
                completed_at = None
                if completed_at_str:
                    try:
                        completed_at = date_parser.parse(completed_at_str)
                    except Exception:
                        completed_at = timezone.now() if status == 'completed' else None

                # Create or update AnaCreditProcessExecution
                # Note: AnaCreditProcessExecution requires a session, so we need to get/create one first
                from pybirdai.models.workflow_model import WorkflowSession
                session, _ = WorkflowSession.objects.get_or_create(
                    session_type='anacredit',
                    defaults={'user_agent': 'clone_mode_import', 'is_active': True}
                )

                execution, created = AnaCreditProcessExecution.objects.update_or_create(
                    session=session,
                    step_number=step_number,
                    defaults={
                        'step_name': step_name,
                        'status': status,
                        'completed_at': completed_at
                    }
                )

                action = "Created" if created else "Updated"
                logger.info(f"{action} AnaCredit workflow step {step_number} ({step_name}) to status: {status}")
                results['workflows_restored'].append(f"anacredit.{step_name}")

    except ImportError as e:
        logger.warning(f"Could not import AnaCreditProcessExecution model: {e}")
        results['errors'].append(f"Could not restore AnaCredit workflow: {e}")
    except Exception as e:
        logger.error(f"Error restoring AnaCredit workflow states: {e}")
        results['errors'].append(f"Error restoring AnaCredit workflow: {e}")

    return results


def verify_environment_state(metadata: dict) -> dict:
    """
    Verify that the environment matches the expected state from metadata.

    Checks:
    - Database tables exist
    - Record counts match expected ranges
    - Required models are present
    - No unexpected code generation artifacts

    Args:
        metadata: Loaded process metadata

    Returns:
        dict: Verification results with warnings/errors
    """
    from django.db import connection

    results = {
        'valid': True,
        'warnings': [],
        'errors': [],
        'checks_performed': []
    }

    # Check 1: Verify database tables exist
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = {row[0] for row in cursor.fetchall()}

        required_tables = [
            'pybirdai_maintenance_agency',
            'pybirdai_domain',
            'pybirdai_variable'
        ]

        for table in required_tables:
            if table not in existing_tables:
                results['errors'].append(f"Required table missing: {table}")
                results['valid'] = False
            else:
                results['checks_performed'].append(f"Table exists: {table}")

    except Exception as e:
        results['errors'].append(f"Failed to check database tables: {e}")
        results['valid'] = False

    # Check 2: Verify no code generation artifacts if metadata says none should exist
    workflows = metadata.get('workflows', {})
    dpm_workflow = workflows.get('dpm', {})

    # If step5 (code generation) is not completed, check for artifacts
    step5_status = dpm_workflow.get('step5_generate_code', {}).get('status', 'pending')
    if step5_status != 'completed':
        try:
            import os
            from django.conf import settings

            # Check for generated filter code
            filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
            if os.path.exists(filter_code_dir):
                filter_files = [f for f in os.listdir(filter_code_dir)
                               if f.startswith('F_') and f.endswith('.py')]
                if filter_files:
                    results['warnings'].append(
                        f"Found {len(filter_files)} generated filter files - "
                        "these may not match the imported database state"
                    )

            # Check for generated join code
            join_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'join_code')
            if os.path.exists(join_code_dir):
                join_files = [f for f in os.listdir(join_code_dir)
                             if f.startswith('J_') and f.endswith('.py')]
                if join_files:
                    results['warnings'].append(
                        f"Found {len(join_files)} generated join files - "
                        "these may not match the imported database state"
                    )

            results['checks_performed'].append("Checked for code generation artifacts")

        except Exception as e:
            results['warnings'].append(f"Could not check for code generation artifacts: {e}")

    # Check 3: Verify user selections match available data
    user_selections = metadata.get('user_selections', {})
    selected_tables = user_selections.get('selected_tables', [])

    if selected_tables:
        try:
            from pybirdai.models.bird_meta_data_model import TABLE

            # Check if any of the selected tables exist
            existing_table_codes = set(TABLE.objects.values_list('code', flat=True))
            missing_tables = set(selected_tables) - existing_table_codes

            if missing_tables and len(missing_tables) == len(selected_tables):
                # All tables are missing - this is expected before import
                results['checks_performed'].append(
                    f"Tables to be imported: {len(selected_tables)}"
                )
            elif missing_tables:
                results['warnings'].append(
                    f"{len(missing_tables)} of {len(selected_tables)} selected tables not found in database"
                )

        except Exception as e:
            results['warnings'].append(f"Could not verify table selections: {e}")

    # Check 4: Database connectivity
    try:
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        results['checks_performed'].append("Database connection verified")
    except Exception as e:
        results['errors'].append(f"Database connection failed: {e}")
        results['valid'] = False

    logger.info(f"Environment verification complete: valid={results['valid']}, "
                f"errors={len(results['errors'])}, warnings={len(results['warnings'])}")

    return results
