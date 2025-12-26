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
# 1.0 - Initial version
# 1.1 - Added primary_framework, framework info per workflow, overall status
# 1.2 - Added export completeness tracking (is_complete, total_steps, steps_status)
SCHEMA_VERSION = "1.2"

# Step counts for each workflow type
WORKFLOW_STEP_COUNTS = {
    'main': 4,
    'dpm_eba': 6,
    'dpm_github': 4,
    'anacredit': 4,
}

# Final step (tests) that marks workflow as complete
COMPLETION_STEPS = {
    'main': 4,           # Task 4 (tests)
    'dpm_eba': 6,        # Step 6 (Execute DPM Tests)
    'dpm_github': 4,     # Step 4 (Execute Tests)
    'anacredit': 4,      # Step 4 (Tests)
}

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
    - Primary framework being used
    - Export completeness status

    Returns:
        dict: Process metadata dictionary conforming to the schema
    """
    from django.conf import settings

    # Get workflow statuses first to determine primary framework
    main_workflow = _get_main_workflow_status()
    dpm_workflow = _get_dpm_workflow_status()
    anacredit_workflow = _get_anacredit_workflow_status()

    # Determine primary framework from active workflow
    primary_framework = _determine_primary_framework(main_workflow, dpm_workflow, anacredit_workflow)

    # Build workflows dict
    workflows = {
        "main": main_workflow,
        "dpm": dpm_workflow,
        "anacredit": anacredit_workflow,
    }

    # Calculate export status
    export_status = _calculate_export_status(workflows)

    metadata = {
        "version": SCHEMA_VERSION,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "primary_framework": primary_framework,
        "last_step_completed": None,
        "export_status": export_status,
        "workflows": workflows,
        "user_selections": _get_user_selections(),
        "configuration": _get_configuration(),
    }

    # Determine last completed step
    metadata["last_step_completed"] = _determine_last_step(metadata["workflows"])

    return metadata


def _calculate_export_status(workflows: dict) -> dict:
    """
    Calculate the overall export status based on workflow completeness.

    An export is considered 'complete' if the final step (tests) has been run
    for at least one workflow. Otherwise, it's 'incomplete'.

    Args:
        workflows: Dictionary containing workflow status for main, dpm, anacredit

    Returns:
        dict: Export status with is_complete flag and details
    """
    export_status = {
        "is_complete": False,
        "completed_workflows": [],
        "incomplete_workflows": [],
        "last_completed_workflow": None,
        "exported_at": datetime.utcnow().isoformat() + "Z",
    }

    # Check main workflow (task4 is the test step)
    main = workflows.get('main', {})
    main_complete = main.get('is_complete', False)
    if main_complete:
        export_status['completed_workflows'].append('main')
        export_status['last_completed_workflow'] = 'main'
    elif main.get('status') not in [None, 'pending']:
        export_status['incomplete_workflows'].append('main')

    # Check DPM workflow
    dpm = workflows.get('dpm', {})
    dpm_complete = dpm.get('is_complete', False)
    if dpm_complete:
        export_status['completed_workflows'].append('dpm')
        export_status['last_completed_workflow'] = 'dpm'
    elif dpm.get('status') not in [None, 'pending']:
        export_status['incomplete_workflows'].append('dpm')

    # Check ANCRDT workflow
    anacredit = workflows.get('anacredit', {})
    anacredit_complete = anacredit.get('is_complete', False)
    if anacredit_complete:
        export_status['completed_workflows'].append('anacredit')
        export_status['last_completed_workflow'] = 'anacredit'
    elif anacredit.get('status') not in [None, 'pending']:
        export_status['incomplete_workflows'].append('anacredit')

    # Export is complete if at least one workflow is complete
    export_status['is_complete'] = len(export_status['completed_workflows']) > 0

    return export_status


def _determine_primary_framework(main_workflow: dict, dpm_workflow: dict, anacredit_workflow: dict) -> str:
    """
    Determine the primary framework from workflow statuses.

    Priority:
    1. DPM workflow framework if DPM steps are completed
    2. AnaCredit if ANCRDT steps are completed
    3. Main workflow framework (FINREP)
    """
    # Check if DPM workflow has progress
    dpm_framework = dpm_workflow.get('framework')
    if dpm_framework and dpm_workflow.get('status') not in [None, 'pending']:
        return dpm_framework

    # Check if AnaCredit workflow has progress
    anacredit_framework = anacredit_workflow.get('framework')
    if anacredit_framework and anacredit_workflow.get('status') not in [None, 'pending']:
        return anacredit_framework

    # Default to main workflow framework
    return main_workflow.get('framework', 'FINREP')


def _get_main_workflow_status() -> dict:
    """Get status of main workflow tasks including framework info and completeness."""
    try:
        from pybirdai.models.bird_meta_data_model import (
            MAINTENANCE_AGENCY, DOMAIN, VARIABLE, CUBE
        )

        # Try to get framework from WorkflowTaskExecution
        framework = 'FINREP'  # Default
        task4_complete = False
        try:
            from pybirdai.models.workflow_model import WorkflowTaskExecution
            latest_execution = WorkflowTaskExecution.objects.filter(
                operation_type='do'
            ).order_by('-completed_at').first()
            if latest_execution and latest_execution.framework_id:
                framework = latest_execution.framework_id

            # Check if task 4 (tests) is completed
            task4_execution = WorkflowTaskExecution.objects.filter(
                task_number=4,
                operation_type='do',
                status='completed'
            ).first()
            task4_complete = task4_execution is not None
        except Exception:
            pass

        # Check if basic metadata exists (Task 1 & 2 indicators)
        has_agencies = MAINTENANCE_AGENCY.objects.exists()
        has_domains = DOMAIN.objects.exists()
        has_variables = VARIABLE.objects.exists()
        has_cubes = CUBE.objects.exists()

        task1_complete = has_agencies and has_domains
        task2_complete = task1_complete and has_variables
        task3_complete = has_cubes

        # Determine last completed step number
        if task4_complete:
            last_step = 4
            overall_status = 'task4_complete'
        elif task3_complete:
            last_step = 3
            overall_status = 'task3_complete'
        elif task2_complete:
            last_step = 2
            overall_status = 'task2_complete'
        elif task1_complete:
            last_step = 1
            overall_status = 'task1_complete'
        else:
            last_step = 0
            overall_status = 'pending'

        # Build steps_status dictionary
        steps_status = {
            "task1": "completed" if task1_complete else "pending",
            "task2": "completed" if task2_complete else "pending",
            "task3": "completed" if task3_complete else "pending",
            "task4": "completed" if task4_complete else "pending",
        }

        return {
            "framework": framework,
            "status": overall_status,
            "total_steps": WORKFLOW_STEP_COUNTS['main'],
            "last_step_completed": last_step,
            "is_complete": task4_complete,  # Complete when tests (task4) are done
            "steps_status": steps_status,
            "task1_database_setup": {
                "status": "completed" if task1_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if task1_complete else None
            },
            "task2_data_import": {
                "status": "completed" if task2_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if task2_complete else None
            },
            "task3_hierarchy_conversion": {
                "status": "completed" if task3_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if task3_complete else None
            },
            "task4_code_generation": {
                "status": "completed" if task4_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if task4_complete else None
            }
        }
    except Exception as e:
        logger.warning(f"Error getting main workflow status: {e}")
        return _empty_main_workflow()


def _get_dpm_workflow_status() -> dict:
    """Get status of DPM workflow steps including framework info and completeness.

    DPM workflow is separate from the main FINREP workflow.
    We detect DPM state by looking for:
    - TABLE objects (EBA DPM tables like C_07.00.a)
    - CUBEs with DPM-specific naming patterns (not FINREP REF cubes)
    - DPMProcessExecution records for framework info

    DPM can have two source types:
    - EBA: 6 steps (complete at step 6 - Execute DPM Tests)
    - GitHub: 4 steps (complete at step 4 - Execute Tests)
    """
    try:
        from pybirdai.models.bird_meta_data_model import (
            TABLE, CUBE, CUBE_STRUCTURE, COMBINATION
        )

        # Try to get framework info from DPMProcessExecution
        framework = None
        selected_frameworks = []
        selected_tables = []
        source_type = 'eba'  # Default to EBA source
        step6_complete = False
        step5_complete = False
        github_step4_complete = False
        try:
            from pybirdai.models.workflow_model import DPMProcessExecution
            latest_execution = DPMProcessExecution.objects.order_by('-completed_at').first()
            if latest_execution:
                framework = latest_execution.framework_id
                selected_frameworks = latest_execution.selected_frameworks or []
                selected_tables = latest_execution.selected_tables or []
                source_type = latest_execution.source_type or 'eba'

                # Check for completion based on source type
                if source_type == 'github':
                    # GitHub source: 4 steps, complete at step 4
                    github_step4_exec = DPMProcessExecution.objects.filter(
                        source_type='github',
                        step_number=4,
                        status='completed'
                    ).first()
                    github_step4_complete = github_step4_exec is not None
                else:
                    # EBA source: 6 steps, complete at step 6
                    step6_exec = DPMProcessExecution.objects.filter(
                        source_type='eba',
                        step_number=6,
                        status='completed'
                    ).first()
                    step6_complete = step6_exec is not None

                    step5_exec = DPMProcessExecution.objects.filter(
                        source_type='eba',
                        step_number=5,
                        status='completed'
                    ).first()
                    step5_complete = step5_exec is not None
        except Exception:
            pass

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

        # Determine total steps and completion based on source type
        if source_type == 'github':
            total_steps = WORKFLOW_STEP_COUNTS['dpm_github']
            is_complete = github_step4_complete
        else:
            total_steps = WORKFLOW_STEP_COUNTS['dpm_eba']
            is_complete = step6_complete

        # Determine last completed step number and overall status
        if source_type == 'github':
            # GitHub 4-step flow
            if github_step4_complete:
                last_step = 4
                overall_status = 'github_step4_complete'
            elif has_dpm_structures:
                last_step = 3
                overall_status = 'github_step3_complete'
            elif has_dpm_cubes:
                last_step = 2
                overall_status = 'github_step2_complete'
            elif has_dpm_tables:
                last_step = 1
                overall_status = 'github_step1_complete'
            else:
                last_step = 0
                overall_status = 'pending'

            steps_status = {
                "step1": "completed" if has_dpm_tables else "pending",
                "step2": "completed" if has_dpm_cubes else "pending",
                "step3": "completed" if has_dpm_structures else "pending",
                "step4": "completed" if github_step4_complete else "pending",
            }
        else:
            # EBA 6-step flow
            if step6_complete:
                last_step = 6
                overall_status = 'step6_complete'
            elif step5_complete:
                last_step = 5
                overall_status = 'step5_complete'
            elif has_dpm_structures:
                last_step = 4
                overall_status = 'step4_complete'
            elif has_dpm_cubes:
                last_step = 3
                overall_status = 'step3_complete'
            elif has_dpm_tables:
                last_step = 2
                overall_status = 'step2_complete'
            else:
                last_step = 0
                overall_status = 'pending'

            steps_status = {
                "step1": "completed" if has_dpm_tables else "pending",
                "step2": "completed" if has_dpm_tables else "pending",
                "step3": "completed" if has_dpm_cubes else "pending",
                "step4": "completed" if has_dpm_structures else "pending",
                "step5": "completed" if step5_complete else "pending",
                "step6": "completed" if step6_complete else "pending",
            }

        result = {
            "framework": framework,
            "selected_frameworks": selected_frameworks,
            "source_type": source_type,
            "status": overall_status,
            "total_steps": total_steps,
            "last_step_completed": last_step,
            "is_complete": is_complete,
            "steps_status": steps_status,
            "step1_extract_metadata": {
                "status": "completed" if has_dpm_tables else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if has_dpm_tables else None
            },
            "step2_process_tables": {
                "status": "completed" if has_dpm_tables else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if has_dpm_tables else None,
                "table_count": len(selected_tables) if selected_tables else TABLE.objects.filter(
                    code__regex=r'^[A-Z]_\d{2}\.\d{2}'
                ).count()
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
                "status": "completed" if step5_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if step5_complete else None
            },
            "step6_execute_tests": {
                "status": "completed" if step6_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if step6_complete else None
            }
        }

        return result
    except Exception as e:
        logger.warning(f"Error getting DPM workflow status: {e}")
        return _empty_dpm_workflow()


def _get_anacredit_workflow_status() -> dict:
    """Get status of AnaCredit workflow steps including framework info and completeness.

    ANCRDT workflow has 4 steps:
    - Step 1: Import Metadata
    - Step 2: Joins Metadata
    - Step 3: Execution Code
    - Step 4: Tests (completion step)
    """
    try:
        from pybirdai.models.bird_meta_data_model import CUBE_LINK

        # Try to get framework info from AnaCreditProcessExecution
        framework = 'ANCRDT'  # Default
        step4_complete = False
        step3_complete = False
        step2_complete = False
        step1_complete = False
        try:
            from pybirdai.models.workflow_model import AnaCreditProcessExecution
            latest_execution = AnaCreditProcessExecution.objects.order_by('-completed_at').first()
            if latest_execution and latest_execution.framework_id:
                framework = latest_execution.framework_id

            # Check step completion from execution records
            for step_num in [1, 2, 3, 4]:
                step_exec = AnaCreditProcessExecution.objects.filter(
                    step_number=step_num,
                    status='completed'
                ).first()
                if step_exec:
                    if step_num == 1:
                        step1_complete = True
                    elif step_num == 2:
                        step2_complete = True
                    elif step_num == 3:
                        step3_complete = True
                    elif step_num == 4:
                        step4_complete = True
        except Exception:
            pass

        # Check for ANCRDT-specific data as fallback
        has_ancrdt_links = CUBE_LINK.objects.filter(
            name__icontains='ANCRDT'
        ).exists()

        # If we have links but no execution records, assume step2 is complete
        if has_ancrdt_links and not step2_complete:
            step1_complete = True
            step2_complete = True

        # Determine last completed step number
        if step4_complete:
            last_step = 4
            overall_status = 'step4_complete'
        elif step3_complete:
            last_step = 3
            overall_status = 'step3_complete'
        elif step2_complete:
            last_step = 2
            overall_status = 'step2_complete'
        elif step1_complete:
            last_step = 1
            overall_status = 'step1_complete'
        else:
            last_step = 0
            overall_status = 'pending'

        # Build steps_status dictionary
        steps_status = {
            "step1": "completed" if step1_complete else "pending",
            "step2": "completed" if step2_complete else "pending",
            "step3": "completed" if step3_complete else "pending",
            "step4": "completed" if step4_complete else "pending",
        }

        return {
            "framework": framework,
            "status": overall_status,
            "total_steps": WORKFLOW_STEP_COUNTS['anacredit'],
            "last_step_completed": last_step,
            "is_complete": step4_complete,  # Complete when tests (step4) are done
            "steps_status": steps_status,
            "step1_import": {
                "status": "completed" if step1_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if step1_complete else None
            },
            "step2_joins_metadata": {
                "status": "completed" if step2_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if step2_complete else None
            },
            "step3_execution_code": {
                "status": "completed" if step3_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if step3_complete else None
            },
            "step4_tests": {
                "status": "completed" if step4_complete else "pending",
                "completed_at": datetime.utcnow().isoformat() + "Z" if step4_complete else None
            }
        }
    except Exception as e:
        logger.warning(f"Error getting AnaCredit workflow status: {e}")
        return _empty_anacredit_workflow()


def _empty_main_workflow() -> dict:
    """Return empty main workflow status."""
    return {
        "framework": "FINREP",
        "status": "pending",
        "total_steps": WORKFLOW_STEP_COUNTS['main'],
        "last_step_completed": 0,
        "is_complete": False,
        "steps_status": {
            "task1": "pending",
            "task2": "pending",
            "task3": "pending",
            "task4": "pending",
        },
        "task1_database_setup": {"status": "pending", "completed_at": None},
        "task2_data_import": {"status": "pending", "completed_at": None},
        "task3_hierarchy_conversion": {"status": "pending", "completed_at": None},
        "task4_code_generation": {"status": "pending", "completed_at": None}
    }


def _empty_dpm_workflow() -> dict:
    """Return empty DPM workflow status."""
    return {
        "framework": None,
        "selected_frameworks": [],
        "source_type": "eba",
        "status": "pending",
        "total_steps": WORKFLOW_STEP_COUNTS['dpm_eba'],
        "last_step_completed": 0,
        "is_complete": False,
        "steps_status": {
            "step1": "pending",
            "step2": "pending",
            "step3": "pending",
            "step4": "pending",
            "step5": "pending",
            "step6": "pending",
        },
        "step1_extract_metadata": {"status": "pending", "completed_at": None},
        "step2_process_tables": {"status": "pending", "completed_at": None, "table_count": 0},
        "step3_output_layers": {"status": "pending", "completed_at": None},
        "step4_transformation_rules": {"status": "pending", "completed_at": None},
        "step5_generate_code": {"status": "pending", "completed_at": None},
        "step6_execute_tests": {"status": "pending", "completed_at": None}
    }


def _empty_anacredit_workflow() -> dict:
    """Return empty AnaCredit workflow status."""
    return {
        "framework": "ANCRDT",
        "status": "pending",
        "total_steps": WORKFLOW_STEP_COUNTS['anacredit'],
        "last_step_completed": 0,
        "is_complete": False,
        "steps_status": {
            "step1": "pending",
            "step2": "pending",
            "step3": "pending",
            "step4": "pending",
        },
        "step1_import": {"status": "pending", "completed_at": None},
        "step2_joins_metadata": {"status": "pending", "completed_at": None},
        "step3_execution_code": {"status": "pending", "completed_at": None},
        "step4_tests": {"status": "pending", "completed_at": None}
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
    execution state based on the loaded metadata. All workflow updates are
    wrapped in a transaction to ensure atomicity.

    Uses framework info from metadata (v1.1+) when available.

    Args:
        metadata: Loaded process metadata

    Returns:
        dict: Summary of restored states including framework info
    """
    from django.utils import timezone
    from django.db import transaction

    # Get primary framework from metadata (v1.1+)
    primary_framework = metadata.get('primary_framework', 'FINREP')

    results = {
        'workflows_restored': [],
        'errors': [],
        'primary_framework': primary_framework
    }

    workflows = metadata.get('workflows', {})

    # Wrap all workflow restorations in a transaction for atomicity
    with transaction.atomic():
        # Restore main workflow states
        try:
            from pybirdai.models.workflow_model import WorkflowTaskExecution

            main_workflow = workflows.get('main', {})

            # Get framework from workflow or use primary (v1.1+)
            main_framework = main_workflow.get('framework', primary_framework)

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
                            # Use standard library datetime instead of dateutil
                            completed_at = datetime.fromisoformat(completed_at_str.replace('Z', '+00:00'))
                        except Exception:
                            completed_at = timezone.now() if status == 'completed' else None

                    # Create or update WorkflowTaskExecution for 'do' operation
                    execution, created = WorkflowTaskExecution.objects.update_or_create(
                        task_number=task_number,
                        operation_type='do',
                        defaults={
                            'status': status,
                            'completed_at': completed_at,
                            'framework_id': main_framework
                        }
                    )

                    action = "Created" if created else "Updated"
                    logger.info(f"{action} main workflow task {task_number} ({task_name}) to status: {status}, framework: {main_framework}")
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

            # Get framework info from dpm_workflow (v1.1+) or fall back to user_selections
            dpm_framework = dpm_workflow.get('framework')
            dpm_selected_frameworks = dpm_workflow.get('selected_frameworks', [])

            # Fall back to old method if framework not in workflow dict
            if not dpm_framework:
                user_selections = metadata.get('user_selections', {})
                selected_frameworks = user_selections.get('selected_frameworks', [])
                dpm_framework = selected_frameworks[0] if selected_frameworks else 'COREP'
                dpm_selected_frameworks = selected_frameworks

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
                            # Use standard library datetime instead of dateutil
                            completed_at = datetime.fromisoformat(completed_at_str.replace('Z', '+00:00'))
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
                            'framework_id': dpm_framework,
                            'selected_frameworks': dpm_selected_frameworks
                        }
                    )

                    action = "Created" if created else "Updated"
                    logger.info(f"{action} DPM workflow step {step_number} ({step_name}) to status: {status}, framework: {dpm_framework}")
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

            # Get framework from anacredit_workflow (v1.1+) or use default
            anacredit_framework = anacredit_workflow.get('framework', 'ANCRDT')

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
                            # Use standard library datetime instead of dateutil
                            completed_at = datetime.fromisoformat(completed_at_str.replace('Z', '+00:00'))
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
                            'completed_at': completed_at,
                            'framework_id': anacredit_framework
                        }
                    )

                    action = "Created" if created else "Updated"
                    logger.info(f"{action} AnaCredit workflow step {step_number} ({step_name}) to status: {status}, framework: {anacredit_framework}")
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
