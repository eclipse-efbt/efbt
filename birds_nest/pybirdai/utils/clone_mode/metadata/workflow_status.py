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
"""
Workflow status functions for process metadata.

Provides functions to get current workflow status from the database
and factory functions for empty workflow structures.
"""
import logging
from typing import Dict, List, Optional, Any

from .constants import WORKFLOW_STEP_COUNTS
from .helpers import utc_timestamp, build_step_status, build_steps_status_dict

logger = logging.getLogger(__name__)


def get_main_workflow_status() -> dict:
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
        steps_status = build_steps_status_dict({
            "task1": task1_complete,
            "task2": task2_complete,
            "task3": task3_complete,
            "task4": task4_complete,
        })

        return {
            "framework": framework,
            "status": overall_status,
            "total_steps": WORKFLOW_STEP_COUNTS['main'],
            "last_step_completed": last_step,
            "is_complete": task4_complete,  # Complete when tests (task4) are done
            "steps_status": steps_status,
            "task1_database_setup": build_step_status(task1_complete),
            "task2_data_import": build_step_status(task2_complete),
            "task3_hierarchy_conversion": build_step_status(task3_complete),
            "task4_code_generation": build_step_status(task4_complete),
        }
    except Exception as e:
        logger.warning(f"Error getting main workflow status: {e}")
        return empty_main_workflow()


def get_dpm_workflow_status() -> dict:
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

            steps_status = build_steps_status_dict({
                "step1": has_dpm_tables,
                "step2": has_dpm_cubes,
                "step3": has_dpm_structures,
                "step4": github_step4_complete,
            })
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

            steps_status = build_steps_status_dict({
                "step1": has_dpm_tables,
                "step2": has_dpm_tables,
                "step3": has_dpm_cubes,
                "step4": has_dpm_structures,
                "step5": step5_complete,
                "step6": step6_complete,
            })

        # Build step2 with additional table_count field
        step2_status = build_step_status(has_dpm_tables)
        step2_status["table_count"] = len(selected_tables) if selected_tables else TABLE.objects.filter(
            code__regex=r'^[A-Z]_\d{2}\.\d{2}'
        ).count()

        return {
            "framework": framework,
            "selected_frameworks": selected_frameworks,
            "source_type": source_type,
            "status": overall_status,
            "total_steps": total_steps,
            "last_step_completed": last_step,
            "is_complete": is_complete,
            "steps_status": steps_status,
            "step1_extract_metadata": build_step_status(has_dpm_tables),
            "step2_process_tables": step2_status,
            "step3_output_layers": build_step_status(has_dpm_cubes),
            "step4_transformation_rules": build_step_status(has_dpm_structures),
            "step5_generate_code": build_step_status(step5_complete),
            "step6_execute_tests": build_step_status(step6_complete),
        }
    except Exception as e:
        logger.warning(f"Error getting DPM workflow status: {e}")
        return empty_dpm_workflow()


def get_anacredit_workflow_status() -> dict:
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
        steps_status = build_steps_status_dict({
            "step1": step1_complete,
            "step2": step2_complete,
            "step3": step3_complete,
            "step4": step4_complete,
        })

        return {
            "framework": framework,
            "status": overall_status,
            "total_steps": WORKFLOW_STEP_COUNTS['anacredit'],
            "last_step_completed": last_step,
            "is_complete": step4_complete,  # Complete when tests (step4) are done
            "steps_status": steps_status,
            "step1_import": build_step_status(step1_complete),
            "step2_joins_metadata": build_step_status(step2_complete),
            "step3_execution_code": build_step_status(step3_complete),
            "step4_tests": build_step_status(step4_complete),
        }
    except Exception as e:
        logger.warning(f"Error getting AnaCredit workflow status: {e}")
        return empty_anacredit_workflow()


def empty_main_workflow() -> dict:
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


def empty_dpm_workflow() -> dict:
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


def empty_anacredit_workflow() -> dict:
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


def determine_primary_framework(main_workflow: dict, dpm_workflow: dict, anacredit_workflow: dict) -> str:
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


def calculate_export_status(workflows: dict) -> dict:
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
        "exported_at": utc_timestamp(),
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


def determine_last_step(workflows: dict) -> Optional[str]:
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


def get_user_selections() -> dict:
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


def get_configuration() -> dict:
    """Get configuration settings."""
    return {
        "data_model_type": "EIL",
        "technical_export_source": "GITHUB",
        "technical_export_github_url": "",
        "enable_lineage_tracking": False
    }
