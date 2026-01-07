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
Workflow state restoration for process metadata.

Provides functionality to restore workflow execution states from metadata
to the Django database.
"""
import logging
from typing import Dict, Any

from .helpers import parse_timestamp

logger = logging.getLogger(__name__)

# Task/step mappings for workflow restoration
MAIN_TASK_MAP = {
    'task1_database_setup': 1,
    'task2_data_import': 2,
    'task3_hierarchy_conversion': 3,
    'task4_code_generation': 4
}

DPM_STEP_MAP = {
    'step1_extract_metadata': 1,
    'step2_process_tables': 2,
    'step3_output_layers': 3,
    'step4_transformation_rules': 4,
    'step5_generate_code': 5,
    'step6_execute_tests': 6
}

ANACREDIT_STEP_MAP = {
    'step1': 1,
    'step2': 2,
    'step3': 3,
    'step4': 4,
    'step5': 5
}


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
        results = _restore_main_workflow(results, workflows, primary_framework)
        results = _restore_dpm_workflow(results, workflows, metadata)
        results = _restore_anacredit_workflow(results, workflows)

    return results


def _restore_main_workflow(results: dict, workflows: dict, primary_framework: str) -> dict:
    """Restore main workflow states."""
    try:
        from django.utils import timezone
        from pybirdai.models.workflow_model import WorkflowTaskExecution

        main_workflow = workflows.get('main', {})

        # Get framework from workflow or use primary (v1.1+)
        main_framework = main_workflow.get('framework', primary_framework)

        for task_name, task_data in main_workflow.items():
            if task_name in MAIN_TASK_MAP:
                task_number = MAIN_TASK_MAP[task_name]
                status = task_data.get('status', 'pending')
                completed_at_str = task_data.get('completed_at')

                # Parse completed_at if provided
                completed_at = parse_timestamp(completed_at_str)
                if completed_at is None and status == 'completed':
                    completed_at = timezone.now()

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

    return results


def _restore_dpm_workflow(results: dict, workflows: dict, metadata: dict) -> dict:
    """Restore DPM workflow states."""
    try:
        from django.utils import timezone
        from pybirdai.models.workflow_model import DPMProcessExecution, WorkflowSession

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

        # Get or create a session for DPM workflow
        session, _ = WorkflowSession.objects.get_or_create(
            session_type='dpm',
            defaults={'user_agent': 'clone_mode_import', 'is_active': True}
        )

        for step_name, step_data in dpm_workflow.items():
            if step_name in DPM_STEP_MAP:
                step_number = DPM_STEP_MAP[step_name]
                status = step_data.get('status', 'pending')
                completed_at_str = step_data.get('completed_at')

                # Parse completed_at if provided
                completed_at = parse_timestamp(completed_at_str)
                if completed_at is None and status == 'completed':
                    completed_at = timezone.now()

                # Create or update DPMProcessExecution
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

    return results


def _restore_anacredit_workflow(results: dict, workflows: dict) -> dict:
    """Restore AnaCredit workflow states."""
    try:
        from django.utils import timezone
        from pybirdai.models.workflow_model import AnaCreditProcessExecution, WorkflowSession

        anacredit_workflow = workflows.get('anacredit', {})

        # Get framework from anacredit_workflow (v1.1+) or use default
        anacredit_framework = anacredit_workflow.get('framework', 'ANCRDT')

        # Get or create a session for AnaCredit workflow
        session, _ = WorkflowSession.objects.get_or_create(
            session_type='anacredit',
            defaults={'user_agent': 'clone_mode_import', 'is_active': True}
        )

        for step_name, step_data in anacredit_workflow.items():
            if step_name in ANACREDIT_STEP_MAP:
                step_number = ANACREDIT_STEP_MAP[step_name]
                status = step_data.get('status', 'pending')
                completed_at_str = step_data.get('completed_at')

                # Parse completed_at if provided
                completed_at = parse_timestamp(completed_at_str)
                if completed_at is None and status == 'completed':
                    completed_at = timezone.now()

                # Create or update AnaCreditProcessExecution
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
