# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - improvements

"""
ANCRDT Workflow Views

Provides separate views for each step of the AnaCredit Data Transformation workflow.
Replaces the monolithic ancrdt_dashboard with individual step screens.
"""

import logging
import glob
import os
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.urls import reverse
from django.conf import settings
from django.http import JsonResponse
from pybirdai.models.workflow_model import WorkflowSession, AnaCreditProcessExecution
from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, VARIABLE, MEMBER, SUBDOMAIN

logger = logging.getLogger(__name__)


def _get_or_create_workflow_session(request):
    """
    Helper function to get or create a workflow session from request.
    Returns (session, session_id) tuple.
    """
    session_id = request.session.get('workflow_session_id')
    if not session_id:
        import uuid
        session_id = str(uuid.uuid4())
        request.session['workflow_session_id'] = session_id

    session, created = WorkflowSession.objects.get_or_create(
        session_id=session_id,
        defaults={'current_task': 0}
    )

    return session, session_id


def _get_step_execution_data(session, step_num, step_name, description, previous_step_status=None):
    """
    Helper function to get execution data for a specific ANCRDT step.

    Args:
        session: WorkflowSession instance
        step_num: Step number (0-3)
        step_name: Name of the step
        description: Description of the step
        previous_step_status: Status of the previous step (for can_execute logic)

    Returns:
        Dictionary with step execution data
    """
    try:
        execution = AnaCreditProcessExecution.objects.filter(
            session=session,
            step_number=step_num
        ).latest('created_at')

        # Calculate duration
        duration = None
        if execution.started_at and execution.completed_at:
            duration = execution.completed_at - execution.started_at

        # Gather generated files based on step number
        generated_files = []
        metadata_counts = None

        if step_num == 0:
            # Fetch Metadata CSV
            csv_pattern = os.path.join(settings.BASE_DIR, "results", "ancrdt_csv", "*.csv")
            generated_files = glob.glob(csv_pattern)
        elif step_num == 1:
            # Import Metadata
            from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE
            generated_files = [
                f"Cubes: {CUBE.objects.filter(framework_id__framework_id__icontains='ANCRDT').count()} records",
                f"Cube Structures: {CUBE_STRUCTURE.objects.count()} records",
            ]
        elif step_num == 2:
            # Create Joins Metadata
            from pybirdai.models.bird_meta_data_model import (
                CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, MEMBER_LINK,
                MEMBER_MAPPING, MEMBER_MAPPING_ITEM, VARIABLE_MAPPING
            )

            metadata_counts = {
                'cube_links': CUBE_LINK.objects.count(),
                'cube_structure_item_links': CUBE_STRUCTURE_ITEM_LINK.objects.count(),
                'member_links': MEMBER_LINK.objects.count(),
                'member_mappings': MEMBER_MAPPING.objects.count(),
                'member_mapping_items': MEMBER_MAPPING_ITEM.objects.count(),
                'variable_mappings': VARIABLE_MAPPING.objects.count(),
            }

            generated_files = [
                f"Cube Links: {metadata_counts['cube_links']} records",
                f"Cube Structure Item Links: {metadata_counts['cube_structure_item_links']} records",
                f"Member Links: {metadata_counts['member_links']} records",
            ]
        elif step_num == 3:
            # Create Executable Joins
            py_pattern = os.path.join(settings.BASE_DIR, "results", "generated_python_joins", "*.py")
            all_files = glob.glob(py_pattern)
            # Filter out .backup and .generated files, and convert to relative paths for display
            generated_files = []
            for f in all_files:
                basename = os.path.basename(f)
                if not (basename.endswith('.backup') or basename.endswith('.generated') or basename == 'tmp'):
                    generated_files.append(basename)

        step_data = {
            'number': step_num,
            'name': step_name,
            'description': description,
            'detail_url': reverse(f'pybirdai:ancrdt_step_{step_num}'),
            'status': execution.status,
            'started_at': execution.started_at,
            'completed_at': execution.completed_at,
            'duration': duration,
            'error_message': execution.error_message,
            'execution_data': execution.execution_data,
            'generated_files': generated_files,
            'can_execute': step_num == 0 or previous_step_status == 'completed',
            'can_review': execution.status in ['completed', 'failed'],
        }

        # Add metadata for step 2
        if step_num == 2:
            step_data['metadata_counts'] = metadata_counts

        return step_data

    except AnaCreditProcessExecution.DoesNotExist:
        # No execution yet
        return {
            'number': step_num,
            'name': step_name,
            'description': description,
            'detail_url': reverse(f'pybirdai:ancrdt_step_{step_num}'),
            'status': 'pending',
            'started_at': None,
            'completed_at': None,
            'duration': None,
            'error_message': None,
            'execution_data': None,
            'generated_files': [],
            'can_execute': step_num == 0 or previous_step_status == 'completed',
            'can_review': False,
        }


def _get_navigation_context(current_step, step_statuses):
    """
    Helper function to generate navigation context.

    Args:
        current_step: Current step number
        step_statuses: List of step statuses [status0, status1, status2, status3]

    Returns:
        Dictionary with navigation URLs and availability flags
    """
    navigation = {
        'current_step': current_step,
        'can_go_previous': current_step > 0,
        'can_go_next': current_step < 3 and step_statuses[current_step] == 'completed',
        'previous_step_url': None,
        'next_step_url': None,
        'landing_url': reverse('pybirdai:ancrdt_workflow_landing'),
    }

    if current_step > 0:
        navigation['previous_step_url'] = reverse(f'pybirdai:ancrdt_step_{current_step - 1}')

    if current_step < 3:
        navigation['next_step_url'] = reverse(f'pybirdai:ancrdt_step_{current_step + 1}')

    return navigation


def ancrdt_step_0_view(request):
    """
    Step 0: Fetch Metadata CSV

    Execute and review the CSV fetch from ECB website.
    """
    try:
        session, session_id = _get_or_create_workflow_session(request)

        # Get step 0 data
        step = _get_step_execution_data(
            session, 0,
            'Fetch Metadata CSV',
            'Fetch ANCRDT CSV data from ECB website'
        )

        # Get navigation context
        step_statuses = []
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses.append(exec_obj.status)
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses.append('pending')

        navigation = _get_navigation_context(0, step_statuses)

        context = {
            'session': session,
            'step': step,
            'navigation': navigation,
        }

        return render(request, 'pybirdai/ancrdt_workflow/step_0_fetch_csv.html', context)

    except Exception as e:
        logger.error(f"Error in ANCRDT Step 0 view: {e}")
        messages.error(request, f'Error loading Step 0: {str(e)}')
        # Stay on page with error message
        return render(request, 'pybirdai/ancrdt_workflow/step_0_fetch_csv.html', {
            'step': {'number': 0, 'name': 'Fetch Metadata CSV', 'description': 'Fetch ANCRDT CSV data from ECB website', 'status': 'error'},
            'navigation': {}
        })


def ancrdt_step_1_view(request):
    """
    Step 1: Import Metadata

    Execute and review metadata import into database.
    """
    try:
        session, session_id = _get_or_create_workflow_session(request)

        # Get previous step status
        try:
            prev_execution = AnaCreditProcessExecution.objects.filter(
                session=session, step_number=0
            ).latest('created_at')
            previous_status = prev_execution.status
        except AnaCreditProcessExecution.DoesNotExist:
            previous_status = 'pending'

        # Get step 1 data
        step = _get_step_execution_data(
            session, 1,
            'Import Metadata',
            'Import ANCRDT data into database',
            previous_status
        )

        # Get database counts
        from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE
        cube_count = CUBE.objects.filter(framework_id__framework_id__icontains='ANCRDT').count()
        cube_structure_count = CUBE_STRUCTURE.objects.count()

        # Get navigation context
        step_statuses = []
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses.append(exec_obj.status)
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses.append('pending')

        navigation = _get_navigation_context(1, step_statuses)

        context = {
            'session': session,
            'step': step,
            'cube_count': cube_count,
            'cube_structure_count': cube_structure_count,
            'navigation': navigation,
        }

        return render(request, 'pybirdai/ancrdt_workflow/step_1_import.html', context)

    except Exception as e:
        logger.error(f"Error in ANCRDT Step 1 view: {e}")
        messages.error(request, f'Error loading Step 1: {str(e)}')
        # Stay on page with error message
        return render(request, 'pybirdai/ancrdt_workflow/step_1_import.html', {
            'step': {'number': 1, 'name': 'Import Metadata', 'description': 'Import ANCRDT data into database', 'status': 'error'},
            'navigation': {},
            'cube_count': 0,
            'cube_structure_count': 0
        })


def ancrdt_step_2_view(request):
    """
    Step 2: Generate & Edit Joins Metadata

    Execute joins metadata generation and provide tabbed editor interface
    for reviewing and editing cube links, structure links, and member links.
    """
    try:
        session, session_id = _get_or_create_workflow_session(request)

        # Get previous step status
        try:
            prev_execution = AnaCreditProcessExecution.objects.filter(
                session=session, step_number=1
            ).latest('created_at')
            previous_status = prev_execution.status
        except AnaCreditProcessExecution.DoesNotExist:
            previous_status = 'pending'

        # Get step 2 data
        step = _get_step_execution_data(
            session, 2,
            'Generate Joins Metadata',
            'Create joins metadata from imported data',
            previous_status
        )

        # Get all cube structure item links for member links editor
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
        all_cube_structure_item_links = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')

        # Get navigation context
        step_statuses = []
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses.append(exec_obj.status)
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses.append('pending')

        navigation = _get_navigation_context(2, step_statuses)

        context = {
            'session': session,
            'step': step,
            'all_cube_structure_item_links': all_cube_structure_item_links,
            'navigation': navigation,
        }

        return render(request, 'pybirdai/ancrdt_workflow/step_2_joins_metadata.html', context)

    except Exception as e:
        logger.error(f"Error in ANCRDT Step 2 view: {e}")
        messages.error(request, f'Error loading Step 2: {str(e)}')
        # Stay on page with error message
        return render(request, 'pybirdai/ancrdt_workflow/step_2_joins_metadata.html', {
            'step': {'number': 2, 'name': 'Generate Joins Metadata', 'description': 'Create joins metadata from imported data', 'status': 'error'},
            'navigation': {},
            'all_cube_structure_item_links': []
        })


def ancrdt_step_3_view(request):
    """
    Step 3: Generate & Edit Execution Code

    Execute execution code generation and provide tabbed editor interface
    for reviewing and editing output tables and logic files.
    """
    try:
        session, session_id = _get_or_create_workflow_session(request)

        # Get previous step status
        try:
            prev_execution = AnaCreditProcessExecution.objects.filter(
                session=session, step_number=2
            ).latest('created_at')
            previous_status = prev_execution.status
        except AnaCreditProcessExecution.DoesNotExist:
            previous_status = 'pending'

        # Get step 3 data
        step = _get_step_execution_data(
            session, 3,
            'Generate Execution Code',
            'Generate Python execution code',
            previous_status
        )

        # Get navigation context
        step_statuses = []
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses.append(exec_obj.status)
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses.append('pending')

        navigation = _get_navigation_context(3, step_statuses)

        # Get sync status for ANCRDT files
        import json
        from pybirdai.utils.code_sync import CodeSyncManager
        try:
            sync_manager = CodeSyncManager()
            sync_status = sync_manager.get_sync_status()

            # Calculate summary statistics
            total_files = len(sync_status)
            synced_files = sum(1 for status in sync_status.values() if status['is_synced'])
            edited_files = sum(1 for status in sync_status.values() if status['is_edited'])
            unsynced_files = total_files - synced_files

            context_sync = {
                'sync_status': json.dumps(sync_status),  # JSON for JavaScript
                'sync_summary': {
                    'total_files': total_files,
                    'synced_files': synced_files,
                    'unsynced_files': unsynced_files,
                    'edited_files': edited_files,
                    'all_synced': synced_files == total_files
                }
            }
        except Exception as e:
            logger.warning(f"Could not load sync status: {e}")
            context_sync = {'sync_status': '{}', 'sync_summary': {'total_files': 0}}

        context = {
            'session': session,
            'step': step,
            'navigation': navigation,
            **context_sync
        }

        return render(request, 'pybirdai/ancrdt_workflow/step_3_execution_code.html', context)

    except Exception as e:
        logger.error(f"Error in ANCRDT Step 3 view: {e}")
        messages.error(request, f'Error loading Step 3: {str(e)}')
        # Stay on page with error message
        return render(request, 'pybirdai/ancrdt_workflow/step_3_execution_code.html', {
            'step': {'number': 3, 'name': 'Generate Execution Code', 'description': 'Generate Python execution code', 'status': 'error'},
            'navigation': {}
        })


def ancrdt_step_1_review_view(request):
    """
    Step 1 Review: Import Metadata Review

    Dedicated review page showing import statistics and database counts.
    """
    try:
        session, session_id = _get_or_create_workflow_session(request)

        # Get step 1 data
        step = _get_step_execution_data(
            session, 1,
            'Import Metadata',
            'Import ANCRDT data into database'
        )

        # Get database counts
        from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE
        cube_count = CUBE.objects.filter(framework_id__framework_id__icontains='ANCRDT').count()
        cube_structure_count = CUBE_STRUCTURE.objects.count()

        context = {
            'session': session,
            'step': step,
            'cube_count': cube_count,
            'cube_structure_count': cube_structure_count,
        }

        return render(request, 'pybirdai/ancrdt_workflow/step_1_review.html', context)

    except Exception as e:
        logger.error(f"Error in ANCRDT Step 1 Review: {e}")
        messages.error(request, f'Error loading Step 1 review: {str(e)}')
        return render(request, 'pybirdai/ancrdt_workflow/step_1_review.html', {'step': {'status': 'error'}})


def ancrdt_step_2_review_view(request):
    """
    Step 2 Review: Joins Metadata Review

    Dedicated review page with tabbed editor for cube links, structure links, and member links.
    """
    try:
        session, session_id = _get_or_create_workflow_session(request)

        # Get step 2 data
        step = _get_step_execution_data(
            session, 2,
            'Generate Joins Metadata',
            'Create joins metadata from imported data'
        )

        # Get all cube structure item links for member links editor
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
        all_cube_structure_item_links = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')

        context = {
            'session': session,
            'step': step,
            'all_cube_structure_item_links': all_cube_structure_item_links,
        }

        return render(request, 'pybirdai/ancrdt_workflow/step_2_review.html', context)

    except Exception as e:
        logger.error(f"Error in ANCRDT Step 2 Review: {e}")
        messages.error(request, f'Error loading Step 2 review: {str(e)}')
        return render(request, 'pybirdai/ancrdt_workflow/step_2_review.html', {'step': {'status': 'error'}})


def ancrdt_step_3_review_view(request):
    """
    Step 3 Review: Execution Code Review

    Dedicated review page with unified code editor for generated Python files.
    Includes sync status for ANCRDT lifecycle management.
    """
    try:
        session, session_id = _get_or_create_workflow_session(request)

        # Get step 3 data
        step = _get_step_execution_data(
            session, 3,
            'Generate Execution Code',
            'Generate Python execution code'
        )

        # Get sync status for ANCRDT files
        import json
        from pybirdai.utils.code_sync import CodeSyncManager
        sync_manager = CodeSyncManager()
        sync_status = sync_manager.get_sync_status()

        # Calculate summary statistics
        total_files = len(sync_status)
        synced_files = sum(1 for status in sync_status.values() if status['is_synced'])
        edited_files = sum(1 for status in sync_status.values() if status['is_edited'])
        unsynced_files = total_files - synced_files

        context = {
            'session': session,
            'step': step,
            'sync_status': json.dumps(sync_status),  # JSON for JavaScript
            'sync_summary': {
                'total_files': total_files,
                'synced_files': synced_files,
                'unsynced_files': unsynced_files,
                'edited_files': edited_files,
                'all_synced': synced_files == total_files
            }
        }

        return render(request, 'pybirdai/ancrdt_workflow/step_3_review.html', context)

    except Exception as e:
        logger.error(f"Error in ANCRDT Step 3 Review: {e}")
        messages.error(request, f'Error loading Step 3 review: {str(e)}')
        return render(request, 'pybirdai/ancrdt_workflow/step_3_review.html', {'step': {'status': 'error'}})


# API Endpoints for Cube Structure Visualization

def api_ancrdt_cubes(request):
    """
    API endpoint to list all ANCRDT cubes.
    Returns JSON array of cubes with id, name, code, and structure_id.
    """
    try:
        cubes = CUBE.objects.filter(
            framework_id__framework_id__icontains='ANCRDT'
        ).select_related('cube_structure_id').order_by('name')

        cube_list = []
        for cube in cubes:
            cube_list.append({
                'cube_id': cube.cube_id,
                'name': cube.name or cube.cube_id,
                'code': cube.code,
                'structure_id': cube.cube_structure_id.cube_structure_id if cube.cube_structure_id else None,
                'structure_name': cube.cube_structure_id.name if cube.cube_structure_id else None,
            })

        return JsonResponse({'cubes': cube_list})

    except Exception as e:
        logger.error(f"Error fetching ANCRDT cubes: {e}")
        return JsonResponse({'error': str(e)}, status=500)


def api_ancrdt_cube_structure(request, cube_id):
    """
    API endpoint to get cube structure items for a specific cube.
    Returns JSON with cube details and hierarchical structure items.
    """
    try:
        # Get the cube
        cube = get_object_or_404(CUBE, cube_id=cube_id)

        if not cube.cube_structure_id:
            return JsonResponse({
                'error': 'Cube has no associated structure'
            }, status=404)

        structure = cube.cube_structure_id

        # Get all structure items for this cube structure, ordered by order field
        structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=structure
        ).select_related(
            'variable_id',
            'member_id',
            'subdomain_id'
        ).order_by('order')

        # Build the items array
        items = []
        for item in structure_items:
            items.append({
                'order': item.order,
                'role': item.role,
                'role_display': dict(CUBE_STRUCTURE_ITEM.TYP_RL).get(item.role, item.role),
                'cube_variable_code': item.cube_variable_code,
                'variable_id': item.variable_id.variable_id if item.variable_id else None,
                'variable_name': item.variable_id.name if item.variable_id else 'Unknown',
                'variable_code': item.variable_id.code if item.variable_id else None,
                'member_id': item.member_id.member_id if item.member_id else None,
                'member_name': item.member_id.name if item.member_id else None,
                'subdomain_id': item.subdomain_id.subdomain_id if item.subdomain_id else None,
                'subdomain_name': item.subdomain_id.name if item.subdomain_id else None,
                'dimension_type': item.dimension_type,
                'is_mandatory': item.is_mandatory,
                'is_identifier': item.is_identifier,
            })

        response_data = {
            'cube_id': cube.cube_id,
            'cube_name': cube.name or cube.cube_id,
            'cube_code': cube.code,
            'structure_id': structure.cube_structure_id,
            'structure_name': structure.name or structure.cube_structure_id,
            'structure_code': structure.code,
            'items': items,
            'item_count': len(items)
        }

        return JsonResponse(response_data)

    except Exception as e:
        logger.error(f"Error fetching cube structure for {cube_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


