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
import zlib
import binascii
import json
import time
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.urls import reverse
from django.conf import settings
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_http_methods
from django.utils import timezone
from pybirdai.models.workflow_model import WorkflowSession, AnaCreditProcessExecution
from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, VARIABLE, MEMBER, SUBDOMAIN

logger = logging.getLogger(__name__)


def encode_file_list(file_list):
    """
    Compress and hex-encode a list of filenames for URL transmission.

    Args:
        file_list: List of filenames (strings)

    Returns:
        Hex-encoded string representing compressed file list
    """
    if not file_list:
        return ""

    # Join filenames with pipe separator
    file_string = "|".join(file_list)

    # Compress using zlib
    compressed = zlib.compress(file_string.encode('utf-8'))

    # Convert to hex string
    hex_string = binascii.hexlify(compressed).decode('ascii')

    return hex_string


def decode_file_list(hex_string):
    """
    Decode and decompress a hex-encoded file list from URL parameter.

    Args:
        hex_string: Hex-encoded compressed file list

    Returns:
        List of filenames, or None if decoding fails
    """
    if not hex_string:
        return None

    try:
        # Convert from hex to bytes
        compressed = binascii.unhexlify(hex_string)

        # Decompress
        file_string = zlib.decompress(compressed).decode('utf-8')

        # Split by pipe separator
        file_list = file_string.split("|")

        return file_list
    except Exception as e:
        logger.error(f"Error decoding file list: {e}")
        return None


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
            # Create Executable Joins - Filter for ANCRDT files only
            py_pattern = os.path.join(settings.BASE_DIR, "results", "generated_python_joins", "*.py")
            all_files = glob.glob(py_pattern)
            # Filter for ANCRDT files only (matching CodeSyncManager patterns)
            generated_files = []
            for f in all_files:
                basename = os.path.basename(f)
                # Include only ANCRDT files, exclude backups and generated bases
                if (basename.startswith('ANCRDT_') or basename.startswith('ancrdt_')) and \
                   not (basename.endswith('.backup') or basename.endswith('.generated') or basename == 'tmp'):
                    generated_files.append(basename)

        step_data = {
            'number': step_num,
            'name': step_name,
            'description': description,
            'detail_url': reverse(f'pybirdai:ancrdt_step_{step_num}_review') if step_num in [1, 2, 3] else reverse(f'pybirdai:ancrdt_step_{step_num}'),
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
            'detail_url': reverse(f'pybirdai:ancrdt_step_{step_num}_review') if step_num in [1, 2, 3] else reverse(f'pybirdai:ancrdt_step_{step_num}'),
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
        step_statuses: Dict of step statuses {0: 'status', 1: 'status', ...}

    Returns:
        Dictionary with navigation URLs and availability flags
    """
    navigation = {
        'current_step': current_step,
        'can_go_previous': current_step > 0,
        'can_go_next': current_step < 5 and step_statuses.get(current_step) == 'completed',
        'previous_step_url': None,
        'next_step_url': None,
        'landing_url': None,
    }

    if current_step > 0:
        if current_step == 4:
            # Step 4 previous should go to step 3 review
            navigation['previous_step_url'] = reverse('pybirdai:ancrdt_step_3_review')
        elif current_step == 5:
            # Step 5 previous should go to step 4
            navigation['previous_step_url'] = reverse('pybirdai:ancrdt_step_4')
        else:
            navigation['previous_step_url'] = reverse(f'pybirdai:ancrdt_step_{current_step - 1}')

    if current_step < 5:
        # For steps 0-4, can navigate to next step
        if current_step == 4:
            # After step 4, go to step 5
            navigation['next_step_url'] = reverse('pybirdai:ancrdt_step_5')
        else:
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
        step_statuses = {}
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses[step_num] = exec_obj.status
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses[step_num] = 'pending'

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
        step_statuses = {}
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses[step_num] = exec_obj.status
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses[step_num] = 'pending'

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
        step_statuses = {}
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses[step_num] = exec_obj.status
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses[step_num] = 'pending'

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
        step_statuses = {}
        for step_num in range(4):
            try:
                exec_obj = AnaCreditProcessExecution.objects.filter(
                    session=session, step_number=step_num
                ).latest('created_at')
                step_statuses[step_num] = exec_obj.status
            except AnaCreditProcessExecution.DoesNotExist:
                step_statuses[step_num] = 'pending'

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

        # Generate encoded file list for Filter Code Editor
        ancrdt_files = ['ANCRDT_INSTRMNT_C_1_logic.py', 'ancrdt_output_tables.py']
        encoded_files = encode_file_list(ancrdt_files)

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
            },
            'encoded_file_filter': encoded_files  # Hex-encoded compressed file whitelist
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


def extract_class_attributes(class_node):
    """Extract all class-level attributes from a ClassDef AST node."""
    import ast
    attributes = {}
    for item in class_node.body:
        if isinstance(item, ast.Assign):
            for target in item.targets:
                if isinstance(target, ast.Name):
                    try:
                        attributes[target.id] = ast.unparse(item.value)
                    except:
                        attributes[target.id] = None
        elif isinstance(item, ast.AnnAssign):
            if isinstance(item.target, ast.Name):
                try:
                    attributes[item.target.id] = ast.unparse(item.value) if item.value else None
                except:
                    attributes[item.target.id] = None
    return attributes


def extract_calc_methods(class_node):
    """Extract methods starting with 'calc_' from a ClassDef AST node."""
    import ast
    calc_methods = []
    for item in class_node.body:
        if isinstance(item, ast.FunctionDef) and item.name.startswith('calc_'):
            try:
                signature = ast.unparse(item)
            except:
                signature = f"def {item.name}(...)"

            try:
                return_type = ast.unparse(item.returns) if item.returns else None
            except:
                return_type = None

            method_info = {
                'name': item.name,
                'signature': signature,
                'return_type': return_type,
            }
            calc_methods.append(method_info)
    return calc_methods


def extract_mapping_enums(class_node):
    """Extract enum values from mapping dictionaries in method bodies."""
    import ast
    dimension_enums = {}

    for item in class_node.body:
        if isinstance(item, ast.FunctionDef):
            # Look for 'mapping = {...}' assignments in method body
            for stmt in item.body:
                if isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if isinstance(target, ast.Name) and target.id == 'mapping':
                            # Extract dictionary keys
                            if isinstance(stmt.value, ast.Dict):
                                try:
                                    keys = [ast.unparse(k) for k in stmt.value.keys if k is not None]
                                    # Clean quotes from keys
                                    keys = [k.strip("'\"") for k in keys]
                                    if keys:
                                        dimension_enums[item.name] = sorted(set(keys))
                                except:
                                    pass

    return dimension_enums


def parse_union_table_metadata(table_name):
    """
    Parse logic file to extract union table metadata.

    Returns:
        Dict with:
        - logic_file: Logic file name
        - union_table_class: Union table class name
        - source_tables: List of source table attribute names
        - dimensions: List of dimension names
        - dimension_enums: Dict mapping dimensions to enum values
    """
    import ast

    logic_file = f'{table_name}_logic.py'
    logic_file_path = os.path.join(
        settings.BASE_DIR,
        'pybirdai',
        'process_steps',
        'filter_code',
        logic_file
    )

    metadata = {
        'logic_file': logic_file,
        'union_table_class': None,
        'source_tables': [],
        'dimensions': [],
        'dimension_enums': {}
    }

    if not os.path.exists(logic_file_path):
        logger.warning(f"Logic file not found: {logic_file_path}")
        return metadata

    try:
        with open(logic_file_path, 'r') as f:
            tree = ast.parse(f.read())

        union_table_class_name = f'{table_name}_UnionTable'

        # Find UnionTable class
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == union_table_class_name:
                metadata['union_table_class'] = node.name

                # Extract source table attributes
                attributes = extract_class_attributes(node)
                source_tables = [
                    attr for attr in attributes.keys()
                    if attr.endswith('_Table') and 'UnionItems' not in attr
                ]
                metadata['source_tables'] = source_tables
                break

        # Find mapping class (e.g., ANCRDT_INSTRMNT_C_1_Loans_and_advances_filtered_and_aggregated)
        # Note: We want the instance class, NOT the Table class (which has no mappings)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and 'filtered_and_aggregated' in node.name and not node.name.endswith('_Table'):
                dimension_enums = extract_mapping_enums(node)
                if dimension_enums:  # Only use if we actually found mappings
                    metadata['dimension_enums'] = dimension_enums
                    metadata['dimensions'] = list(dimension_enums.keys())
                    break

        return metadata

    except Exception as e:
        logger.error(f"Error parsing logic file {logic_file}: {e}")
        return metadata


def get_dimension_members(table_name, dimension, codes):
    """
    Get full member details for a given dimension and list of codes.

    Args:
        table_name: ANCRDT table name (cube_id)
        dimension: Dimension name (cube_variable_code, e.g., 'PRPS')
        codes: List of member codes (e.g., ['7', '8', '13'])

    Returns:
        List of dicts with member details:
        [{
            'member_id': 'PRPS_7',
            'code': '7',
            'name': 'House purchase',
            'description': '...'
        }, ...]
    """
    from pybirdai.models.bird_meta_data_model import (
        CUBE, CUBE_STRUCTURE_ITEM, SUBDOMAIN_ENUMERATION, MEMBER
    )

    try:
        # Get the cube
        cube = CUBE.objects.filter(cube_id=table_name).first()
        if not cube or not cube.cube_structure_id:
            logger.warning(f"No cube structure found for table {table_name}")
            return []

        # Find the cube structure item for this dimension
        structure_item = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube.cube_structure_id,
            cube_variable_code=dimension
        ).select_related('subdomain_id').first()

        if not structure_item or not structure_item.subdomain_id:
            logger.warning(f"No subdomain found for dimension {dimension} in table {table_name}")
            return []

        # Get all members for this subdomain that match the codes
        subdomain_enums = SUBDOMAIN_ENUMERATION.objects.filter(
            subdomain_id=structure_item.subdomain_id,
            member_id__code__in=codes
        ).select_related('member_id').order_by('order')

        # Build member details list
        members = []
        for enum in subdomain_enums:
            if enum.member_id:
                members.append({
                    'member_id': enum.member_id.member_id,
                    'code': enum.member_id.code,
                    'name': enum.member_id.name or enum.member_id.code,
                    'description': enum.member_id.description or ''
                })

        return members

    except Exception as e:
        logger.error(f"Error fetching members for dimension {dimension}: {e}")
        return []


def get_implemented_ancrdt_tables():
    """
    Get detailed metadata for ANCRDT tables implemented in ancrdt_output_tables.py.

    Uses AST parsing to find class definitions without executing the code.

    Returns:
        List of dicts with table metadata including:
        - table_name: Base table name (e.g., 'ANCRDT_INSTRMNT_C_1')
        - class_name: Table class name (e.g., 'ANCRDT_INSTRMNT_C_1_Table')
        - union_table_attr: Union table attribute name
        - calc_function: Name of calc_ method
        - calc_signature: Full method signature
        - return_type: Return type annotation
        - logic_file: Logic file name
        - union_table_class: Union table class name
        - source_tables: List of source table names
        - dimensions: List of dimension/column names
        - dimension_enums: Dict mapping dimensions to possible values
    """
    import ast

    # Path to ancrdt_output_tables.py
    output_tables_path = os.path.join(
        settings.BASE_DIR,
        'pybirdai',
        'process_steps',
        'filter_code',
        'ancrdt_output_tables.py'
    )

    if not os.path.exists(output_tables_path):
        logger.warning(f"ancrdt_output_tables.py not found at {output_tables_path}")
        return []

    try:
        # Parse the file as AST without executing it
        with open(output_tables_path, 'r') as f:
            tree = ast.parse(f.read())

        implemented_tables = []

        # Find all *_Table classes
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name.endswith('_Table'):
                table_name = node.name[:-6]  # Remove '_Table' suffix

                # Extract class attributes
                attributes = extract_class_attributes(node)

                # Extract calc_ methods
                calc_methods = extract_calc_methods(node)

                # Find union table attribute
                union_table_attr = f'{table_name}_UnionTable'

                # Build table metadata
                table_info = {
                    'table_name': table_name,
                    'class_name': node.name,
                    'union_table_attr': union_table_attr if union_table_attr in attributes else None,
                    'calc_function': calc_methods[0]['name'] if calc_methods else None,
                    'calc_signature': calc_methods[0]['signature'] if calc_methods else None,
                    'return_type': calc_methods[0]['return_type'] if calc_methods else None,
                }

                # Parse logic file for additional metadata
                logic_metadata = parse_union_table_metadata(table_name)
                table_info.update(logic_metadata)

                implemented_tables.append(table_info)

        logger.info(f"Found {len(implemented_tables)} implemented ANCRDT tables with metadata")
        return implemented_tables

    except Exception as e:
        logger.error(f"Error parsing ancrdt_output_tables.py: {e}")
        return []


def format_display_text(text):
    """
    Replace underscores with spaces for display.

    Args:
        text: String to format

    Returns:
        Formatted string with underscores replaced by spaces
    """
    return text.replace('_', ' ') if text else text


def ancrdt_step_4_execute_view(request):
    """
    Step 5: Execute ANCRDT Tables

    Provides interface for:
    - Viewing table implementation details (join/union info, calc functions)
    - Setting filter parameters based on dimension enums
    - Executing tables with database data
    - Viewing execution results
    """
    # Get or create workflow session
    session_id = request.session.get('workflow_session_id')
    if not session_id:
        messages.error(request, "No active workflow session. Please start from Step 0.")
        return redirect('pybirdai:ancrdt_step_0')

    workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

    # Get enhanced metadata for implemented tables
    implemented_tables = get_implemented_ancrdt_tables()

    if not implemented_tables:
        logger.warning("No implemented ANCRDT tables found in ancrdt_output_tables.py")
    else:
        logger.info(f"Found {len(implemented_tables)} implemented ANCRDT tables with metadata")

    # Enhance each table with database display name and prepare for template
    tables_list = []
    for table_info in implemented_tables:
        table_name = table_info['table_name']
        display_name = table_name  # Default to technical name

        try:
            # Try to find matching cube in database by cube_id for display name
            cube_entry = CUBE.objects.filter(cube_id=table_name).first()
            if cube_entry and cube_entry.name:
                display_name = cube_entry.name
        except Exception as e:
            logger.warning(f"Could not fetch cube metadata for {table_name}: {e}")

        # Build dimension_members with full member details from database
        dimension_enums = table_info.get('dimension_enums', {})
        dimension_members = {}

        # DEBUG: Log what dimensions were found
        logger.info(f"[DEBUG] Table {table_name} dimension_enums: {dimension_enums}")
        logger.info(f"[DEBUG] Table {table_name} dimensions list: {table_info.get('dimensions', [])}")

        for dimension, codes in dimension_enums.items():
            members = get_dimension_members(table_name, dimension, codes)
            if members:
                dimension_members[dimension] = members
            else:
                # Fallback: if no members found in DB, create minimal structure from codes
                logger.warning(f"Using fallback for dimension {dimension} in table {table_name} - creating from {len(codes)} parsed codes")
                dimension_members[dimension] = [
                    {
                        'member_id': f"{dimension}_{code}",
                        'code': code,
                        'name': f"{dimension} {code}",
                        'description': f"Code {code}"
                    }
                    for code in codes
                ]

        # DEBUG: Log final dimension_members
        logger.info(f"[DEBUG] Table {table_name} dimension_members keys: {list(dimension_members.keys())}")
        for dim_key in dimension_members.keys():
            logger.info(f"[DEBUG] {dim_key} has {len(dimension_members[dim_key])} members")

        # Build dimensions list with display names and members for template
        dimensions_with_display = []
        for dim in table_info.get('dimensions', []):
            if dim in dimension_members:
                dimensions_with_display.append({
                    'key': dim,
                    'display_name': format_display_text(dim),
                    'members': dimension_members[dim]
                })
            else:
                logger.warning(f"[DEBUG] Dimension {dim} in dimensions list but NOT in dimension_members!")

        # DEBUG: Log final result
        logger.info(f"[DEBUG] Table {table_name} dimensions_with_display: {[d['key'] for d in dimensions_with_display]}")

        # Build table entry for template
        table_entry = {
            'name': display_name,
            'cube_id': table_name,
            'union_table': table_info.get('union_table_attr'),
            'union_table_class': table_info.get('union_table_class'),
            'union_table_class_display': format_display_text(table_info.get('union_table_class')),
            'calc_function': table_info.get('calc_function'),
            'calc_function_display': format_display_text(table_info.get('calc_function')),
            'calc_signature': table_info.get('calc_signature'),
            'return_type': table_info.get('return_type'),
            'return_type_display': format_display_text(table_info.get('return_type')),
            'source_tables': table_info.get('source_tables', []),
            'source_tables_display': [
                format_display_text(s) for s in table_info.get('source_tables', [])
            ],
            'dimensions': table_info.get('dimensions', []),
            'dimension_enums': dimension_enums,
            'dimension_members': dimension_members,  # Full member details for Selectize
            'dimensions_with_display': dimensions_with_display  # For template iteration
        }

        tables_list.append(table_entry)

    logger.info(f"Built tables list with {len(tables_list)} entries")

    # Check if Step 3 (code generation) has been completed
    try:
        prev_execution = AnaCreditProcessExecution.objects.filter(
            session=workflow_session, step_number=3
        ).latest('created_at')
        previous_status = prev_execution.status
    except AnaCreditProcessExecution.DoesNotExist:
        previous_status = 'pending'

    step_3_completed = previous_status == 'completed'

    # Validate prerequisites - Step 3 must be completed before executing Step 4
    if not step_3_completed:
        messages.warning(
            request,
            'Step 3 (Generate Execution Code) must be completed successfully before executing tables. '
            'Please complete Step 3 first.'
        )
        logger.warning(f"Step 4 accessed without completing Step 3. Previous status: {previous_status}")

        # If step 3 failed, show specific message
        if previous_status == 'failed':
            messages.error(
                request,
                'Step 3 failed during execution. Please review the errors and re-run Step 3.'
            )

    # Step 4 context (not tracked in AnaCreditProcessExecution, but we provide step info for UI consistency)
    step = {
        'number': 4,
        'name': 'Execute Tables',
        'description': 'Execute ANCRDT tables with filter parameters',
        'status': 'completed' if step_3_completed else 'pending',
        'can_execute': step_3_completed
    }

    # Get navigation context
    step_statuses = {}
    for step_num in range(5):  # Now we have steps 0-4
        try:
            exec_obj = AnaCreditProcessExecution.objects.filter(
                session=workflow_session, step_number=step_num
            ).latest('created_at')
            step_statuses[step_num] = exec_obj.status
        except AnaCreditProcessExecution.DoesNotExist:
            step_statuses[step_num] = 'pending'

    navigation = _get_navigation_context(4, step_statuses)

    context = {
        'workflow_session': workflow_session,
        'step': step,
        'navigation': navigation,
        'tables': tables_list,
        'can_execute': step_3_completed
    }

    return render(request, 'pybirdai/ancrdt_workflow/step_4_execute.html', context)


def convert_row_to_dict(row):
    """
    Convert ANCRDT row object to JSON-serializable dictionary.

    Args:
        row: ANCRDT row object or dictionary

    Returns:
        Dictionary with row data
    """
    if isinstance(row, dict):
        return row

    # If it's an object with __dict__, convert it
    if hasattr(row, '__dict__'):
        row_dict = {}
        for key, value in row.__dict__.items():
            if not key.startswith('_'):  # Skip private attributes
                # Convert callables (methods) to their return values
                if callable(value):
                    try:
                        row_dict[key] = value()
                    except Exception:
                        row_dict[key] = str(value)
                else:
                    row_dict[key] = value
        return row_dict

    # Fallback: try to convert to string representation
    return str(row)


@require_http_methods(["GET", "POST"])
def execute_ancrdt_table_with_fixture(request, table_name):
    """
    Execute an ANCRDT table with optional filters.

    This endpoint:
    1. Cleans test data from database
    2. Executes the ANCRDT table with existing database data
    3. Applies post-execution filters if provided
    4. Returns JSON or HTML based on format parameter

    GET Request (with query parameters):
        /pybirdai/ancrdt-workflow/execute-table/ANCRDT_INSTRMNT_C_1/?format=html&PRPS=7,8&TYP_INSTRMNT=80

    POST Request (with JSON body):
    {
        "filters": {                       # Optional
            "PRPS": "7,8",
            "TYP_INSTRMNT": "80,51"
        }
    }

    Query Parameters:
        format: 'html' (default) or 'json' - Response format
        Other params: Filter dimensions (e.g., PRPS=7,8)

    Returns:
        JsonResponse or HTML template based on format parameter
    """
    start_time = time.time()

    try:
        # Validate prerequisites - Check if Step 3 is completed
        session_id = request.session.get('workflow_session_id')
        if session_id:
            try:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)
                prev_execution = AnaCreditProcessExecution.objects.filter(
                    session=workflow_session, step_number=3
                ).latest('created_at')

                if prev_execution.status != 'completed':
                    error_msg = (
                        f'Cannot execute table: Step 3 (Generate Execution Code) status is "{prev_execution.status}". '
                        f'Please ensure Step 3 completes successfully before executing tables.'
                    )
                    logger.error(error_msg)

                    response_format = request.GET.get('format', 'html')
                    if response_format == 'json':
                        return JsonResponse({
                            'success': False,
                            'error': error_msg,
                            'prerequisite_failed': True
                        }, status=400)
                    else:
                        return render(request, 'pybirdai/ancrdt_workflow/execution_results.html', {
                            'success': False,
                            'error': error_msg,
                            'table_name': table_name,
                            'prerequisite_failed': True
                        })

            except AnaCreditProcessExecution.DoesNotExist:
                error_msg = 'Cannot execute table: Step 3 (Generate Execution Code) has not been run. Please complete Step 3 first.'
                logger.error(error_msg)

                response_format = request.GET.get('format', 'html')
                if response_format == 'json':
                    return JsonResponse({
                        'success': False,
                        'error': error_msg,
                        'prerequisite_failed': True
                    }, status=400)
                else:
                    return render(request, 'pybirdai/ancrdt_workflow/execution_results.html', {
                        'success': False,
                        'error': error_msg,
                        'table_name': table_name,
                        'prerequisite_failed': True
                    })
            except WorkflowSession.DoesNotExist:
                logger.warning("No workflow session found for execution request")

        # Determine response format
        response_format = request.GET.get('format', 'html')

        # Parse parameters based on request method
        if request.method == 'POST':
            # POST: Parse JSON body
            data = json.loads(request.body)
            filters = data.get('filters', {})
        else:
            # GET: Parse query parameters as filters
            filters = {}
            # Collect all query params except 'format' as filters
            for key, value in request.GET.items():
                if key != 'format':
                    filters[key] = value

        # Import required modules
        from pybirdai.process_steps.ancrdt_transformation.execute_ancrdt_table import ExecuteANCRDTTable

        # Execute the ANCRDT table with filters on existing database data
        logger.info(f"Executing ANCRDT table: {table_name}")
        logger.info(f"Filters: {filters}")

        result = ExecuteANCRDTTable.execute_table(
            table_name=table_name,
            filters=filters if filters else None
        )

        # Calculate execution time
        execution_time = time.time() - start_time

        # Create execution history record
        try:
            session_id = request.session.get('workflow_session_id')
            if session_id:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)

                # Create execution record for step 5
                AnaCreditProcessExecution.objects.create(
                    session=workflow_session,
                    step_number=5,
                    step_name='Execute Tables',
                    status='completed',
                    started_at=time.time() - execution_time,  # Calculate start time
                    completed_at=time.time(),
                    execution_data={
                        'table_name': table_name,
                        'filters': filters,
                        'row_count': result['row_count'],
                        'row_count_total': result.get('row_count_total', result['row_count']),
                        'csv_path': result.get('csv_path'),
                        'execution_time_seconds': execution_time
                    }
                )
                logger.info(f"Created execution history record for table: {table_name}")
        except Exception as e:
            logger.warning(f"Could not create execution history record: {e}")

        # Step 4: Prepare response data
        # Convert row objects to JSON-serializable dictionaries
        rows = result.get('rows', [])[:10]  # Get first 10 rows
        serializable_rows = [convert_row_to_dict(row) for row in rows]

        response_data = {
            'success': True,
            'table_name': result['table_name'],
            'row_count': result['row_count'],
            'row_count_total': result.get('row_count_total', result['row_count']),
            'csv_path': result.get('csv_path'),
            'filters_applied': result.get('filters_applied', {}),
            'execution_time': execution_time,
            'rows': serializable_rows
        }

        logger.info(f"Table execution completed: {table_name}, rows={result['row_count']}, time={execution_time:.3f}s")

        # Return based on format
        if response_format == 'json':
            return JsonResponse(response_data)
        else:
            # Return HTML
            return render(request, 'pybirdai/ancrdt_workflow/execution_results.html', response_data)

    except json.JSONDecodeError:
        if response_format == 'json':
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON in request body'
            }, status=400)
        else:
            return render(request, 'pybirdai/ancrdt_workflow/execution_results.html', {
                'success': False,
                'error': 'Invalid JSON in request body',
                'table_name': table_name
            })

    except Exception as e:
        logger.error(f"Error executing ANCRDT table {table_name}: {e}", exc_info=True)

        # Create failed execution history record
        try:
            session_id = request.session.get('workflow_session_id')
            if session_id:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)
                execution_time = time.time() - start_time

                AnaCreditProcessExecution.objects.create(
                    session=workflow_session,
                    step_number=5,
                    step_name='Execute Tables',
                    status='failed',
                    started_at=time.time() - execution_time,
                    completed_at=time.time(),
                    error_message=str(e),
                    execution_data={
                        'table_name': table_name,
                        'filters': filters if 'filters' in locals() else {},
                        'execution_time_seconds': execution_time
                    }
                )
                logger.info(f"Created failed execution history record for table: {table_name}")
        except Exception as tracking_error:
            logger.warning(f"Could not create failed execution history record: {tracking_error}")

        if response_format == 'json':
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)
        else:
            return render(request, 'pybirdai/ancrdt_workflow/execution_results.html', {
                'success': False,
                'error': str(e),
                'table_name': table_name
            })


def download_ancrdt_csv(request, table_name):
    """
    Download the CSV file for an executed ANCRDT table with applied filters.
    Makes an HTTP GET request to the execute endpoint and converts JSON to CSV.

    Args:
        request: HTTP request with optional filter query parameters
        table_name: Name of the ANCRDT table (cube_id)

    Query Parameters:
        Filter dimensions (e.g., PRPS=7,8&TYP_INSTRMNT=51)

    Returns:
        HttpResponse with CSV content or error message
    """
    from django.http import HttpResponse
    import csv
    import io
    import requests

    try:
        logger.info(f"Downloading CSV for table {table_name} with filters: {dict(request.GET)}")

        # Build query string with format=json and all filter parameters
        query_params = request.GET.copy()
        query_params['format'] = 'json'
        query_string = query_params.urlencode()

        # Build URL to execute endpoint with dynamic host detection
        scheme = 'https' if request.is_secure() else 'http'
        host = request.get_host()
        url = f"{scheme}://{host}/pybirdai/execute-ancrdt-table/{table_name}/?{query_string}"

        logger.info(f"Making request to: {url}")

        # Make HTTP GET request with session cookies
        response = requests.get(url, cookies=request.COOKIES)

        # Parse JSON response
        response_data = response.json()

        # Check if execution was successful
        if not response_data.get('success'):
            error_msg = response_data.get('error', 'Unknown error')
            logger.error(f"Execution failed for table {table_name}: {error_msg}")
            http_response = HttpResponse(f"Error: {error_msg}", content_type='text/plain')
            http_response.status_code = 500
            return http_response

        # Extract rows from response
        rows = response_data.get('rows', [])
        if not rows:
            logger.warning(f"No data available for table {table_name}")
            http_response = HttpResponse("No data available for this table with the applied filters.", content_type='text/plain')
            http_response.status_code = 404
            return http_response

        # Create CSV in memory
        output = io.StringIO()

        # Get column names from first row
        fieldnames = list(rows[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Write data rows, replacing None/null with empty string
        for row in rows:
            # Replace None values with empty strings
            cleaned_row = {k: (v if v is not None else "") for k, v in row.items()}
            writer.writerow(cleaned_row)

        # Create response
        csv_content = output.getvalue()
        http_response = HttpResponse(csv_content, content_type='text/csv')
        http_response['Content-Disposition'] = f'attachment; filename="{table_name}_export.csv"'

        logger.info(f"Successfully generated CSV for table {table_name} with {len(rows)} rows")
        return http_response

    except Exception as e:
        logger.error(f"Error generating CSV for table {table_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        http_response = HttpResponse(f"Error generating CSV: {str(e)}", content_type='text/plain')
        http_response.status_code = 500
        return http_response


def load_ancrdt_test_results():
    """Load and parse ANCRDT test results from JSON files"""
    test_results = []
    base_dir = getattr(settings, 'BASE_DIR', os.getcwd())

    try:
        # ANCRDT test results are stored in ancrdt-test-suite/tests/test_results/json/
        json_files_path = os.path.join(base_dir, 'tests', 'ancrdt-test-suite', 'tests', 'test_results', 'json', '*.json')
        logger.info(f"Looking for ANCRDT test results: {json_files_path}")

        json_files = glob.glob(json_files_path)
        logger.info(f"Found {len(json_files)} ANCRDT JSON test result file(s)")

        for json_file in json_files:
            try:
                logger.debug(f"Loading ANCRDT test result file: {json_file}")
                with open(json_file, 'r', encoding='utf-8') as f:
                    result_data = json.load(f)
                    # Add filename for reference
                    result_data['filename'] = os.path.basename(json_file)
                    result_data['suite_name'] = 'ancrdt-test-suite'
                    test_results.append(result_data)
                    logger.debug(f"Successfully loaded {json_file}")
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Error loading ANCRDT test result file {json_file}: {e}")
                continue

        # Sort by timestamp (newest first)
        test_results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        logger.info(f"Loaded {len(test_results)} ANCRDT test result(s) successfully")

    except Exception as e:
        logger.error(f"Error loading ANCRDT test results: {e}")

    return test_results


def ancrdt_step_5_test_suite_view(request):
    """
    Step 4: Full Execution with Test Suite (DO operation)

    Runs the complete ANCRDT test suite to validate table transformations.
    This step executes all configured ANCRDT tests using the ANCRDTTestRunner.
    """
    # Get or create workflow session
    session_id = request.session.get('workflow_session_id')
    if not session_id:
        messages.error(request, "No active workflow session. Please start from Step 0.")
        return redirect('pybirdai:ancrdt_step_0')

    workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

    # Check if step 3 (prerequisite) is completed
    try:
        prev_execution = AnaCreditProcessExecution.objects.filter(
            session=workflow_session, step_number=3
        ).latest('created_at')
        previous_status = prev_execution.status
    except AnaCreditProcessExecution.DoesNotExist:
        previous_status = 'pending'
        messages.warning(request, "Step 3 (Create Executable Joins) should be completed before running tests.")

    # Get or create step 4 execution record
    try:
        step_execution = AnaCreditProcessExecution.objects.filter(
            session=workflow_session, step_number=4
        ).latest('created_at')
    except AnaCreditProcessExecution.DoesNotExist:
        step_execution = None

    # Handle POST request - Run tests
    if request.method == 'POST' and request.POST.get('action') == 'run_tests':
        try:
            from pybirdai.entry_points.run_ancrdt_tests import RunANCRDTTests

            # Create or update execution record
            if not step_execution:
                step_execution = AnaCreditProcessExecution.objects.create(
                    session=workflow_session,
                    step_number=4,
                    step_name='Full Execution with Test Suite',
                    status='running',
                    started_at=timezone.now()
                )
            else:
                step_execution.status = 'running'
                step_execution.started_at = timezone.now()
                step_execution.save()

            logger.info("Starting ANCRDT test suite execution...")

            # Run ANCRDT tests
            config_file = 'configuration_file_tests.json'
            exit_code = RunANCRDTTests.run_tests(
                config_file_path=config_file,
                suite_name='ancrdt-test-suite',
                use_uv=False
            )

            # Update execution status
            if exit_code == 0:
                step_execution.status = 'completed'
                step_execution.completed_at = timezone.now()
                # Mark tests as executed in execution_data
                if not step_execution.execution_data:
                    step_execution.execution_data = {}
                step_execution.execution_data['tests_executed'] = True
                step_execution.save()
                messages.success(request, "ANCRDT test suite executed successfully!")
                logger.info("ANCRDT test suite completed successfully")
                return redirect('pybirdai:ancrdt_step_5_review')
            else:
                step_execution.status = 'failed'
                step_execution.completed_at = timezone.now()
                step_execution.save()
                messages.error(request, "ANCRDT test suite execution failed. Check logs for details.")
                logger.error(f"ANCRDT test suite failed with exit code: {exit_code}")

        except Exception as e:
            logger.error(f"Error during ANCRDT test execution: {e}", exc_info=True)
            if step_execution:
                step_execution.status = 'failed'
                step_execution.error_message = str(e)
                step_execution.completed_at = timezone.now()
                step_execution.save()
            messages.error(request, f"Test execution error: {str(e)}")

        return redirect('pybirdai:ancrdt_step_5')

    # Get all step statuses for navigation
    step_statuses = {}
    for step_num in [0, 1, 2, 3, 5]:
        try:
            exec_obj = AnaCreditProcessExecution.objects.filter(
                session=workflow_session, step_number=step_num
            ).latest('created_at')
            step_statuses[step_num] = exec_obj.status
        except AnaCreditProcessExecution.DoesNotExist:
            step_statuses[step_num] = 'pending'

    # Get navigation context
    navigation = _get_navigation_context(5, step_statuses)

    # Set default execution_data if step_execution exists
    if step_execution and not step_execution.execution_data:
        step_execution.execution_data = {}

    # Prepare context - use task_execution for consistency with template
    context = {
        'session_id': session_id,
        'workflow_session': workflow_session,
        'step_number': 4,
        'step_name': 'Full Execution with Test Suite',
        'task_execution': step_execution or type('obj', (object,), {
            'status': 'pending',
            'execution_data': {}
        })(),
        'previous_status': previous_status,
        'navigation': navigation,
        'config_file': 'configuration_file_tests.json',
    }

    return render(request, 'pybirdai/ancrdt_workflow/step_5_do.html', context)


def ancrdt_step_5_review_view(request):
    """
    Step 5: Full Execution with Test Suite (REVIEW operation)

    Displays test results from the ANCRDT test suite execution.
    """
    # Get or create workflow session
    session_id = request.session.get('workflow_session_id')
    if not session_id:
        messages.error(request, "No active workflow session. Please start from Step 0.")
        return redirect('pybirdai:ancrdt_step_0')

    workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

    # Get step 4 execution record
    try:
        step_execution = AnaCreditProcessExecution.objects.filter(
            session=workflow_session, step_number=4
        ).latest('created_at')
    except AnaCreditProcessExecution.DoesNotExist:
        step_execution = None
        messages.warning(request, "No test execution found. Please run the tests first.")
        return redirect('pybirdai:ancrdt_step_5')

    # Load test results from JSON files
    test_results = load_ancrdt_test_results()

    # Calculate summary statistics
    total_tests = len(test_results)
    passed_tests = 0
    failed_tests = 0

    for result in test_results:
        test_data = result.get('test_results', {})
        passed_list = test_data.get('passed', [])
        failed_list = test_data.get('failed', [])

        # Count actual test results
        if passed_list:
            passed_tests += len(passed_list) if isinstance(passed_list, list) else 1
        if failed_list:
            failed_tests += len(failed_list) if isinstance(failed_list, list) else 1

    logger.info(f"ANCRDT Test summary - Total: {total_tests}, Passed: {passed_tests}, Failed: {failed_tests}")

    # Get execution data
    execution_data = step_execution.execution_data if step_execution and step_execution.execution_data else {}

    # Get all step statuses for navigation
    step_statuses = {}
    for step_num in [0, 1, 2, 3, 5]:
        try:
            exec_obj = AnaCreditProcessExecution.objects.filter(
                session=workflow_session, step_number=step_num
            ).latest('created_at')
            step_statuses[step_num] = exec_obj.status
        except AnaCreditProcessExecution.DoesNotExist:
            step_statuses[step_num] = 'pending'

    # Get navigation context
    navigation = _get_navigation_context(5, step_statuses)

    # Prepare context - use task_execution for consistency with template
    context = {
        'session_id': session_id,
        'workflow_session': workflow_session,
        'step_number': 4,
        'step_name': 'Full Execution with Test Suite',
        'task_execution': step_execution,
        'execution_data': execution_data,
        'test_results': test_results,
        'total_tests': total_tests,
        'passed_tests': passed_tests,
        'failed_tests': failed_tests,
        'navigation': navigation,
    }

    return render(request, 'pybirdai/ancrdt_workflow/step_5_review.html', context)


