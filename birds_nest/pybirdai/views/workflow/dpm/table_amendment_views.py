# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

"""
Table Amendment Views

This module provides views for the Visual Table Amendment Editor workflow,
allowing users to create, modify, and save amended report tables based on
existing DPM tables.

Workflow:
1. Select source table (or start from scratch)
2. Clone table structure (optional)
3. Visual editor for axes and ordinates
4. Save as new amended table with BIRD metadata
"""

import json
import uuid
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.db import transaction
from django.utils import timezone

from pybirdai.models.bird_meta_data_model import (
    TABLE,
    AXIS,
    AXIS_ORDINATE,
    ORDINATE_ITEM,
    TABLE_CELL,
    CELL_POSITION,
    TABLE_AMENDMENT,
    VARIABLE,
    MEMBER,
    MEMBER_HIERARCHY,
    MAINTENANCE_AGENCY,
)


# =============================================================================
# WORKFLOW VIEWS
# =============================================================================

def table_amendment_start(request):
    """
    Step 1: Start the table amendment workflow.
    User selects a source table to amend or chooses to create from scratch.
    """
    if request.method == 'POST':
        source_table_id = request.POST.get('source_table_id')
        new_table_name = request.POST.get('new_table_name', '').strip()
        new_table_code = request.POST.get('new_table_code', '').strip()
        description = request.POST.get('description', '').strip()
        clone_structure = request.POST.get('clone_structure') == 'true'

        if not new_table_name or not new_table_code:
            messages.error(request, 'Table name and code are required.')
            return redirect('pybirdai:table_amendment_start')

        # Check if table code already exists
        if TABLE.objects.filter(code=new_table_code).exists():
            messages.error(request, f'A table with code "{new_table_code}" already exists.')
            return redirect('pybirdai:table_amendment_start')

        # Store in session
        request.session['ta_source_table_id'] = source_table_id
        request.session['ta_new_table_name'] = new_table_name
        request.session['ta_new_table_code'] = new_table_code
        request.session['ta_description'] = description
        request.session['ta_clone_structure'] = clone_structure

        return redirect('pybirdai:table_amendment_editor')

    # GET: Show table selection form
    # Only show tables that have at least one axis (i.e., tables that can be cloned)
    tables_with_axes = AXIS.objects.values_list('table_id', flat=True).distinct()
    tables = TABLE.objects.filter(table_id__in=tables_with_axes).order_by('code')

    context = {
        'tables': tables,
        'page_title': 'Create Table Amendment',
    }

    return render(request, 'pybirdai/workflow/table_amendment/step1_start.html', context)


def table_amendment_editor(request, amendment_id=None):
    """
    Step 2: Main visual editor for table amendments.
    Displays the interactive table canvas with axis and ordinate management.

    If amendment_id is provided, load the existing amendment for editing.
    Otherwise, use session data from step 1.
    """
    source_table = None
    initial_structure = None

    if amendment_id:
        # Editing an existing amendment
        amendment = get_object_or_404(TABLE_AMENDMENT, amendment_id=amendment_id)
        amended_table = amendment.amended_table_id

        if amended_table:
            new_table_name = amended_table.name
            new_table_code = amended_table.code
            description = amended_table.description or ''
            source_table = amendment.source_table_id
            initial_structure = get_table_structure(amended_table)
        else:
            messages.error(request, 'Amendment has no associated table.')
            return redirect('pybirdai:table_amendment_list')
    else:
        # Creating a new amendment - validate session data
        if 'ta_new_table_name' not in request.session:
            messages.error(request, 'Session expired. Please start again.')
            return redirect('pybirdai:table_amendment_start')

        source_table_id = request.session.get('ta_source_table_id')
        new_table_name = request.session.get('ta_new_table_name')
        new_table_code = request.session.get('ta_new_table_code')
        description = request.session.get('ta_description', '')
        clone_structure = request.session.get('ta_clone_structure', False)

        # Load source table data if cloning
        if source_table_id and clone_structure:
            source_table = get_object_or_404(TABLE, table_id=source_table_id)
            initial_structure = get_table_structure(source_table)
        else:
            # Empty structure for new table
            initial_structure = {
                'table': {
                    'table_id': None,
                    'name': new_table_name,
                    'code': new_table_code,
                    'description': description,
                },
                'axes': [],
            }

    # Get available variables and members for ordinate items
    variables = list(VARIABLE.objects.filter(
        domain_id__isnull=False
    ).values('variable_id', 'name', 'code', 'domain_id'))

    context = {
        'source_table': source_table,
        'new_table_name': new_table_name,
        'new_table_code': new_table_code,
        'description': description,
        'initial_structure': json.dumps(initial_structure),
        'variables': json.dumps(variables),
        'page_title': f'Edit Table: {new_table_name}',
        'amendment_id': amendment_id,
    }

    return render(request, 'pybirdai/workflow/table_amendment/step2_editor.html', context)


def table_amendment_list(request):
    """
    View all table amendments.
    """
    amendments = TABLE_AMENDMENT.objects.all().select_related(
        'source_table_id', 'amended_table_id'
    ).order_by('-created_date')

    context = {
        'amendments': amendments,
        'page_title': 'Table Amendments',
    }

    return render(request, 'pybirdai/workflow/table_amendment/list.html', context)


# =============================================================================
# API ENDPOINTS
# =============================================================================

@require_http_methods(["POST"])
def api_create_amendment(request):
    """
    Create a new table amendment with initial structure.

    Request body:
    {
        "source_table_id": "optional",
        "name": "required",
        "code": "required",
        "description": "optional",
        "clone_structure": true/false
    }

    Returns the created amendment and table IDs.
    """
    try:
        data = json.loads(request.body)

        name = data.get('name', '').strip()
        code = data.get('code', '').strip()

        if not name or not code:
            return JsonResponse({
                'success': False,
                'error': 'Table name and code are required.'
            }, status=400)

        # Check for existing table code
        if TABLE.objects.filter(code=code).exists():
            return JsonResponse({
                'success': False,
                'error': f'A table with code "{code}" already exists.'
            }, status=400)

        with transaction.atomic():
            # Create the new table
            table_id = f"TBL_AMEND_{uuid.uuid4().hex[:12].upper()}"

            # Get or create a custom maintenance agency
            agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
                maintenance_agency_id='CUSTOM',
                defaults={'name': 'Custom', 'code': 'CUSTOM'}
            )

            new_table = TABLE.objects.create(
                table_id=table_id,
                name=name,
                code=code,
                description=data.get('description', ''),
                maintenance_agency_id=agency,
                valid_from=timezone.now(),
            )

            # Create amendment record
            amendment_id = f"AMEND_{uuid.uuid4().hex[:12].upper()}"
            source_table_id = data.get('source_table_id')
            source_table = None

            if source_table_id:
                source_table = TABLE.objects.filter(table_id=source_table_id).first()

            amendment = TABLE_AMENDMENT.objects.create(
                amendment_id=amendment_id,
                source_table_id=source_table,
                amended_table_id=new_table,
                amendment_type='DERIVED' if source_table else 'CUSTOM',
                name=name,
                description=data.get('description', ''),
            )

            # Clone structure if requested
            initial_structure = None
            if source_table and data.get('clone_structure', False):
                initial_structure = clone_table_structure(source_table, new_table)
            else:
                initial_structure = {
                    'table': {
                        'table_id': table_id,
                        'name': name,
                        'code': code,
                        'description': data.get('description', ''),
                    },
                    'axes': [],
                }

            return JsonResponse({
                'success': True,
                'amendment_id': amendment_id,
                'table_id': table_id,
                'initial_structure': initial_structure,
            })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body.'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def api_add_axis(request, table_id):
    """
    Add a new axis to a table.

    Request body:
    {
        "orientation": "X" or "Y",
        "name": "optional",
        "order": 1
    }
    """
    try:
        table = get_object_or_404(TABLE, table_id=table_id)
        data = json.loads(request.body)

        orientation = data.get('orientation', 'X')
        if orientation not in ['X', 'Y', '1', '2']:
            return JsonResponse({
                'success': False,
                'error': 'Orientation must be X (columns) or Y (rows).'
            }, status=400)

        # Normalize orientation
        if orientation == '1':
            orientation = 'X'
        elif orientation == '2':
            orientation = 'Y'

        with transaction.atomic():
            axis_id = f"AXIS_{uuid.uuid4().hex[:12].upper()}"

            # Determine order (next in sequence)
            max_order = AXIS.objects.filter(
                table_id=table, orientation=orientation
            ).order_by('-order').values_list('order', flat=True).first()
            order = (max_order or 0) + 1

            axis = AXIS.objects.create(
                axis_id=axis_id,
                table_id=table,
                orientation=orientation,
                name=data.get('name', f'{orientation}-Axis {order}'),
                code=data.get('code', f'{orientation}_{order}'),
                order=data.get('order', order),
                is_open_axis=data.get('is_open_axis', False),
            )

            return JsonResponse({
                'success': True,
                'axis_id': axis_id,
                'axis': {
                    'axis_id': axis.axis_id,
                    'orientation': axis.orientation,
                    'name': axis.name,
                    'code': axis.code,
                    'order': axis.order,
                    'is_open_axis': axis.is_open_axis,
                    'ordinates': [],
                }
            })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body.'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["PUT", "DELETE"])
def api_axis_detail(request, axis_id):
    """
    Update or delete an axis.
    """
    axis = get_object_or_404(AXIS, axis_id=axis_id)

    if request.method == 'DELETE':
        try:
            with transaction.atomic():
                # Delete related ordinates, ordinate items, cell positions, and cells
                ordinates = AXIS_ORDINATE.objects.filter(axis_id=axis)
                ordinate_ids = list(ordinates.values_list('axis_ordinate_id', flat=True))

                # Delete ordinate items
                ORDINATE_ITEM.objects.filter(axis_ordinate_id__in=ordinate_ids).delete()

                # Delete cell positions and cells
                cell_positions = CELL_POSITION.objects.filter(axis_ordinate_id__in=ordinate_ids)
                cell_ids = list(cell_positions.values_list('cell_id', flat=True).distinct())
                cell_positions.delete()
                TABLE_CELL.objects.filter(cell_id__in=cell_ids).delete()

                # Delete ordinates and axis
                ordinates.delete()
                axis.delete()

                return JsonResponse({'success': True})

        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)

    elif request.method == 'PUT':
        try:
            data = json.loads(request.body)

            if 'name' in data:
                axis.name = data['name']
            if 'code' in data:
                axis.code = data['code']
            if 'order' in data:
                axis.order = data['order']
            if 'is_open_axis' in data:
                axis.is_open_axis = data['is_open_axis']

            axis.save()

            return JsonResponse({
                'success': True,
                'axis': {
                    'axis_id': axis.axis_id,
                    'orientation': axis.orientation,
                    'name': axis.name,
                    'code': axis.code,
                    'order': axis.order,
                    'is_open_axis': axis.is_open_axis,
                }
            })

        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON in request body.'
            }, status=400)
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@require_http_methods(["POST"])
def api_add_ordinate(request, axis_id):
    """
    Add a new ordinate to an axis.

    Request body:
    {
        "name": "required",
        "code": "optional",
        "order": 1,
        "level": 0,
        "parent_id": "optional",
        "is_abstract_header": false,
        "ordinate_items": [
            {"variable_id": "...", "member_id": "..."}
        ]
    }
    """
    try:
        axis = get_object_or_404(AXIS, axis_id=axis_id)
        data = json.loads(request.body)

        name = data.get('name', '').strip()
        if not name:
            return JsonResponse({
                'success': False,
                'error': 'Ordinate name is required.'
            }, status=400)

        with transaction.atomic():
            ordinate_id = f"ORD_{uuid.uuid4().hex[:12].upper()}"

            # Determine order
            max_order = AXIS_ORDINATE.objects.filter(
                axis_id=axis
            ).order_by('-order').values_list('order', flat=True).first()
            order = data.get('order', (max_order or 0) + 1)

            # Get parent if specified
            parent_id = data.get('parent_id')
            parent = None
            level = data.get('level', 0)

            if parent_id:
                parent = AXIS_ORDINATE.objects.filter(axis_ordinate_id=parent_id).first()
                if parent:
                    level = (parent.level or 0) + 1

            ordinate = AXIS_ORDINATE.objects.create(
                axis_ordinate_id=ordinate_id,
                axis_id=axis,
                name=name,
                code=data.get('code', name.upper().replace(' ', '_')[:20]),
                order=order,
                level=level,
                parent_axis_ordinate_id=parent,
                is_abstract_header=data.get('is_abstract_header', False),
                path=data.get('path', ''),
            )

            # Create ordinate items if provided
            ordinate_items_data = data.get('ordinate_items', [])
            created_items = []

            for item_data in ordinate_items_data:
                variable_id = item_data.get('variable_id')
                member_id = item_data.get('member_id')

                if variable_id:
                    variable = VARIABLE.objects.filter(variable_id=variable_id).first()
                    member = MEMBER.objects.filter(member_id=member_id).first() if member_id else None
                    hierarchy = None

                    if item_data.get('member_hierarchy_id'):
                        hierarchy = MEMBER_HIERARCHY.objects.filter(
                            member_hierarchy_id=item_data['member_hierarchy_id']
                        ).first()

                    item = ORDINATE_ITEM.objects.create(
                        axis_ordinate_id=ordinate,
                        variable_id=variable,
                        member_id=member,
                        member_hierarchy_id=hierarchy,
                    )
                    created_items.append({
                        'id': item.id,
                        'variable_id': variable_id,
                        'member_id': member_id,
                    })

            return JsonResponse({
                'success': True,
                'axis_ordinate_id': ordinate_id,
                'ordinate': {
                    'axis_ordinate_id': ordinate.axis_ordinate_id,
                    'name': ordinate.name,
                    'code': ordinate.code,
                    'order': ordinate.order,
                    'level': ordinate.level,
                    'parent_id': parent_id,
                    'is_abstract_header': ordinate.is_abstract_header,
                    'ordinate_items': created_items,
                }
            })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body.'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["PUT", "DELETE"])
def api_ordinate_detail(request, ordinate_id):
    """
    Update or delete an ordinate.
    """
    ordinate = get_object_or_404(AXIS_ORDINATE, axis_ordinate_id=ordinate_id)

    if request.method == 'DELETE':
        try:
            with transaction.atomic():
                # Delete child ordinates recursively
                delete_ordinate_recursive(ordinate)

                return JsonResponse({'success': True})

        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)

    elif request.method == 'PUT':
        try:
            data = json.loads(request.body)

            if 'name' in data:
                ordinate.name = data['name']
            if 'code' in data:
                ordinate.code = data['code']
            if 'order' in data:
                ordinate.order = data['order']
            if 'level' in data:
                ordinate.level = data['level']
            if 'is_abstract_header' in data:
                ordinate.is_abstract_header = data['is_abstract_header']
            if 'parent_id' in data:
                if data['parent_id']:
                    parent = AXIS_ORDINATE.objects.filter(
                        axis_ordinate_id=data['parent_id']
                    ).first()
                    ordinate.parent_axis_ordinate_id = parent
                else:
                    ordinate.parent_axis_ordinate_id = None

            ordinate.save()

            # Update ordinate items if provided
            if 'ordinate_items' in data:
                # Clear existing items
                ORDINATE_ITEM.objects.filter(axis_ordinate_id=ordinate).delete()

                # Create new items
                for item_data in data['ordinate_items']:
                    variable_id = item_data.get('variable_id')
                    if variable_id:
                        variable = VARIABLE.objects.filter(variable_id=variable_id).first()
                        member = None
                        if item_data.get('member_id'):
                            member = MEMBER.objects.filter(member_id=item_data['member_id']).first()

                        ORDINATE_ITEM.objects.create(
                            axis_ordinate_id=ordinate,
                            variable_id=variable,
                            member_id=member,
                        )

            return JsonResponse({
                'success': True,
                'ordinate': {
                    'axis_ordinate_id': ordinate.axis_ordinate_id,
                    'name': ordinate.name,
                    'code': ordinate.code,
                    'order': ordinate.order,
                    'level': ordinate.level,
                    'is_abstract_header': ordinate.is_abstract_header,
                }
            })

        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON in request body.'
            }, status=400)
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@require_http_methods(["PUT"])
def api_reorder_ordinates(request, axis_id):
    """
    Reorder ordinates within an axis.

    Request body:
    {
        "ordinate_ids": ["ord1", "ord2", "ord3"]  // New order
    }
    """
    try:
        axis = get_object_or_404(AXIS, axis_id=axis_id)
        data = json.loads(request.body)

        ordinate_ids = data.get('ordinate_ids', [])

        with transaction.atomic():
            for index, ordinate_id in enumerate(ordinate_ids):
                AXIS_ORDINATE.objects.filter(
                    axis_ordinate_id=ordinate_id,
                    axis_id=axis
                ).update(order=index + 1)

        return JsonResponse({'success': True})

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body.'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def api_regenerate_cells(request, table_id):
    """
    Regenerate all cells for a table based on current axes and ordinates.
    Creates a TABLE_CELL for each (row_ordinate, col_ordinate) intersection.

    Request body (optional):
    {
        "cell_overrides": {
            "ord_row_1|ord_col_1": {"is_shaded": true, "name": "Cell Name"}
        }
    }
    """
    try:
        table = get_object_or_404(TABLE, table_id=table_id)
        data = json.loads(request.body) if request.body else {}
        cell_overrides = data.get('cell_overrides', {})

        with transaction.atomic():
            # Get existing axes
            axes = AXIS.objects.filter(table_id=table)

            # Separate row and column axes
            row_axes = axes.filter(orientation__in=['Y', '2'])
            col_axes = axes.filter(orientation__in=['X', '1'])

            # Get all ordinates
            row_ordinates = AXIS_ORDINATE.objects.filter(
                axis_id__in=row_axes
            ).order_by('level', 'order')
            col_ordinates = AXIS_ORDINATE.objects.filter(
                axis_id__in=col_axes
            ).order_by('level', 'order')

            # Delete existing cells for this table
            existing_cells = TABLE_CELL.objects.filter(table_id=table)
            cell_ids = list(existing_cells.values_list('cell_id', flat=True))
            CELL_POSITION.objects.filter(cell_id__in=cell_ids).delete()
            existing_cells.delete()

            # Create new cells for each intersection
            created_cells = []

            for row_ord in row_ordinates:
                for col_ord in col_ordinates:
                    cell_key = f"{row_ord.axis_ordinate_id}|{col_ord.axis_ordinate_id}"
                    override = cell_overrides.get(cell_key, {})

                    cell_id = f"CELL_{uuid.uuid4().hex[:12].upper()}"

                    cell = TABLE_CELL.objects.create(
                        cell_id=cell_id,
                        table_id=table,
                        is_shaded=override.get('is_shaded', False),
                        name=override.get('name', ''),
                    )

                    # Create cell positions (one for row, one for column)
                    CELL_POSITION.objects.create(
                        cell_id=cell,
                        axis_ordinate_id=row_ord,
                    )
                    CELL_POSITION.objects.create(
                        cell_id=cell,
                        axis_ordinate_id=col_ord,
                    )

                    created_cells.append({
                        'cell_id': cell_id,
                        'row_ordinate_id': row_ord.axis_ordinate_id,
                        'col_ordinate_id': col_ord.axis_ordinate_id,
                        'is_shaded': cell.is_shaded,
                        'name': cell.name,
                    })

            return JsonResponse({
                'success': True,
                'cells_created': len(created_cells),
                'cells': created_cells,
            })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body.'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["PUT"])
def api_update_cell(request, cell_id):
    """
    Update a specific cell's properties.

    Request body:
    {
        "is_shaded": true/false,
        "name": "Cell Name"
    }
    """
    try:
        cell = get_object_or_404(TABLE_CELL, cell_id=cell_id)
        data = json.loads(request.body)

        if 'is_shaded' in data:
            cell.is_shaded = data['is_shaded']
        if 'name' in data:
            cell.name = data['name']

        cell.save()

        return JsonResponse({
            'success': True,
            'cell': {
                'cell_id': cell.cell_id,
                'is_shaded': cell.is_shaded,
                'name': cell.name,
            }
        })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body.'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def api_get_table_structure(request, table_id):
    """
    Get the full structure of a table including axes, ordinates, and cells.
    """
    try:
        table = get_object_or_404(TABLE, table_id=table_id)
        structure = get_table_structure(table)

        return JsonResponse({
            'success': True,
            'structure': structure,
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def api_get_members_for_variable(request, variable_id):
    """
    Get all members available for a given variable (via its domain/subdomain).
    """
    try:
        variable = get_object_or_404(VARIABLE, variable_id=variable_id)

        # Get members from the variable's domain
        members = []
        if variable.domain_id:
            from pybirdai.models.bird_meta_data_model import SUBDOMAIN, MEMBER

            # Get subdomains for this domain
            subdomains = SUBDOMAIN.objects.filter(domain_id=variable.domain_id)

            # Get members from these subdomains
            from pybirdai.models.bird_meta_data_model import SUBDOMAIN_ENUMERATION

            subdomain_ids = list(subdomains.values_list('subdomain_id', flat=True))
            member_ids = SUBDOMAIN_ENUMERATION.objects.filter(
                subdomain_id__in=subdomain_ids
            ).values_list('member_id', flat=True).distinct()

            members = list(MEMBER.objects.filter(
                member_id__in=member_ids
            ).values('member_id', 'name', 'code'))

        return JsonResponse({
            'success': True,
            'variable_id': variable_id,
            'members': members,
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def api_save_table(request, table_id):
    """
    Final save of the table with all its structure.
    Validates the table structure and marks the amendment as complete.
    """
    try:
        table = get_object_or_404(TABLE, table_id=table_id)

        # Validate table has at least one axis of each orientation
        axes = AXIS.objects.filter(table_id=table)
        has_x = axes.filter(orientation__in=['X', '1']).exists()
        has_y = axes.filter(orientation__in=['Y', '2']).exists()

        warnings = []
        if not has_x:
            warnings.append('Table has no column (X) axis.')
        if not has_y:
            warnings.append('Table has no row (Y) axis.')

        # Count ordinates
        ordinate_count = AXIS_ORDINATE.objects.filter(axis_id__in=axes).count()
        if ordinate_count == 0:
            warnings.append('Table has no ordinates defined.')

        # Count cells
        cell_count = TABLE_CELL.objects.filter(table_id=table).count()
        if cell_count == 0:
            warnings.append('Table has no cells. Consider regenerating cells.')

        # Update amendment record
        amendment = TABLE_AMENDMENT.objects.filter(amended_table_id=table).first()
        if amendment:
            amendment.modified_date = timezone.now()
            amendment.save()

        return JsonResponse({
            'success': True,
            'table_id': table_id,
            'warnings': warnings,
            'stats': {
                'axes': axes.count(),
                'ordinates': ordinate_count,
                'cells': cell_count,
            }
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_table_structure(table):
    """
    Get the complete structure of a table as a dictionary.
    Used for both display and cloning.
    """
    structure = {
        'table': {
            'table_id': table.table_id,
            'name': table.name,
            'code': table.code,
            'description': table.description,
        },
        'axes': [],
    }

    axes = AXIS.objects.filter(table_id=table).order_by('orientation', 'order')

    for axis in axes:
        axis_data = {
            'axis_id': axis.axis_id,
            'orientation': axis.orientation,
            'name': axis.name,
            'code': axis.code,
            'order': axis.order,
            'is_open_axis': axis.is_open_axis,
            'ordinates': [],
        }

        ordinates = AXIS_ORDINATE.objects.filter(
            axis_id=axis
        ).order_by('level', 'order')

        for ordinate in ordinates:
            ordinate_data = {
                'axis_ordinate_id': ordinate.axis_ordinate_id,
                'name': ordinate.name,
                'code': ordinate.code,
                'order': ordinate.order,
                'level': ordinate.level,
                'parent_id': ordinate.parent_axis_ordinate_id_id if ordinate.parent_axis_ordinate_id else None,
                'is_abstract_header': ordinate.is_abstract_header,
                'ordinate_items': [],
            }

            # Get ordinate items
            items = ORDINATE_ITEM.objects.filter(
                axis_ordinate_id=ordinate
            ).select_related('variable_id', 'member_id')

            for item in items:
                ordinate_data['ordinate_items'].append({
                    'variable_id': item.variable_id_id if item.variable_id else None,
                    'variable_name': item.variable_id.name if item.variable_id else None,
                    'member_id': item.member_id_id if item.member_id else None,
                    'member_name': item.member_id.name if item.member_id else None,
                })

            axis_data['ordinates'].append(ordinate_data)

        structure['axes'].append(axis_data)

    return structure


def clone_table_structure(source_table, new_table):
    """
    Clone the structure (axes, ordinates, ordinate items) from source to new table.
    Returns the cloned structure.
    """
    # Mapping from old IDs to new IDs
    axis_id_map = {}
    ordinate_id_map = {}

    structure = {
        'table': {
            'table_id': new_table.table_id,
            'name': new_table.name,
            'code': new_table.code,
            'description': new_table.description,
        },
        'axes': [],
    }

    # Clone axes
    source_axes = AXIS.objects.filter(table_id=source_table).order_by('orientation', 'order')

    for source_axis in source_axes:
        new_axis_id = f"AXIS_{uuid.uuid4().hex[:12].upper()}"
        axis_id_map[source_axis.axis_id] = new_axis_id

        new_axis = AXIS.objects.create(
            axis_id=new_axis_id,
            table_id=new_table,
            orientation=source_axis.orientation,
            name=source_axis.name,
            code=source_axis.code,
            order=source_axis.order,
            is_open_axis=source_axis.is_open_axis,
            description=source_axis.description,
        )

        axis_data = {
            'axis_id': new_axis_id,
            'orientation': new_axis.orientation,
            'name': new_axis.name,
            'code': new_axis.code,
            'order': new_axis.order,
            'is_open_axis': new_axis.is_open_axis,
            'ordinates': [],
        }

        # Clone ordinates (first pass - create all ordinates)
        source_ordinates = AXIS_ORDINATE.objects.filter(
            axis_id=source_axis
        ).order_by('level', 'order')

        for source_ord in source_ordinates:
            new_ord_id = f"ORD_{uuid.uuid4().hex[:12].upper()}"
            ordinate_id_map[source_ord.axis_ordinate_id] = new_ord_id

        # Second pass - create ordinates with correct parent references
        for source_ord in source_ordinates:
            new_ord_id = ordinate_id_map[source_ord.axis_ordinate_id]

            parent = None
            if source_ord.parent_axis_ordinate_id:
                parent_new_id = ordinate_id_map.get(source_ord.parent_axis_ordinate_id_id)
                if parent_new_id:
                    parent = AXIS_ORDINATE.objects.filter(axis_ordinate_id=parent_new_id).first()

            new_ordinate = AXIS_ORDINATE.objects.create(
                axis_ordinate_id=new_ord_id,
                axis_id=new_axis,
                name=source_ord.name,
                code=source_ord.code,
                order=source_ord.order,
                level=source_ord.level,
                path=source_ord.path,
                parent_axis_ordinate_id=parent,
                is_abstract_header=source_ord.is_abstract_header,
                description=source_ord.description,
            )

            ordinate_data = {
                'axis_ordinate_id': new_ord_id,
                'name': new_ordinate.name,
                'code': new_ordinate.code,
                'order': new_ordinate.order,
                'level': new_ordinate.level,
                'parent_id': parent.axis_ordinate_id if parent else None,
                'is_abstract_header': new_ordinate.is_abstract_header,
                'ordinate_items': [],
            }

            # Clone ordinate items
            source_items = ORDINATE_ITEM.objects.filter(
                axis_ordinate_id=source_ord
            ).select_related('variable_id', 'member_id', 'member_hierarchy_id')

            for source_item in source_items:
                new_item = ORDINATE_ITEM.objects.create(
                    axis_ordinate_id=new_ordinate,
                    variable_id=source_item.variable_id,
                    member_id=source_item.member_id,
                    member_hierarchy_id=source_item.member_hierarchy_id,
                    member_hierarchy_valid_from=source_item.member_hierarchy_valid_from,
                    starting_member_id=source_item.starting_member_id,
                    is_starting_member_included=source_item.is_starting_member_included,
                )

                ordinate_data['ordinate_items'].append({
                    'variable_id': source_item.variable_id_id if source_item.variable_id else None,
                    'variable_name': source_item.variable_id.name if source_item.variable_id else None,
                    'member_id': source_item.member_id_id if source_item.member_id else None,
                    'member_name': source_item.member_id.name if source_item.member_id else None,
                })

            axis_data['ordinates'].append(ordinate_data)

        structure['axes'].append(axis_data)

    return structure


def delete_ordinate_recursive(ordinate):
    """
    Delete an ordinate and all its children recursively.
    Also cleans up related ordinate items and cell positions.
    """
    # Find children
    children = AXIS_ORDINATE.objects.filter(parent_axis_ordinate_id=ordinate)

    for child in children:
        delete_ordinate_recursive(child)

    # Delete ordinate items
    ORDINATE_ITEM.objects.filter(axis_ordinate_id=ordinate).delete()

    # Delete cell positions
    CELL_POSITION.objects.filter(axis_ordinate_id=ordinate).delete()

    # Delete the ordinate
    ordinate.delete()


# =============================================================================
# ENHANCED TEMPLATE EDITOR APIs
# =============================================================================

@require_http_methods(["GET"])
def api_get_hierarchical_structure(request, table_id):
    """
    Get the table structure with nested children arrays for hierarchical tree view.

    Returns a structure where ordinates have a 'children' array instead of flat list.
    """
    try:
        table = get_object_or_404(TABLE, table_id=table_id)

        structure = {
            'table': {
                'table_id': table.table_id,
                'name': table.name,
                'code': table.code,
                'description': table.description,
            },
            'axes': {
                'Y': [],
                'X': [],
            },
        }

        axes = AXIS.objects.filter(table_id=table).order_by('orientation', 'order')

        for axis in axes:
            orientation_key = 'Y' if axis.orientation in ['Y', '2'] else 'X'

            axis_data = {
                'axis_id': axis.axis_id,
                'orientation': axis.orientation,
                'name': axis.name,
                'code': axis.code,
                'order': axis.order,
                'ordinates': [],
            }

            # Get all ordinates for this axis
            ordinates = AXIS_ORDINATE.objects.filter(
                axis_id=axis
            ).order_by('level', 'order')

            # Build ordinate lookup and children map
            ordinate_dict = {}
            children_map = {}

            for ordinate in ordinates:
                # Get ordinate items
                items = ORDINATE_ITEM.objects.filter(
                    axis_ordinate_id=ordinate
                ).select_related('variable_id', 'member_id')

                ordinate_items = [{
                    'variable_id': item.variable_id_id if item.variable_id else None,
                    'variable_name': item.variable_id.name if item.variable_id else None,
                    'member_id': item.member_id_id if item.member_id else None,
                    'member_name': item.member_id.name if item.member_id else None,
                } for item in items]

                ordinate_data = {
                    'ordinate_id': ordinate.axis_ordinate_id,
                    'name': ordinate.name or '',
                    'code': ordinate.code or '',
                    'order': ordinate.order or 0,
                    'level': ordinate.level or 0,
                    'is_abstract_header': ordinate.is_abstract_header or False,
                    'parent_id': ordinate.parent_axis_ordinate_id_id if ordinate.parent_axis_ordinate_id else None,
                    'ordinate_items': ordinate_items,
                    'children': [],
                }

                ordinate_dict[ordinate.axis_ordinate_id] = ordinate_data

                # Track parent-child relationships
                parent_id = ordinate.parent_axis_ordinate_id_id if ordinate.parent_axis_ordinate_id else None
                if parent_id not in children_map:
                    children_map[parent_id] = []
                children_map[parent_id].append(ordinate.axis_ordinate_id)

            # Build tree structure
            roots = children_map.get(None, [])

            def build_tree(ordinate_id):
                node = ordinate_dict[ordinate_id]
                child_ids = children_map.get(ordinate_id, [])
                node['children'] = [build_tree(cid) for cid in child_ids]
                return node

            axis_data['ordinates'] = [build_tree(rid) for rid in roots]
            structure['axes'][orientation_key].append(axis_data)

        return JsonResponse({
            'success': True,
            'structure': structure,
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["PUT"])
def api_reparent_ordinate(request, ordinate_id):
    """
    Move an ordinate to a new parent, updating levels for all descendants.

    Request body:
    {
        "new_parent_id": "PARENT_ID" or null for root level,
        "new_order": 0  // optional, position within siblings
    }
    """
    try:
        ordinate = get_object_or_404(AXIS_ORDINATE, axis_ordinate_id=ordinate_id)
        data = json.loads(request.body)

        new_parent_id = data.get('new_parent_id')
        new_order = data.get('new_order')

        with transaction.atomic():
            # Get the new parent (if any)
            new_parent = None
            new_level = 0

            if new_parent_id:
                new_parent = get_object_or_404(AXIS_ORDINATE, axis_ordinate_id=new_parent_id)

                # Validate parent is in the same axis
                if new_parent.axis_id_id != ordinate.axis_id_id:
                    return JsonResponse({
                        'success': False,
                        'error': 'Parent must be in the same axis.'
                    }, status=400)

                # Prevent circular references
                if is_descendant_of(new_parent, ordinate):
                    return JsonResponse({
                        'success': False,
                        'error': 'Cannot move ordinate under its own descendant.'
                    }, status=400)

                new_level = (new_parent.level or 0) + 1

            # Calculate level difference
            old_level = ordinate.level or 0
            level_diff = new_level - old_level

            # Update the ordinate
            ordinate.parent_axis_ordinate_id = new_parent
            ordinate.level = new_level

            if new_order is not None:
                ordinate.order = new_order

            ordinate.save()

            # Update all descendants' levels
            updated_ordinates = [{'ordinate_id': ordinate_id, 'level': new_level}]
            update_descendant_levels(ordinate, level_diff, updated_ordinates)

        return JsonResponse({
            'success': True,
            'updated_ordinates': updated_ordinates,
        })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body.'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def is_descendant_of(potential_descendant, ancestor):
    """Check if potential_descendant is a descendant of ancestor."""
    current = potential_descendant
    while current.parent_axis_ordinate_id:
        if current.parent_axis_ordinate_id_id == ancestor.axis_ordinate_id:
            return True
        current = current.parent_axis_ordinate_id
    return False


def update_descendant_levels(parent, level_diff, updated_list):
    """Recursively update levels of all descendants."""
    children = AXIS_ORDINATE.objects.filter(parent_axis_ordinate_id=parent)

    for child in children:
        child.level = (child.level or 0) + level_diff
        child.save()
        updated_list.append({
            'ordinate_id': child.axis_ordinate_id,
            'level': child.level
        })
        update_descendant_levels(child, level_diff, updated_list)
