"""
Views for the new Output Layer Mapping Workflow.
This module provides a multi-step workflow for creating output layer mappings
with non-reference combinations, domain management, and cube structure generation.
"""

from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.db import transaction, models
from django.forms import formset_factory
import json
import datetime

from pybirdai.models.bird_meta_data_model import (
    TABLE, TABLE_CELL, COMBINATION, COMBINATION_ITEM,
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION,
    MAPPING_DEFINITION, VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM,
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, MAPPING_TO_CUBE,
    DOMAIN, MEMBER, SUBDOMAIN, SUBDOMAIN_ENUMERATION,
    VARIABLE, FRAMEWORK, MAINTENANCE_AGENCY
)
from pybirdai.process_steps.output_layer_mapping_workflow.mapping_orchestrator import OutputLayerMappingOrchestrator
from pybirdai.process_steps.output_layer_mapping_workflow.combination_creator import CombinationCreator
from pybirdai.process_steps.output_layer_mapping_workflow.domain_manager import DomainManager
from pybirdai.process_steps.output_layer_mapping_workflow.cube_structure_generator import CubeStructureGenerator
from pybirdai.process_steps.output_layer_mapping_workflow.naming_utils import NamingUtils


def select_table_for_mapping(request):
    """
    Step 1: Allow user to select a table for mapping.
    Displays dropdowns for Framework, Version, and Table Code.
    """
    if request.method == 'POST':
        # Store selection in session
        request.session['olmw_framework'] = request.POST.get('framework')
        request.session['olmw_version'] = request.POST.get('version')
        request.session['olmw_table_code'] = request.POST.get('table_code')
        request.session['olmw_table_id'] = request.POST.get('table_id')

        # Redirect to check for existing mappings
        return redirect('pybirdai:output_layer_mapping_step1_5')

    # GET request - show selection form
    # Only pass frameworks; versions and tables will be loaded via AJAX
    frameworks = FRAMEWORK.objects.all().order_by('framework_id')

    context = {
        'frameworks': frameworks,
        'step': 1,
        'total_steps': 5
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step1_select_table.html', context)


def check_existing_mappings(request):
    """
    Step 1.5: Check for existing mappings for the selected table.
    Allows user to choose between using existing mapping, creating new, or modifying existing.
    Uses string matching on MAPPING_TO_CUBE.cube_mapping_id based on table code and version.
    """
    from pybirdai.models.bird_meta_data_model import MAPPING_TO_CUBE

    # Check if table was selected in previous step
    if 'olmw_table_id' not in request.session:
        messages.error(request, 'Please select a table first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    try:
        table = TABLE.objects.get(table_id=table_id)
    except TABLE.DoesNotExist:
        messages.error(request, 'Selected table not found.')
        return redirect('pybirdai:output_layer_mapping_step1')

    # Query for existing mappings related to this table using string matching on cube_mapping_id
    # Use table.code and table.version to construct search pattern

    # Start with filtering by table code (required)
    mapping_query = MAPPING_TO_CUBE.objects.filter(
        cube_mapping_id__icontains=table.code
    )

    # Add version filter if available (version format: "3.0" may appear as "3_0" or "3.0" in cube_mapping_id)
    if table.version:
        # Try both formats: with dot and with underscore
        version_patterns = [
            table.version,  # e.g., "3.0"
            table.version.replace('.', '_'),  # e.g., "3_0"
            table.version.replace('.', '')  # e.g., "30"
        ]

        # Build Q object for OR condition on version patterns
        version_q = models.Q()
        for pattern in version_patterns:
            version_q |= models.Q(cube_mapping_id__icontains=pattern)

        mapping_query = mapping_query.filter(version_q)

    # Get mapping IDs
    mapping_to_cube_links = mapping_query.values_list('mapping_id', flat=True).distinct()

    # Get the actual mapping definitions
    existing_mappings = MAPPING_DEFINITION.objects.filter(
        mapping_id__in=mapping_to_cube_links
    ).distinct().select_related('variable_mapping_id', 'member_mapping_id')

    if request.method == 'POST':
        mapping_mode = request.POST.get('mapping_mode', 'new')
        request.session['olmw_mapping_mode'] = mapping_mode

        if mapping_mode == 'use_existing':
            # User wants to use an existing mapping as-is
            existing_mapping_id = request.POST.get('existing_mapping_id')
            if not existing_mapping_id:
                messages.error(request, 'Please select a mapping to use.')
                return redirect('pybirdai:output_layer_mapping_step1_5')

            request.session['olmw_existing_mapping_id'] = existing_mapping_id
            messages.success(request, f'Using existing mapping: {MAPPING_DEFINITION.objects.get(mapping_id=existing_mapping_id).name}')
            # Skip to confirmation
            return redirect('pybirdai:output_layer_mapping_step5')

        elif mapping_mode == 'modify_existing':
            # User wants to modify an existing mapping
            existing_mapping_id = request.POST.get('existing_mapping_id')
            if not existing_mapping_id:
                messages.error(request, 'Please select a mapping to modify.')
                return redirect('pybirdai:output_layer_mapping_step1_5')

            request.session['olmw_existing_mapping_id'] = existing_mapping_id
            # Load existing mapping data into session for modification
            _load_existing_mapping_to_session(request, existing_mapping_id)
            messages.info(request, 'Existing mapping loaded. You can now modify it.')
            return redirect('pybirdai:output_layer_mapping_step2_5')

        else:  # mapping_mode == 'new'
            # User wants to create a completely new mapping
            messages.info(request, 'Creating new mapping from scratch.')
            return redirect('pybirdai:output_layer_mapping_step2_5')

    # Prepare context for template
    mapping_details = []
    for mapping in existing_mappings:
        details = {
            'mapping': mapping,
            'variable_count': 0,
            'member_count': 0,
            'created_date': mapping.maintenance_agency_id.name if mapping.maintenance_agency_id else 'Unknown',
        }

        if mapping.variable_mapping_id:
            details['variable_count'] = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id=mapping.variable_mapping_id
            ).count()

        if mapping.member_mapping_id:
            details['member_count'] = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping.member_mapping_id
            ).count()

        mapping_details.append(details)

    context = {
        'table': table,
        'existing_mappings': mapping_details,
        'has_existing': len(mapping_details) > 0,
        'step': 1.5,
        'total_steps': 7  # Updated from 5 to 7
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step1_5_existing_mappings.html', context)


def _load_existing_mapping_to_session(request, mapping_id):
    """
    Helper function to load existing mapping data into session for modification.
    """
    try:
        mapping = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)

        # Load variable mappings
        if mapping.variable_mapping_id:
            var_items = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id=mapping.variable_mapping_id
            )
            # Convert to session format
            # This is a placeholder - actual implementation would need to match the session structure
            breakdowns = {}
            for item in var_items:
                if item.variable_id:
                    breakdowns[str(item.variable_id.variable_id)] = {
                        'source': str(item.variable_id.variable_id),
                        'rule': 'direct'  # Default, would need to be extracted from algorithm
                    }
            request.session['olmw_breakdowns'] = json.dumps(breakdowns)

        # Load member mappings and other details
        # This would need to be expanded based on the actual mapping structure

    except MAPPING_DEFINITION.DoesNotExist:
        messages.error(request, 'Mapping not found.')


def define_variable_breakdown(request):
    """
    Step 2.5: Allow user to define the breakdown of source variables.
    For each dimension in the selected table, show current domain/members
    and allow selection of source variables and transformation rules.
    This step requires ordinates to be selected first (in Step 2).
    """
    # Check if we have table selection in session
    if 'olmw_table_id' not in request.session:
        messages.error(request, 'Please select a table first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    table = TABLE.objects.get(table_id=table_id)

    # Check if ordinates have been selected (required from Step 2)
    if 'olmw_selected_ordinates' not in request.session:
        messages.warning(request, 'Please select ordinates first to determine which dimensions to map.')
        return redirect('pybirdai:output_layer_mapping_step2_5')

    selected_ordinates = request.session.get('olmw_selected_ordinates', [])

    # If ordinates were selected, filter variables to only those from selected ordinates
    if selected_ordinates:
        from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM

        # Get variables directly from selected ordinates
        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('variable_id', 'member_id')

        # Build variables dict from ordinate items
        variables = {}
        for item in ordinate_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if var_id not in variables:
                    variables[var_id] = {
                        'variable': item.variable_id,
                        'domain': item.variable_id.domain_id if hasattr(item.variable_id, 'domain_id') else None,
                        'members': set(),
                        'subdomain': None  # ORDINATE_ITEM doesn't have subdomain_id
                    }
                if item.member_id:
                    variables[var_id]['members'].add(item.member_id)
    else:
        # Fallback: Get all variables from table combinations (original behavior)
        # Get table cells with their combinations
        cells = TABLE_CELL.objects.filter(table_id=table).select_related()

        # Get unique combinations from cells
        combination_ids = cells.values_list('table_cell_combination_id', flat=True).distinct()
        combinations = COMBINATION.objects.filter(combination_id__in=combination_ids)

        # Get unique variables from combination items
        combination_items = COMBINATION_ITEM.objects.filter(
            combination_id__in=combinations
        ).select_related('variable_id', 'member_id', 'subdomain_id')

        # Group by variable for display
        variables = {}
        for item in combination_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if var_id not in variables:
                    variables[var_id] = {
                        'variable': item.variable_id,
                        'domain': item.variable_id.domain_id if hasattr(item.variable_id, 'domain_id') else None,
                        'members': set(),
                        'subdomain': item.subdomain_id
                    }
                if item.member_id:
                    variables[var_id]['members'].add(item.member_id)

    if request.method == 'POST':
        # Store breakdown definitions in session
        breakdowns = {}
        for var_id in variables:
            source_var = request.POST.get(f'source_{var_id}')
            transform_rule = request.POST.get(f'rule_{var_id}')
            if source_var:
                breakdowns[var_id] = {
                    'source': source_var,
                    'rule': transform_rule
                }

        request.session['olmw_breakdowns'] = json.dumps(breakdowns)

        # Process and store variable groups
        groups = {}
        # Extract all group IDs from POST data
        group_ids = set()
        for key in request.POST.keys():
            if key.startswith('group_') and key.endswith('_name'):
                # Extract group ID from key like "group_1_name"
                group_id = key.replace('_name', '')
                group_ids.add(group_id)

        # Build groups dictionary
        for group_id in group_ids:
            group_name = request.POST.get(f'{group_id}_name', '')
            group_variables = request.POST.get(f'{group_id}_variables', '')
            group_mapping_type = request.POST.get(f'{group_id}_mapping_type', 'many_to_one')
            group_targets = request.POST.get(f'{group_id}_targets', '')

            # Parse comma-separated lists
            variable_ids = [v.strip() for v in group_variables.split(',') if v.strip()]
            target_ids = [t.strip() for t in group_targets.split(',') if t.strip()]

            if variable_ids:  # Only store non-empty groups
                groups[group_id] = {
                    'name': group_name,
                    'variable_ids': variable_ids,
                    'mapping_type': group_mapping_type,
                    'targets': target_ids
                }

        request.session['olmw_variable_groups'] = json.dumps(groups)

        return redirect('pybirdai:output_layer_mapping_step3')

    # GET request - show breakdown form
    # Get all variables for dropdown
    all_variables = VARIABLE.objects.all().order_by('name')

    context = {
        'table': table,
        'variables': variables,
        'all_variables': all_variables,
        'step': 2,
        'total_steps': 5
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step2_variable_breakdown.html', context)


def select_axis_ordinates(request):
    """
    Step 2.5: Allow user to select which axis ordinates should be mapped.
    These ordinates will be used to filter which cells are processed.
    """
    from pybirdai.models.bird_meta_data_model import (
        CELL_POSITION, AXIS_ORDINATE, AXIS
    )

    # Check if table was selected in previous step
    if 'olmw_table_id' not in request.session:
        messages.error(request, 'Please select a table first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    try:
        table = TABLE.objects.get(table_id=table_id)
    except TABLE.DoesNotExist:
        messages.error(request, 'Selected table not found.')
        return redirect('pybirdai:output_layer_mapping_step1')

    # Get all cells for this table
    table_cells = TABLE_CELL.objects.filter(table_id=table)

    # Get all cell positions for these cells
    cell_positions = CELL_POSITION.objects.filter(
        cell_id__in=table_cells
    ).select_related('axis_ordinate_id', 'axis_ordinate_id__axis_id')

    # Build unique axis ordinates with their details
    ordinates_data = {}
    for cp in cell_positions:
        ordinate = cp.axis_ordinate_id
        if ordinate and ordinate.axis_ordinate_id not in ordinates_data:
            # Use the ordinate name directly instead of querying ORDINATE_ITEM
            ordinates_data[ordinate.axis_ordinate_id] = {
                'ordinate': ordinate,
                'axis': ordinate.axis_id,
                'axis_name': ordinate.axis_id.name if ordinate.axis_id else 'Unknown',
                'axis_orientation': ordinate.axis_id.orientation if ordinate.axis_id else 'Unknown',
                'name': ordinate.name or ordinate.axis_ordinate_id,
                'is_abstract': ordinate.is_abstract_header,
                'level': ordinate.level or 0,
                'order': ordinate.order or 0,
                'cell_count': 0,
                'parent': ordinate.parent_axis_ordinate_id
            }

    # Count cells for each ordinate
    for cp in cell_positions:
        if cp.axis_ordinate_id and cp.axis_ordinate_id.axis_ordinate_id in ordinates_data:
            ordinates_data[cp.axis_ordinate_id.axis_ordinate_id]['cell_count'] += 1

    if request.method == 'POST':
        # Get selected ordinates from form
        selected_ordinates = request.POST.getlist('selected_ordinates')

        if not selected_ordinates:
            messages.warning(request, 'No ordinates selected. All cells will be mapped.')
            request.session['olmw_selected_ordinates'] = []
        else:
            request.session['olmw_selected_ordinates'] = selected_ordinates
            messages.success(request, f'Selected {len(selected_ordinates)} ordinate(s) for mapping.')

        # Continue to Step 2 (Variable Breakdown)
        return redirect('pybirdai:output_layer_mapping_step2')

    # Prepare data for template
    # Sort ordinates by axis orientation (row/column) and then by order
    sorted_ordinates = sorted(
        ordinates_data.values(),
        key=lambda x: (
            x['axis_orientation'] if x['axis_orientation'] else 'Z',
            x['level'],
            x['order']
        )
    )

    # Group by axis for better display
    axes_groups = {}
    for ord_data in sorted_ordinates:
        axis_name = ord_data['axis_name']
        if axis_name not in axes_groups:
            axes_groups[axis_name] = {
                'name': axis_name,
                'orientation': ord_data['axis_orientation'],
                'ordinates': []
            }
        axes_groups[axis_name]['ordinates'].append(ord_data)

    context = {
        'table': table,
        'axes_groups': axes_groups,
        'total_ordinates': len(ordinates_data),
        'total_cells': table_cells.count(),
        'step': 2.5,
        'total_steps': 7  # Updated from 5 to 7
    }

    # Generate embedded table HTML for visual selection
    table_html = generate_table_html(table, ordinates_data)
    context['embedded_table_html'] = table_html
    context['has_table_preview'] = True

    return render(request, 'pybirdai/output_layer_mapping_workflow/step2_5_select_ordinates.html', context)


def generate_table_html(table, ordinates_data):
    """
    Generate HTML representation of TABLE with selectable ordinates.
    Creates an interactive table matching the actual table structure.
    """
    from pybirdai.models.bird_meta_data_model import (
        TABLE_CELL, CELL_POSITION, AXIS_ORDINATE, AXIS
    )

    # Get all cells for this table
    cells = TABLE_CELL.objects.filter(table_id=table).select_related()

    # Build structure: (row_ordinate_id, col_ordinate_id) -> cell
    cell_matrix = {}
    row_ordinates_set = set()
    col_ordinates_set = set()

    for cell in cells:
        positions = CELL_POSITION.objects.filter(cell_id=cell).select_related(
            'axis_ordinate_id', 'axis_ordinate_id__axis_id'
        )

        row_ord_id = None
        col_ord_id = None

        for pos in positions:
            if pos.axis_ordinate_id and pos.axis_ordinate_id.axis_id:
                orientation = pos.axis_ordinate_id.axis_id.orientation
                ord_id = pos.axis_ordinate_id.axis_ordinate_id

                if orientation in ['Y', '2']:  # Row
                    row_ord_id = ord_id
                    row_ordinates_set.add(ord_id)
                elif orientation in ['X', '1']:  # Column
                    col_ord_id = ord_id
                    col_ordinates_set.add(ord_id)

        if row_ord_id and col_ord_id:
            cell_matrix[(row_ord_id, col_ord_id)] = {
                'cell': cell,
                'is_shaded': cell.is_shaded,
                'name': cell.name or ''
            }

    # Get ordered lists of ordinates
    row_ordinates = []
    col_ordinates = []

    for ord_id, ord_data in ordinates_data.items():
        orientation = ord_data.get('axis_orientation', '')
        if orientation in ['Y', '2'] and ord_id in row_ordinates_set:
            row_ordinates.append({
                'id': ord_id,
                'data': ord_data,
                'order': ord_data.get('order', 0),
                'level': ord_data.get('level', 0)
            })
        elif orientation in ['X', '1'] and ord_id in col_ordinates_set:
            col_ordinates.append({
                'id': ord_id,
                'data': ord_data,
                'order': ord_data.get('order', 0),
                'level': ord_data.get('level', 0)
            })

    # Sort by level and order
    row_ordinates.sort(key=lambda x: (x['level'], x['order']))
    col_ordinates.sort(key=lambda x: (x['level'], x['order']))

    # Generate HTML
    html = ['<table class="embedded-table table-bordered">']

    # Header row with column ordinates
    html.append('<thead><tr>')
    html.append('<th class="corner-cell">Select Ordinates</th>')  # Corner cell

    for col_ord in col_ordinates:
        name = col_ord['data'].get('name', 'N/A')

        html.append(
            f'<th class="col-header" data-ordinate-id="{col_ord["id"]}" '
            f'title="Click to select this column\n{name}">'
            f'{name}</th>'
        )

    html.append('</tr></thead>')

    # Body rows
    html.append('<tbody>')

    for row_ord in row_ordinates:
        html.append('<tr>')

        # Row header
        name = row_ord['data'].get('name', 'N/A')

        # Add indentation for hierarchy
        level = row_ord.get('level', 0)
        indent_style = f'padding-left: {level * 20 + 8}px;' if level > 0 else ''

        html.append(
            f'<th class="row-header" data-ordinate-id="{row_ord["id"]}" '
            f'style="{indent_style}" '
            f'title="Click to select this row\n{name}">'
            f'{name}</th>'
        )

        # Data cells
        for col_ord in col_ordinates:
            cell_key = (row_ord['id'], col_ord['id'])

            if cell_key in cell_matrix:
                cell_info = cell_matrix[cell_key]
                cell_name = cell_info['name']
                is_shaded = cell_info['is_shaded']

                shade_class = 'cell-shaded' if is_shaded else ''
                html.append(
                    f'<td class="data-cell {shade_class}" '
                    f'data-row-ordinate="{row_ord["id"]}" '
                    f'data-col-ordinate="{col_ord["id"]}" '
                    f'title="Cell: {cell_name}\nClick to select both ordinates">'
                    f'<span class="cell-indicator">●</span></td>'
                )
            else:
                # Empty cell - no data
                html.append(
                    f'<td class="data-cell cell-empty" '
                    f'data-row-ordinate="{row_ord["id"]}" '
                    f'data-col-ordinate="{col_ord["id"]}" '
                    f'title="No data in this cell">'
                    f'<span class="empty-indicator">—</span></td>'
                )

        html.append('</tr>')

    html.append('</tbody>')
    html.append('</table>')

    return '\n'.join(html)


def edit_mappings_tabbed(request):
    """
    Step 3: Tabbed interface for editing mappings.
    Provides tabs for Dimensions, Measures, Filters, and Validation.
    """
    # Check session
    if 'olmw_table_id' not in request.session or 'olmw_breakdowns' not in request.session:
        messages.error(request, 'Please complete previous steps first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    table = TABLE.objects.get(table_id=table_id)
    breakdowns = json.loads(request.session['olmw_breakdowns'])

    # Filter by selected ordinates if any were selected
    selected_ordinates = request.session.get('olmw_selected_ordinates', [])
    if selected_ordinates:
        from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM

        # Get variables from selected ordinates
        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('variable_id')

        # Get unique variable IDs from selected ordinates
        allowed_variable_ids = set(
            str(item.variable_id.variable_id)
            for item in ordinate_items
            if item.variable_id
        )

        # Filter breakdowns to only include allowed variables
        filtered_breakdowns = {}
        for var_id, breakdown in breakdowns.items():
            if var_id in allowed_variable_ids:
                filtered_breakdowns[var_id] = breakdown
        breakdowns = filtered_breakdowns

        # Add message to inform user about filtering
        if len(filtered_breakdowns) < len(json.loads(request.session['olmw_breakdowns'])):
            messages.info(request, f'Filtered to {len(filtered_breakdowns)} variables based on selected ordinates.')

    # Prepare data for tabs
    dimension_mappings = []
    measure_mappings = []
    filter_conditions = []

    # Get variables and categorize them
    for var_id, breakdown in breakdowns.items():
        variable = VARIABLE.objects.get(variable_id=var_id)

        # Determine if dimension or measure based on variable properties
        # This is a simplified logic - you may need to adjust based on actual business rules
        if variable.variable_id.startswith('TYP_') or variable.variable_id.endswith('_ID'):
            dimension_mappings.append({
                'variable': variable,
                'source': breakdown['source'],
                'rule': breakdown.get('rule', 'DIRECT_MAP')
            })
        else:
            measure_mappings.append({
                'variable': variable,
                'source': breakdown['source'],
                'rule': breakdown.get('rule', 'SUM')
            })

    if request.method == 'POST':
        # Save mapping definitions
        mappings = {
            'dimensions': {},
            'measures': {},
            'filters': {}
        }

        # Process dimension mappings
        for dim in dimension_mappings:
            var_id = dim['variable'].variable_id
            mappings['dimensions'][var_id] = {
                'source': request.POST.get(f'dim_source_{var_id}'),
                'target': request.POST.get(f'dim_target_{var_id}'),
                'rule': request.POST.get(f'dim_rule_{var_id}'),
                'when': request.POST.get(f'dim_when_{var_id}', '')
            }

        # Process measure mappings
        for meas in measure_mappings:
            var_id = meas['variable'].variable_id
            mappings['measures'][var_id] = {
                'source': request.POST.get(f'meas_source_{var_id}'),
                'target': request.POST.get(f'meas_target_{var_id}'),
                'aggregation': request.POST.get(f'meas_agg_{var_id}'),
                'formula': request.POST.get(f'meas_formula_{var_id}', '')
            }

        # Process filters
        filter_count = int(request.POST.get('filter_count', 0))
        for i in range(filter_count):
            mappings['filters'][f'filter_{i}'] = {
                'variable': request.POST.get(f'filter_var_{i}'),
                'operator': request.POST.get(f'filter_op_{i}'),
                'value': request.POST.get(f'filter_val_{i}')
            }

        request.session['olmw_mappings'] = json.dumps(mappings)
        return redirect('pybirdai:output_layer_mapping_step4')

    # GET request - show tabbed editor
    context = {
        'table': table,
        'dimension_mappings': dimension_mappings,
        'measure_mappings': measure_mappings,
        'filter_conditions': filter_conditions,
        'all_variables': VARIABLE.objects.all().order_by('name'),
        'step': 3,
        'total_steps': 5
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step3_mapping_editor.html', context)


def review_and_name_mapping(request):
    """
    Step 4: Review the complete mapping and provide a name.
    Shows summary of all mappings and prompts for a user-friendly name.
    """
    # Check session
    if 'olmw_mappings' not in request.session:
        messages.error(request, 'Please complete mapping configuration first.')
        return redirect('pybirdai:output_layer_mapping_step3')

    table_id = request.session['olmw_table_id']
    table = TABLE.objects.get(table_id=table_id)
    mappings = json.loads(request.session['olmw_mappings'])

    # Prepare summary
    summary = {
        'table': table,
        'dimension_count': len(mappings.get('dimensions', {})),
        'measure_count': len(mappings.get('measures', {})),
        'filter_count': len(mappings.get('filters', {})),
        'dimensions': mappings.get('dimensions', {}),
        'measures': mappings.get('measures', {}),
        'filters': mappings.get('filters', {})
    }

    if request.method == 'POST':
        mapping_name = request.POST.get('mapping_name')
        mapping_description = request.POST.get('mapping_description', '')

        if not mapping_name:
            messages.error(request, 'Please provide a name for the mapping.')
            return render(request, 'pybirdai/output_layer_mapping_workflow/step4_review_and_name.html', {
                'summary': summary,
                'step': 4,
                'total_steps': 5
            })

        # Store name and description
        request.session['olmw_mapping_name'] = mapping_name
        request.session['olmw_mapping_description'] = mapping_description

        # Generate internal ID using naming convention
        internal_id = NamingUtils.generate_internal_id(mapping_name)
        request.session['olmw_internal_id'] = internal_id

        return redirect('pybirdai:output_layer_mapping_step5')

    # GET request - show review form
    context = {
        'summary': summary,
        'step': 4,
        'total_steps': 5
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step4_review_and_name.html', context)


@transaction.atomic
def generate_structures(request):
    """
    Step 5: Generate all the required structures.
    Creates MAPPING_DEFINITION, VARIABLE_MAPPING, CUBE_STRUCTURE, etc.
    """
    # Check session
    if 'olmw_mapping_name' not in request.session:
        messages.error(request, 'Please provide a mapping name first.')
        return redirect('pybirdai:output_layer_mapping_step4')

    # Retrieve all session data
    table_id = request.session['olmw_table_id']
    framework = request.session['olmw_framework']
    version = request.session['olmw_version']
    table_code = request.session['olmw_table_code']
    mapping_name = request.session['olmw_mapping_name']
    mapping_description = request.session.get('olmw_mapping_description', '')
    internal_id = request.session['olmw_internal_id']
    mappings = json.loads(request.session['olmw_mappings'])

    if request.method == 'POST' and request.POST.get('confirm') == 'true':
        try:
            # Initialize orchestrator
            orchestrator = OutputLayerMappingOrchestrator()

            # Get or create maintenance agency
            maintenance_agency = MAINTENANCE_AGENCY.objects.first()
            if not maintenance_agency:
                maintenance_agency = MAINTENANCE_AGENCY.objects.create(
                    maintenance_agency_id='EFBT',
                    name='EFBT System',
                    code='EFBT'
                )

            # Generate timestamp for unique IDs
            timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

            # 1. Create VARIABLE_MAPPING
            variable_mapping = VARIABLE_MAPPING.objects.create(
                variable_mapping_id=f"{internal_id}_VAR_MAP_{timestamp}",
                maintenance_agency_id=maintenance_agency,
                name=mapping_name,
                code=internal_id
            )

            # 2. Create VARIABLE_MAPPING_ITEMS
            all_mappings = list(mappings['dimensions'].items()) + list(mappings['measures'].items())
            for var_id, mapping_def in all_mappings:
                # Source item
                source_var = VARIABLE.objects.filter(variable_id=mapping_def['source']).first()
                if source_var:
                    VARIABLE_MAPPING_ITEM.objects.create(
                        variable_mapping_id=variable_mapping,
                        variable_id=source_var,
                        is_source="True"
                    )

                # Target item
                target_var = VARIABLE.objects.filter(variable_id=mapping_def.get('target', var_id)).first()
                if target_var:
                    VARIABLE_MAPPING_ITEM.objects.create(
                        variable_mapping_id=variable_mapping,
                        variable_id=target_var,
                        is_source="False"
                    )

            # 3. Create MEMBER_MAPPING if needed
            member_mapping = None
            if mappings.get('dimensions'):
                member_mapping = MEMBER_MAPPING.objects.create(
                    member_mapping_id=f"{internal_id}_MEM_MAP_{timestamp}",
                    maintenance_agency_id=maintenance_agency,
                    name=f"{mapping_name} - Member Mappings",
                    code=f"{internal_id}_MEM"
                )

                # Create member mapping items
                row_counter = 1
                for var_id, mapping_def in mappings['dimensions'].items():
                    variable = VARIABLE.objects.filter(variable_id=var_id).first()
                    if variable and variable.domain_id:
                        # Get members from domain
                        members = MEMBER.objects.filter(domain_id=variable.domain_id)
                        for member in members:
                            MEMBER_MAPPING_ITEM.objects.create(
                                member_mapping_id=member_mapping,
                                member_mapping_row=str(row_counter),
                                variable_id=variable,
                                is_source="True",
                                member_id=member
                            )
                            row_counter += 1

            # 4. Create MAPPING_DEFINITION
            # Build algorithm description from mappings
            algorithm_parts = []
            for var_id, mapping_def in mappings['dimensions'].items():
                rule = mapping_def.get('rule', 'DIRECT_MAP')
                when = mapping_def.get('when', '')
                algorithm_parts.append(
                    f"SOURCE: {mapping_def['source']}\n"
                    f"TARGET: {mapping_def.get('target', var_id)}\n"
                    f"RULE: {rule}"
                )
                if when:
                    algorithm_parts.append(f"WHEN: {when}")

            for var_id, mapping_def in mappings['measures'].items():
                aggregation = mapping_def.get('aggregation', 'SUM')
                formula = mapping_def.get('formula', '')
                algorithm_parts.append(
                    f"MEASURE: {mapping_def['source']}\n"
                    f"TARGET: {mapping_def.get('target', var_id)}\n"
                    f"AGGREGATION: {aggregation}"
                )
                if formula:
                    algorithm_parts.append(f"FORMULA: {formula}")

            algorithm = "\n\n".join(algorithm_parts)

            mapping_definition = MAPPING_DEFINITION.objects.create(
                mapping_id=f"{internal_id}_MAP_DEF_{timestamp}",
                maintenance_agency_id=maintenance_agency,
                name=mapping_name,
                code=internal_id,
                mapping_type="VARIABLE_TO_VARIABLE",
                algorithm=algorithm,
                variable_mapping_id=variable_mapping,
                member_mapping_id=member_mapping
            )

            # 5. Create CUBE_STRUCTURE (reference)
            cube_structure = CUBE_STRUCTURE.objects.create(
                cube_structure_id=f"{table_code}_REF_STRUCTURE_{timestamp}",
                maintenance_agency_id=maintenance_agency,
                name=f"Reference structure for {mapping_name}",
                code=f"{internal_id}_CS",
                description=mapping_description or f"Generated reference cube structure for {table_code}",
                version=version
            )

            # 6. Create CUBE_STRUCTURE_ITEMS
            csi_generator = CubeStructureGenerator()
            order_counter = 1

            # Create CSI for dimensions
            for var_id in mappings['dimensions']:
                variable = VARIABLE.objects.filter(variable_id=var_id).first()
                if variable:
                    # Create or get subdomain
                    subdomain = csi_generator.create_or_get_subdomain(
                        variable, cube_structure.cube_structure_id
                    )

                    CUBE_STRUCTURE_ITEM.objects.create(
                        cube_structure_id=cube_structure,
                        cube_variable_code=f"{cube_structure.code}__{var_id}",
                        variable_id=variable,
                        role="D",  # Dimension
                        order=order_counter,
                        subdomain_id=subdomain,
                        dimension_type="B",  # Business dimension
                        is_mandatory=True,
                        description=f"Dimension for {variable.name}"
                    )
                    order_counter += 1

            # Create CSI for measures
            for var_id in mappings['measures']:
                variable = VARIABLE.objects.filter(variable_id=var_id).first()
                if variable:
                    CUBE_STRUCTURE_ITEM.objects.create(
                        cube_structure_id=cube_structure,
                        cube_variable_code=f"{cube_structure.code}__{var_id}",
                        variable_id=variable,
                        role="O",  # Observation/Metric
                        order=order_counter,
                        is_mandatory=True,
                        is_flow=True,
                        description=f"Measure for {variable.name}"
                    )
                    order_counter += 1

            # 7. Create CUBE
            framework_obj = FRAMEWORK.objects.filter(framework_id=framework).first()
            cube = CUBE.objects.create(
                cube_id=f"{table_code}_REF_{framework}_{version}_{timestamp}",
                maintenance_agency_id=maintenance_agency,
                name=f"Reference cube for {mapping_name}",
                code=f"{internal_id}_CUBE",
                framework_id=framework_obj,
                cube_structure_id=cube_structure,
                cube_type="RC",  # Reference Cube
                is_allowed=True,
                published=False,
                version=version,
                description=f"Generated reference cube for {mapping_name}"
            )

            # 8. Create non-reference combinations and link to cube
            table = TABLE.objects.get(table_id=table_id)
            cells = TABLE_CELL.objects.filter(table_id=table)

            # Filter cells based on selected ordinates if any
            selected_ordinates = request.session.get('olmw_selected_ordinates', [])
            if selected_ordinates:
                from pybirdai.models.bird_meta_data_model import CELL_POSITION

                # Get cells that have positions in selected ordinates
                cell_positions = CELL_POSITION.objects.filter(
                    axis_ordinate_id__in=selected_ordinates,
                    cell_id__in=cells
                ).values_list('cell_id', flat=True).distinct()

                # Filter to only cells with selected ordinates
                cells = cells.filter(cell_id__in=cell_positions)
                messages.info(request, f'Creating combinations for {cells.count()} cells matching selected ordinates.')

            combination_creator = CombinationCreator()
            created_combinations = []

            for cell in cells:
                # Create combination for this cell
                combination = combination_creator.create_combination_for_cell(
                    cell, cube, timestamp
                )
                if combination:
                    created_combinations.append(combination)

                    # Create CUBE_TO_COMBINATION link
                    CUBE_TO_COMBINATION.objects.create(
                        cube_id=cube,
                        combination_id=combination
                    )

            # 9. Create/Update domains and members as needed
            domain_manager = DomainManager()
            for var_id in mappings['dimensions']:
                variable = VARIABLE.objects.filter(variable_id=var_id).first()
                if variable:
                    domain_manager.ensure_domain_and_members(variable, maintenance_agency)

            # Clear session data
            for key in list(request.session.keys()):
                if key.startswith('olmw_'):
                    del request.session[key]

            # Prepare success context
            context = {
                'success': True,
                'mapping_name': mapping_name,
                'generated': {
                    'variable_mapping_id': variable_mapping.variable_mapping_id,
                    'mapping_definition_id': mapping_definition.mapping_id,
                    'cube_structure_id': cube_structure.cube_structure_id,
                    'cube_id': cube.cube_id,
                    'combinations_created': len(created_combinations)
                },
                'step': 5,
                'total_steps': 5
            }

            messages.success(request, f'Successfully created output layer mapping: {mapping_name}')

        except Exception as e:
            # Rollback is automatic with @transaction.atomic
            messages.error(request, f'Error creating structures: {str(e)}')
            context = {
                'success': False,
                'error': str(e),
                'step': 5,
                'total_steps': 5
            }

    else:
        # GET request - show confirmation page
        context = {
            'mapping_name': mapping_name,
            'mapping_description': mapping_description,
            'internal_id': internal_id,
            'table_code': table_code,
            'framework': framework,
            'version': version,
            'step': 5,
            'total_steps': 5,
            'preview': True
        }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step5_confirmation.html', context)


@require_http_methods(["GET"])
def get_table_cells_api(request):
    """
    API endpoint to get table cells and their combinations for AJAX requests.
    """
    table_id = request.GET.get('table_id')
    if not table_id:
        return JsonResponse({'error': 'Table ID required'}, status=400)

    try:
        table = TABLE.objects.get(table_id=table_id)
        cells = TABLE_CELL.objects.filter(table_id=table)

        cell_data = []
        for cell in cells:
            cell_info = {
                'cell_id': cell.cell_id,
                'name': cell.name,
                'is_shaded': cell.is_shaded,
                'combination_id': cell.table_cell_combination_id
            }

            # Get combination details if exists
            if cell.table_cell_combination_id:
                try:
                    combination = COMBINATION.objects.get(combination_id=cell.table_cell_combination_id)
                    cell_info['combination'] = {
                        'id': combination.combination_id,
                        'name': combination.name,
                        'code': combination.code
                    }
                except COMBINATION.DoesNotExist:
                    pass

            cell_data.append(cell_info)

        return JsonResponse({
            'table': {
                'id': table.table_id,
                'name': table.name,
                'code': table.code
            },
            'cells': cell_data
        })

    except TABLE.DoesNotExist:
        return JsonResponse({'error': 'Table not found'}, status=404)


@require_http_methods(["GET"])
def get_variable_domain_api(request):
    """
    API endpoint to get domain and members for a variable.
    """
    variable_id = request.GET.get('variable_id')
    if not variable_id:
        return JsonResponse({'error': 'Variable ID required'}, status=400)

    try:
        variable = VARIABLE.objects.get(variable_id=variable_id)

        response_data = {
            'variable': {
                'id': variable.variable_id,
                'name': variable.name,
                'code': variable.code
            }
        }

        # Get domain if exists
        if hasattr(variable, 'domain_id') and variable.domain_id:
            domain = variable.domain_id
            response_data['domain'] = {
                'id': domain.domain_id,
                'name': domain.name,
                'code': domain.code,
                'is_enumerated': domain.is_enumerated
            }

            # Get members if domain is enumerated
            if domain.is_enumerated:
                members = MEMBER.objects.filter(domain_id=domain).order_by('name')
                response_data['members'] = [
                    {
                        'id': member.member_id,
                        'name': member.name,
                        'code': member.code
                    }
                    for member in members
                ]

        return JsonResponse(response_data)

    except VARIABLE.DoesNotExist:
        return JsonResponse({'error': 'Variable not found'}, status=404)


@require_http_methods(["GET"])
def get_filter_options_api(request):
    """
    API endpoint for cascading filter options.
    Returns frameworks, versions filtered by framework, or tables filtered by framework+version.

    Query Parameters:
    - framework_code: Optional. If provided, filters versions and tables by this framework.
    - version: Optional. If provided (with framework_code), filters tables by both.

    Returns:
    - If no params: all frameworks
    - If framework_code only: versions that have tables matching that framework
    - If framework_code + version: tables matching both criteria
    """
    framework_code = request.GET.get('framework_code')
    version = request.GET.get('version')

    try:
        # Case 1: No parameters - return all frameworks
        if not framework_code:
            frameworks = FRAMEWORK.objects.all().order_by('framework_id')
            return JsonResponse({
                'status': 'success',
                'frameworks': [
                    {
                        'id': fw.framework_id,
                        'name': fw.name,
                        'code': fw.code
                    }
                    for fw in frameworks
                ]
            })

        # Case 2: Framework provided - return filtered versions
        if framework_code and not version:
            # Get all tables that match the framework (case-insensitive string matching)
            # Check if framework code appears in table_id or code
            tables = TABLE.objects.filter(
                models.Q(table_id__icontains=framework_code) |
                models.Q(code__icontains=framework_code)
            )

            # Get distinct versions from matching tables
            versions_list = tables.values_list('version', flat=True).distinct().order_by('version')
            versions_list = [v for v in versions_list if v]  # Filter out None/empty values

            return JsonResponse({
                'status': 'success',
                'versions': versions_list
            })

        # Case 3: Framework and version provided - return filtered tables
        if framework_code and version:
            # Filter tables by both framework (string matching) and version
            tables = TABLE.objects.filter(
                models.Q(table_id__icontains=framework_code) |
                models.Q(code__icontains=framework_code),
                version=version
            ).order_by('name')

            return JsonResponse({
                'status': 'success',
                'tables': [
                    {
                        'id': table.table_id,
                        'code': table.code,
                        'name': table.name,
                        'version': table.version
                    }
                    for table in tables
                ]
            })

        return JsonResponse({'error': 'Invalid parameters'}, status=400)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def get_domains(request):
    """
    API endpoint to get all domains for variable creation modal
    """
    try:
        from pybirdai.models.bird_meta_data_model import DOMAIN
        domains = DOMAIN.objects.all().values('domain_id', 'name')
        return JsonResponse({'domains': list(domains)})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def create_variable(request):
    """
    API endpoint to create a new variable
    """
    if request.method == 'POST':
        try:
            from pybirdai.models.bird_meta_data_model import VARIABLE, DOMAIN
            data = json.loads(request.body)

            variable_id = data.get('variable_id')

            # Check if variable already exists
            if VARIABLE.objects.filter(variable_id=variable_id).exists():
                return JsonResponse({'success': False, 'error': 'Variable ID already exists'})

            # Create variable
            variable = VARIABLE()
            variable.variable_id = variable_id
            variable.name = data.get('name')
            variable.code = data.get('code', '')
            variable.description = data.get('description', '')

            # Set domain if provided
            if data.get('domain_id'):
                try:
                    variable.domain_id = DOMAIN.objects.get(domain_id=data['domain_id'])
                except DOMAIN.DoesNotExist:
                    return JsonResponse({'success': False, 'error': 'Domain not found'})

            variable.save()

            return JsonResponse({
                'success': True,
                'variable': {
                    'variable_id': variable.variable_id,
                    'name': variable.name,
                    'code': variable.code,
                    'description': variable.description
                }
            })

        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    return JsonResponse({'success': False, 'error': 'Method not allowed'}, status=405)