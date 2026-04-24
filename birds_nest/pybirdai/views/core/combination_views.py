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
"""
CRUD views for combination and output layer operations.
"""
import hashlib
from collections import Counter, defaultdict
from urllib.parse import unquote
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction

from pybirdai.models.bird_meta_data_model import (
    COMBINATION, COMBINATION_ITEM, CUBE, CUBE_STRUCTURE_ITEM,
    CUBE_TO_COMBINATION, TABLE, TABLE_CELL, AXIS, ORDINATE_ITEM
)
from pybirdai.services.table_rendering_service import TableRenderingService
from .view_helpers import paginated_modelformset_view, redirect_with_allowed_query_params


def _get_output_layer_combination_ids(cube):
    """Return the combination IDs linked to an output layer cube."""
    if not cube:
        return []

    return list(
        CUBE_TO_COMBINATION.objects.filter(
            cube_id=cube,
            combination_id__isnull=False,
        ).values_list('combination_id__combination_id', flat=True).distinct()
    )


def _get_output_layer_combination_lookup(cube, combination_ids):
    """
    Build a lookup from report-cell combination IDs to output-layer combination IDs.

    In the report filter flow, combinations linked to a cube are often qualified as
    ``{cube_id}_{table_cell_combination_id}``, while TABLE_CELL stores only the
    short ``table_cell_combination_id``. This lookup keeps both forms aligned.
    """
    lookup = defaultdict(list)
    cube_prefix = f"{cube.cube_id}_" if cube and cube.cube_id else ""

    for combination_id in combination_ids:
        if not combination_id:
            continue

        candidate_keys = [combination_id]
        if cube_prefix and combination_id.startswith(cube_prefix):
            candidate_keys.append(combination_id[len(cube_prefix):])

        for key in dict.fromkeys(candidate_keys):
            if combination_id not in lookup[key]:
                lookup[key].append(combination_id)

    return dict(lookup)


def _resolve_output_layer_table(cube, combination_lookup):
    """
    Resolve the report TABLE that backs an output layer.

    Prefer matching TABLE_CELL rows by linked combination IDs because it works
    across both legacy and newer cube naming conventions. If no match is found,
    fall back to the newer cube naming pattern where the table ID is the cube ID
    without the trailing ``_CUBE`` suffix.
    """
    lookup_keys = list(combination_lookup.keys())
    matched_cells = list(
        TABLE_CELL.objects.filter(
            table_cell_combination_id__in=lookup_keys,
        ).exclude(
            table_id__isnull=True,
        ).select_related('table_id')
    ) if lookup_keys else []

    matched_cell_combination_ids = sorted({
        cell.table_cell_combination_id
        for cell in matched_cells
        if cell.table_cell_combination_id
    })
    matched_output_layer_combination_ids = sorted({
        linked_combination_id
        for cell in matched_cells
        for linked_combination_id in combination_lookup.get(cell.table_cell_combination_id, [])
    })
    table_match_counts = Counter(
        cell.table_id.table_id
        for cell in matched_cells
        if cell.table_id
    )

    resolved_table = None
    resolution_method = None

    if table_match_counts:
        resolved_table_id, _ = max(
            table_match_counts.items(),
            key=lambda item: (item[1], item[0]),
        )
        resolved_table = TABLE.objects.filter(table_id=resolved_table_id).first()
        if resolved_table:
            resolution_method = 'combination-match'

    if resolved_table is None and cube and cube.cube_id and cube.cube_id.endswith('_CUBE'):
        fallback_table_id = cube.cube_id[:-5]
        resolved_table = TABLE.objects.filter(table_id=fallback_table_id).first()
        if resolved_table:
            resolution_method = 'cube-id-fallback'

    return {
        'table': resolved_table,
        'resolution_method': resolution_method,
        'matched_cell_combination_ids': matched_cell_combination_ids,
        'matched_output_layer_combination_ids': matched_output_layer_combination_ids,
        'table_match_counts': dict(table_match_counts),
    }


def _annotate_report_layout_with_output_layer_combinations(report_layout, combination_lookup):
    """Attach resolved output-layer combination IDs to rendered report cells."""
    if not report_layout or not report_layout.get('rows'):
        return report_layout

    for row in report_layout['rows']:
        for cell in row.get('cells', []):
            table_combination_id = cell.get('combination_id')
            linked_combination_ids = combination_lookup.get(table_combination_id, [])

            if linked_combination_ids:
                cell['output_layer_combination_ids'] = linked_combination_ids
                cell['output_layer_combination_id'] = linked_combination_ids[0]
            else:
                cell['output_layer_combination_ids'] = []
                cell['output_layer_combination_id'] = None

    return report_layout


def _build_non_reference_combination_signature(ordinate_items):
    """Mirror the NROLC signature based on metric plus variable/member pairs."""
    metric_id = None
    item_pairs = set()

    for item in ordinate_items:
        if not item.variable_id:
            continue

        variable_id = item.variable_id.variable_id
        member_id = item.member_id.member_id if item.member_id else None
        item_pairs.add((variable_id, member_id))

        if metric_id is None and item.member_id is None:
            metric_id = variable_id

    return metric_id, tuple(sorted(item_pairs))


def _build_non_reference_combination_id(table, signature):
    """Create a deterministic synthetic ID for a computed non-reference combination."""
    metric_id, item_pairs = signature
    signature_parts = [metric_id or '']
    signature_parts.extend(
        f"{variable_id}:{member_id or ''}"
        for variable_id, member_id in item_pairs
    )
    digest = hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:12]
    table_key = "".join(
        character if character.isalnum() else "_"
        for character in table.table_id
    ).strip("_")
    return f"NONREF_{table_key}_{digest}"


def _build_non_reference_combination_data(table):
    """
    Compute non-reference combinations directly from the table's ordinate items.

    Non-reference EBA tables often do not carry persisted TABLE_CELL rows, so
    we reconstruct the report grid from the table axes and then compute each
    cell's combination from the row/column leaf ordinate items.
    """
    axes = AXIS.objects.filter(table_id=table).order_by('orientation', 'order')
    row_axes = [axis for axis in axes if axis.orientation in ('Y', '2')]
    col_axes = [axis for axis in axes if axis.orientation in ('X', '1')]

    row_tree = TableRenderingService._build_ordinate_tree(row_axes)
    col_tree = TableRenderingService._build_ordinate_tree(col_axes)
    row_leaves = TableRenderingService._get_leaf_ordinates(row_tree)
    col_leaves = TableRenderingService._get_leaf_ordinates(col_tree)

    leaf_ids = [
        ordinate.axis_ordinate_id
        for ordinate in row_leaves + col_leaves
    ]
    ordinate_items = ORDINATE_ITEM.objects.filter(
        axis_ordinate_id__in=leaf_ids
    ).select_related(
        'axis_ordinate_id',
        'variable_id',
        'member_id',
    )

    ordinate_to_items = defaultdict(list)
    for item in ordinate_items:
        ordinate_to_items[item.axis_ordinate_id_id].append(item)

    combination_items_by_id = {}
    combination_id_by_signature = {}
    rows = []

    for row_index, row_ordinate in enumerate(row_leaves):
        row_ordinate_items = ordinate_to_items.get(row_ordinate.axis_ordinate_id, [])
        row_cells = []

        for column_index, column_ordinate in enumerate(col_leaves):
            column_ordinate_items = ordinate_to_items.get(column_ordinate.axis_ordinate_id, [])
            visible_items = [
                item for item in (row_ordinate_items + column_ordinate_items)
                if item.variable_id
            ]

            combination_id = None
            if visible_items:
                signature = _build_non_reference_combination_signature(visible_items)
                combination_id = combination_id_by_signature.get(signature)
                if combination_id is None:
                    combination_id = _build_non_reference_combination_id(table, signature)
                    combination_id_by_signature[signature] = combination_id
                    combination_items_by_id[combination_id] = sorted(
                        [
                            {
                                'variable_id': item.variable_id,
                                'member_id': item.member_id,
                            }
                            for item in visible_items
                        ],
                        key=lambda item: (
                            item['variable_id'].variable_id if item['variable_id'] else "",
                            item['member_id'].member_id if item['member_id'] else "",
                        ),
                    )

            row_cells.append({
                'cell_id': f"{row_ordinate.axis_ordinate_id}__{column_ordinate.axis_ordinate_id}",
                'combination_id': None,
                'non_reference_combination_id': combination_id,
                'row_ordinate_id': row_ordinate.axis_ordinate_id,
                'column_ordinate_id': column_ordinate.axis_ordinate_id,
                'row_index': row_index,
                'column_index': column_index,
                'is_shaded': False,
                'is_executable': False,
                'datapoint_id': None,
                'name': None,
            })

        rows.append({
            'row_ordinate_id': row_ordinate.axis_ordinate_id,
            'row_index': row_index,
            'cells': row_cells,
        })

    return {
        'report_layout': {
            'success': True,
            'table_id': table.table_id,
            'name': table.name or '',
            'code': table.code or '',
            'description': table.description or '',
            'column_headers': TableRenderingService._build_column_headers(col_tree),
            'row_headers': TableRenderingService._build_row_headers(row_tree),
            'rows': rows,
            'metadata': {
                'total_cells': len(row_leaves) * len(col_leaves),
                'executable_cells': 0,
                'shaded_cells': 0,
                'row_count': len(row_leaves),
                'column_count': len(col_leaves),
            },
        },
        'combination_items_by_id': combination_items_by_id,
    }


def _resolve_non_reference_table(reference_table, cube=None):
    """
    Resolve the non-reference report table that corresponds to a reference table.

    Reference tables use ECB/REF metadata while the non-reference report tables
    come from the EBA templates. We primarily match by stripped table code and
    version, with a direct cube/table fallback when an EBA table is already in play.
    """
    if reference_table:
        if reference_table.maintenance_agency_id_id == 'EBA':
            return reference_table

        base_code = (reference_table.code or '').replace('_REF', '')
        if base_code:
            candidates = TABLE.objects.filter(
                maintenance_agency_id_id='EBA',
                code=base_code,
            ).order_by('table_id')

            exact_version_match = candidates.filter(version=reference_table.version).first()
            if exact_version_match:
                return exact_version_match

            if reference_table.version:
                prefix_version_match = candidates.filter(
                    version__startswith=reference_table.version
                ).first()
                if prefix_version_match:
                    return prefix_version_match

            first_candidate = candidates.first()
            if first_candidate:
                return first_candidate

    if cube and cube.cube_id:
        direct_table = TABLE.objects.filter(
            table_id=cube.cube_id,
            maintenance_agency_id_id='EBA',
        ).first()
        if direct_table:
            return direct_table

    return None


def combinations(request):
    """Paginated edit view for combinations."""
    return paginated_modelformset_view(request, COMBINATION, 'pybirdai/miscellaneous/combinations.html', order_by='combination_id')


def combination_items(request):
    """Paginated edit view with filters for combination items."""
    if request.GET.get('source') == 'non_reference':
        table_id = request.GET.get('table_id', '')
        selected_combination = request.GET.get('combination_id', '')
        selected_member = request.GET.get('member_id', '')
        selected_variable = request.GET.get('variable_id', '')
        selected_table = TABLE.objects.filter(table_id=table_id).first()
        computed_error = None
        computed_items = []
        unique_member_ids = []
        unique_variable_ids = []

        if not selected_table:
            computed_error = 'The non-reference report table could not be found.'
        elif not selected_combination:
            computed_error = 'Choose a non-reference combination to inspect its items.'
        else:
            combination_data = _build_non_reference_combination_data(selected_table)
            all_items = list(
                combination_data['combination_items_by_id'].get(selected_combination, [])
            )
            if not all_items:
                computed_error = (
                    'That non-reference combination could not be reconstructed from the report table.'
                )
            else:
                unique_member_ids = sorted({
                    item['member_id'].member_id
                    for item in all_items
                    if item.get('member_id')
                })
                unique_variable_ids = sorted({
                    item['variable_id'].variable_id
                    for item in all_items
                    if item.get('variable_id')
                })

                computed_items = [
                    item for item in all_items
                    if (not selected_variable or (
                        item.get('variable_id')
                        and item['variable_id'].variable_id == selected_variable
                    )) and (not selected_member or (
                        item.get('member_id')
                        and item['member_id'].member_id == selected_member
                    ))
                ]

        page_number = request.GET.get('page', 1)
        paginator = Paginator(computed_items, 20)
        page_obj = paginator.get_page(page_number)

        context = {
            'computed_mode': True,
            'computed_error': computed_error,
            'page_obj': page_obj,
            'selected_combination': selected_combination,
            'selected_member': selected_member,
            'selected_variable': selected_variable,
            'selected_non_reference_table': selected_table,
            'unique_member_ids': unique_member_ids,
            'unique_variable_ids': unique_variable_ids,
        }
        return render(request, 'pybirdai/miscellaneous/combination_items.html', context)

    # Get unique values for filters
    unique_combinations = COMBINATION_ITEM.objects.values_list('combination_id', flat=True).distinct()
    unique_member_ids = COMBINATION_ITEM.objects.values_list('member_id', flat=True).distinct()
    unique_variable_ids = COMBINATION_ITEM.objects.values_list('variable_id', flat=True).distinct()

    # Get all combinations for the create form
    all_combinations = COMBINATION.objects.all().order_by('combination_id')

    # Get filter values from request
    selected_combination = request.GET.get('combination_id', '')
    selected_member = request.GET.get('member_id', '')
    selected_variable = request.GET.get('variable_id', '')

    # Apply filters and ordering
    queryset = COMBINATION_ITEM.objects.select_related(
        'variable_id',
        'member_id',
    ).all().order_by('id')
    if selected_combination:
        queryset = queryset.filter(combination_id=selected_combination)
    if selected_member:
        queryset = queryset.filter(member_id=selected_member)
    if selected_variable:
        queryset = queryset.filter(variable_id=selected_variable)

    # Add pagination and formset creation
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(COMBINATION_ITEM, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'COMBINATION_ITEM updated successfully.')
            return redirect_with_allowed_query_params(
                request,
                'pybirdai:combination_items',
                ('page', 'combination_id', 'member_id', 'variable_id'),
            )
        else:
            messages.error(request, 'There was an error updating the COMBINATION_ITEM.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'unique_combinations': unique_combinations,
        'unique_member_ids': unique_member_ids,
        'unique_variable_ids': unique_variable_ids,
        'selected_combination': selected_combination,
        'selected_member': selected_member,
        'selected_variable': selected_variable,
        'all_combinations': all_combinations,
    }
    return render(request, 'pybirdai/miscellaneous/combination_items.html', context)


def output_layers(request):
    """Paginated edit view for output layers (RC cubes)."""
    # Output layers are identified by cube_type='RC'; cube IDs are framework/table based.
    page_number = request.GET.get('page', 1)
    queryset = CUBE.objects.filter(cube_type='RC').order_by('cube_id')
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)
    selected_output_layer = request.GET.get('output_layer', '')

    ModelFormSet = modelformset_factory(CUBE, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Output Layers updated successfully.')
            return redirect_with_allowed_query_params(
                request,
                'pybirdai:output_layers',
                ('page', 'output_layer'),
            )
        else:
            messages.error(request, 'There was an error updating the Output Layers.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    selected_output_layer_obj = None
    selected_output_layer_items = []
    selected_output_layer_table = None
    selected_output_layer_report_layout = None
    selected_output_layer_report_error = None
    selected_output_layer_combination_ids = []
    selected_output_layer_matched_combination_count = 0
    selected_output_layer_resolution_method = None
    selected_output_layer_combination_lookup = {}
    selected_output_layer_non_reference_table = None
    selected_output_layer_non_reference_report_layout = None
    selected_output_layer_non_reference_report_error = None
    selected_output_layer_non_reference_combination_count = 0

    if selected_output_layer:
        selected_output_layer_obj = queryset.filter(cube_id=selected_output_layer).select_related(
            'cube_structure_id'
        ).first()
        if selected_output_layer_obj and selected_output_layer_obj.cube_structure_id:
            selected_output_layer_items = list(
                CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=selected_output_layer_obj.cube_structure_id
                ).select_related(
                    'variable_id',
                    'member_id',
                    'subdomain_id',
                    'variable_set_id',
                    'attribute_associated_variable',
                ).order_by('order', 'cube_variable_code', 'id')
            )

        if selected_output_layer_obj:
            selected_output_layer_combination_ids = _get_output_layer_combination_ids(
                selected_output_layer_obj
            )
            selected_output_layer_combination_lookup = _get_output_layer_combination_lookup(
                selected_output_layer_obj,
                selected_output_layer_combination_ids,
            )
            if selected_output_layer_combination_ids:
                table_resolution = _resolve_output_layer_table(
                    selected_output_layer_obj,
                    selected_output_layer_combination_lookup,
                )
                selected_output_layer_table = table_resolution['table']
                selected_output_layer_resolution_method = table_resolution['resolution_method']
                selected_output_layer_matched_combination_count = len(
                    table_resolution['matched_output_layer_combination_ids']
                )

                if selected_output_layer_table:
                    selected_output_layer_report_layout = TableRenderingService.render_table(
                        selected_output_layer_table.table_id
                    )
                    if not selected_output_layer_report_layout.get('success'):
                        selected_output_layer_report_error = selected_output_layer_report_layout.get(
                            'error', 'Unable to render the report layout for this output layer.'
                        )
                        selected_output_layer_report_layout = None
                    else:
                        selected_output_layer_report_layout = _annotate_report_layout_with_output_layer_combinations(
                            selected_output_layer_report_layout,
                            selected_output_layer_combination_lookup,
                        )
                else:
                    selected_output_layer_report_error = (
                        'No report table could be resolved from the combinations linked to this output layer.'
                    )
            else:
                selected_output_layer_report_error = (
                    'This output layer has no linked combinations yet, so there is no report layout to inspect.'
                )

        selected_output_layer_non_reference_table = _resolve_non_reference_table(
            selected_output_layer_table,
            selected_output_layer_obj,
        )

        if selected_output_layer_non_reference_table:
            non_reference_combination_data = _build_non_reference_combination_data(
                selected_output_layer_non_reference_table
            )
            selected_output_layer_non_reference_combination_count = len(
                non_reference_combination_data['combination_items_by_id']
            )
            selected_output_layer_non_reference_report_layout = (
                non_reference_combination_data['report_layout']
            )
            if not selected_output_layer_non_reference_report_layout.get('success'):
                selected_output_layer_non_reference_report_error = (
                    selected_output_layer_non_reference_report_layout.get(
                        'error',
                        'Unable to render the non-reference report layout for this output layer.',
                    )
                )
                selected_output_layer_non_reference_report_layout = None
        elif selected_output_layer_obj:
            selected_output_layer_non_reference_report_error = (
                'No non-reference report table could be resolved for this output layer.'
            )

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'output_layers': queryset.values_list('cube_id', flat=True),
        'selected_output_layer': selected_output_layer,
        'selected_output_layer_obj': selected_output_layer_obj,
        'selected_output_layer_items': selected_output_layer_items,
        'selected_output_layer_table': selected_output_layer_table,
        'selected_output_layer_report_layout': selected_output_layer_report_layout,
        'selected_output_layer_report_error': selected_output_layer_report_error,
        'selected_output_layer_combination_ids': selected_output_layer_combination_ids,
        'selected_output_layer_combination_count': len(selected_output_layer_combination_ids),
        'selected_output_layer_matched_combination_count': selected_output_layer_matched_combination_count,
        'selected_output_layer_unmatched_combination_count': max(
            len(selected_output_layer_combination_ids) - selected_output_layer_matched_combination_count,
            0,
        ),
        'selected_output_layer_resolution_method': selected_output_layer_resolution_method,
        'selected_output_layer_non_reference_table': selected_output_layer_non_reference_table,
        'selected_output_layer_non_reference_report_layout': selected_output_layer_non_reference_report_layout,
        'selected_output_layer_non_reference_report_error': selected_output_layer_non_reference_report_error,
        'selected_output_layer_non_reference_combination_count': selected_output_layer_non_reference_combination_count,
    }
    return render(request, 'pybirdai/miscellaneous/output_layers.html', context)


def delete_combination(request, combination_id):
    """Delete combination."""
    try:
        item = get_object_or_404(COMBINATION, combination_id=combination_id)
        item.delete()
        messages.success(request, 'COMBINATION deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'COMBINATION deletion')
    return redirect('pybirdai:edit_combinations')


def delete_combination_item(request, item_id):
    """Delete combination item."""
    try:
        # Get the combination_id and member_id from the POST data
        combination_id = request.POST.get('combination_id')
        member_id = request.POST.get('member_id')

        if not all([combination_id, member_id]):
            raise ValueError("Missing required fields for deletion")

        # Get the item using the composite key fields
        item = COMBINATION_ITEM.objects.get(
            combination_id=COMBINATION.objects.get(combination_id=combination_id),
            member_id=member_id
        )
        item.delete()
        messages.success(request, 'COMBINATION_ITEM deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'COMBINATION_ITEM deletion')
    return redirect('pybirdai:edit_combination_items')


def delete_cube(request, cube_id):
    """Delete cube with URL decoding."""
    try:
        decoded_cube_id = unquote(cube_id)
        item = get_object_or_404(CUBE, cube_id=decoded_cube_id)
        item.delete()
        messages.success(request, 'CUBE deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'CUBE deletion')
    return redirect('pybirdai:edit_output_layers')
