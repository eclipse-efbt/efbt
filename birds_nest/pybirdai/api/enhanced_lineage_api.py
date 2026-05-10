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
#
"""
Enhanced lineage API that includes information about which rows and fields were actually used in calculations.
"""

from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.db.models import Prefetch, Q
from django.contrib.contenttypes.models import ContentType
from pybirdai.models import (
    Trail, MetaDataTrail, DatabaseTable, DerivedTable,
    DatabaseField, Function, FunctionText, TableCreationFunction,
    PopulatedDataBaseTable, EvaluatedDerivedTable, DatabaseRow,
    DerivedTableRow, DatabaseColumnValue, EvaluatedFunction,
    AortaTableReference, FunctionColumnReference, DerivedRowSourceReference,
    EvaluatedFunctionSourceValue, TableCreationSourceTable, TableCreationFunctionColumn,
    CalculationUsedRow, CalculationUsedField, CalculationChain, DataFlowEdge, CellLineage,
    CUBE, CUBE_STRUCTURE_ITEM
)
import ast
import importlib
import inspect
import json
from datetime import datetime
from functools import lru_cache
import logging
import re
from pybirdai.utils.secure_logging import sanitize_log_value
from textwrap import dedent


def serialize_datetime(obj):
    """JSON serializer for datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _resolve_reference_output_cube(output_table_name):
    """
    Resolve the reference output-layer cube for a derived output table name.

    Output tables in lineage often append a product suffix such as
    ``F_05_01_REF_FINREP_3_0_Other_loans`` while the cube is stored as
    ``F_05_01_REF_FINREP_3_0``. We progressively trim trailing segments until
    we find a matching cube.
    """
    if not output_table_name:
        return None

    exact_cube = CUBE.objects.filter(cube_id=output_table_name).select_related('cube_structure_id').first()
    if exact_cube:
        return exact_cube

    parts = output_table_name.split('_')
    for part_count in range(len(parts) - 1, 0, -1):
        candidate_cube_id = '_'.join(parts[:part_count])
        candidate_cube = CUBE.objects.filter(cube_id=candidate_cube_id).select_related('cube_structure_id').first()
        if candidate_cube:
            return candidate_cube

    return None


def _get_complete_rol_columns(output_table_name):
    """Return ordered cube-structure items for the matching reference output layer."""
    output_cube = _resolve_reference_output_cube(output_table_name)
    if not output_cube or not output_cube.cube_structure_id:
        return {
            "output_cube_id": None,
            "output_cube_structure_id": None,
            "complete_rol_columns": [],
        }

    cube_structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
        cube_structure_id=output_cube.cube_structure_id
    ).select_related(
        'variable_id'
    ).order_by(
        'order',
        'cube_variable_code',
        'id',
    )

    return {
        "output_cube_id": output_cube.cube_id,
        "output_cube_structure_id": output_cube.cube_structure_id.cube_structure_id,
        "complete_rol_columns": [
            {
                "variable_id": cube_structure_item.variable_id.variable_id,
                "name": cube_structure_item.variable_id.name,
                "description": cube_structure_item.variable_id.description,
            }
            for cube_structure_item in cube_structure_items
            if cube_structure_item.variable_id
        ],
    }


def _resolve_reference_output_table_name(trail, table_name, output_table_names=None):
    """
    Resolve the reference output-table alias backing a lineage table name.

    Bird's-eye lineage often renders product tables like ``Non_Negotiable_bonds``
    even when the corresponding output-layer wrapper table
    ``F_05_01_REF_FINREP_3_0_Non_Negotiable_bonds`` was filtered out. When that
    happens we still want to use the wrapper table's cube structure to render the
    complete ROL columns.
    """
    if not table_name:
        return None

    output_table_names = set(output_table_names or [])
    if table_name in output_table_names:
        return table_name

    candidate_query = EvaluatedDerivedTable.objects.filter(
        trail=trail,
        table__name__endswith=f'_{table_name}',
    )
    if output_table_names:
        candidate_query = candidate_query.filter(table__name__in=output_table_names)
    else:
        candidate_query = candidate_query.filter(
            Q(table__name__startswith='F_') | Q(table__name__startswith='Cell_')
        )

    candidate_names = list(
        candidate_query.values_list('table__name', flat=True).distinct()
    )

    if not candidate_names:
        return None

    candidate_names.sort(
        key=lambda name: (
            '_REF_' in name,
            name.startswith('F_'),
            len(name),
            name,
        ),
        reverse=True,
    )
    return candidate_names[0]


def _get_table_names_for_derived_rows(row_ids):
    if not row_ids:
        return set()

    return set(
        DerivedTableRow.objects.filter(
            id__in=row_ids
        ).values_list(
            'populated_table__table__name',
            flat=True,
        )
    )


def _candidate_output_tables_from_cell_name(cell_name, available_table_names):
    """
    Resolve output tables from a generated cell/calculation name without knowing
    any specific report table names.

    Generated cells usually embed the output table name after ``Cell_`` and
    before the final cell identifier. We choose the longest available evaluated
    table name that prefixes that remainder.
    """
    if not cell_name:
        return []

    remainder = cell_name[5:] if cell_name.startswith('Cell_') else cell_name
    matches = [
        table_name
        for table_name in available_table_names
        if remainder == table_name or remainder.startswith(f'{table_name}_')
    ]
    return sorted(matches, key=len, reverse=True)


def _candidate_output_tables_from_declared_output(output_table_name, available_table_names):
    """
    Resolve product-level output tables when the declared report output table was
    not evaluated directly.

    Some datapoints execute product tables such as
    ``F_05_01_REF_FINREP_3_0_Other_loans`` without creating the parent
    ``F_05_01_REF_FINREP_3_0`` union table. Those product tables still carry the
    reference output-layer cube context and should be treated as output wrappers
    for complete ROL display.
    """
    if not output_table_name:
        return []

    matches = [
        table_name
        for table_name in available_table_names
        if (
            table_name == output_table_name
            or (
                table_name.startswith(f'{output_table_name}_')
                and not table_name.endswith('_Table')
            )
        )
    ]
    return sorted(matches, key=len, reverse=True)


def _get_output_table_names(trail, calculation_name=None):
    """
    Infer output tables from lineage metadata instead of from literal table names.

    Prefer explicit calculation chains/cell lineage. Fall back to data-flow sinks,
    which are tables that receive lineage edges but do not feed another table.
    """
    available_table_names = set(
        EvaluatedDerivedTable.objects.filter(
            trail=trail
        ).values_list(
            'table__name',
            flat=True,
        )
    )
    output_table_names = set()

    chain_query = CalculationChain.objects.filter(trail=trail)
    if calculation_name:
        chain_query = chain_query.filter(chain_name=calculation_name)

    for chain in chain_query:
        for candidate_name in _candidate_output_tables_from_declared_output(
            chain.output_table,
            available_table_names,
        ):
            output_table_names.add(candidate_name)

        for candidate_name in _candidate_output_tables_from_cell_name(
            chain.output_cell_name or chain.chain_name,
            available_table_names,
        ):
            output_table_names.add(candidate_name)

    cell_query = CellLineage.objects.filter(trail=trail)
    if calculation_name:
        cell_query = cell_query.filter(cell_code=calculation_name)

    for cell in cell_query:
        cell_table_name = cell.cell_code if str(cell.cell_code).startswith('Cell_') else f'Cell_{cell.cell_code}'
        if cell_table_name in available_table_names:
            output_table_names.add(cell_table_name)

        for candidate_name in _candidate_output_tables_from_cell_name(cell.cell_code, available_table_names):
            output_table_names.add(candidate_name)

    if not output_table_names:
        source_labels = set(
            DataFlowEdge.objects.filter(
                trail=trail
            ).values_list(
                'source_label',
                flat=True,
            )
        )
        target_labels = set(
            DataFlowEdge.objects.filter(
                trail=trail
            ).values_list(
                'target_label',
                flat=True,
            )
        )
        for sink_name in target_labels - source_labels:
            if sink_name in available_table_names:
                output_table_names.add(sink_name)

    return output_table_names


def _get_root_derived_row_ids(derived_row_ids, output_table_names=None):
    if not derived_row_ids:
        return set()

    output_table_names = set(output_table_names or [])
    if output_table_names:
        output_row_ids = set(
            DerivedTableRow.objects.filter(
                id__in=derived_row_ids,
                populated_table__table__name__in=output_table_names,
            ).values_list('id', flat=True)
        )
        if output_row_ids:
            return output_row_ids

    derived_row_content_type = ContentType.objects.get_for_model(DerivedTableRow)
    rows_reused_by_other_used_rows = set(
        DerivedRowSourceReference.objects.filter(
            derived_row_id__in=derived_row_ids,
            content_type=derived_row_content_type,
            object_id__in=derived_row_ids,
        ).values_list('object_id', flat=True)
    )

    root_row_ids = set(derived_row_ids) - rows_reused_by_other_used_rows
    return root_row_ids or set(derived_row_ids)


def _trace_used_rows_backwards(root_derived_row_ids):
    """
    Walk row-source lineage backwards from dynamic calculation roots.

    Returns the derived/database row ids that transitively contributed to those
    roots, plus whether at least one row-source relationship existed.
    """
    from collections import deque

    traced_derived_row_ids = set(root_derived_row_ids)
    traced_database_row_ids = set()
    queue = deque(root_derived_row_ids)
    found_row_sources = False

    while queue:
        current_row_id = queue.popleft()
        refs = DerivedRowSourceReference.objects.filter(
            derived_row_id=current_row_id
        ).select_related(
            'content_type'
        )

        if refs.exists():
            found_row_sources = True

        for ref in refs:
            if ref.content_type.model == 'derivedtablerow':
                source_row_id = ref.object_id
                if source_row_id not in traced_derived_row_ids:
                    traced_derived_row_ids.add(source_row_id)
                    queue.append(source_row_id)
            elif ref.content_type.model == 'databaserow':
                traced_database_row_ids.add(ref.object_id)

    return found_row_sources, traced_derived_row_ids, traced_database_row_ids


def _get_display_table_for_output(trail, output_table_name, eval_table_by_name, output_table_names=None):
    """Find the non-output table whose rows should be displayed inside an output composite."""
    candidates = []
    for table_name, eval_table_data in eval_table_by_name.items():
        if table_name == output_table_name:
            continue

        resolved_output_table = _resolve_reference_output_table_name(
            trail,
            table_name,
            output_table_names,
        )
        if resolved_output_table != output_table_name:
            continue

        candidates.append(eval_table_data)

    if not candidates:
        return None

    candidates.sort(
        key=lambda table_data: (
            len(table_data.get('rows', [])) > 0,
            table_data.get('derivation_type') == 'property',
            len(table_data.get('rows', [])),
            table_data.get('table_name', ''),
        ),
        reverse=True,
    )
    return candidates[0]


def _get_display_table_from_row_sources(output_table, eval_table_by_row_id, output_table_names=None):
    """Find display rows by walking runtime row-source lineage from an output table."""
    from collections import deque

    output_table_names = set(output_table_names or [])
    output_table_name = output_table.get('table_name')
    root_row_ids = [
        row.get('id')
        for row in output_table.get('rows', [])
        if row.get('id')
    ]
    queue = deque(root_row_ids)
    visited_row_ids = set(root_row_ids)
    candidate_tables_by_name = {}

    while queue:
        current_row_id = queue.popleft()
        row_refs = DerivedRowSourceReference.objects.filter(
            derived_row_id=current_row_id,
        ).select_related(
            'content_type',
        )

        for ref in row_refs:
            if ref.content_type.model != 'derivedtablerow':
                continue

            source_row_id = ref.object_id
            if source_row_id not in visited_row_ids:
                visited_row_ids.add(source_row_id)
                queue.append(source_row_id)

            source_table = eval_table_by_row_id.get(source_row_id)
            if not source_table:
                continue

            source_table_name = source_table.get('table_name')
            if not source_table_name or source_table_name == output_table_name:
                continue
            if source_table_name in output_table_names:
                continue

            candidate_tables_by_name[source_table_name] = source_table

    candidates = list(candidate_tables_by_name.values())
    if not candidates:
        return None

    candidates.sort(
        key=lambda table_data: (
            max(
                [len(row.get('evaluated_functions', [])) for row in table_data.get('rows', [])] or [0]
            ),
            len(table_data.get('rows', [])),
            table_data.get('table_name', ''),
        ),
        reverse=True,
    )
    return candidates[0]


_NO_STATIC_DEFAULT = object()


def _extract_literal_ast_value(node):
    """Extract a JSON-serializable literal from a return AST node."""
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        if isinstance(node.operand.value, (int, float)):
            return -node.operand.value

    return _NO_STATIC_DEFAULT


def _extract_static_default_from_method(method):
    """
    Return a method's literal default when every return statement is constant.

    This is intended for output-layer columns like `TYP_INSTRMNT` that are
    intentionally defaulted in the logic code and therefore have no upstream
    lineage edges of their own.
    """
    try:
        method_source = dedent(inspect.getsource(method))
        tree = ast.parse(method_source)
    except (OSError, TypeError, SyntaxError):
        return _NO_STATIC_DEFAULT

    function_node = next(
        (node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))),
        None,
    )
    if function_node is None:
        return _NO_STATIC_DEFAULT

    return_values = []
    for node in ast.walk(function_node):
        if isinstance(node, ast.Return):
            literal_value = _extract_literal_ast_value(node.value)
            if literal_value is _NO_STATIC_DEFAULT:
                return _NO_STATIC_DEFAULT
            return_values.append(literal_value)

    if not return_values:
        return _NO_STATIC_DEFAULT

    first_value = return_values[0]
    if all(value == first_value for value in return_values[1:]):
        return first_value

    return _NO_STATIC_DEFAULT


@lru_cache(maxsize=256)
def _get_static_default_values(output_table_name, class_name):
    """Return column defaults declared as literal-return methods in the logic class."""
    output_cube = _resolve_reference_output_cube(output_table_name)
    if not output_cube or not class_name:
        return {}

    module_name = f"pybirdai.process_steps.filter_code.{output_cube.cube_id}_logic"
    try:
        logic_module = importlib.import_module(module_name)
        logic_class = getattr(logic_module, class_name, None)
    except (ImportError, AttributeError):
        return {}

    if logic_class is None:
        return {}

    static_defaults = {}
    for attr_name, attr_value in logic_class.__dict__.items():
        if not attr_name.isupper() or not inspect.isfunction(attr_value):
            continue

        literal_default = _extract_static_default_from_method(attr_value)
        if literal_default is not _NO_STATIC_DEFAULT:
            static_defaults[attr_name] = literal_default

    return static_defaults


def _extract_dependency_strings_from_function_text(function_text):
    """Extract declared @lineage dependency strings from stored function source."""
    if not function_text:
        return []

    lineage_match = re.search(
        r'@lineage\s*\(\s*dependencies\s*=\s*\{([^}]*)\}\s*\)',
        function_text,
        flags=re.DOTALL,
    )
    if not lineage_match:
        lineage_match = re.search(
            r'#\s*Lineage dependencies:\s*\{([^}]*)\}',
            function_text,
            flags=re.DOTALL,
        )
    if not lineage_match:
        return []

    dependency_block = lineage_match.group(1)
    return re.findall(r'["\']([^"\']+)["\']', dependency_block)


@require_http_methods(["GET"])
def get_trail_filtered_lineage(request, trail_id):
    """
    API endpoint that returns lineage information filtered to only include
    rows and fields that were actually used in calculations.
    
    Query parameters:
    - calculation_name: (optional) Filter to a specific calculation
    - include_unused: (optional) If 'true', include all data (default is to filter)
    
    Returns a JSON structure containing:
    - Trail metadata
    - Only the tables, rows, and fields that were used in calculations
    - Lineage relationships for the used data
    """
    trail = get_object_or_404(Trail, pk=trail_id)
    
    # Get query parameters
    calculation_name = request.GET.get('calculation_name')
    include_unused = request.GET.get('include_unused', 'false').lower() == 'true'
    
    try:
        # Get all calculations for this trail
        if calculation_name:
            used_rows = CalculationUsedRow.objects.filter(
                trail=trail,
                calculation_name=calculation_name
            )
            used_fields = CalculationUsedField.objects.filter(
                trail=trail,
                calculation_name=calculation_name
            )
        else:
            used_rows = CalculationUsedRow.objects.filter(trail=trail)
            used_fields = CalculationUsedField.objects.filter(trail=trail)
        
        # Build sets of used row and field IDs for efficient filtering
        used_row_ids = {
            'DatabaseRow': set(),
            'DerivedTableRow': set()
        }
        used_field_ids = {
            'DatabaseField': set(),
            'Function': set()
        }
        
        for used_row in used_rows:
            if used_row.content_type.model == 'databaserow':
                used_row_ids['DatabaseRow'].add(used_row.object_id)
            elif used_row.content_type.model == 'derivedtablerow':
                used_row_ids['DerivedTableRow'].add(used_row.object_id)
        
        for used_field in used_fields:
            if used_field.content_type.model == 'databasefield':
                used_field_ids['DatabaseField'].add(used_field.object_id)
            elif used_field.content_type.model == 'function':
                used_field_ids['Function'].add(used_field.object_id)

        output_table_names = _get_output_table_names(trail, calculation_name)
        
        # STRICT: Only include explicitly tracked functions and their direct dependencies
        calculation_relevant_function_ids = used_field_ids['Function'].copy()
        calculation_relevant_function_names = set()
        
        if not include_unused:
            # Add explicitly tracked function names
            for field_id in calculation_relevant_function_ids:
                try:
                    func = Function.objects.get(id=field_id)
                    calculation_relevant_function_names.add(func.name)
                except Function.DoesNotExist:
                    pass
            
            # Add direct dependencies of tracked functions
            for field_id in calculation_relevant_function_ids:
                deps = FunctionColumnReference.objects.filter(
                    function_id=field_id,
                    content_type__model='function',
                    trail=trail  # Scope to current trail execution
                ).values_list('object_id', flat=True)
                
                for dep_id in deps:
                    try:
                        dep_func = Function.objects.get(id=dep_id)
                        calculation_relevant_function_names.add(dep_func.name)
                    except Function.DoesNotExist:
                        pass
        
        # Trace backwards from this calculation's actual root rows. This keeps
        # the filtered view focused without assuming any report table name.
        allowed_derived_row_ids = set(used_row_ids['DerivedTableRow'])
        allowed_db_row_ids = set(used_row_ids['DatabaseRow'])
        
        print(f"Initial tracking data: {len(allowed_derived_row_ids)} DerivedTableRows, {len(allowed_db_row_ids)} DatabaseRows")
        
        if not include_unused and allowed_derived_row_ids:
            try:
                root_derived_row_ids = _get_root_derived_row_ids(
                    allowed_derived_row_ids,
                    output_table_names,
                )
                root_table_names = _get_table_names_for_derived_rows(root_derived_row_ids)
                if not output_table_names:
                    output_table_names.update(root_table_names)

                print(
                    "Tracing backwards from calculation roots: "
                    f"{len(root_derived_row_ids)} rows across {sorted(root_table_names)}"
                )

                backwards_tracing_worked, traced_derived_row_ids, traced_database_row_ids = _trace_used_rows_backwards(
                    root_derived_row_ids
                )

                if backwards_tracing_worked:
                    original_derived_count = len(allowed_derived_row_ids)
                    original_db_count = len(allowed_db_row_ids)
                    allowed_derived_row_ids = traced_derived_row_ids
                    allowed_db_row_ids |= traced_database_row_ids
                    print(
                        "Backwards tracing results: "
                        f"derived rows {original_derived_count} -> {len(allowed_derived_row_ids)}, "
                        f"database rows {original_db_count} -> {len(allowed_db_row_ids)}"
                    )
                else:
                    print("No DerivedRowSourceReference entries found for calculation roots; keeping explicitly tracked rows")
                    
            except Exception as e:
                print(f"Error in backwards tracing: {e}")
        
        print(f"Final filtered sets: {len(allowed_derived_row_ids)} DerivedTableRows, {len(allowed_db_row_ids)} DatabaseRows")

        # Initialize the lineage structure
        lineage_data = {
            "trail": {
                "id": trail.id,
                "name": trail.name,
                "created_at": trail.created_at.isoformat(),
                "execution_context": trail.execution_context,
                "metadata_trail_id": trail.metadata_trail.id
            },
            "calculation_filter": {
                "calculation_name": calculation_name or "all",
                "include_unused": include_unused,
                "total_used_rows": len(used_rows),
                "total_used_fields": len(used_fields)
            },
            "database_tables": [],
            "derived_tables": [],
            "populated_database_tables": [],
            "evaluated_derived_tables": [],
            "lineage_relationships": {
                "function_column_references": [],
                "derived_row_source_references": [],
                "evaluated_function_source_values": [],
                "table_creation_source_tables": [],
                "table_creation_function_columns": []
            },
            "metadata": {
                "generation_timestamp": datetime.now().isoformat(),
                "total_counts": {}
            }
        }
        
        # Process populated database tables
        populated_db_tables = PopulatedDataBaseTable.objects.filter(
            trail=trail
        ).select_related('table').prefetch_related(
            'table__database_fields',
            'databaserow_set__column_values__column'
        )
        
        tables_with_used_data = set()
        
        for pop_table in populated_db_tables:
            table = pop_table.table
            
            # Include explicitly used rows plus rows reached by lineage tracing.
            table_has_used_rows = False
            if not include_unused:
                for row in pop_table.databaserow_set.all():
                    if row.id in allowed_db_row_ids:
                        table_has_used_rows = True
                        break
            else:
                table_has_used_rows = True
            
            if table_has_used_rows:
                tables_with_used_data.add(table.id)
                
                # Add table definition
                if not any(dt['id'] == table.id for dt in lineage_data['database_tables']):
                    table_data = {
                        "id": table.id,
                        "name": table.name,
                        "fields": []
                    }
                    
                    # Add fields (either used ones or all if include_unused is True)
                    for field in table.database_fields.all():
                        if include_unused or field.id in used_field_ids['DatabaseField']:
                            table_data['fields'].append({
                                "id": field.id,
                                "name": field.name,
                                "table_id": table.id,
                                "was_used": field.id in used_field_ids['DatabaseField']
                            })
                    
                    # If no fields were found but the table was used, still include the table
                    # This can happen if field tracking is incomplete but row tracking worked
                    
                    lineage_data['database_tables'].append(table_data)
                
                # Add populated table instance
                pop_table_data = {
                    "id": pop_table.id,
                    "table_id": table.id,
                    "table_name": table.name,
                    "trail_id": trail.id,
                    "rows": []
                }
                
                # Add explicitly used rows plus traced source rows.
                for row in pop_table.databaserow_set.all():
                    if include_unused or row.id in allowed_db_row_ids:
                        row_data = {
                            "id": row.id,
                            "row_identifier": row.row_identifier,
                            "populated_table_id": pop_table.id,
                            "was_used": row.id in used_row_ids['DatabaseRow'],
                            "values": []
                        }
                        
                        # Build set of used field names for this specific table
                        # CRITICAL FIX: Use table name instead of ID to handle table ID mismatches
                        used_field_names_for_table = set()
                        for field_id in used_field_ids['DatabaseField']:
                            try:
                                field = DatabaseField.objects.get(id=field_id)
                                # Match by table name instead of ID to handle schema recreation/migration
                                if field.table.name == table.name:  # Field belongs to this table by name
                                    used_field_names_for_table.add(field.name)
                            except DatabaseField.DoesNotExist:
                                pass
                        
                        # Add column values - show ONLY fields that were explicitly tracked as used
                        for col_value in row.column_values.all():
                            field_name = col_value.column.name
                            
                            # Show only precisely tracked fields (or all if include_unused is True)
                            if include_unused or field_name in used_field_names_for_table:
                                row_data['values'].append({
                                    "id": col_value.id,
                                    "value": col_value.value,
                                    "string_value": col_value.string_value,
                                    "column_id": col_value.column.id,
                                    "column_name": col_value.column.name,
                                    "row_id": row.id,
                                    "was_used": field_name in used_field_names_for_table,
                                    "precision": "exact_field_tracking"
                                })
                        
                        pop_table_data['rows'].append(row_data)
                
                if pop_table_data['rows']:  # Only add if there are rows
                    lineage_data['populated_database_tables'].append(pop_table_data)
        
        # Process evaluated derived tables
        evaluated_tables = EvaluatedDerivedTable.objects.filter(
            trail=trail
        ).select_related('table', 'table__table_creation_function').prefetch_related(
            'table__derived_functions__function_text',
            'derivedtablerow_set__evaluated_functions__function'
        )
        
        for eval_table in evaluated_tables:
            table = eval_table.table
            
            # Include explicitly used rows plus rows reached by lineage tracing.
            table_has_used_rows = False
            if not include_unused:
                for row in eval_table.derivedtablerow_set.all():
                    if row.id in allowed_derived_row_ids:
                        table_has_used_rows = True
                        break
            else:
                table_has_used_rows = True
            
            # Check if this table appears in evaluated_derived_tables (for consistency)
            table_will_appear_in_evaluated = False
            if eval_table.derivedtablerow_set.exists():
                table_will_appear_in_evaluated = EvaluatedFunction.objects.filter(
                    row__populated_table=eval_table
                ).exists()
            
            # ENHANCED LOGIC: Also include tables that have functions used in calculations
            # but we'll still only show the specific rows that were used
            table_has_used_functions = False
            if not table_has_used_rows:
                # Check if any functions in this table were used
                table_functions_used = any(
                    func.table.id == table.id 
                    for field_id in used_field_ids['Function']
                    for func in [Function.objects.get(id=field_id)]
                    if Function.objects.filter(id=field_id).exists()
                )
                
                # Also check if any functions with the same table name were used (including dependencies)
                same_name_function_used = any(
                    func.table.name == table.name
                    for field_id in calculation_relevant_function_ids
                    for func in [Function.objects.get(id=field_id)]
                    if Function.objects.filter(id=field_id).exists()
                )
                
                # ENHANCED: Check for polymorphic functions that reference this table
                # Format: "F_05_01_REF_FINREP_3_0_UnionItem.GRSS_CRRYNG_AMNT@Other_loans"
                polymorphic_function_used = False
                table_name_lower = table.name
                for field_id in calculation_relevant_function_ids:
                    try:
                        func = Function.objects.get(id=field_id)
                        # Check if this function name contains "@table_name" (polymorphic reference)
                        if f"@{table_name_lower}" in func.name:
                            polymorphic_function_used = True
                            print(f"🔍 Found polymorphic function referencing {table_name_lower}: {func.name}")
                            break
                    except Function.DoesNotExist:
                        pass
                
                # Include table definition if it has used functions (but still filter rows strictly)
                if table_functions_used or same_name_function_used or table_will_appear_in_evaluated or polymorphic_function_used:
                    table_has_used_functions = True
            
            # Include table if it has used rows OR used functions
            if table_has_used_rows or table_has_used_functions:
                # Add table definition
                if not any(dt['id'] == table.id for dt in lineage_data['derived_tables']):
                    table_data = {
                        "id": table.id,
                        "name": table.name,
                        "table_creation_function_id": table.table_creation_function.id if table.table_creation_function else None,
                        "is_output_table": table.name in output_table_names,
                        "functions": []
                    }
                    
                    # Check if this table has any directly tracked functions
                    table_has_tracked_functions = any(
                        Function.objects.filter(id=fid, table_id=table.id).exists()
                        for fid in used_field_ids['Function']
                    )
                    
                    # We'll show all functions that were actually used in the calculation
                    
                    # First, get all function names that have evaluated values for this table
                    evaluated_function_names = set()
                    for row in eval_table.derivedtablerow_set.all():
                        for eval_func in row.evaluated_functions.all():
                            # Only include functions that match the table name pattern
                            if eval_func.function.table.name == table.name:
                                # Extract just the function name without table prefix
                                func_name = eval_func.function.name
                                if '.' in func_name:
                                    func_name = func_name.split('.')[-1]
                                evaluated_function_names.add(func_name)
                    
                    # Add functions that are used OR have evaluated values in this trail
                    # This ensures we show functions that contributed to the calculation
                    for function in table.derived_functions.all():
                        # Check if function is used by ID or by name (to handle data consistency issues)
                        is_used_by_id = function.id in used_field_ids['Function']
                        is_used_by_name = any(
                            Function.objects.filter(id=field_id).exists() and 
                            Function.objects.get(id=field_id).name == function.name
                            for field_id in used_field_ids['Function']
                        )
                        
                        # Check if this function has evaluated values in this table for this trail
                        # The relationship is: EvaluatedFunction -> row (DerivedTableRow) -> populated_table (EvaluatedDerivedTable)
                        # Also ensure the function belongs to the correct table
                        has_evaluated_values_by_id = EvaluatedFunction.objects.filter(
                            function_id=function.id,
                            function__table_id=table.id,
                            row__populated_table=eval_table
                        ).exists()
                        
                        # BALANCED: Include functions that are calculation-relevant OR have evaluated values in included tables
                        has_evaluated_values_by_name = False
                        
                        # Extract function name without table prefix for comparison
                        func_name_only = function.name
                        if '.' in func_name_only:
                            func_name_only = func_name_only.split('.')[-1]
                        
                        # Check if this function name appears in the evaluated function names
                        if func_name_only in evaluated_function_names:
                            has_evaluated_values_by_name = True
                        
                        # Check for polymorphic functions (functions with @ in the name indicating concrete class delegation)
                        has_polymorphic_values = False
                        if not has_evaluated_values_by_id and not has_evaluated_values_by_name:
                            # Look for polymorphic functions that match this base function
                            polymorphic_functions = Function.objects.filter(name__contains=f"{function.name}@")
                            for poly_func in polymorphic_functions:
                                # Check if the polymorphic function has evaluated values
                                poly_evaluated = EvaluatedFunction.objects.filter(
                                    function_id=poly_func.id,
                                    row__populated_table__trail=trail
                                ).exists()
                                if poly_evaluated:
                                    has_polymorphic_values = True
                                    break
                        
                        # Also include polymorphic functions directly (functions with @ in their name)
                        is_polymorphic_function = '@' in function.name
                        
                        has_evaluated_values = has_evaluated_values_by_id or has_evaluated_values_by_name or has_polymorphic_values or is_polymorphic_function
                        
                        if include_unused or is_used_by_id or is_used_by_name or has_evaluated_values or is_polymorphic_function:
                            function_data = {
                                "id": function.id,
                                "name": function.name,
                                "table_id": table.id,
                                "function_text_id": function.function_text.id if function.function_text else None,
                                "function_text": function.function_text.text if function.function_text else None,
                                "function_language": function.function_text.language if function.function_text else None,
                                "was_used": is_used_by_id or is_used_by_name or has_evaluated_values or is_polymorphic_function
                            }
                            
                            # Add all functions that were actually used in the calculation
                            # This includes:
                            # 1. Functions directly tracked via CalculationUsedField (is_used_by_id or is_used_by_name)
                            # 2. Functions that have evaluated values (were computed during calculation)
                            # 3. Polymorphic functions (with @ in their name) - these are always included
                            if is_used_by_id or is_used_by_name or has_evaluated_values or is_polymorphic_function:
                                table_data['functions'].append(function_data)
                    
                    lineage_data['derived_tables'].append(table_data)
                
                # Add evaluated table instance
                eval_table_data = {
                    "id": eval_table.id,
                    "table_id": table.id,
                    "table_name": table.name,
                    "is_output_table": table.name in output_table_names,
                    "trail_id": trail.id,
                    "rows": []
                }
                
                # Build set of used field names for this specific table
                used_field_names_for_derived_table = set()
                for field_id in used_field_ids['Function']:
                    try:
                        function = Function.objects.get(id=field_id)
                        # Match by table name instead of ID to handle data consistency issues
                        if function.table.name == table.name:  # Function belongs to this table by name
                            used_field_names_for_derived_table.add(function.name)
                        # ENHANCED: Also include polymorphic functions that reference this table
                        elif f"@{table.name}" in function.name:  # Polymorphic function references this table
                            used_field_names_for_derived_table.add(function.name)
                            # print(f"🔍 Including polymorphic function for {table.name}: {function.name}")
                    except Function.DoesNotExist:
                        pass
                
                # Add only rows that contributed to the final calculation (via backwards tracing)
                for row in eval_table.derivedtablerow_set.all():
                    # Use dynamic backwards tracing from the current calculation roots.
                    include_row = (
                        include_unused or
                        row.id in allowed_derived_row_ids
                    )

                    if include_row:
                        row_data = {
                            "id": row.id,
                            "row_identifier": row.row_identifier,
                            "populated_table_id": eval_table.id,
                            "was_used": row.id in used_row_ids['DerivedTableRow'],
                            "evaluated_functions": []
                        }
                        
                        # Add evaluated functions for rows in the traced calculation
                        # path. Some LDM output rows are reached through row lineage
                        # even when their output-layer functions were not directly
                        # recorded as CalculationUsedField entries.
                        for eval_func in row.evaluated_functions.all():
                            # IMPORTANT: Only include functions that actually belong to this table
                            # OR polymorphic functions that reference this table
                            # This filters out dependency functions from other tables
                            # Use table name comparison instead of ID due to data consistency issues
                            function_belongs_to_table = eval_func.function.table.name == table.name
                            is_polymorphic_for_this_table = f"@{table.name}" in eval_func.function.name
                            
                            if not function_belongs_to_table and not is_polymorphic_for_this_table:
                                continue
                                
                            function_name = eval_func.function.name
                            show_function = (include_unused or 
                                           eval_func.function.id in used_field_ids['Function'] or
                                           function_name in used_field_names_for_derived_table or
                                           row.id in allowed_derived_row_ids)
                            
                            if show_function:
                                row_data['evaluated_functions'].append({
                                    "id": eval_func.id,
                                    "value": eval_func.value,
                                    "string_value": eval_func.string_value,
                                    "function_id": eval_func.function.id,
                                    "function_name": eval_func.function.name,
                                    "row_id": row.id,
                                    "was_used": (eval_func.function.id in used_field_ids['Function'] or
                                               function_name in used_field_names_for_derived_table),
                                    "precision": "derived_table_function"
                                })
                        
                        # If no evaluated functions but the table has used functions, add placeholders 
                        # to show that these functions were accessed even if not calculated for this row
                        if not row_data['evaluated_functions'] and used_field_names_for_derived_table:
                            for field_id in used_field_ids['Function']:
                                try:
                                    function = Function.objects.get(id=field_id)
                                    if function.table.name == table.name:
                                        row_data['evaluated_functions'].append({
                                            "id": None,
                                            "value": None,
                                            "string_value": "Function was accessed but not calculated for this row",
                                            "function_id": function.id,
                                            "function_name": function.name,
                                            "row_id": row.id,
                                            "was_used": True,
                                            "precision": "function_accessed_but_not_calculated"
                                        })
                                except Function.DoesNotExist:
                                    pass
                        
                        # Keep traced rows even when they have no evaluated functions:
                        # they still anchor row-level lineage relationships.
                        if row_data['evaluated_functions'] or include_unused or row.id in allowed_derived_row_ids:
                            eval_table_data['rows'].append(row_data)
                
                if eval_table_data['rows']:  # Only add if there are rows
                    lineage_data['evaluated_derived_tables'].append(eval_table_data)
        
        # Get lineage relationships only for used data
        if not include_unused:
            # Function column references - include used functions AND Cell metric_value functions
            # First get used function IDs
            function_ids_to_include = set(used_field_ids['Function'])

            # Also include functions for all derived tables that made it into the
            # filtered payload, so output-layer @lineage property dependencies are
            # available for visual column lineage even when a function was not
            # directly tracked as a used calculation field.
            for derived_table in lineage_data['derived_tables']:
                for function_data in derived_table.get('functions', []):
                    function_id = function_data.get('id')
                    if function_id:
                        function_ids_to_include.add(function_id)

            # Also include ALL functions from Cell tables (metric_value, calc_referenced_items, etc.)
            # These functions have @lineage decorators with important dependency information
            cell_functions = Function.objects.filter(
                table__name__startswith='Cell_'
            ).values_list('id', flat=True)
            function_ids_to_include.update(cell_functions)

            # Filter by trail to get only lineage from THIS execution
            func_refs = FunctionColumnReference.objects.filter(
                function__id__in=function_ids_to_include,
                trail=trail  # Scope to current trail
            ).select_related('function', 'function__table', 'content_type')

            for ref in func_refs:
                # Resolve the referenced object to get its name and table
                ref_name = None
                ref_table_name = None

                if ref.content_type.model == 'databasefield':
                    try:
                        field = DatabaseField.objects.select_related('table').get(id=ref.object_id)
                        ref_name = field.name
                        ref_table_name = field.table.name
                    except DatabaseField.DoesNotExist:
                        pass
                elif ref.content_type.model == 'function':
                    try:
                        func = Function.objects.select_related('table').get(id=ref.object_id)
                        ref_name = func.name
                        ref_table_name = func.table.name if func.table else None
                    except Function.DoesNotExist:
                        pass

                # Parse dependency_string to get the logical table.column reference
                dep_table_name = None
                dep_column_name = None
                if ref.dependency_string and '.' in ref.dependency_string:
                    dep_clean = ref.dependency_string.replace('base.', '') if ref.dependency_string.startswith('base.') else ref.dependency_string
                    dep_table_name, dep_column_name = dep_clean.rsplit('.', 1)

                lineage_data['lineage_relationships']['function_column_references'].append({
                    "id": ref.id,
                    "function_id": ref.function.id,
                    "function_name": ref.function.name,
                    "function_table_name": ref.function.table.name if ref.function.table else None,
                    "referenced_object_type": ref.content_type.model,
                    "referenced_object_id": ref.object_id,
                    "referenced_name": ref_name,
                    "referenced_table_name": ref_table_name,
                    # Original dependency from @lineage decorator - use this for display
                    "dependency_string": ref.dependency_string,
                    "dependency_table_name": dep_table_name,
                    "dependency_column_name": dep_column_name,
                    "declared_dependency_only": False,
                })

            # Derived row source references for rows included in the filtered lineage.
            row_refs = DerivedRowSourceReference.objects.filter(
                derived_row__id__in=allowed_derived_row_ids
            ).select_related('derived_row', 'content_type')
            
            for ref in row_refs:
                lineage_data['lineage_relationships']['derived_row_source_references'].append({
                    "id": ref.id,
                    "derived_row_id": ref.derived_row.id,
                    "source_object_type": ref.content_type.model,
                    "source_object_id": ref.object_id
                })

            table_creation_function_ids = {
                table_data.get('table_creation_function_id')
                for table_data in lineage_data['derived_tables']
                if table_data.get('table_creation_function_id')
            }

            if table_creation_function_ids:
                display_target_table_names_by_output = {}
                for table_data in lineage_data['derived_tables']:
                    table_name = table_data.get('name')
                    tcf_id = table_data.get('table_creation_function_id')
                    if not table_name or tcf_id or table_name in output_table_names:
                        continue

                    resolved_output_table = _resolve_reference_output_table_name(
                        trail,
                        table_name,
                        output_table_names,
                    )
                    if not resolved_output_table or resolved_output_table == table_name:
                        continue

                    display_target_table_names_by_output.setdefault(resolved_output_table, [])
                    if table_name not in display_target_table_names_by_output[resolved_output_table]:
                        display_target_table_names_by_output[resolved_output_table].append(table_name)

                target_table_names_by_tcf = {}
                target_output_table_names_by_tcf = {}
                for table_data in lineage_data['derived_tables']:
                    tcf_id = table_data.get('table_creation_function_id')
                    table_name = table_data.get('name')
                    if not tcf_id or not table_name:
                        continue

                    target_output_table_names_by_tcf.setdefault(tcf_id, [])
                    if table_name not in target_output_table_names_by_tcf[tcf_id]:
                        target_output_table_names_by_tcf[tcf_id].append(table_name)

                    display_target_names = display_target_table_names_by_output.get(table_name) or [table_name]
                    target_table_names_by_tcf.setdefault(tcf_id, [])
                    for display_target_name in display_target_names:
                        if display_target_name not in target_table_names_by_tcf[tcf_id]:
                            target_table_names_by_tcf[tcf_id].append(display_target_name)

                table_src_refs = TableCreationSourceTable.objects.filter(
                    table_creation_function_id__in=table_creation_function_ids
                ).select_related('table_creation_function', 'content_type')

                for ref in table_src_refs:
                    source_table_name = ""
                    if ref.content_type.model == 'databasetable':
                        try:
                            source_table_name = DatabaseTable.objects.get(id=ref.object_id).name
                        except DatabaseTable.DoesNotExist:
                            pass
                    elif ref.content_type.model == 'derivedtable':
                        try:
                            source_table_name = DerivedTable.objects.get(id=ref.object_id).name
                        except DerivedTable.DoesNotExist:
                            pass

                    target_table_names = target_table_names_by_tcf.get(ref.table_creation_function_id, [])
                    target_output_table_names = target_output_table_names_by_tcf.get(ref.table_creation_function_id, [])
                    for target_table_name in target_table_names:
                        lineage_data['lineage_relationships']['table_creation_source_tables'].append({
                            "id": ref.id,
                            "table_creation_function_id": ref.table_creation_function.id,
                            "table_creation_function_name": ref.table_creation_function.name,
                            "source_object_type": ref.content_type.model,
                            "source_object_id": ref.object_id,
                            "source_table_name": source_table_name,
                            "target_table_name": target_table_name,
                            "target_output_table_name": target_output_table_names[0] if target_output_table_names else target_table_name,
                        })

                col_refs = TableCreationFunctionColumn.objects.filter(
                    table_creation_function_id__in=table_creation_function_ids
                ).select_related('table_creation_function', 'content_type')

                for ref in col_refs:
                    table_name = None
                    column_name = None
                    if ref.reference_text and '.' in ref.reference_text:
                        table_name, column_name = ref.reference_text.split('.', 1)

                    target_table_names = target_table_names_by_tcf.get(ref.table_creation_function_id, [])
                    target_output_table_names = target_output_table_names_by_tcf.get(ref.table_creation_function_id, [])
                    for target_table_name in target_table_names:
                        lineage_data['lineage_relationships']['table_creation_function_columns'].append({
                            "id": ref.id,
                            "table_creation_function_id": ref.table_creation_function.id,
                            "table_creation_function_name": ref.table_creation_function.name,
                            "reference_text": ref.reference_text,
                            "table_name": table_name,
                            "column_name": column_name,
                            "target_table_name": target_table_name,
                            "target_output_table_name": target_output_table_names[0] if target_output_table_names else target_table_name,
                            "is_resolved": ref.content_type.model in ('databasefield', 'function'),
                            "resolved_object_type": ref.content_type.model if ref.content_type.model in ('databasefield', 'function') else None,
                        })
        else:
            # Include all relationships
            # (Copy the original logic from get_trail_complete_lineage here)
            pass
        
        # Add summary counts
        lineage_data['metadata']['total_counts'] = {
            "database_tables": len(lineage_data['database_tables']),
            "derived_tables": len(lineage_data['derived_tables']),
            "populated_database_tables": len(lineage_data['populated_database_tables']),
            "evaluated_derived_tables": len(lineage_data['evaluated_derived_tables']),
            "total_database_rows": sum(len(pt['rows']) for pt in lineage_data['populated_database_tables']),
            "total_derived_rows": sum(len(et['rows']) for et in lineage_data['evaluated_derived_tables']),
            "function_column_references": len(lineage_data['lineage_relationships']['function_column_references']),
            "derived_row_source_references": len(lineage_data['lineage_relationships']['derived_row_source_references'])
        }
        
        # Add calculation usage summary
        if calculation_name:
            lineage_data['calculation_usage'] = {
                "calculation_name": calculation_name,
                "used_rows": used_rows.count(),
                "used_fields": used_fields.count(),
                "used_tables": list(tables_with_used_data)
            }

        # Add data_flow_edges for Bird's Eye view
        lineage_data['data_flow_edges'] = []
        data_flow_edges = DataFlowEdge.objects.filter(trail=trail).select_related(
            'source_content_type', 'target_content_type'
        )
        for edge in data_flow_edges:
            lineage_data['data_flow_edges'].append({
                "id": edge.id,
                "source_table_type": edge.source_content_type.model if edge.source_content_type else None,
                "source_table_id": edge.source_object_id,
                "source_table_name": edge.source_label,
                "target_table_type": edge.target_content_type.model if edge.target_content_type else None,
                "target_table_id": edge.target_object_id,
                "target_table_name": edge.target_label,
                "flow_type": edge.flow_type,
                "row_count": edge.row_count
            })

        # Add cell_lineages for Bird's Eye view
        lineage_data['cell_lineages'] = []
        cell_lineages = CellLineage.objects.filter(trail=trail).prefetch_related(
            'source_rows__row_content_type'
        )
        for cell in cell_lineages:
            lineage_data['cell_lineages'].append({
                "id": cell.id,
                "report_template": cell.report_template,
                "framework": cell.framework,
                "cell_code": cell.cell_code,
                "computed_value": cell.computed_value,
                "computed_string_value": cell.computed_string_value,
                "string_value": cell.string_value if hasattr(cell, 'string_value') else None,
                "source_rows": [
                    {
                        "row_type": source_row.row_content_type.model,
                        "row_id": source_row.row_object_id,
                        "contribution_type": source_row.contribution_type,
                        "contributed_value": source_row.contributed_value,
                    }
                    for source_row in cell.source_rows.all()
                ]
            })

        report_dependency_columns_by_table = {}
        declared_cell_dependency_refs = []
        seen_cell_function_names = set()
        for cell in cell_lineages:
            cell_table_name = cell.cell_code if str(cell.cell_code).startswith('Cell_') else f"Cell_{cell.cell_code}"
            for function_suffix in ('calc_referenced_items', 'metric_value'):
                function_name = f"{cell_table_name}.{function_suffix}"
                if function_name in seen_cell_function_names:
                    continue
                seen_cell_function_names.add(function_name)

                cell_function = Function.objects.filter(
                    name=function_name
                ).select_related(
                    'function_text'
                ).first()
                if not cell_function or not cell_function.function_text:
                    continue

                for dependency in _extract_dependency_strings_from_function_text(cell_function.function_text.text):
                    dep_clean = dependency.replace('base.', '') if dependency.startswith('base.') else dependency
                    if '.' not in dep_clean:
                        continue
                    table_name, column_name = dep_clean.rsplit('.', 1)
                    if not table_name or not column_name:
                        continue
                    existing_columns = report_dependency_columns_by_table.setdefault(table_name, [])
                    if column_name not in existing_columns:
                        existing_columns.append(column_name)
                    declared_cell_dependency_refs.append({
                        "id": None,
                        "function_id": cell_function.id,
                        "function_name": function_name,
                        "function_table_name": cell_table_name,
                        "referenced_object_type": "declared_dependency",
                        "referenced_object_id": None,
                        "referenced_name": column_name,
                        "referenced_table_name": table_name,
                        "dependency_string": dependency,
                        "dependency_table_name": table_name,
                        "dependency_column_name": column_name,
                        "declared_dependency_only": True,
                    })

        existing_ref_keys = {
            (
                ref.get("function_name"),
                ref.get("dependency_table_name") or ref.get("referenced_table_name"),
                ref.get("dependency_column_name") or ref.get("referenced_name"),
            )
            for ref in lineage_data['lineage_relationships']['function_column_references']
        }

        for ref in declared_cell_dependency_refs:
            ref_key = (
                ref.get("function_name"),
                ref.get("dependency_table_name"),
                ref.get("dependency_column_name"),
            )
            if ref_key in existing_ref_keys:
                continue
            lineage_data['lineage_relationships']['function_column_references'].append(ref)
            existing_ref_keys.add(ref_key)

        # Build table_hierarchy for Bird's Eye composite view
        lineage_data['table_hierarchy'] = {
            "output_table_compositions": [],
            "transformation_to_output_map": {},
            "output_to_transformations_map": {}
        }

        # Build a map of table names to their evaluated table data
        eval_table_by_name = {et['table_name']: et for et in lineage_data['evaluated_derived_tables']}
        pop_db_table_by_name = {pt['table_name']: pt for pt in lineage_data['populated_database_tables']}
        eval_table_by_row_id = {
            row['id']: et
            for et in lineage_data['evaluated_derived_tables']
            for row in et.get('rows', [])
            if row.get('id')
        }
        pop_db_table_by_row_id = {
            row['id']: pt
            for pt in lineage_data['populated_database_tables']
            for row in pt.get('rows', [])
            if row.get('id')
        }

        # Identify output tables from calculation/cell metadata and lineage sinks.
        output_tables = [
            et for et in lineage_data['evaluated_derived_tables']
            if et['table_name'] and et['table_name'] in output_table_names
        ]

        # For each output table, find its source tables using data_flow_edges
        for output_table in output_tables:
            output_name = output_table['table_name']
            source_tables = []
            seen_sources = set()
            rol_column_data = _get_complete_rol_columns(output_name)
            output_table['output_cube_id'] = rol_column_data['output_cube_id']
            output_table['output_cube_structure_id'] = rol_column_data['output_cube_structure_id']
            output_table['complete_rol_columns'] = rol_column_data['complete_rol_columns']
            output_table['is_output_table'] = True

            def add_source_table(source_name, source_table_data, table_type, row_count=None):
                if not source_name or source_name in seen_sources:
                    return

                seen_sources.add(source_name)
                source_tables.append({
                    "table_name": source_name,
                    "table_id": source_table_data['table_id'] if table_type == "derived" else source_table_data['id'],
                    "table_type": table_type,
                    "derivation_type": source_table_data.get('derivation_type', 'unknown' if table_type == "derived" else 'source'),
                    "row_count": row_count if row_count is not None else len(source_table_data.get('rows', [])),
                    "rows": source_table_data.get('rows', [])
                })

                if table_type == "derived":
                    lineage_data['table_hierarchy']['transformation_to_output_map'][source_name] = output_name

            for edge in lineage_data['data_flow_edges']:
                if edge['target_table_name'] == output_name:
                    source_name = edge['source_table_name']
                    if source_name in eval_table_by_name:
                        add_source_table(
                            source_name,
                            eval_table_by_name[source_name],
                            "derived",
                            edge.get('row_count') or len(eval_table_by_name[source_name].get('rows', [])),
                        )
                    elif source_name in pop_db_table_by_name:
                        add_source_table(
                            source_name,
                            pop_db_table_by_name[source_name],
                            "database",
                            edge.get('row_count') or len(pop_db_table_by_name[source_name].get('rows', [])),
                        )

            output_row_ids = [
                row.get('id')
                for row in output_table.get('rows', [])
                if row.get('id')
            ]
            row_source_refs = DerivedRowSourceReference.objects.filter(
                derived_row_id__in=output_row_ids,
            ).select_related(
                'content_type',
            )
            for ref in row_source_refs:
                if ref.content_type.model == 'derivedtablerow':
                    source_et = eval_table_by_row_id.get(ref.object_id)
                    if source_et:
                        add_source_table(
                            source_et.get('table_name'),
                            source_et,
                            "derived",
                        )
                elif ref.content_type.model == 'databaserow':
                    source_pt = pop_db_table_by_row_id.get(ref.object_id)
                    if source_pt:
                        add_source_table(
                            source_pt.get('table_name'),
                            source_pt,
                            "database",
                        )

            display_table = _get_display_table_for_output(
                trail,
                output_name,
                eval_table_by_name,
                output_table_names,
            )
            if not display_table:
                display_table = _get_display_table_from_row_sources(
                    output_table,
                    eval_table_by_row_id,
                    output_table_names,
                )
            if display_table and display_table.get('table_name') != output_name:
                add_source_table(
                    display_table.get('table_name'),
                    display_table,
                    "derived",
                )
            display_table_rows = display_table.get('rows', []) if display_table else output_table.get('rows', [])
            display_table_name = display_table.get('table_name') if display_table else output_name
            display_static_defaults = (
                _get_static_default_values(output_name, display_table_name)
                if display_table
                else output_table.get('static_default_values', {})
            )
            display_report_dependency_columns = (
                report_dependency_columns_by_table.get(display_table_name, [])
                if display_table
                else output_table.get('report_dependency_columns', [])
            )

            if source_tables or display_table:
                lineage_data['table_hierarchy']['output_table_compositions'].append({
                    "output_table_name": output_name,
                    "output_table_id": output_table['table_id'],
                    "output_table_rows": output_table.get('rows', []),
                    "display_table_name": display_table_name,
                    "display_table_rows": display_table_rows,
                    "display_static_default_values": display_static_defaults,
                    "display_report_dependency_columns": display_report_dependency_columns,
                    "output_cube_id": output_table.get('output_cube_id'),
                    "output_cube_structure_id": output_table.get('output_cube_structure_id'),
                    "complete_rol_columns": output_table.get('complete_rol_columns', []),
                    "source_tables": source_tables,
                    "total_source_rows": sum(st['row_count'] for st in source_tables)
                })
                lineage_data['table_hierarchy']['output_to_transformations_map'][output_name] = [
                    st['table_name'] for st in source_tables
                ]

        rol_column_cache = {}

        # Update evaluated_derived_tables with parent output table info
        transform_to_output = lineage_data['table_hierarchy']['transformation_to_output_map']
        for et in lineage_data['evaluated_derived_tables']:
            table_name = et.get('table_name', '')
            matched_output_table = transform_to_output.get(table_name)
            if not matched_output_table:
                matched_output_table = _resolve_reference_output_table_name(
                    trail,
                    table_name,
                    output_table_names,
                )

            if matched_output_table:
                if matched_output_table not in rol_column_cache:
                    rol_column_cache[matched_output_table] = _get_complete_rol_columns(matched_output_table)

                rol_column_data = rol_column_cache[matched_output_table]
                static_default_values = _get_static_default_values(matched_output_table, table_name)
                et['parent_output_table'] = matched_output_table
                et['is_output_table'] = table_name in output_table_names
                et['is_content_of_output'] = matched_output_table != table_name
                et['output_cube_id'] = rol_column_data['output_cube_id']
                et['output_cube_structure_id'] = rol_column_data['output_cube_structure_id']
                et['complete_rol_columns'] = rol_column_data['complete_rol_columns']
                et['static_default_values'] = static_default_values
                et['report_dependency_columns'] = report_dependency_columns_by_table.get(table_name, [])
            else:
                et['parent_output_table'] = None
                et['is_output_table'] = table_name in output_table_names
                et['is_content_of_output'] = False
                et['output_cube_id'] = et.get('output_cube_id')
                et['output_cube_structure_id'] = et.get('output_cube_structure_id')
                et['complete_rol_columns'] = et.get('complete_rol_columns', [])
                et['static_default_values'] = et.get('static_default_values', {})
                et['report_dependency_columns'] = report_dependency_columns_by_table.get(table_name, et.get('report_dependency_columns', []))

        return JsonResponse(lineage_data, json_dumps_params={'default': serialize_datetime})
    
    except Exception as e:
        logging.exception(
            "Exception in get_trail_filtered_lineage for trail_id=%s",
            sanitize_log_value(trail_id),
        )
        return JsonResponse({
            "error": "An internal error occurred.",
            "trail_id": trail_id
        }, status=500)


@require_http_methods(["GET"])
def get_calculation_summary(request, trail_id):
    """
    Get a summary of all calculations performed in a trail.
    
    Returns:
    - List of calculations with counts of used rows and fields
    """
    trail = get_object_or_404(Trail, pk=trail_id)
    
    # Get all unique calculation names
    calculation_names = set()
    calculation_names.update(
        CalculationUsedRow.objects.filter(trail=trail).values_list('calculation_name', flat=True).distinct()
    )
    calculation_names.update(
        CalculationUsedField.objects.filter(trail=trail).values_list('calculation_name', flat=True).distinct()
    )
    
    calculations = []
    for calc_name in sorted(calculation_names):
        used_rows = CalculationUsedRow.objects.filter(trail=trail, calculation_name=calc_name).count()
        used_fields = CalculationUsedField.objects.filter(trail=trail, calculation_name=calc_name).count()
        
        calculations.append({
            "calculation_name": calc_name,
            "used_rows": used_rows,
            "used_fields": used_fields
        })
    
    return JsonResponse({
        "trail_id": trail_id,
        "trail_name": trail.name,
        "total_calculations": len(calculations),
        "calculations": calculations
    })
