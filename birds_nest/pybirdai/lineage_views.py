from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.db.models import Prefetch
from pybirdai.aorta_model import (
    Trail, MetaDataTrail, DatabaseTable, DerivedTable,
    DatabaseField, Function,
    PopulatedDataBaseTable, EvaluatedDerivedTable,
    FunctionColumnReference, DatabaseRow, DerivedTableRow,
    DatabaseColumnValue, EvaluatedFunction,
    DerivedRowSourceReference, EvaluatedFunctionSourceValue,
    TableCreationFunction, TableCreationFunctionColumn
)
import json


def trail_lineage_viewer(request, trail_id):
    """Main view for displaying trail lineage visualization"""
    trail = get_object_or_404(Trail, pk=trail_id)

    context = {
        'trail': trail,
        'trail_id': trail_id,
    }
    return render(request, 'pybirdai/lineage_viewer.html', context)


@require_http_methods(["GET"])
def get_trail_lineage_data(request, trail_id):
    """API endpoint to fetch comprehensive trail lineage data including rows and values"""
    trail = get_object_or_404(Trail, pk=trail_id)

    # Check detail level
    detail_level = request.GET.get('detail', 'table')  # table, column, row, value
    max_rows_per_table = int(request.GET.get('max_rows', '5'))
    max_values_per_row = int(request.GET.get('max_values', '3'))
    hide_empty_tables = request.GET.get('hide_empty', 'false').lower() == 'true'

    # Build the lineage graph data
    nodes = []
    edges = []
    node_id_counter = 0
    node_map = {}  # Maps (type, id) to node_id for creating edges

    def get_node_id(obj_type, obj_id):
        """Get or create a node ID for an object"""
        key = (obj_type, obj_id)
        if key not in node_map:
            nonlocal node_id_counter
            node_map[key] = f"node_{node_id_counter}"
            node_id_counter += 1
        return node_map[key]

    try:
        # 1. Add database tables and their complete lineage
        populated_db_tables = PopulatedDataBaseTable.objects.filter(
            trail=trail
        ).select_related('table').prefetch_related(
            'table__database_fields',
            'databaserow_set__column_values__column'
        )

        for pop_table in populated_db_tables:
            table = pop_table.table
            table_node_id = get_node_id('database_table', table.id)

            # Add table node with instance information
            row_count = pop_table.databaserow_set.count()

            # Skip empty tables if requested
            if hide_empty_tables and row_count == 0:
                continue

            # Create a more descriptive label for tables with the same name
            label = table.name
            if row_count == 0:
                label += " (empty)"
            else:
                label += f" ({row_count} rows)"

            # Add table instance ID to make it unique
            label += f" [T{table.id}]"

            nodes.append({
                'id': table_node_id,
                'label': label,
                'type': 'database_table',
                'details': {
                    'name': table.name,
                    'row_count': row_count,
                    'column_count': table.database_fields.count(),
                    'table_id': table.id,
                    'populated_table_id': pop_table.id,
                    'is_empty': row_count == 0
                }
            })

            # Add columns if detail level includes columns
            if detail_level in ['column', 'row', 'value']:
                for field in table.database_fields.all():
                    field_node_id = get_node_id('database_field', field.id)
                    nodes.append({
                        'id': field_node_id,
                        'label': field.name,
                        'type': 'database_field',
                        'details': {
                            'name': field.name,
                            'table': table.name,
                            'field_id': field.id
                        }
                    })

                    edges.append({
                        'source': table_node_id,
                        'target': field_node_id,
                        'type': 'has_field'
                    })

            # Add rows if detail level includes rows
            if detail_level in ['row', 'value']:
                for row in pop_table.databaserow_set.all()[:max_rows_per_table]:
                    row_node_id = get_node_id('database_row', row.id)
                    nodes.append({
                        'id': row_node_id,
                        'label': f"Row {row.row_identifier or row.id}",
                        'type': 'database_row',
                        'details': {
                            'row_id': row.id,
                            'identifier': row.row_identifier,
                            'table': table.name
                        }
                    })

                    edges.append({
                        'source': table_node_id,
                        'target': row_node_id,
                        'type': 'contains_row'
                    })

                    # Add column values if detail level includes values
                    if detail_level == 'value':
                        for col_value in row.column_values.all()[:max_values_per_row]:
                            value_node_id = get_node_id('database_column_value', col_value.id)
                            value_display = str(col_value.value or col_value.string_value or 'NULL')[:20]

                            nodes.append({
                                'id': value_node_id,
                                'label': f"{col_value.column.name}: {value_display}",
                                'type': 'database_column_value',
                                'details': {
                                    'value': col_value.value or col_value.string_value,
                                    'column': col_value.column.name,
                                    'row_id': row.id,
                                    'value_id': col_value.id
                                }
                            })

                            edges.append({
                                'source': row_node_id,
                                'target': value_node_id,
                                'type': 'has_value'
                            })

                            # Connect to column if shown
                            if detail_level in ['column', 'row', 'value']:
                                field_node_id = get_node_id('database_field', col_value.column.id)
                                edges.append({
                                    'source': value_node_id,
                                    'target': field_node_id,
                                    'type': 'instance_of_field'
                                })

        # 2. Add derived tables and their complete lineage
        evaluated_tables = EvaluatedDerivedTable.objects.filter(
            trail=trail
        ).select_related('table').prefetch_related(
            'table__derived_functions__function_text',
            'derivedtablerow_set__evaluated_functions__function'
        )

        # Store eval table IDs for later use in queries
        eval_table_ids = []

        for eval_table in evaluated_tables:
            table = eval_table.table
            table_node_id = get_node_id('derived_table', table.id)
            eval_table_ids.append(eval_table.id)  # Store for later queries

            # Add table node with instance information
            row_count = eval_table.derivedtablerow_set.count()

            # Skip empty tables if requested
            if hide_empty_tables and row_count == 0:
                continue

            # Create a more descriptive label for tables with the same name
            label = table.name
            if row_count == 0:
                label += " (empty)"
            else:
                label += f" ({row_count} rows)"

            # Add table instance ID to make it unique
            label += f" [DT{table.id}]"

            nodes.append({
                'id': table_node_id,
                'label': label,
                'type': 'derived_table',
                'details': {
                    'name': table.name,
                    'row_count': row_count,
                    'function_count': table.derived_functions.count(),
                    'table_id': table.id,
                    'evaluated_table_id': eval_table.id,
                    'creation_function': table.table_creation_function.name if table.table_creation_function else None,
                    'is_empty': row_count == 0
                }
            })

            # Add functions if detail level includes columns
            if detail_level in ['column', 'row', 'value']:
                for function in table.derived_functions.all():
                    func_node_id = get_node_id('function', function.id)
                    nodes.append({
                        'id': func_node_id,
                        'label': function.name,
                        'type': 'function',
                        'details': {
                            'name': function.name,
                            'table': table.name,
                            'function_id': function.id,
                            'function_text': function.function_text.text[:100] + '...' if function.function_text else None
                        }
                    })

                    edges.append({
                        'source': table_node_id,
                        'target': func_node_id,
                        'type': 'has_function'
                    })

            # Add rows if detail level includes rows
            if detail_level in ['row', 'value']:
                for row in eval_table.derivedtablerow_set.all()[:max_rows_per_table]:
                    row_node_id = get_node_id('derived_row', row.id)
                    nodes.append({
                        'id': row_node_id,
                        'label': f"Row {row.row_identifier or row.id}",
                        'type': 'derived_row',
                        'details': {
                            'row_id': row.id,
                            'identifier': row.row_identifier,
                            'table': table.name
                        }
                    })

                    edges.append({
                        'source': table_node_id,
                        'target': row_node_id,
                        'type': 'contains_row'
                    })

                    # Add evaluated functions (computed values) if detail level includes values
                    if detail_level == 'value':
                        for eval_func in row.evaluated_functions.all()[:max_values_per_row]:
                            eval_func_node_id = get_node_id('evaluated_function', eval_func.id)
                            value_display = str(eval_func.value or eval_func.string_value or 'NULL')[:20]

                            nodes.append({
                                'id': eval_func_node_id,
                                'label': f"{eval_func.function.name}: {value_display}",
                                'type': 'evaluated_function',
                                'details': {
                                    'value': eval_func.value or eval_func.string_value,
                                    'function': eval_func.function.name,
                                    'row_id': row.id,
                                    'eval_func_id': eval_func.id
                                }
                            })

                            edges.append({
                                'source': row_node_id,
                                'target': eval_func_node_id,
                                'type': 'has_evaluated_function'
                            })

                            # Connect to function definition if shown
                            if detail_level in ['column', 'row', 'value']:
                                func_node_id = get_node_id('function', eval_func.function.id)

                                # Ensure the Function node exists (in case it wasn't added in the table loop)
                                function_exists = any(node['id'] == func_node_id for node in nodes)
                                if not function_exists:
                                    function = eval_func.function
                                    nodes.append({
                                        'id': func_node_id,
                                        'label': function.name,
                                        'type': 'function',
                                        'details': {
                                            'name': function.name,
                                            'table': function.table.name,
                                            'function_id': function.id,
                                            'function_text': function.function_text.text[:100] + '...' if function.function_text else None
                                        }
                                    })

                                edges.append({
                                    'source': func_node_id,
                                    'target': eval_func_node_id,
                                    'type': 'instance_of_function',
                                    'label': 'instantiated as'
                                })

        # 3. Add lineage connections at the value level
        if detail_level == 'value':
            # Connect evaluated functions to their source values
            # Get evaluated table IDs first to avoid trail object in query
            eval_table_ids = [eval_table.id for eval_table in evaluated_tables]

            if eval_table_ids:
                eval_func_sources = EvaluatedFunctionSourceValue.objects.filter(
                    evaluated_function__row__populated_table__id__in=eval_table_ids
                ).select_related(
                    'evaluated_function', 'content_type'
                )[:100]  # Limit to prevent overwhelming
            else:
                eval_func_sources = []

            for source_ref in eval_func_sources:
                try:
                    eval_func_node_id = get_node_id('evaluated_function', source_ref.evaluated_function.id)

                    # Create node for EvaluatedFunctionSourceValue
                    source_ref_node_id = get_node_id('eval_func_source_value', source_ref.id)
                    nodes.append({
                        'id': source_ref_node_id,
                        'label': f"SourceRef {source_ref.id}",
                        'type': 'eval_func_source_value',
                        'details': {
                            'id': source_ref.id,
                            'evaluated_function_id': source_ref.evaluated_function.id,
                            'source_type': source_ref.content_type.model,
                            'source_id': source_ref.object_id
                        }
                    })

                    # Edge from EvaluatedFunction to EvaluatedFunctionSourceValue
                    edges.append({
                        'source': eval_func_node_id,
                        'target': source_ref_node_id,
                        'type': 'has_source_reference',
                        'label': 'references'
                    })

                    # Edge from EvaluatedFunctionSourceValue to actual source value
                    if source_ref.content_type.model == 'databasecolumnvalue':
                        source_node_id = get_node_id('database_column_value', source_ref.object_id)

                        # Ensure the DatabaseColumnValue node exists
                        source_exists = any(node['id'] == source_node_id for node in nodes)
                        if not source_exists:
                            try:
                                from pybirdai.aorta_model import DatabaseColumnValue
                                col_value = DatabaseColumnValue.objects.get(id=source_ref.object_id)
                                value_display = str(col_value.value or col_value.string_value or 'NULL')[:20]
                                nodes.append({
                                    'id': source_node_id,
                                    'label': f"{col_value.column.name}: {value_display}",
                                    'type': 'database_column_value',
                                    'details': {
                                        'value': col_value.value or col_value.string_value,
                                        'column': col_value.column.name,
                                        'row_id': col_value.row.id,
                                        'value_id': col_value.id
                                    }
                                })
                            except DatabaseColumnValue.DoesNotExist:
                                continue

                    elif source_ref.content_type.model == 'evaluatedfunction':
                        source_node_id = get_node_id('evaluated_function', source_ref.object_id)

                        # Ensure the EvaluatedFunction node exists
                        source_exists = any(node['id'] == source_node_id for node in nodes)
                        if not source_exists:
                            try:
                                from pybirdai.aorta_model import EvaluatedFunction
                                eval_func = EvaluatedFunction.objects.get(id=source_ref.object_id)
                                value_display = str(eval_func.value or eval_func.string_value or 'NULL')[:20]
                                nodes.append({
                                    'id': source_node_id,
                                    'label': f"{eval_func.function.name}: {value_display}",
                                    'type': 'evaluated_function',
                                    'details': {
                                        'value': eval_func.value or eval_func.string_value,
                                        'function': eval_func.function.name,
                                        'row_id': eval_func.row.id,
                                        'eval_func_id': eval_func.id
                                    }
                                })
                            except EvaluatedFunction.DoesNotExist:
                                continue
                    else:
                        continue

                    edges.append({
                        'source': source_ref_node_id,
                        'target': source_node_id,
                        'type': 'points_to_source',
                        'label': 'points to'
                    })
                except Exception:
                    continue  # Skip if source not found

        # 4. Add row-level lineage connections
        if detail_level in ['row', 'value']:
            # Use the eval_table_ids we already have
            if eval_table_ids:
                row_sources = DerivedRowSourceReference.objects.filter(
                    derived_row__populated_table__id__in=eval_table_ids
                ).select_related('derived_row', 'content_type')[:50]
            else:
                row_sources = []

            for source_ref in row_sources:
                try:
                    derived_row_node_id = get_node_id('derived_row', source_ref.derived_row.id)

                    if source_ref.content_type.model == 'databaserow':
                        source_node_id = get_node_id('database_row', source_ref.object_id)
                    elif source_ref.content_type.model == 'derivedtablerow':
                        source_node_id = get_node_id('derived_row', source_ref.object_id)
                    else:
                        continue

                    edges.append({
                        'source': source_node_id,
                        'target': derived_row_node_id,
                        'type': 'derived_from_row',
                        'label': 'feeds into'
                    })
                except Exception:
                    continue  # Skip if source not found

        # 5. Add function-level dependencies (column references)
        if detail_level in ['column', 'row', 'value']:
            from django.contrib.contenttypes.models import ContentType

            # Get derived table IDs that exist in this trail
            derived_table_ids_for_refs = [eval_table.table.id for eval_table in evaluated_tables]

            if derived_table_ids_for_refs:
                function_refs = FunctionColumnReference.objects.filter(
                    function__table__id__in=derived_table_ids_for_refs
                ).select_related('function', 'content_type')[:100]
            else:
                function_refs = []

            for ref in function_refs:
                try:
                    func_node_id = get_node_id('function', ref.function.id)

                    if ref.content_type.model == 'databasefield':
                        ref_node_id = get_node_id('database_field', ref.object_id)
                    elif ref.content_type.model == 'function':
                        ref_node_id = get_node_id('function', ref.object_id)
                    else:
                        continue

                    edges.append({
                        'source': func_node_id,
                        'target': ref_node_id,
                        'type': 'depends_on',
                        'label': 'uses'
                    })
                except Exception:
                    continue  # Skip if reference not found

        # 6. Add table creation functions and their column references
        if detail_level in ['column', 'row', 'value']:
            # Get table creation functions for derived tables in this trail
            if derived_table_ids_for_refs:
                table_creation_functions = TableCreationFunction.objects.filter(
                    derivedtable__id__in=derived_table_ids_for_refs
                ).prefetch_related('column_references')
            else:
                table_creation_functions = []

            for table_func in table_creation_functions:
                # Add table creation function node
                table_func_node_id = get_node_id('table_creation_function', table_func.id)
                nodes.append({
                    'id': table_func_node_id,
                    'label': f"calc_{table_func.name.split('.')[-1] if '.' in table_func.name else table_func.name}",
                    'type': 'table_creation_function',
                    'details': {
                        'name': table_func.name,
                        'function_id': table_func.id,
                        'function_text': table_func.function_text.text[:100] + '...' if table_func.function_text else None
                    }
                })

                # Connect to the derived table it creates
                derived_table = table_func.derivedtable_set.first()
                if derived_table:
                    derived_table_node_id = get_node_id('derived_table', derived_table.id)
                    edges.append({
                        'source': table_func_node_id,
                        'target': derived_table_node_id,
                        'type': 'creates_table',
                        'label': 'creates'
                    })

                # Add column references from table creation function
                for col_ref in table_func.column_references.all():
                    try:
                        if col_ref.content_type.model == 'databasefield':
                            ref_node_id = get_node_id('database_field', col_ref.object_id)
                        elif col_ref.content_type.model == 'function':
                            ref_node_id = get_node_id('function', col_ref.object_id)
                        else:
                            continue

                        edges.append({
                            'source': table_func_node_id,
                            'target': ref_node_id,
                            'type': 'references_column',
                            'label': 'references'
                        })
                    except Exception:
                        continue  # Skip if reference not found

        # Prepare summary statistics
        summary = {
            'trail_name': trail.name,
            'trail_created': trail.created_at.isoformat(),
            'execution_context': trail.execution_context,
            'table_count': {
                'database': populated_db_tables.count(),
                'derived': evaluated_tables.count()
            },
            'total_nodes': len(nodes),
            'total_edges': len(edges),
            'detail_level': detail_level,
            'max_rows_per_table': max_rows_per_table,
            'max_values_per_row': max_values_per_row,
            'hide_empty_tables': hide_empty_tables
        }

        return JsonResponse({
            'nodes': nodes,
            'edges': edges,
            'summary': summary
        })

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in get_trail_lineage_data: {error_details}")

        return JsonResponse({
            'error': str(e),
            'nodes': [],
            'edges': [],
            'summary': {
                'trail_name': trail.name,
                'error': 'Failed to load lineage data',
                'detail_level': detail_level
            }
        }, status=500)


@require_http_methods(["GET"])
def get_node_details(request, trail_id, node_type, node_id):
    """Get detailed information about a specific node"""
    trail = get_object_or_404(Trail, pk=trail_id)

    details = {}

    try:
        if node_type == 'database_table':
            table = get_object_or_404(DatabaseTable, pk=node_id)
            details = {
                'name': table.name,
                'type': 'Database Table',
                'fields': [{'name': f.name, 'id': f.id} for f in table.database_fields.all()[:20]]
            }

        elif node_type == 'derived_table':
            table = get_object_or_404(DerivedTable, pk=node_id)
            details = {
                'name': table.name,
                'type': 'Derived Table',
                'creation_function': table.table_creation_function.name if table.table_creation_function else None,
                'functions': [
                    {
                        'name': f.name,
                        'id': f.id,
                        'text': f.function_text.text[:100] + '...' if f.function_text else None
                    }
                    for f in table.derived_functions.all()[:20]
                ]
            }

        elif node_type == 'function':
            function = get_object_or_404(Function, pk=node_id)
            details = {
                'name': function.name,
                'type': 'Function',
                'table': function.table.name,
                'function_text': function.function_text.text if function.function_text else None,
                'dependencies': []
            }

            for col_ref in function.column_references.all()[:10]:
                if col_ref.content_type.model == 'databasefield':
                    try:
                        field = DatabaseField.objects.get(id=col_ref.object_id)
                        details['dependencies'].append({
                            'type': 'field',
                            'name': f"{field.table.name}.{field.name}"
                        })
                    except DatabaseField.DoesNotExist:
                        continue
                elif col_ref.content_type.model == 'function':
                    try:
                        ref_func = Function.objects.get(id=col_ref.object_id)
                        details['dependencies'].append({
                            'type': 'function',
                            'name': f"{ref_func.table.name}.{ref_func.name}"
                        })
                    except Function.DoesNotExist:
                        continue

        return JsonResponse(details)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def trail_list(request):
    """List all available trails"""
    trails = Trail.objects.all().order_by('-created_at')

    context = {
        'trails': trails
    }
    return render(request, 'pybirdai/trail_list.html', context)
