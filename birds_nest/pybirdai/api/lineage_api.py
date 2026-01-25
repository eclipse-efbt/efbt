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
from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.db.models import Prefetch
from django.contrib.contenttypes.models import ContentType
from pybirdai.models import (
    Trail, MetaDataTrail, DatabaseTable, DerivedTable,
    DatabaseField, Function, FunctionText, TableCreationFunction,
    PopulatedDataBaseTable, EvaluatedDerivedTable, DatabaseRow,
    DerivedTableRow, DatabaseColumnValue, EvaluatedFunction,
    AortaTableReference, FunctionColumnReference, DerivedRowSourceReference,
    EvaluatedFunctionSourceValue, TableCreationSourceTable, TableCreationFunctionColumn,
    DataFlowEdge, CellLineage
)
import json
from datetime import datetime


def serialize_datetime(obj):
    """JSON serializer for datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


@require_http_methods(["GET"])
def get_trail_complete_lineage(request, trail_id):
    """
    Comprehensive API endpoint that returns ALL lineage information for a given trail.
    
    Returns a complete JSON structure containing:
    - Trail metadata
    - All tables (database and derived)
    - All columns/functions
    - All rows and values
    - All lineage relationships
    """
    trail = get_object_or_404(Trail, pk=trail_id)
    
    try:
        # Initialize the complete lineage structure
        lineage_data = {
            "trail": {
                "id": trail.id,
                "name": trail.name,
                "created_at": trail.created_at.isoformat(),
                "execution_context": trail.execution_context,
                "metadata_trail_id": trail.metadata_trail.id
            },
            "metadata_trail": {
                "id": trail.metadata_trail.id
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
            "data_flow_edges": [],
            "cell_lineages": [],
            "metadata": {
                "table_references": [],
                "generation_timestamp": datetime.now().isoformat(),
                "total_counts": {}
            }
        }
        
        # 1. Get all populated database tables for this trail
        populated_db_tables = PopulatedDataBaseTable.objects.filter(
            trail=trail
        ).select_related('table').prefetch_related(
            'table__database_fields',
            'databaserow_set__column_values__column'
        )
        
        # Process database tables
        database_table_ids = set()
        for pop_table in populated_db_tables:
            table = pop_table.table
            database_table_ids.add(table.id)
            
            # Add table definition if not already added
            if not any(dt['id'] == table.id for dt in lineage_data['database_tables']):
                table_data = {
                    "id": table.id,
                    "name": table.name,
                    "fields": []
                }
                
                # Add fields
                for field in table.database_fields.all():
                    table_data['fields'].append({
                        "id": field.id,
                        "name": field.name,
                        "table_id": table.id
                    })
                
                lineage_data['database_tables'].append(table_data)
            
            # Add populated table instance
            pop_table_data = {
                "id": pop_table.id,
                "table_id": table.id,
                "table_name": table.name,
                "trail_id": trail.id,
                "rows": []
            }
            
            # Add rows and values
            for row in pop_table.databaserow_set.all():
                row_data = {
                    "id": row.id,
                    "row_identifier": row.row_identifier,
                    "populated_table_id": pop_table.id,
                    "values": []
                }
                
                # Add column values
                for col_value in row.column_values.all():
                    row_data['values'].append({
                        "id": col_value.id,
                        "value": col_value.value,
                        "string_value": col_value.string_value,
                        "column_id": col_value.column.id,
                        "column_name": col_value.column.name,
                        "row_id": row.id
                    })
                
                pop_table_data['rows'].append(row_data)
            
            lineage_data['populated_database_tables'].append(pop_table_data)
        
        # 2. Get all evaluated derived tables for this trail
        evaluated_tables = EvaluatedDerivedTable.objects.filter(
            trail=trail
        ).select_related('table', 'table__table_creation_function').prefetch_related(
            'table__derived_functions__function_text',
            'derivedtablerow_set__evaluated_functions__function'
        )

        # Pre-fetch table creation source tables to determine derivation type
        # Tables with source tables are "transformation" tables; others are "property" tables
        transformation_function_ids = set(
            TableCreationSourceTable.objects.filter(
                table_creation_function__derivedtable__id__in=[et.table.id for et in evaluated_tables]
            ).values_list('table_creation_function_id', flat=True)
        )

        # Also check for corresponding DatabaseTable to confirm property-based tables
        db_table_names = set(
            DatabaseTable.objects.values_list('name', flat=True)
        )

        # Process derived tables
        derived_table_ids = set()
        for eval_table in evaluated_tables:
            table = eval_table.table
            derived_table_ids.add(table.id)

            # Determine derivation type
            has_source_tables = (
                table.table_creation_function and
                table.table_creation_function.id in transformation_function_ids
            )
            has_corresponding_db_table = table.name in db_table_names

            if has_source_tables:
                derivation_type = "transformation"  # Uses @lineage with source tables
            elif has_corresponding_db_table:
                derivation_type = "property"  # Property-based derived from source table
            else:
                derivation_type = "unknown"

            # Add table definition if not already added
            if not any(dt['id'] == table.id for dt in lineage_data['derived_tables']):
                table_data = {
                    "id": table.id,
                    "name": table.name,
                    "table_creation_function_id": table.table_creation_function.id if table.table_creation_function else None,
                    "derivation_type": derivation_type,
                    "has_source_tables": has_source_tables,
                    "has_corresponding_db_table": has_corresponding_db_table,
                    "functions": []
                }

                # Add functions
                for function in table.derived_functions.all():
                    function_data = {
                        "id": function.id,
                        "name": function.name,
                        "table_id": table.id,
                        "function_text_id": function.function_text.id,
                        "function_text": function.function_text.text if function.function_text else None,
                        "function_language": function.function_text.language if function.function_text else None
                    }
                    table_data['functions'].append(function_data)

                lineage_data['derived_tables'].append(table_data)
            
            # Add evaluated table instance
            eval_table_data = {
                "id": eval_table.id,
                "table_id": table.id,
                "table_name": table.name,
                "derivation_type": derivation_type,
                "trail_id": trail.id,
                "rows": []
            }
            
            # Add rows and evaluated functions
            for row in eval_table.derivedtablerow_set.all():
                row_data = {
                    "id": row.id,
                    "row_identifier": row.row_identifier,
                    "populated_table_id": eval_table.id,
                    "evaluated_functions": []
                }
                
                # Add evaluated functions
                for eval_func in row.evaluated_functions.all():
                    row_data['evaluated_functions'].append({
                        "id": eval_func.id,
                        "value": eval_func.value,
                        "string_value": eval_func.string_value,
                        "function_id": eval_func.function.id,
                        "function_name": eval_func.function.name,
                        "row_id": row.id
                    })
                
                eval_table_data['rows'].append(row_data)
            
            lineage_data['evaluated_derived_tables'].append(eval_table_data)
        
        # 3. Get all lineage relationships
        
        # Function column references
        if derived_table_ids:
            func_refs = FunctionColumnReference.objects.filter(
                function__table__id__in=derived_table_ids
            ).select_related('function', 'content_type')
            
            for ref in func_refs:
                lineage_data['lineage_relationships']['function_column_references'].append({
                    "id": ref.id,
                    "function_id": ref.function.id,
                    "function_name": ref.function.name,
                    "referenced_object_type": ref.content_type.model,
                    "referenced_object_id": ref.object_id
                })
        
        # Derived row source references
        eval_table_ids = [et.id for et in evaluated_tables]
        if eval_table_ids:
            row_refs = DerivedRowSourceReference.objects.filter(
                derived_row__populated_table__id__in=eval_table_ids
            ).select_related('derived_row', 'content_type')
            
            for ref in row_refs:
                lineage_data['lineage_relationships']['derived_row_source_references'].append({
                    "id": ref.id,
                    "derived_row_id": ref.derived_row.id,
                    "source_object_type": ref.content_type.model,
                    "source_object_id": ref.object_id
                })
        
        # Evaluated function source values
        if eval_table_ids:
            value_refs = EvaluatedFunctionSourceValue.objects.filter(
                evaluated_function__row__populated_table__id__in=eval_table_ids
            ).select_related('evaluated_function', 'content_type')
            
            for ref in value_refs:
                lineage_data['lineage_relationships']['evaluated_function_source_values'].append({
                    "id": ref.id,
                    "evaluated_function_id": ref.evaluated_function.id,
                    "source_object_type": ref.content_type.model,
                    "source_object_id": ref.object_id
                })
        
        # Table creation source tables
        if derived_table_ids:
            table_creation_functions = TableCreationFunction.objects.filter(
                derivedtable__id__in=derived_table_ids
            )
            
            table_src_refs = TableCreationSourceTable.objects.filter(
                table_creation_function__in=table_creation_functions
            ).select_related('table_creation_function', 'content_type')
            
            for ref in table_src_refs:
                lineage_data['lineage_relationships']['table_creation_source_tables'].append({
                    "id": ref.id,
                    "table_creation_function_id": ref.table_creation_function.id,
                    "table_creation_function_name": ref.table_creation_function.name,
                    "source_object_type": ref.content_type.model,
                    "source_object_id": ref.object_id
                })
            
            # Table creation function columns
            col_refs = TableCreationFunctionColumn.objects.filter(
                table_creation_function__in=table_creation_functions
            ).select_related('table_creation_function', 'content_type')

            for ref in col_refs:
                # Parse the qualified reference (TABLE.COLUMN format)
                table_name = None
                column_name = None
                if ref.reference_text and '.' in ref.reference_text:
                    parts = ref.reference_text.split('.', 1)
                    table_name = parts[0]
                    column_name = parts[1] if len(parts) > 1 else None

                # Check if the column was resolved to an actual object
                is_resolved = ref.content_type.model in ('databasefield', 'function')

                lineage_data['lineage_relationships']['table_creation_function_columns'].append({
                    "id": ref.id,
                    "table_creation_function_id": ref.table_creation_function.id,
                    "table_creation_function_name": ref.table_creation_function.name,
                    "reference_text": ref.reference_text,
                    "table_name": table_name,
                    "column_name": column_name,
                    "is_resolved": is_resolved,
                    "resolved_object_type": ref.content_type.model if is_resolved else None,
                    "resolved_object_id": ref.object_id if is_resolved else None
                })
        
        # 4. Get metadata trail references
        table_refs = AortaTableReference.objects.filter(
            metadata_trail=trail.metadata_trail
        )

        for ref in table_refs:
            lineage_data['metadata']['table_references'].append({
                "id": ref.id,
                "table_content_type": ref.table_content_type,
                "table_id": ref.table_id
            })

        # 5. Get data flow edges for this trail
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

        # 6. Get cell lineages for this trail
        cell_lineages = CellLineage.objects.filter(trail=trail)
        for cell in cell_lineages:
            lineage_data['cell_lineages'].append({
                "id": cell.id,
                "report_template": cell.report_template,
                "framework": cell.framework,
                "cell_code": cell.cell_code,
                "computed_value": cell.computed_value,
                "string_value": cell.string_value if hasattr(cell, 'string_value') else None
            })

        # 7. Add summary counts
        lineage_data['metadata']['total_counts'] = {
            "database_tables": len(lineage_data['database_tables']),
            "derived_tables": len(lineage_data['derived_tables']),
            "populated_database_tables": len(lineage_data['populated_database_tables']),
            "evaluated_derived_tables": len(lineage_data['evaluated_derived_tables']),
            "total_database_rows": sum(len(pt['rows']) for pt in lineage_data['populated_database_tables']),
            "total_derived_rows": sum(len(et['rows']) for et in lineage_data['evaluated_derived_tables']),
            "total_column_values": sum(len(v['values']) for pt in lineage_data['populated_database_tables'] for v in pt['rows']),
            "total_evaluated_functions": sum(len(ef['evaluated_functions']) for et in lineage_data['evaluated_derived_tables'] for ef in et['rows']),
            "function_column_references": len(lineage_data['lineage_relationships']['function_column_references']),
            "derived_row_source_references": len(lineage_data['lineage_relationships']['derived_row_source_references']),
            "evaluated_function_source_values": len(lineage_data['lineage_relationships']['evaluated_function_source_values']),
            "table_creation_source_tables": len(lineage_data['lineage_relationships']['table_creation_source_tables']),
            "table_creation_function_columns": len(lineage_data['lineage_relationships']['table_creation_function_columns'])
        }
        
        return JsonResponse(lineage_data, json_dumps_params={'indent': 2})
        
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error in get_trail_complete_lineage for trail {trail_id}: {str(e)}", exc_info=True)
        
        return JsonResponse({
            'error': 'Complete lineage extraction failed',
            'trail_id': trail_id,
            'trail_name': trail.name,
            'error_type': 'complete_lineage_extraction_failed'
        }, status=500)


@require_http_methods(["GET"])
def get_trail_lineage_summary(request, trail_id):
    """
    Lightweight endpoint that returns just summary statistics for a trail.
    """
    trail = get_object_or_404(Trail, pk=trail_id)
    
    try:
        # Get counts
        populated_db_tables = PopulatedDataBaseTable.objects.filter(trail=trail)
        evaluated_tables = EvaluatedDerivedTable.objects.filter(trail=trail)
        
        total_db_rows = DatabaseRow.objects.filter(
            populated_table__trail=trail
        ).count()
        
        total_derived_rows = DerivedTableRow.objects.filter(
            populated_table__trail=trail
        ).count()
        
        total_column_values = DatabaseColumnValue.objects.filter(
            row__populated_table__trail=trail
        ).count()
        
        total_evaluated_functions = EvaluatedFunction.objects.filter(
            row__populated_table__trail=trail
        ).count()
        
        summary = {
            "trail": {
                "id": trail.id,
                "name": trail.name,
                "created_at": trail.created_at.isoformat()
            },
            "summary": {
                "database_tables": populated_db_tables.count(),
                "derived_tables": evaluated_tables.count(),
                "total_rows": total_db_rows + total_derived_rows,
                "database_rows": total_db_rows,
                "derived_rows": total_derived_rows,
                "column_values": total_column_values,
                "evaluated_functions": total_evaluated_functions,
                "has_lineage_data": total_derived_rows > 0 or total_evaluated_functions > 0
            }
        }
        
        return JsonResponse(summary)
        
    except Exception as e:
        return JsonResponse({
            'error': 'Trail lineage retrieval failed. Please check system logs.',
            'trail_id': trail_id
        }, status=500)


@require_http_methods(["GET"])
def get_all_trails(request):
    """
    Returns a list of all available trails, ordered by most recent first.
    """
    try:
        trails = Trail.objects.all().order_by('-id')[:50]  # Limit to 50 most recent

        trail_list = []
        for trail in trails:
            trail_list.append({
                "id": trail.id,
                "name": trail.name,
                "created_at": trail.created_at.isoformat() if hasattr(trail, 'created_at') and trail.created_at else None
            })

        return JsonResponse({
            "trails": trail_list,
            "total_count": Trail.objects.count()
        })

    except Exception as e:
        return JsonResponse({
            'error': 'Failed to retrieve trails',
            'details': str(e)
        }, status=500)