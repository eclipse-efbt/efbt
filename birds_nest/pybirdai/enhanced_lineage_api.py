"""
Enhanced lineage API that includes information about which rows and fields were actually used in calculations.
"""

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
    CalculationUsedRow, CalculationUsedField
)
import json
from datetime import datetime


def serialize_datetime(obj):
    """JSON serializer for datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


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
            
            # Check if any rows from this table were used
            table_has_used_rows = False
            if not include_unused:
                for row in pop_table.databaserow_set.all():
                    if row.id in used_row_ids['DatabaseRow']:
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
                
                # Add only used rows
                for row in pop_table.databaserow_set.all():
                    if include_unused or row.id in used_row_ids['DatabaseRow']:
                        row_data = {
                            "id": row.id,
                            "row_identifier": row.row_identifier,
                            "populated_table_id": pop_table.id,
                            "was_used": row.id in used_row_ids['DatabaseRow'],
                            "values": []
                        }
                        
                        # Build set of used field names for this specific table
                        used_field_names_for_table = set()
                        for field_id in used_field_ids['DatabaseField']:
                            try:
                                field = DatabaseField.objects.get(id=field_id)
                                if field.table.id == table.id:  # Field belongs to this table
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
            
            # Check if any rows from this table were used
            table_has_used_rows = False
            if not include_unused:
                for row in eval_table.derivedtablerow_set.all():
                    if row.id in used_row_ids['DerivedTableRow']:
                        table_has_used_rows = True
                        break
            else:
                table_has_used_rows = True
            
            # If this derived table has functions that were used, include it
            # Also include if a function with the same table name was used (for data consistency)
            if not table_has_used_rows and eval_table.derivedtablerow_set.exists():
                # Check if any functions in this table were used
                table_functions_used = any(
                    func.table.id == table.id 
                    for field_id in used_field_ids['Function']
                    for func in [Function.objects.get(id=field_id)]
                    if Function.objects.filter(id=field_id).exists()
                )
                
                # Also check if any functions with the same table name were used
                same_name_function_used = any(
                    func.table.name == table.name
                    for field_id in used_field_ids['Function']
                    for func in [Function.objects.get(id=field_id)]
                    if Function.objects.filter(id=field_id).exists()
                )
                
                if table_functions_used or same_name_function_used:
                    table_has_used_rows = True
            
            if table_has_used_rows:
                # Add table definition
                if not any(dt['id'] == table.id for dt in lineage_data['derived_tables']):
                    table_data = {
                        "id": table.id,
                        "name": table.name,
                        "table_creation_function_id": table.table_creation_function.id if table.table_creation_function else None,
                        "functions": []
                    }
                    
                    # Add only used functions
                    for function in table.derived_functions.all():
                        if include_unused or function.id in used_field_ids['Function']:
                            function_data = {
                                "id": function.id,
                                "name": function.name,
                                "table_id": table.id,
                                "function_text_id": function.function_text.id,
                                "function_text": function.function_text.text if function.function_text else None,
                                "function_language": function.function_text.language if function.function_text else None,
                                "was_used": function.id in used_field_ids['Function']
                            }
                            table_data['functions'].append(function_data)
                    
                    lineage_data['derived_tables'].append(table_data)
                
                # Add evaluated table instance
                eval_table_data = {
                    "id": eval_table.id,
                    "table_id": table.id,
                    "table_name": table.name,
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
                    except Function.DoesNotExist:
                        pass
                
                # Add only used rows
                for row in eval_table.derivedtablerow_set.all():
                    # Include row if explicitly tracked or if the table was determined to be used
                    include_row = (include_unused or 
                                 row.id in used_row_ids['DerivedTableRow'] or
                                 table_has_used_rows)
                    
                    if include_row:
                        row_data = {
                            "id": row.id,
                            "row_identifier": row.row_identifier,
                            "populated_table_id": eval_table.id,
                            "was_used": row.id in used_row_ids['DerivedTableRow'] or table_has_used_rows,
                            "evaluated_functions": []
                        }
                        
                        # Add evaluated functions - show ONLY precisely tracked functions
                        for eval_func in row.evaluated_functions.all():
                            function_name = eval_func.function.name
                            show_function = (include_unused or 
                                           eval_func.function.id in used_field_ids['Function'] or
                                           function_name in used_field_names_for_derived_table)
                            
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
                        
                        # Include row if it has functions OR if the table itself has used functions
                        # (even if this specific row doesn't have calculated values yet)
                        table_has_used_functions = len(used_field_names_for_derived_table) > 0
                        if row_data['evaluated_functions'] or include_unused or table_has_used_functions:
                            eval_table_data['rows'].append(row_data)
                
                if eval_table_data['rows']:  # Only add if there are rows
                    lineage_data['evaluated_derived_tables'].append(eval_table_data)
        
        # Get lineage relationships only for used data
        if not include_unused:
            # Function column references - only for used functions
            func_refs = FunctionColumnReference.objects.filter(
                function__id__in=used_field_ids['Function']
            ).select_related('function', 'content_type')
            
            for ref in func_refs:
                # Check if the referenced column was also used
                ref_used = False
                if ref.content_type.model == 'databasefield' and ref.object_id in used_field_ids['DatabaseField']:
                    ref_used = True
                elif ref.content_type.model == 'function' and ref.object_id in used_field_ids['Function']:
                    ref_used = True
                
                if ref_used or include_unused:
                    lineage_data['lineage_relationships']['function_column_references'].append({
                        "id": ref.id,
                        "function_id": ref.function.id,
                        "function_name": ref.function.name,
                        "referenced_object_type": ref.content_type.model,
                        "referenced_object_id": ref.object_id
                    })
            
            # Derived row source references - only for used rows
            row_refs = DerivedRowSourceReference.objects.filter(
                derived_row__id__in=used_row_ids['DerivedTableRow']
            ).select_related('derived_row', 'content_type')
            
            for ref in row_refs:
                lineage_data['lineage_relationships']['derived_row_source_references'].append({
                    "id": ref.id,
                    "derived_row_id": ref.derived_row.id,
                    "source_object_type": ref.content_type.model,
                    "source_object_id": ref.object_id
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
        
        return JsonResponse(lineage_data, json_dumps_params={'default': serialize_datetime})
    
    except Exception as e:
        return JsonResponse({
            "error": str(e),
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