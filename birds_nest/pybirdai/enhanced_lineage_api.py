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
import logging


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
                    content_type__model='function'
                ).values_list('object_id', flat=True)
                
                for dep_id in deps:
                    try:
                        dep_func = Function.objects.get(id=dep_id)
                        calculation_relevant_function_names.add(dep_func.name)
                    except Function.DoesNotExist:
                        pass
        
        # ALTERNATIVE APPROACH: Use DerivedRowSourceReference to trace backwards from F_05_01_REF_FINREP_3_0 rows
        # This finds which UnionItem rows actually contributed to the final result
        allowed_derived_row_ids = set(used_row_ids['DerivedTableRow'])
        allowed_db_row_ids = set(used_row_ids['DatabaseRow'])
        
        print(f"Initial tracking data: {len(allowed_derived_row_ids)} DerivedTableRows, {len(allowed_db_row_ids)} DatabaseRows")
        
        if not include_unused and allowed_derived_row_ids:
            try:
                print(f"Tracing backwards from F_05_01_REF_FINREP_3_0 to find contributing UnionItem rows")
                
                # Find all F_05_01_REF_FINREP_3_0 rows that were tracked as used
                f05_row_ids = []
                for row_id in allowed_derived_row_ids:
                    try:
                        row = DerivedTableRow.objects.get(id=row_id)
                        if row.populated_table.table.name == 'F_05_01_REF_FINREP_3_0':
                            f05_row_ids.append(row_id)
                    except DerivedTableRow.DoesNotExist:
                        pass
                
                print(f"Found {len(f05_row_ids)} F_05_01_REF_FINREP_3_0 rows to trace from")
                
                if f05_row_ids:
                    # Use BFS to find all rows that these F_05_01_REF_FINREP_3_0 rows depend on
                    from collections import deque
                    
                    transitive_allowed_rows = set(f05_row_ids)
                    queue = deque(f05_row_ids)
                    backwards_tracing_worked = False
                    
                    while queue:
                        current_row_id = queue.popleft()
                        
                        # Find all source rows for this row via DerivedRowSourceReference
                        refs = DerivedRowSourceReference.objects.filter(derived_row_id=current_row_id)
                        
                        if refs.exists():
                            backwards_tracing_worked = True
                        
                        for ref in refs:
                            if ref.content_type.model == 'derivedtablerow':
                                source_row_id = ref.object_id
                                if source_row_id not in transitive_allowed_rows:
                                    transitive_allowed_rows.add(source_row_id)
                                    queue.append(source_row_id)
                                    
                                    # Log the relationship for debugging
                                    try:
                                        source_row = DerivedTableRow.objects.get(id=source_row_id)
                                        source_table = source_row.populated_table.table.name
                                        print(f"Traced: F_05_01_REF_FINREP_3_0 -> {source_table} row {source_row_id}")
                                    except DerivedTableRow.DoesNotExist:
                                        pass
                    
                    if backwards_tracing_worked:
                        # Filter the allowed set to only the transitively reachable rows
                        original_count = len(allowed_derived_row_ids)
                        allowed_derived_row_ids &= transitive_allowed_rows
                        removed_count = original_count - len(allowed_derived_row_ids)
                    else:
                        print(f"ERROR: No DerivedRowSourceReference entries found for current F_05_01_REF_FINREP_3_0 rows")
                        print(f"Object relationship tracking failed - need to re-execute datapoint to populate relationships")
                        removed_count = original_count - len(f05_row_ids)  # Only keep F_05_01_REF_FINREP_3_0 rows
                        allowed_derived_row_ids = set(f05_row_ids)  # Filter to only F_05_01_REF_FINREP_3_0 rows
                    
                    print(f"Backwards tracing results: kept {len(allowed_derived_row_ids)}, removed {removed_count} non-contributing rows")
                    
                    # Log which UnionItem rows were kept
                    unionitem_kept = 0
                    for row_id in allowed_derived_row_ids:
                        try:
                            row = DerivedTableRow.objects.get(id=row_id)
                            if row.populated_table.table.name == 'F_05_01_REF_FINREP_3_0_UnionItem':
                                unionitem_kept += 1
                        except DerivedTableRow.DoesNotExist:
                            pass
                    print(f"UnionItem rows kept after backwards tracing: {unionitem_kept}")
                    
                else:
                    print(f"No F_05_01_REF_FINREP_3_0 rows found to trace from")
                    
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
            
            # CRITICAL FIX: Check if any rows from this table were actually used in calculations
            table_has_used_rows = False
            if not include_unused:
                # Only include table if it has rows that were explicitly tracked as used
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
                
                # Add only used rows - CRITICAL FIX: Only include explicitly tracked rows
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
            
            # CRITICAL FIX: Check if any rows from this table were actually used in calculations
            table_has_used_rows = False
            if not include_unused:
                # Only include table if it has rows that were explicitly tracked as used
                for row in eval_table.derivedtablerow_set.all():
                    if row.id in used_row_ids['DerivedTableRow']:
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
                     # CRITICAL FIX: Use filtered allowed_derived_row_ids from backwards tracing
                     # This ensures only rows that transitively contribute to F_05_01_REF_FINREP_3_0 are included
                     include_row = (include_unused or 
                                  row.id in allowed_derived_row_ids)
                     
                     if include_row:
                        row_data = {
                            "id": row.id,
                            "row_identifier": row.row_identifier,
                            "populated_table_id": eval_table.id,
                            "was_used": row.id in used_row_ids['DerivedTableRow'],
                            "evaluated_functions": []
                        }
                        
                        # Add evaluated functions - show ONLY precisely tracked functions
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
                        
                        # CRITICAL FIX: Include row only if it has evaluated functions or is explicitly unused mode
                        # Don't include rows just because the table has used functions - only include rows that have actual data
                        if row_data['evaluated_functions'] or include_unused:
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
        logging.exception(f"Exception in get_trail_filtered_lineage for trail_id={trail_id}")
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