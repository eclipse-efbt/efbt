"""
Debug utilities to help diagnose lineage tracking issues.
"""

def debug_object_type(obj, context=""):
    """Print detailed information about an object to help with debugging"""
    print(f"=== DEBUG OBJECT ({context}) ===")
    print(f"Type: {type(obj)}")
    print(f"Class name: {obj.__class__.__name__}")
    print(f"Has _meta: {hasattr(obj, '_meta')}")
    if hasattr(obj, '_meta'):
        print(f"Has _meta.model: {hasattr(obj._meta, 'model')}")
        if hasattr(obj._meta, 'model'):
            print(f"Model: {obj._meta.model}")
    print(f"Has __dict__: {hasattr(obj, '__dict__')}")
    print(f"Dir (non-private): {[attr for attr in dir(obj) if not attr.startswith('_')][:10]}...")
    
    # Check for common Django model attributes
    django_attrs = ['pk', 'id', '_state', 'objects', 'DoesNotExist']
    django_found = [attr for attr in django_attrs if hasattr(obj, attr)]
    print(f"Django model attributes found: {django_found}")
    
    # Check for business object attributes that might contain Django models
    business_attrs = []
    for attr_name in dir(obj):
        if not attr_name.startswith('_') and not callable(getattr(obj, attr_name, None)):
            attr_value = getattr(obj, attr_name, None)
            if attr_value and hasattr(attr_value, '_meta'):
                business_attrs.append(f"{attr_name}:{type(attr_value).__name__}")
    print(f"Business attrs with Django models: {business_attrs}")
    print("=== END DEBUG ===\n")


def debug_tracking_wrapper(original_track_method):
    """Decorator to add debugging to tracking methods"""
    def debug_wrapper(self, calculation_name, row):
        print(f"\n--- TRACKING DEBUG: {calculation_name} ---")
        debug_object_type(row, f"Row being tracked for {calculation_name}")
        
        # Call the original method - note: original_track_method is already bound to self
        try:
            result = original_track_method(calculation_name, row)
            print(f"✓ Successfully tracked {type(row).__name__}")
            return result
        except Exception as e:
            print(f"✗ Failed to track {type(row).__name__}: {e}")
            raise
    return debug_wrapper


def add_debug_to_orchestration(orchestration):
    """Add debugging to an orchestration instance"""
    if hasattr(orchestration, 'track_calculation_used_row'):
        original_method = orchestration.track_calculation_used_row
        orchestration.track_calculation_used_row = debug_tracking_wrapper(original_method).__get__(orchestration)
        print("Added debugging to track_calculation_used_row")


def debug_populated_tables(trail_id):
    """Debug function to check what's in the database for a trail"""
    from pybirdai.models import (
        Trail, DatabaseTable, DerivedTable, 
        PopulatedDataBaseTable, EvaluatedDerivedTable,
        CalculationUsedRow, CalculationUsedField
    )
    
    print(f"\n=== DATABASE DEBUG FOR TRAIL {trail_id} ===")
    
    try:
        trail = Trail.objects.get(id=trail_id)
        print(f"Trail: {trail.name}")
        
        # Check all tables
        db_tables = DatabaseTable.objects.all()
        print(f"Total DatabaseTables in system: {db_tables.count()}")
        for table in db_tables:
            print(f"  - {table.name} (id: {table.id})")
        
        # Check populated tables for this trail
        pop_tables = PopulatedDataBaseTable.objects.filter(trail=trail)
        print(f"PopulatedDataBaseTables for trail {trail_id}: {pop_tables.count()}")
        for table in pop_tables:
            rows_count = table.databaserow_set.count()
            print(f"  - {table.table.name} with {rows_count} rows")
        
        # Check derived tables
        derived_tables = DerivedTable.objects.all()
        print(f"Total DerivedTables in system: {derived_tables.count()}")
        
        eval_tables = EvaluatedDerivedTable.objects.filter(trail=trail)
        print(f"EvaluatedDerivedTables for trail {trail_id}: {eval_tables.count()}")
        for table in eval_tables:
            rows_count = table.derivedtablerow_set.count()
            print(f"  - {table.table.name} with {rows_count} rows")
        
        # Check usage tracking
        used_rows = CalculationUsedRow.objects.filter(trail=trail)
        print(f"CalculationUsedRows: {used_rows.count()}")
        for row in used_rows:
            print(f"  - {row.calculation_name}: {row.content_type.model} id {row.object_id}")
        
        used_fields = CalculationUsedField.objects.filter(trail=trail)
        print(f"CalculationUsedFields: {used_fields.count()}")
        
    except Exception as e:
        print(f"Error during debug: {e}")
    
    print("=== END DATABASE DEBUG ===\n")


def create_debug_api_endpoint():
    """Create a debug API endpoint to check trail data"""
    from django.http import JsonResponse
    from django.views.decorators.http import require_http_methods
    from pybirdai.models import Trail
    
    @require_http_methods(["GET"])
    def debug_trail_data(request, trail_id):
        """Debug endpoint to check what data exists for a trail"""
        try:
            debug_populated_tables(trail_id)
            
            # Return the debug info as JSON
            from pybirdai.models import (
                DatabaseTable, DerivedTable, 
                PopulatedDataBaseTable, EvaluatedDerivedTable,
                CalculationUsedRow, CalculationUsedField
            )
            
            trail = Trail.objects.get(id=trail_id)
            
            debug_info = {
                "trail_id": trail_id,
                "trail_name": trail.name,
                "total_database_tables": DatabaseTable.objects.count(),
                "total_derived_tables": DerivedTable.objects.count(),
                "populated_database_tables": PopulatedDataBaseTable.objects.filter(trail=trail).count(),
                "evaluated_derived_tables": EvaluatedDerivedTable.objects.filter(trail=trail).count(),
                "used_rows": CalculationUsedRow.objects.filter(trail=trail).count(),
                "used_fields": CalculationUsedField.objects.filter(trail=trail).count(),
                "database_tables_list": list(DatabaseTable.objects.values_list('name', flat=True)),
                "populated_tables_list": list(
                    PopulatedDataBaseTable.objects.filter(trail=trail).values_list('table__name', flat=True)
                )
            }
            
            return JsonResponse(debug_info)
            
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    
    return debug_trail_data