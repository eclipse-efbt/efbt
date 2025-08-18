import functools
import inspect
from typing import Dict, Any, Optional

# Global registry to track lineage execution context
_lineage_context = {
    'orchestration': None,
    'current_trail': None,
    'function_cache': {},  # Cache function objects to avoid recreating
}

def set_lineage_orchestration(orchestration):
    """Set the orchestration instance for lineage tracking"""
    _lineage_context['orchestration'] = orchestration
    if orchestration and hasattr(orchestration, 'trail'):
        _lineage_context['current_trail'] = orchestration.trail

def lineage(dependencies: Dict[str, Any] = None):
    """
    Decorator to track function lineage in AORTA.
    
    Args:
        dependencies: Dictionary mapping dependency names to their paths
                     e.g., {"base.GRSS_CRRYNG_AMNT", "FNNCL_ASST_INSTRMNT.PRPS"}
    """
    if dependencies is None:
        dependencies = {}
    
    def decorator_lineage(func):
        @functools.wraps(func)
        def wrapper_lineage(*args, **kwargs):
            # Get orchestration from context
            orchestration = _lineage_context.get('orchestration')
            
            # Execute the function
            value = func(*args, **kwargs)
            
            # Track lineage if orchestration is available
            if orchestration and hasattr(orchestration, 'lineage_enabled') and orchestration.lineage_enabled:
                try:
                    # Extract source information
                    func_name = func.__name__
                    if args and hasattr(args[0], '__class__'):
                        # Check if this is a wrapper object with a base attribute
                        if hasattr(args[0], 'base') and args[0].base is not None:
                            # Use the base object's class name for proper context
                            class_name = args[0].base.__class__.__name__
                        elif hasattr(args[0], 'unionOfLayers') and args[0].unionOfLayers is not None:
                            # Handle F_05_01_REF_FINREP_3_0 -> UnionItem case
                            # Keep the original class name for these top-level objects
                            class_name = args[0].__class__.__name__
                        else:
                            class_name = args[0].__class__.__name__
                        full_func_name = f"{class_name}.{func_name}"
                    else:
                        full_func_name = func_name
                    
                    # Track the function execution
                    source_columns = list(dependencies.keys()) if isinstance(dependencies, dict) else list(dependencies)
                    
                    # Track field usage for calculations
                    if hasattr(orchestration, 'track_calculation_used_field'):
                        calculation_name = getattr(orchestration, 'current_calculation', None)
                        if calculation_name:
                            # Track each accessed field
                            for field_dep in source_columns:
                                orchestration.track_calculation_used_field(calculation_name, field_dep)
                            # Also track the current function/field being called
                            if args and hasattr(args[0], '__class__'):
                                orchestration.track_calculation_used_field(calculation_name, func_name, args[0] if len(args) > 0 else None)
                    
                    # Get the function source code if not cached
                    if full_func_name not in _lineage_context['function_cache']:
                        try:
                            source_code = inspect.getsource(func)
                        except:
                            source_code = str(func)
                        
                        # Store in cache and track in orchestration
                        if hasattr(orchestration, 'track_function_execution'):
                            function_obj = orchestration.track_function_execution(
                                full_func_name, 
                                source_columns,
                                result_column=func_name,
                                source_code=source_code
                            )
                            _lineage_context['function_cache'][full_func_name] = function_obj
                    else:
                        # Function already cached - no need to track again
                        # print(f"Using cached function: {full_func_name}")
                        pass
                    
                    # Track value computation if we have source values
                    if hasattr(orchestration, 'track_value_computation'):
                        # Extract source values from the calling object
                        source_values = []
                        if args and hasattr(args[0], '__dict__'):
                            obj = args[0]
                            for dep in source_columns:
                                try:
                                    # Handle nested dependencies like "base.GRSS_CRRYNG_AMNT"
                                    parts = dep.split('.')
                                    current = obj
                                    for part in parts[:-1]:
                                        if hasattr(current, part):
                                            current = getattr(current, part)
                                        else:
                                            current = None
                                            break
                                    
                                    if current and hasattr(current, parts[-1]):
                                        source_value = getattr(current, parts[-1])
                                        # Handle callable attributes
                                        if callable(source_value):
                                            try:
                                                source_value = source_value()
                                            except:
                                                pass
                                        source_values.append(source_value)
                                except Exception as e:
                                    print(f"Error extracting source value for {dep}: {e}")
                        
                        # Track value computation in these cases:
                        # 1. We have a derived row context
                        # 2. This is the metric_value function
                        # 3. This is a @lineage decorated method on a derived table object
                        
                        # Debug logging
                        has_derived_context = (hasattr(orchestration, 'current_rows') and 
                                             orchestration.current_rows.get('derived'))
                        is_metric_value = func_name == 'metric_value'
                        
                        # Check if this is a derived method - check both wrapper and base object
                        is_derived_method = False
                        if args and hasattr(args[0], '__class__'):
                            class_to_check = args[0].__class__.__name__
                            # For UnionItem objects, check the base object class
                            if (hasattr(args[0], 'base') and args[0].base is not None 
                                and not hasattr(args[0], 'unionOfLayers')):
                                class_to_check = args[0].base.__class__.__name__
                            is_derived_method = any(pattern in class_to_check 
                                                  for pattern in ['F_01_01_REF_FINREP', 'F_05_01_REF_FINREP', 'Other_loans', 'Trade_receivables', 'Finance_leases'])
                        
                        should_track = has_derived_context or is_metric_value or is_derived_method
                        
                        # Debug output (uncomment for debugging)
                        # if func_name in ['GRSS_CRRYNG_AMNT', 'ACCNTNG_CLSSFCTN', 'TYP_INSTRMNT'] or is_metric_value:
                        #     print(f"DEBUG @lineage: {full_func_name}")
                        #     print(f"  - has_derived_context: {has_derived_context}")
                        #     print(f"  - is_metric_value: {is_metric_value}")
                        #     print(f"  - is_derived_method: {is_derived_method}")
                        #     if args and hasattr(args[0], '__class__'):
                        #         print(f"  - wrapper_class: {args[0].__class__.__name__}")
                        #         if hasattr(args[0], 'base') and args[0].base is not None:
                        #             print(f"  - base_class: {args[0].base.__class__.__name__}")
                        #         if hasattr(args[0], 'unionOfLayers'):
                        #             print(f"  - has_unionOfLayers: True")
                        #     print(f"  - should_track: {should_track}")
                        #     print(f"  - source_values: {source_values}")
                        
                        if should_track:
                            # Get the actual object for context (use base if it's a UnionItem wrapper)
                            actual_obj = args[0] if args else None
                            if (actual_obj and hasattr(actual_obj, 'base') and actual_obj.base is not None
                                and not hasattr(actual_obj, 'unionOfLayers')):
                                # Only use base for UnionItem objects, not for top-level report objects
                                actual_obj = actual_obj.base
                            
                            # If this is a derived method but no derived context exists, create one
                            if is_derived_method and not has_derived_context and actual_obj:
                                derived_row_id = orchestration._ensure_derived_row_context(actual_obj, full_func_name)
                                if derived_row_id:
                                    # Set the global context for backward compatibility
                                    orchestration.current_rows['derived'] = derived_row_id
                            
                            # Use object-specific context for tracking
                            if is_derived_method and actual_obj:
                                # Ensure each object gets its own context, even if there's already a global context
                                # This is important for top-level objects that should have separate contexts
                                obj_derived_row_id = orchestration.get_derived_context_for_object(actual_obj)
                                if not obj_derived_row_id:
                                    # Create context if it doesn't exist
                                    obj_derived_row_id = orchestration._ensure_derived_row_context(actual_obj, full_func_name)
                                
                                if obj_derived_row_id:
                                    original_derived_context = orchestration.current_rows.get('derived')
                                    orchestration.current_rows['derived'] = obj_derived_row_id
                                    try:
                                        orchestration.track_value_computation(
                                            full_func_name,
                                            source_values,
                                            value
                                        )
                                    finally:
                                        # Restore original context
                                        if original_derived_context:
                                            orchestration.current_rows['derived'] = original_derived_context
                                        else:
                                            orchestration.current_rows.pop('derived', None)
                                else:
                                    orchestration.track_value_computation(
                                        full_func_name,
                                        source_values,
                                        value
                                    )
                            else:
                                orchestration.track_value_computation(
                                    full_func_name,
                                    source_values,
                                    value
                                )
                
                except Exception as e:
                    # Don't let lineage tracking errors break the actual computation
                    print(f"Lineage tracking error: {e}")
            
            return value
        return wrapper_lineage
    return decorator_lineage

def track_table_init(func):
    """Decorator for tracking table initialization"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get orchestration from context
        orchestration = _lineage_context.get('orchestration')
        
        # Execute the function
        result = func(*args, **kwargs)
        
        # Track table initialization if orchestration is available
        if orchestration and args and hasattr(args[0], '__class__'):
            class_name = args[0].__class__.__name__
            if class_name.endswith('_Table') and hasattr(orchestration, '_track_object_initialization'):
                orchestration._track_object_initialization(args[0])
                
                # Also track any data that was populated during initialization
                table_obj = args[0]
                table_name = class_name.replace('_Table', '')
                
                # Look for list attributes that might contain data
                for attr_name in dir(table_obj):
                    if not attr_name.startswith('_') and hasattr(table_obj, attr_name):
                        attr_value = getattr(table_obj, attr_name)
                        if isinstance(attr_value, list) and len(attr_value) > 0:
                            # This looks like a data collection
                            if hasattr(orchestration, 'track_data_processing'):
                                orchestration.track_data_processing(f"{table_name}_{attr_name}", attr_value)
        
        return result
    return wrapper

    