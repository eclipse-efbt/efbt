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
                        class_name = args[0].__class__.__name__
                        full_func_name = f"{class_name}.{func_name}"
                    else:
                        full_func_name = func_name
                    
                    # Track the function execution
                    source_columns = list(dependencies.keys()) if isinstance(dependencies, dict) else list(dependencies)
                    
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
                                result_column=func_name
                            )
                            _lineage_context['function_cache'][full_func_name] = function_obj
                    
                    # Track value computation if we have source values
                    if hasattr(orchestration, 'track_value_computation'):
                        # Extract source values from the calling object
                        source_values = []
                        if args and hasattr(args[0], '__dict__'):
                            obj = args[0]
                            for dep in source_columns:
                                # Handle nested dependencies like "base.GRSS_CRRYNG_AMNT"
                                parts = dep.split('.')
                                current = obj
                                for part in parts[:-1]:
                                    if hasattr(current, part):
                                        current = getattr(current, part)
                                if current and hasattr(current, parts[-1]):
                                    source_values.append(getattr(current, parts[-1]))
                        
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
        
        return result
    return wrapper

    