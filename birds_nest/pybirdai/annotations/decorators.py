import functools
import inspect
import ast
import re
from typing import Dict, Any, Optional, Set

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
                        if func_name in ['GRSS_CRRYNG_AMNT', 'ACCNTNG_CLSSFCTN', 'TYP_INSTRMNT'] or is_metric_value:
                            print(f"DEBUG @lineage_polymorphic: {full_func_name}")
                            print(f"  - has_derived_context: {has_derived_context}")
                            print(f"  - is_metric_value: {is_metric_value}")
                            print(f"  - is_derived_method: {is_derived_method}")
                            if args and hasattr(args[0], '__class__'):
                                print(f"  - wrapper_class: {args[0].__class__.__name__}")
                                if hasattr(args[0], 'base') and args[0].base is not None:
                                    print(f"  - base_class: {args[0].base.__class__.__name__}")
                                if hasattr(args[0], 'unionOfLayers'):
                                    print(f"  - has_unionOfLayers: True")
                            print(f"  - should_track: {should_track}")
                            print(f"  - source_values: {source_values}")
                            if hasattr(orchestration, 'current_calculation'):
                                print(f"  - current_calculation: {getattr(orchestration, 'current_calculation', 'NONE')}")
                            else:
                                print(f"  - orchestration has no current_calculation attribute")
                        
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

def lineage_polymorphic(base_dependencies: Set[str] = None, 
                       concrete_dependencies: Dict[str, Set[str]] = None):
    """
    Enhanced lineage decorator for polymorphic base dependencies.
    
    Args:
        base_dependencies: Dependencies using base notation e.g., {"base.GRSS_CRRYNG_AMNT"}
        concrete_dependencies: Per-class concrete dependencies for advanced tracking
                             e.g., {"Other_loans": {"INSTRMNT_RL.GRSS_CRRYNG_AMNT"}}
    """
    if base_dependencies is None:
        base_dependencies = set()
    if concrete_dependencies is None:
        concrete_dependencies = {}
    
    print(f"lineage_polymorphic decorator applied with dependencies: {base_dependencies}")
    
    def decorator_lineage_polymorphic(func):
        @functools.wraps(func)
        def wrapper_lineage(*args, **kwargs):
            # Get orchestration from context
            orchestration = _lineage_context.get('orchestration')
            
            print(f"lineage_polymorphic wrapper called for {func.__name__}, orchestration: {orchestration is not None}")
            
            # Execute the function
            value = func(*args, **kwargs)
            
            # Allow all polymorphic tracking to proceed normally
            
            # Track lineage if orchestration is available
            if orchestration and hasattr(orchestration, 'lineage_enabled') and orchestration.lineage_enabled:
                try:
                    # Extract source information
                    func_name = func.__name__
                    if args and hasattr(args[0], '__class__'):
                        wrapper_obj = args[0]
                        
                        # For UnionItem objects, get the actual base class
                        if hasattr(wrapper_obj, 'base') and wrapper_obj.base is not None:
                            base_obj = wrapper_obj.base
                            base_class_name = base_obj.__class__.__name__
                            wrapper_class_name = wrapper_obj.__class__.__name__
                            full_func_name = f"{wrapper_class_name}.{func_name}"
                            
                            # Resolve concrete dependencies
                            resolved_dependencies = []
                            
                            # Method 1: Use base_dependencies and resolve at runtime
                            for dep_key in base_dependencies:
                                if dep_key.startswith('base.'):
                                    method_name = dep_key[5:]  # Remove "base."
                                    
                                    # Find actual dependency by inspecting the base object
                                    concrete_deps = resolve_concrete_dependency(
                                        base_obj, method_name, orchestration
                                    )
                                    if concrete_deps:
                                        resolved_dependencies.extend(concrete_deps)
                            
                            # Method 2: Use pre-configured concrete dependencies
                            if base_class_name in concrete_dependencies:
                                resolved_dependencies.extend(concrete_dependencies[base_class_name])
                            
                            # Track the function with resolved dependencies
                            if resolved_dependencies or base_dependencies:
                                created_function = None
                                if hasattr(orchestration, 'track_polymorphic_function_execution'):
                                    created_function = orchestration.track_polymorphic_function_execution(
                                        full_func_name,
                                        base_class_name,
                                        resolved_dependencies or list(base_dependencies),
                                        result_column=func_name,
                                        wrapper_obj=wrapper_obj,
                                        base_obj=base_obj
                                    )
                                else:
                                    # Fallback to regular tracking
                                    orchestration.track_function_execution(
                                        f"{full_func_name}@{base_class_name}",
                                        resolved_dependencies or list(base_dependencies),
                                        result_column=func_name,
                                        source_code=f"def {func_name}(self) -> Any: return self.base.{func_name}()"
                                    )
                                
                                # Track the polymorphic function usage - do this for EVERY call, not just new contexts
                                if hasattr(orchestration, 'track_calculation_used_field'):
                                    # Get current calculation context
                                    current_calc = getattr(orchestration, 'current_calculation', None)
                                    if not current_calc:
                                        # Try to infer from wrapper object
                                        current_calc = f"Polymorphic_{wrapper_obj.__class__.__name__}"
                                    
                                    # CRITICAL FIX: Use the created_function object directly instead of name to avoid ID mismatch
                                    if created_function:
                                        # Use the actual function object that was just created/retrieved
                                        orchestration.track_calculation_used_field(current_calc, created_function.name, function_obj=created_function)
                                        print(f"Polymorphic: Successfully registered function {created_function.name} (ID: {created_function.id}) as used")
                                    else:
                                        # Fallback to the old logic if no created_function
                                        from pybirdai.models import Function
                                        poly_func_name = f"{full_func_name}@{base_class_name}"
                                        try:
                                            poly_function = Function.objects.filter(name=poly_func_name).first()
                                            if poly_function:
                                                orchestration.track_calculation_used_field(current_calc, poly_function.name)
                                                print(f"Polymorphic: Successfully registered function {poly_func_name} as used (fallback)")
                                            else:
                                                print(f"WARNING:  Polymorphic function {poly_func_name} not found in database")
                                        except Exception as fe:
                                            print(f"WARNING:  Failed to track polymorphic function usage: {fe}")
                                
                                # Track value computation with resolved source values
                                if hasattr(orchestration, 'track_value_computation'):
                                    source_values = extract_polymorphic_source_values(
                                        base_obj, resolved_dependencies or list(base_dependencies)
                                    )
                                    
                                    # Ensure each wrapper object gets its own derived row context
                                    # This is important for polymorphic functions where each instance should be tracked separately
                                    wrapper_obj_context = orchestration.get_derived_context_for_object(wrapper_obj)
                                    if not wrapper_obj_context:
                                        # Create derived row context for this specific wrapper object
                                        print(f"Polymorphic: Creating derived row context for {full_func_name}")
                                        derived_row_id = orchestration._ensure_derived_row_context(wrapper_obj, full_func_name)
                                        print(f"Polymorphic: Derived row ID: {derived_row_id}")
                                        if derived_row_id:
                                            # CRITICAL: Register this UnionItem row as used by the current calculation (only on first call)
                                            print(f"Polymorphic: Has track_calculation_used_row: {hasattr(orchestration, 'track_calculation_used_row')}")
                                            if hasattr(orchestration, 'track_calculation_used_row'):
                                                try:
                                                    from pybirdai.models import DerivedTableRow
                                                    derived_row = DerivedTableRow.objects.get(id=derived_row_id)
                                                    
                                                    print(f"Polymorphic: Calling track_calculation_used_row for row {derived_row_id}, calc: {current_calc}")
                                                    orchestration.track_calculation_used_row(current_calc, derived_row)
                                                    print(f"Polymorphic: Successfully registered UnionItem row {derived_row_id} as used")
                                                except Exception as e:
                                                    print(f"WARNING:  Failed to register UnionItem row as used: {e}")
                                                    import traceback
                                                    traceback.print_exc()
                                            else:
                                                print(f"WARNING:  Orchestration missing track_calculation_used_row method")
                                            
                                            # Temporarily set this as the current context for this computation
                                            original_context = orchestration.current_rows.get('derived')
                                            orchestration.current_rows['derived'] = derived_row_id
                                            
                                            try:
                                                # Track the value computation with the object-specific context
                                                # CRITICAL FIX: Use polymorphic function name to match what was created in database
                                                poly_func_name = f"{full_func_name}@{base_class_name}"
                                                print(f"Polymorphic: Calling track_value_computation for {poly_func_name}, value={value}, context={derived_row_id}")
                                                result = orchestration.track_value_computation(
                                                    poly_func_name,  # Use the polymorphic function name
                                                    source_values,
                                                    value
                                                )
                                                print(f"Polymorphic: track_value_computation returned {result}")
                                            finally:
                                                # Restore the original context
                                                if original_context:
                                                    orchestration.current_rows['derived'] = original_context
                                                else:
                                                    orchestration.current_rows.pop('derived', None)
                                            
                                            # Skip the normal track_value_computation call below
                                            return value
                                    else:
                                        # CRITICAL: ALWAYS create evaluations in BOTH contexts for polymorphic functions
                                        # This ensures functions appear in both UnionItem and Other_loans tables
                                        
                                        poly_func_name = f"{full_func_name}@{base_class_name}"
                                        original_context = orchestration.current_rows.get('derived')
                                        
                                        # STEP 1: Always ensure wrapper context exists and track there
                                        wrapper_obj_context = orchestration.get_derived_context_for_object(wrapper_obj)
                                        if not wrapper_obj_context:
                                            wrapper_obj_context = orchestration._ensure_derived_row_context(wrapper_obj, f"{wrapper_class_name}.{func_name}")
                                            print(f"Polymorphic: Created wrapper context for {wrapper_class_name}: {wrapper_obj_context}")
                                        else:
                                            print(f"Polymorphic: Using existing wrapper context: {wrapper_obj_context}")
                                        
                                        # Track in wrapper context (UnionItem table)
                                        orchestration.current_rows['derived'] = wrapper_obj_context
                                        try:
                                            print(f"Polymorphic: Tracking in wrapper context (UnionItem): {poly_func_name}, value={value}")
                                            orchestration.track_value_computation(poly_func_name, source_values, value)
                                            print(f"Polymorphic: Successfully tracked in UnionItem context")
                                        except Exception as e:
                                            print(f"WARNING: Error tracking in wrapper context: {e}")
                                        
                                        # STEP 2: Always ensure base context exists and track there
                                        base_obj_context = orchestration.get_derived_context_for_object(base_obj)
                                        if not base_obj_context:
                                            base_obj_context = orchestration._ensure_derived_row_context(base_obj, f"{base_class_name}.{func_name}")
                                            print(f"Polymorphic: Created base context for {base_class_name}: {base_obj_context}")
                                        else:
                                            print(f"Polymorphic: Using existing base context: {base_obj_context}")
                                        
                                        # Track in base context (Other_loans table)
                                        orchestration.current_rows['derived'] = base_obj_context
                                        try:
                                            print(f"Polymorphic: Tracking in base context (Other_loans): {poly_func_name}, value={value}")
                                            orchestration.track_value_computation(poly_func_name, source_values, value)
                                            print(f"Polymorphic: Successfully tracked in Other_loans context")
                                        except Exception as e:
                                            print(f"WARNING: Error tracking in base context: {e}")
                                        finally:
                                            # Restore the original context
                                            if original_context:
                                                orchestration.current_rows['derived'] = original_context
                                            else:
                                                orchestration.current_rows.pop('derived', None)
                                        
                                        print(f"Polymorphic: Completed dual-context tracking for {poly_func_name}")
                                        
                                        # Skip the normal track_value_computation call below
                                        return value
                        else:
                            # Fall back to regular lineage tracking
                            class_name = wrapper_obj.__class__.__name__
                            full_func_name = f"{class_name}.{func_name}"
                            
                            # Convert dependencies to regular format for fallback
                            regular_deps = list(base_dependencies)
                            if hasattr(orchestration, 'track_function_execution'):
                                orchestration.track_function_execution(
                                    full_func_name,
                                    regular_deps,
                                    result_column=func_name
                                )
                
                except Exception as e:
                    print(f"Polymorphic lineage tracking error: {e}")
            
            return value
        return wrapper_lineage
    return decorator_lineage_polymorphic

def resolve_concrete_dependency(base_obj, method_name, orchestration):
    """
    Resolve the concrete dependencies for a method on a base object.
    This inspects the actual implementation to find what it depends on.
    """
    resolved_deps = []
    
    try:
        if hasattr(base_obj, method_name):
            method = getattr(base_obj, method_name)
            
            # Check if the method has @lineage decorators itself
            if hasattr(method, '__wrapped__'):
                # Extract dependencies from the wrapped method
                try:
                    source_code = inspect.getsource(method)
                    dependencies = extract_lineage_dependencies_from_source(source_code)
                    if dependencies:
                        resolved_deps.extend(dependencies)
                except:
                    pass
            
            # For methods that don't have lineage decorators, try to infer dependencies
            # by looking at the source code and finding attribute accesses
            try:
                source_code = inspect.getsource(method)
                inferred_deps = infer_dependencies_from_source(source_code, base_obj.__class__.__name__)
                resolved_deps.extend(inferred_deps)
            except:
                pass
                
    except Exception as e:
        print(f"Error resolving concrete dependency for {method_name}: {e}")
    
    return resolved_deps

def extract_lineage_dependencies_from_source(source_code):
    """Extract dependencies from @lineage decorator in source code."""
    dependencies = []
    
    try:
        # Look for @lineage(dependencies={...}) patterns
        lineage_pattern = r'@lineage\s*\(\s*dependencies\s*=\s*\{([^}]+)\}'
        matches = re.findall(lineage_pattern, source_code)
        
        for match in matches:
            # Extract quoted strings from the dependencies set
            dep_pattern = r'["\']([^"\']+)["\']'
            deps = re.findall(dep_pattern, match)
            dependencies.extend(deps)
    
    except Exception as e:
        print(f"Error extracting lineage dependencies: {e}")
    
    return dependencies

def infer_dependencies_from_source(source_code, class_name):
    """
    Infer dependencies by parsing method source code for attribute accesses.
    """
    dependencies = []
    
    try:
        # First try to clean up the source code
        # Remove any leading whitespace that might cause indent issues
        lines = source_code.strip().split('\n')
        if lines:
            # Find the minimum indentation (excluding empty lines)
            min_indent = float('inf')
            for line in lines:
                if line.strip():  # Skip empty lines
                    indent = len(line) - len(line.lstrip())
                    min_indent = min(min_indent, indent)
            
            # Remove the common indentation
            if min_indent != float('inf') and min_indent > 0:
                cleaned_lines = []
                for line in lines:
                    if line.strip():  # Non-empty line
                        cleaned_lines.append(line[min_indent:])
                    else:
                        cleaned_lines.append('')
                source_code = '\n'.join(cleaned_lines)
        
        # Parse the source code using AST
        tree = ast.parse(source_code)
        
        # Find attribute accesses
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                # Look for patterns like self.INSTRMNT_RL.GRSS_CRRYNG_AMNT
                if isinstance(node.value, ast.Attribute):
                    if isinstance(node.value.value, ast.Name) and node.value.value.id == 'self':
                        dep = f"{node.value.attr}.{node.attr}"
                        if dep not in dependencies:
                            dependencies.append(dep)
        
        # Also use regex to catch additional patterns
        attr_pattern = r'self\.([A-Z_]+)\.([A-Z_]+)'
        matches = re.findall(attr_pattern, source_code)
        for table, column in matches:
            dep = f"{table}.{column}"
            if dep not in dependencies:
                dependencies.append(dep)
                
    except Exception as e:
        # If AST parsing fails, fall back to regex only
        try:
            attr_pattern = r'self\.([A-Z_]+)\.([A-Z_]+)'
            matches = re.findall(attr_pattern, source_code)
            for table, column in matches:
                dep = f"{table}.{column}"
                if dep not in dependencies:
                    dependencies.append(dep)
        except Exception as regex_error:
            print(f"Error inferring dependencies from source (both AST and regex failed): {e}, {regex_error}")
    
    return dependencies

def extract_polymorphic_source_values(base_obj, dependencies):
    """
    Extract actual source values from the base object using resolved dependencies.
    """
    source_values = []
    
    for dep in dependencies:
        try:
            if '.' in dep:
                parts = dep.split('.')
                current = base_obj
                
                for part in parts:
                    if hasattr(current, part):
                        current = getattr(current, part)
                        # Handle callable attributes
                        if callable(current):
                            try:
                                current = current()
                            except:
                                current = None
                                break
                    else:
                        current = None
                        break
                
                if current is not None:
                    source_values.append(current)
            elif dep.startswith('base.'):
                # Handle base dependencies by looking directly at the base object
                attr_name = dep[5:]  # Remove 'base.'
                if hasattr(base_obj, attr_name):
                    current = getattr(base_obj, attr_name)
                    if callable(current):
                        try:
                            current = current()
                        except:
                            current = None
                    if current is not None:
                        source_values.append(current)
                    
        except Exception as e:
            print(f"Error extracting source value for {dep}: {e}")
    
    return source_values

    