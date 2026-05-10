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
Automatic tracking wrapper that can add lineage tracking to existing report cells
without requiring code modifications.
"""

import inspect
import os
from pybirdai.annotations.decorators import _lineage_context
from pybirdai.process_steps.pybird.lineage_collector import get_collector

_DEBUG_LINEAGE = os.environ.get('PYBIRDAI_DEBUG_LINEAGE', '').lower() in {'1', 'true', 'yes', 'on'}

def _tracking_debug(message):
    if _DEBUG_LINEAGE:
        print(message)

def wrap_cell_with_tracking(cell_instance):
    """
    Wrap an existing cell instance with automatic tracking.
    This adds tracking without requiring code modifications.
    """
    
    # Get the calculation name from the class
    calculation_name = cell_instance.__class__.__name__
    
    # Get orchestration from context
    orchestration = _lineage_context.get('orchestration')
    if not orchestration or not hasattr(orchestration, 'track_calculation_used_row'):
        return cell_instance
    
    # Set the calculation context
    orchestration.current_calculation = calculation_name
    
    # Store original methods
    if hasattr(cell_instance, 'calc_referenced_items'):
        original_calc = cell_instance.calc_referenced_items
    else:
        return cell_instance
    
    # Find the filtered items attribute (usually ends with 's')
    filtered_items_attr = None
    for attr_name in dir(cell_instance):
        if not attr_name.startswith('_') and not callable(getattr(cell_instance, attr_name)):
            attr_value = getattr(cell_instance, attr_name)
            if isinstance(attr_value, list) and attr_name.endswith('s'):
                filtered_items_attr = attr_name
                break
    
    if not filtered_items_attr:
        return cell_instance
    
    # Create wrapped calc_referenced_items method
    def wrapped_calc_referenced_items():
        # Set calculation context
        orchestration.current_calculation = calculation_name
        
        # Clear the filtered items list
        setattr(cell_instance, filtered_items_attr, [])
        
        # Call original method - this will populate the filtered items
        original_calc()
        
        # Track all items that were added (these passed the filters)
        filtered_items = getattr(cell_instance, filtered_items_attr)
        for item in filtered_items:
            orchestration.track_calculation_used_row(calculation_name, item)
            
            # Track only fields that are actually used by analyzing the cell's source code
            # No longer using hardcoded common fields that cause unnecessary evaluations
    
    # Replace the method
    cell_instance.calc_referenced_items = wrapped_calc_referenced_items
    
    return cell_instance


def auto_wrap_cell_execution(cell_class_name, data_point_id):
    """
    Automatically wrap a cell during execution.
    This can be called from execute_datapoint.py
    """
    
    # Import the report_cells module
    import pybirdai.process_steps.filter_code.report_cells as report_cells
    
    # Get the cell class
    klass = getattr(report_cells, 'Cell_' + str(data_point_id), None)
    if not klass:
        _tracking_debug(f"Could not find cell class for data point {data_point_id}")
        return None
    
    # Create the cell instance
    cell_instance = klass()
    
    # Wrap it with tracking
    wrapped_cell = wrap_cell_with_tracking(cell_instance)
    
    return wrapped_cell


def inspect_cell_for_field_usage(cell_instance, source_code=None):
    """
    Inspect a cell's source code to determine which fields it uses.
    This provides a more accurate tracking than the heuristic approach.
    """
    
    if not source_code:
        try:
            source_code = inspect.getsource(cell_instance.calc_referenced_items)
        except:
            return []
    
    # Extract field names from the source code
    import re
    
    # Pattern to find item.FIELD_NAME() calls
    field_pattern = r'item\.(\w+)\(\)'
    
    field_names = []
    for match in re.finditer(field_pattern, source_code):
        field_name = match.group(1)
        if field_name not in field_names:
            field_names.append(field_name)
    
    return field_names


def create_smart_tracking_wrapper(cell_instance, orchestration=None):
    """
    Create a smarter tracking wrapper that analyzes the cell's code
    to determine exactly which fields are used.
    """
    
    calculation_name = cell_instance.__class__.__name__
    _tracking_debug(f"🛠️ Creating smart tracking wrapper for: {calculation_name}")
    
    # Get orchestration from parameter or context
    if not orchestration:
        orchestration = _lineage_context.get('orchestration')
        _tracking_debug(f"Got orchestration from context: {orchestration}")
    
    if not orchestration or not hasattr(orchestration, 'track_calculation_used_row'):
        _tracking_debug(f"❌ No orchestration available for tracking in {calculation_name}")
        return cell_instance
    
    # Set the calculation context
    orchestration.current_calculation = calculation_name
    _tracking_debug(f"✅ Orchestration ready for {calculation_name}")
    
    # Get the fields used by this cell by analyzing source code
    try:
        used_fields = inspect_cell_for_field_usage(cell_instance)
    except Exception as e:
        _tracking_debug(f"⚠️ Failed to analyze cell source code: {e}")
        used_fields = []
    _tracking_debug(f"Fields to track: {used_fields}")
    
    # Store original methods
    if hasattr(cell_instance, 'calc_referenced_items'):
        original_calc = cell_instance.calc_referenced_items
        _tracking_debug(f"✅ Found calc_referenced_items method")
    else:
        _tracking_debug(f"❌ No calc_referenced_items method found")
        return cell_instance
    
    # Find the filtered items attribute
    filtered_items_attr = None
    for attr_name in dir(cell_instance):
        if not attr_name.startswith('_') and not callable(getattr(cell_instance, attr_name)):
            attr_value = getattr(cell_instance, attr_name)
            if isinstance(attr_value, list) and attr_name.endswith('s'):
                filtered_items_attr = attr_name
                _tracking_debug(f"Found filtered items attribute: {attr_name}")
                break
    
    if not filtered_items_attr:
        _tracking_debug(f"❌ No filtered items attribute found")
        return cell_instance
    
    # Create wrapped calc_referenced_items method
    def smart_wrapped_calc():
        # Set calculation context
        orchestration.current_calculation = calculation_name
        _tracking_debug(f"🚀 SMART WRAPPED CALC CALLED: {calculation_name}")
        _tracking_debug(f"Setting calculation context: {calculation_name}")
        
        # Clear the filtered items list
        setattr(cell_instance, filtered_items_attr, [])
        _tracking_debug(f"Cleared {filtered_items_attr}")
        
        # Call original method
        _tracking_debug(f"Calling original calc_referenced_items...")
        original_calc()
        _tracking_debug(f"Original calc_referenced_items completed")
        
        # Track all items that were added and their fields
        filtered_items = getattr(cell_instance, filtered_items_attr)
        _tracking_debug(f"Found {len(filtered_items)} filtered items in {calculation_name}")
        
        for item in filtered_items:
            try:
                # Track the row
                _tracking_debug(f"ATTEMPTING TO TRACK ROW: {type(item).__name__} for {calculation_name}")
                orchestration.track_calculation_used_row(calculation_name, item)
                _tracking_debug(f"✓ Successfully tracked row for {calculation_name}: {type(item).__name__}")
                
                # If this is a business object, also try to track any underlying data sources
                if hasattr(item, 'base') and item.base is not None:
                    try:
                        _tracking_debug(f"ATTEMPTING TO TRACK BASE: {type(item.base).__name__}")
                        orchestration.track_calculation_used_row(calculation_name, item.base)
                        _tracking_debug(f"✓ Successfully tracked base object for {calculation_name}: {type(item.base).__name__}")
                    except Exception as e:
                        _tracking_debug(f"✗ Failed to track base object: {e}")
                
                # Track each field that this cell uses with PRECISE database field tracking
                for field_name in used_fields:
                    if hasattr(item, field_name):
                        try:
                            # Get the method
                            method = getattr(item, field_name)
                            if callable(method):
                                _tracking_debug(f"🎯 Tracking precise field access: {field_name} on {type(item).__name__}")
                                
                                # Set up database access monitoring
                                accessed_db_fields = []
                                original_getattr = None
                                
                                # Create a monitoring wrapper for database field access
                                def track_db_field_access(obj, attr_name):
                                    """Monitor when Django model fields are accessed"""
                                    if hasattr(obj, '_meta') and hasattr(obj._meta, 'model'):
                                        # This is a Django model - track field access
                                        if hasattr(obj._meta.model, attr_name):
                                            model_name = type(obj).__name__
                                            accessed_db_fields.append(f"{model_name}.{attr_name}")
                                            _tracking_debug(f"  📊 DB field accessed: {model_name}.{attr_name}")
                                    
                                    # Call the original getattr
                                    return object.__getattribute__(obj, attr_name)
                                
                                # Temporarily replace __getattribute__ on Django models if the item has access to them
                                # This is complex, so let's use a simpler approach first
                                
                                # Execute the method only if the lineage value was not already
                                # captured during the original filter calculation.
                                if (
                                    hasattr(orchestration, 'has_tracked_value_for_object')
                                    and orchestration.has_tracked_value_for_object(item, field_name)
                                ):
                                    field_value = None
                                else:
                                    field_value = method()
                                
                                # Track the business logic field access
                                orchestration.track_calculation_used_field(calculation_name, field_name, item)
                                _tracking_debug(f"Tracked field {field_name} for {calculation_name} (value: {field_value})")

                                # Register runtime dependency with collector
                                collector = get_collector()
                                item_class_name = type(item).__name__
                                collector.add_runtime_dependency(calculation_name, item_class_name, field_name)
                                
                                # Try to trace what database sources this business method used
                                # Look for database model attributes in the business object
                                db_sources_found = []
                                for attr_name in dir(item):
                                    if not attr_name.startswith('_'):
                                        try:
                                            attr_value = getattr(item, attr_name)
                                            if attr_value and hasattr(attr_value, '_meta') and hasattr(attr_value._meta, 'model'):
                                                # This is a Django model instance
                                                model_name = type(attr_value).__name__
                                                db_sources_found.append((model_name, attr_value))
                                        except:
                                            pass
                                
                                # For each Django model source, track it as a used row and try to identify relevant fields
                                for model_name, model_instance in db_sources_found:
                                    try:
                                        # Track the model instance as a used row
                                        orchestration.track_calculation_used_row(calculation_name, model_instance)
                                        _tracking_debug(f"  📋 Tracked source model: {model_name}")
                                        
                                        # Try to identify which specific fields of this model might contain the computed value
                                        # Look for fields with similar names or that might contribute to the calculation
                                        if hasattr(model_instance, '_meta'):
                                            for model_field in model_instance._meta.fields:
                                                field_name_lower = field_name.lower()
                                                model_field_lower = model_field.name.lower()
                                                
                                                # Check if the model field might be related to the business field
                                                if (field_name_lower in model_field_lower or 
                                                    model_field_lower in field_name_lower or
                                                    'amnt' in model_field_lower):  # Amount fields are often key
                                                    
                                                    try:
                                                        # Get the actual database field object for tracking
                                                        db_table = orchestration.current_populated_tables.get(model_name)
                                                        if db_table and hasattr(db_table, 'table'):
                                                            db_fields = db_table.table.database_fields.filter(name=model_field.name)
                                                            if db_fields.exists():
                                                                db_field = db_fields.first()
                                                                orchestration.track_calculation_used_field(calculation_name, model_field.name, model_instance)
                                                                _tracking_debug(f"    🎯 Precise field: {model_name}.{model_field.name}")
                                                    except Exception as e:
                                                        _tracking_debug(f"    ⚠️ Could not track precise field {model_field.name}: {e}")
                                    
                                    except Exception as e:
                                        _tracking_debug(f"  ❌ Failed to track source model {model_name}: {e}")
                                        
                        except Exception as e:
                            _tracking_debug(f"Could not track field {field_name}: {e}")
                
                # Also track any Django ORM objects that this business object references
                # These are typically the underlying database records
                for attr_name in dir(item):
                    if not attr_name.startswith('_') and not callable(getattr(item, attr_name)):
                        attr_value = getattr(item, attr_name)

                        # Check if this looks like a Django model instance
                        if attr_value and hasattr(attr_value, '_meta') and hasattr(attr_value._meta, 'model'):
                            try:
                                # This is a Django model instance - track it as a database row
                                orchestration.track_calculation_used_row(calculation_name, attr_value)
                                model_class_name = type(attr_value).__name__
                                _tracking_debug(f"Tracked Django model {model_class_name} for {calculation_name}")

                                # IMPORTANT: Also register the attribute name as an alias
                                # The attribute name (e.g., INSTRMNT_RL) may differ from the
                                # Django model class name (e.g., FNNCL_ASST_INSTRMNT_RL)
                                collector = get_collector()
                                table_info = collector.table_name_to_db_id.get(model_class_name)
                                if table_info and attr_name != model_class_name:
                                    # Register alias so dependencies like INSTRMNT_RL.FIELD can resolve
                                    collector.table_name_to_db_id[attr_name] = table_info
                                    _tracking_debug(f"  Registered table alias: {attr_name} -> {model_class_name}")
                                
                                # Also track the fields of this Django object that are accessed
                                model_class_name = type(attr_value).__name__
                                for used_field in used_fields:
                                    # Check if this field might belong to this model
                                    if hasattr(attr_value, used_field):
                                        orchestration.track_calculation_used_field(calculation_name, used_field, attr_value)
                                        _tracking_debug(f"Tracked model field {used_field} on {model_class_name}")

                                        # Register runtime dependency with collector
                                        collector = get_collector()
                                        collector.add_runtime_dependency(calculation_name, model_class_name, used_field)
                            except Exception as e:
                                _tracking_debug(f"Could not track Django model {type(attr_value).__name__}: {e}")
            except Exception as e:
                _tracking_debug(f"Error tracking item in {calculation_name}: {e}")
    
    # Replace the method
    _tracking_debug(f"🔄 Replacing calc_referenced_items method on {calculation_name}")
    cell_instance.calc_referenced_items = smart_wrapped_calc
    _tracking_debug(f"✅ Method replacement complete. New method: {cell_instance.calc_referenced_items}")
    
    return cell_instance
