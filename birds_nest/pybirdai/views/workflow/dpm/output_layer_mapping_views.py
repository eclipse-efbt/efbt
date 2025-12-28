"""
Views for the new Output Layer Mapping Workflow.
This module provides a multi-step workflow for creating output layer mappings
with non-reference combinations, domain management, and cube structure generation.
"""

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.db import transaction, models
from django.forms import formset_factory
import json
import datetime
import logging
import re

logger = logging.getLogger(__name__)

from pybirdai.models.bird_meta_data_model import (
    TABLE, TABLE_CELL, COMBINATION, COMBINATION_ITEM,
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION,
    MAPPING_DEFINITION, VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM,
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, MAPPING_TO_CUBE,
    MAPPING_ORDINATE_LINK,
    DOMAIN, MEMBER, SUBDOMAIN, SUBDOMAIN_ENUMERATION,
    VARIABLE, FRAMEWORK, FRAMEWORK_TABLE, MAINTENANCE_AGENCY,
    MEMBER_HIERARCHY,
    AXIS, AXIS_ORDINATE, CELL_POSITION, ORDINATE_ITEM
)
from pybirdai.process_steps.output_layer_mapping_workflow.mapping_orchestrator import OutputLayerMappingOrchestrator
from pybirdai.process_steps.output_layer_mapping_workflow.combination_creator import CombinationCreator
from pybirdai.process_steps.output_layer_mapping_workflow.domain_manager import DomainManager
from pybirdai.process_steps.output_layer_mapping_workflow.cube_structure_generator import CubeStructureGenerator
from pybirdai.process_steps.output_layer_mapping_workflow.naming_utils import NamingUtils
from pybirdai.process_steps.output_layer_mapping_workflow.reference_table_generator import generate_reference_table_artifacts

# Phase-based transaction handling
from pybirdai.process_steps.output_layer_mapping_workflow.phase_executor import PhaseExecutor
from pybirdai.process_steps.output_layer_mapping_workflow.phases.phase1_base_setup import execute_phase1_base_setup
from pybirdai.process_steps.output_layer_mapping_workflow.phases.phase2_domains_members import execute_phase2_domains_members
from pybirdai.process_steps.output_layer_mapping_workflow.phases.phase3_mappings import execute_phase3_mappings
from pybirdai.process_steps.output_layer_mapping_workflow.phases.phase4_cube_structures import execute_phase4_cube_structures
from pybirdai.process_steps.output_layer_mapping_workflow.phases.phase5_combinations import execute_phase5_combinations

import os
from pathlib import Path


# ============================================================================
# DEBUG EXPORT FUNCTIONALITY
# ============================================================================

def serialize_model_instance(instance):
    """
    Serialize a Django model instance to a JSON-serializable dictionary.
    Handles foreign keys and special field types.
    """
    data = {}

    # Get all fields from the model
    for field in instance._meta.get_fields():
        field_name = field.name

        try:
            # Skip reverse relations
            if field.many_to_many or field.one_to_many:
                continue

            field_value = getattr(instance, field_name, None)

            # Handle foreign keys - store the FK value, not the entire object
            if field.many_to_one or field.one_to_one:
                if field_value:
                    # Get the primary key value of the related object
                    data[field_name] = str(field_value.pk) if hasattr(field_value, 'pk') else str(field_value)
                else:
                    data[field_name] = None
            # Handle datetime fields
            elif isinstance(field_value, (datetime.datetime, datetime.date)):
                data[field_name] = field_value.isoformat()
            # Handle boolean, string, number fields
            else:
                data[field_name] = field_value

        except Exception as e:
            logger.warning(f"Could not serialize field {field_name}: {str(e)}")
            data[field_name] = f"<ERROR: {str(e)}>"

    return data


def export_debug_data(debug_data, export_folder):
    """
    Export collected debug data to JSON files organized by model class.

    Args:
        debug_data: Dict mapping model class names to lists of instances
        export_folder: Path to folder where exports should be saved
    """
    try:
        # Create export folder if it doesn't exist
        export_path = Path(export_folder)
        export_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"[DEBUG EXPORT] Exporting debug data to {export_path}")

        # Export each model class to its own JSON file
        for model_name, instances in debug_data.items():
            if not instances:
                continue

            filename = f"{model_name}.json"
            filepath = export_path / filename

            # Serialize all instances
            serialized_instances = []
            for instance in instances:
                try:
                    serialized = serialize_model_instance(instance)
                    serialized_instances.append(serialized)
                except Exception as e:
                    logger.error(f"[DEBUG EXPORT] Failed to serialize {model_name} instance: {str(e)}")

            # Write to JSON file
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump({
                        'model': model_name,
                        'count': len(serialized_instances),
                        'instances': serialized_instances
                    }, f, indent=2, default=str)

                logger.info(f"[DEBUG EXPORT] Exported {len(serialized_instances)} {model_name} instances to {filename}")
            except Exception as e:
                logger.error(f"[DEBUG EXPORT] Failed to write {filename}: {str(e)}")

        # Create summary file
        summary_path = export_path / "_summary.json"
        summary = {
            'timestamp': datetime.datetime.now().isoformat(),
            'models': {
                model_name: len(instances)
                for model_name, instances in debug_data.items()
            },
            'total_objects': sum(len(instances) for instances in debug_data.values())
        }

        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"[DEBUG EXPORT] Export complete. Total objects: {summary['total_objects']}")
        logger.info(f"[DEBUG EXPORT] Summary: {summary['models']}")

        return True

    except Exception as e:
        logger.error(f"[DEBUG EXPORT] Export failed: {str(e)}")
        return False


# ============================================================================
# END DEBUG EXPORT FUNCTIONALITY
# ============================================================================


def is_valid_variable_for_group_type(variable, group_type):
    """
    Validate if a variable is valid for a given group type based on its domain.

    Args:
        variable: VARIABLE instance
        group_type: str - 'Dimension', 'Observation', or 'Attribute'

    Returns:
        bool: True if variable is valid for the group type, False otherwise

    Validation rules:
        - Dimension: domain.is_enumerated == True
        - Observation: domain code/name/id contains 'Integer', 'EBA_Integer', 'Float', or 'EBA_Float'
        - Attribute: domain code/name/id contains 'String' or 'EBA_String'
    """
    if not variable or not hasattr(variable, 'domain_id') or not variable.domain_id:
        return False

    domain = variable.domain_id

    if group_type == 'Dimension':
        # Dimension variables must have enumerated domains
        return domain.is_enumerated == True

    elif group_type == 'Observation':
        # CRITICAL: Enumerated variables CANNOT be observations
        # This prevents dimension variables from being incorrectly used as measures
        if domain.is_enumerated:
            return False

        # Observation variables must have numeric domains
        numeric_patterns = [
            'Integer', 'EBA_Integer', 'INTGR',
            'Float', 'EBA_Float',
            'Decimal', 'EBA_Decimal',
            'Monetary', 'EBA_Monetary', 'MNTRY'
        ]
        domain_fields = [
            str(domain.code) if domain.code else '',
            str(domain.name) if domain.name else '',
            str(domain.domain_id) if domain.domain_id else '',
            str(domain.data_type) if domain.data_type else ''  # Check the actual data type field
        ]
        return any(
            pattern.lower() in field.lower()  # Case-insensitive matching
            for pattern in numeric_patterns
            for field in domain_fields
        )

    elif group_type == 'Attribute':
        # Attribute variables must have string domains
        string_patterns = ['String', 'EBA_String']
        domain_fields = [
            str(domain.code) if domain.code else '',
            str(domain.name) if domain.name else '',
            str(domain.domain_id) if domain.domain_id else '',
            str(domain.data_type) if domain.data_type else ''  # Check the actual data type field
        ]
        return any(
            pattern.lower() in field.lower()  # Case-insensitive matching
            for pattern in string_patterns
            for field in domain_fields
        )

    return False


def suggest_variable_role(variable):
    """
    Automatically suggest the role (group type) for a variable based on its domain.

    Args:
        variable: VARIABLE instance

    Returns:
        str: 'Dimension', 'Observation', or 'Attribute' (default: 'Dimension')

    Classification rules:
        1. EBA_Float or EBA_Integer domains → Observation
        2. EBA_String domains OR non-enumerated domains → Attribute
        3. Enumerated domains → Dimension
    """
    if not variable or not hasattr(variable, 'domain_id') or not variable.domain_id:
        return 'Dimension'  # Default

    domain = variable.domain_id
    domain_id = domain.domain_id if hasattr(domain, 'domain_id') else str(domain)

    # Rule 1: Numeric domains are observations/measures
    MEASURE_DOMAINS = {
        'EBA_Float', 'EBA_Integer', 'EBA_Decimal', 'EBA_Monetary',
        'EBA_Double', 'EBA_Long'
    }
    if domain_id in MEASURE_DOMAINS:
        return 'Observation'

    # Rule 2a: String domains are attributes
    if domain_id == 'EBA_String':
        return 'Attribute'

    # Rule 2b: Non-enumerated domains are attributes
    if hasattr(domain, 'is_enumerated') and domain.is_enumerated == False:
        return 'Attribute'

    # Rule 3: Enumerated domains are dimensions
    if hasattr(domain, 'is_enumerated') and domain.is_enumerated == True:
        return 'Dimension'

    # Default to dimension for unknown cases
    return 'Dimension'


def get_domain_for_group_type(group_type):
    """
    Get an appropriate domain for creating a new variable based on group_type.

    Args:
        group_type: str - 'dimension', 'observation', or 'attribute'

    Returns:
        DOMAIN object or None if no suitable domain found

    Domain selection rules:
        - observation: Float domain (preferred for metrics)
        - dimension: First enumerated domain found
        - attribute: String domain
    """
    group_type_lower = group_type.lower() if group_type else ''

    if group_type_lower == 'observation':
        # Try Float first, then Integer
        for domain_id in ['Float', 'EBA_Float', 'Integer', 'EBA_Integer']:
            domain = DOMAIN.objects.filter(domain_id=domain_id).first()
            if domain:
                logger.info(f"[STEP 4 VARIABLE CREATION] Selected domain '{domain_id}' for Observation variable")
                return domain
        logger.warning("[STEP 4 VARIABLE CREATION] No Float or Integer domain found for Observation variable")
        return None

    elif group_type_lower == 'dimension':
        # Need an enumerated domain
        domain = DOMAIN.objects.filter(is_enumerated=True).first()
        if domain:
            logger.info(f"[STEP 4 VARIABLE CREATION] Selected enumerated domain '{domain.domain_id}' for Dimension variable")
        else:
            logger.warning("[STEP 4 VARIABLE CREATION] No enumerated domain found for Dimension variable")
        return domain

    elif group_type_lower == 'attribute':
        # Try String domains
        for domain_id in ['String', 'EBA_String']:
            domain = DOMAIN.objects.filter(domain_id=domain_id).first()
            if domain:
                logger.info(f"[STEP 4 VARIABLE CREATION] Selected domain '{domain_id}' for Attribute variable")
                return domain
        logger.warning("[STEP 4 VARIABLE CREATION] No String domain found for Attribute variable")
        return None

    return None


def select_table_for_mapping(request):
    """
    Step 1: Allow user to select a table for mapping.
    Displays dropdowns for Framework, Version, and Table Code.
    """
    if request.method == 'POST':
        # Store selection in session
        request.session['olmw_framework'] = request.POST.get('framework')
        request.session['olmw_version'] = request.POST.get('version')
        request.session['olmw_table_code'] = request.POST.get('table_code')
        request.session['olmw_table_id'] = request.POST.get('table_id')

        # Redirect to check for existing mappings
        return redirect('pybirdai:output_layer_mapping_step2')

    # GET request - show selection form
    # Only pass frameworks; versions and tables will be loaded via AJAX
    frameworks = FRAMEWORK.objects.all().order_by('framework_id')

    context = {
        'frameworks': frameworks,
        'step': 1,
        'total_steps': 7
    }

    return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step1_select_table.html', context)


@transaction.atomic
def delete_mapping_artifacts(mapping_ids):
    """
    Delete all output layer artifacts (CUBE, CUBE_STRUCTURE, COMBINATION, SUBDOMAIN, etc.)
    associated with the given mapping definition IDs.

    Args:
        mapping_ids: List of mapping_definition_id values

    Returns:
        dict: Statistics about what was deleted
    """
    stats = {
        'mapping_to_cube': 0,
        'cube_to_combination': 0,
        'combination_item': 0,
        'combination': 0,
        'subdomain_enumeration': 0,
        'cube_structure_item': 0,
        'subdomain': 0,
        'cube_structure': 0,
        'cube': 0
    }

    logger.info(f"Starting deletion of artifacts for mappings: {mapping_ids}")

    # Step 1: Find all MAPPING_TO_CUBE records for these mappings
    mapping_to_cube_links = MAPPING_TO_CUBE.objects.filter(mapping_id__in=mapping_ids)

    # Collect cube_mapping_id patterns to find related CUBEs
    cube_mapping_ids = set()
    for link in mapping_to_cube_links:
        if link.cube_mapping_id:
            cube_mapping_ids.add(link.cube_mapping_id)

    logger.info(f"Found {len(cube_mapping_ids)} unique cube_mapping_id patterns")

    # Step 2: Find all CUBEs that match these patterns
    # The CUBE.cube_id typically contains the cube_mapping_id as a substring
    cubes_to_delete = set()
    cube_structures_to_delete = set()

    for cube_mapping_id in cube_mapping_ids:
        # Find CUBEs where cube_id contains this pattern
        matching_cubes = CUBE.objects.filter(cube_id__icontains=cube_mapping_id)
        for cube in matching_cubes:
            cubes_to_delete.add(cube.cube_id)
            if cube.cube_structure_id:
                cube_structures_to_delete.add(cube.cube_structure_id.cube_structure_id)

    logger.info(f"Found {len(cubes_to_delete)} CUBEs to delete")
    logger.info(f"Found {len(cube_structures_to_delete)} CUBE_STRUCTUREs to delete")

    # Step 3: COLLECT combination IDs (before deleting anything!)
    combinations_to_delete = set(CUBE_TO_COMBINATION.objects.filter(
        cube_id__in=cubes_to_delete
    ).values_list('combination_id', flat=True).distinct())
    logger.info(f"Found {len(combinations_to_delete)} COMBINATIONs to delete")

    # Step 4: COLLECT subdomain IDs from CUBE_STRUCTURE_ITEM records
    subdomains_to_delete = set(CUBE_STRUCTURE_ITEM.objects.filter(
        cube_structure_id__in=cube_structures_to_delete
    ).values_list('subdomain_id', flat=True).distinct())
    logger.info(f"Found {len(subdomains_to_delete)} SUBDOMAINs to delete")

    # ========== NOW DELETE IN CORRECT ORDER ==========

    # Step 5: Delete CUBE_TO_COMBINATION records
    cube_to_combo_deleted = CUBE_TO_COMBINATION.objects.filter(
        cube_id__in=cubes_to_delete
    ).delete()
    stats['cube_to_combination'] = cube_to_combo_deleted[0]

    # Step 6: Delete COMBINATION_ITEM records
    combination_items_deleted = COMBINATION_ITEM.objects.filter(
        combination_id__in=combinations_to_delete
    ).delete()
    stats['combination_item'] = combination_items_deleted[0]

    # Step 7: Delete COMBINATION records
    combinations_deleted = COMBINATION.objects.filter(
        combination_id__in=combinations_to_delete
    ).delete()
    stats['combination'] = combinations_deleted[0]

    # Step 8: Delete SUBDOMAIN_ENUMERATION records
    subdomain_enum_deleted = SUBDOMAIN_ENUMERATION.objects.filter(
        subdomain_id__in=subdomains_to_delete
    ).delete()
    stats['subdomain_enumeration'] = subdomain_enum_deleted[0]

    # Step 9: Delete CUBE_STRUCTURE_ITEM records
    cube_structure_items_deleted = CUBE_STRUCTURE_ITEM.objects.filter(
        cube_structure_id__in=cube_structures_to_delete
    ).delete()
    stats['cube_structure_item'] = cube_structure_items_deleted[0]

    # Step 10: Delete SUBDOMAIN records
    subdomains_deleted = SUBDOMAIN.objects.filter(
        subdomain_id__in=subdomains_to_delete
    ).delete()
    stats['subdomain'] = subdomains_deleted[0]

    # Step 11: Delete CUBE_STRUCTURE records
    cube_structures_deleted = CUBE_STRUCTURE.objects.filter(
        cube_structure_id__in=cube_structures_to_delete
    ).delete()
    stats['cube_structure'] = cube_structures_deleted[0]

    # Step 12: Delete CUBE records
    cubes_deleted = CUBE.objects.filter(
        cube_id__in=cubes_to_delete
    ).delete()
    stats['cube'] = cubes_deleted[0]

    # NOTE: MAPPING_TO_CUBE records are NOT deleted
    # They must be kept so the generation page can regenerate the output layer

    logger.info(f"Deletion complete. Stats: {stats}")

    return stats


@transaction.atomic
def delete_output_layers_by_table(table_id):
    """
    Delete all output layer artifacts (CUBE, CUBE_STRUCTURE, COMBINATION, etc.)
    for a specific table_id. This is used before regenerating output layers.

    Args:
        table_id: str - The table_id (e.g., 'EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx1')

    Returns:
        dict: Statistics about what was deleted
    """
    stats = {
        'cube_to_combination': 0,
        'combination_item': 0,
        'table_cell_cleared': 0,
        'combination': 0,
        'subdomain_enumeration': 0,
        'cube_structure_item': 0,
        'subdomain': 0,
        'cube_structure': 0,
        'cube': 0,
        'mapping_to_cube': 0
    }

    logger.info(f"[DELETE_OUTPUT_LAYERS] Starting deletion for table: {table_id}")

    # Step 1: Find CUBE_STRUCTURE matching this table_id
    # CUBE_STRUCTURE ID pattern: {table_id}_STRUCTURE
    cube_structure_id = f"{table_id}_STRUCTURE"
    cube_structures_to_delete = list(CUBE_STRUCTURE.objects.filter(
        cube_structure_id=cube_structure_id
    ).values_list('cube_structure_id', flat=True))

    if not cube_structures_to_delete:
        logger.info(f"[DELETE_OUTPUT_LAYERS] No existing CUBE_STRUCTURE found for {cube_structure_id}")
        return stats

    logger.info(f"[DELETE_OUTPUT_LAYERS] Found {len(cube_structures_to_delete)} CUBE_STRUCTURE(s) to delete")

    # Step 2: Find CUBEs linked to this cube structure
    cubes_to_delete = list(CUBE.objects.filter(
        cube_structure_id__in=cube_structures_to_delete
    ).values_list('cube_id', flat=True))
    logger.info(f"[DELETE_OUTPUT_LAYERS] Found {len(cubes_to_delete)} CUBE(s) to delete")

    # Step 3: Collect combination IDs (before deleting anything!)
    combinations_to_delete = list(CUBE_TO_COMBINATION.objects.filter(
        cube_id__in=cubes_to_delete
    ).values_list('combination_id', flat=True).distinct())
    logger.info(f"[DELETE_OUTPUT_LAYERS] Found {len(combinations_to_delete)} COMBINATION(s) to delete")

    # Step 4: Collect subdomain IDs from CUBE_STRUCTURE_ITEM records
    subdomains_to_delete = list(CUBE_STRUCTURE_ITEM.objects.filter(
        cube_structure_id__in=cube_structures_to_delete
    ).values_list('subdomain_id', flat=True).distinct())
    # Filter out None values
    subdomains_to_delete = [s for s in subdomains_to_delete if s]
    logger.info(f"[DELETE_OUTPUT_LAYERS] Found {len(subdomains_to_delete)} SUBDOMAIN(s) to delete")

    # ========== DELETE IN CORRECT ORDER (child tables first) ==========

    # Step 5: Delete CUBE_TO_COMBINATION records
    cube_to_combo_deleted = CUBE_TO_COMBINATION.objects.filter(
        cube_id__in=cubes_to_delete
    ).delete()
    stats['cube_to_combination'] = cube_to_combo_deleted[0]

    # Step 6: Delete COMBINATION_ITEM records
    combination_items_deleted = COMBINATION_ITEM.objects.filter(
        combination_id__in=combinations_to_delete
    ).delete()
    stats['combination_item'] = combination_items_deleted[0]

    # Step 6.5: Clear TABLE_CELL.table_cell_combination_id references
    # This prevents orphaned references to deleted combinations
    table_cells_cleared = TABLE_CELL.objects.filter(
        table_cell_combination_id__in=combinations_to_delete
    ).update(table_cell_combination_id=None)
    stats['table_cell_cleared'] = table_cells_cleared
    logger.info(f"[DELETE_OUTPUT_LAYERS] Cleared {table_cells_cleared} TABLE_CELL combination references")

    # Step 7: Delete COMBINATION records
    combinations_deleted = COMBINATION.objects.filter(
        combination_id__in=combinations_to_delete
    ).delete()
    stats['combination'] = combinations_deleted[0]

    # Step 8: Delete SUBDOMAIN_ENUMERATION records
    subdomain_enum_deleted = SUBDOMAIN_ENUMERATION.objects.filter(
        subdomain_id__in=subdomains_to_delete
    ).delete()
    stats['subdomain_enumeration'] = subdomain_enum_deleted[0]

    # Step 9: Delete CUBE_STRUCTURE_ITEM records
    cube_structure_items_deleted = CUBE_STRUCTURE_ITEM.objects.filter(
        cube_structure_id__in=cube_structures_to_delete
    ).delete()
    stats['cube_structure_item'] = cube_structure_items_deleted[0]

    # Step 10: Delete SUBDOMAIN records
    subdomains_deleted = SUBDOMAIN.objects.filter(
        subdomain_id__in=subdomains_to_delete
    ).delete()
    stats['subdomain'] = subdomains_deleted[0]

    # Step 11: Delete CUBE records
    cubes_deleted = CUBE.objects.filter(
        cube_id__in=cubes_to_delete
    ).delete()
    stats['cube'] = cubes_deleted[0]

    # Step 12: Delete CUBE_STRUCTURE records
    cube_structures_deleted = CUBE_STRUCTURE.objects.filter(
        cube_structure_id__in=cube_structures_to_delete
    ).delete()
    stats['cube_structure'] = cube_structures_deleted[0]

    # Step 13: Delete MAPPING_TO_CUBE records for this table
    # Extract table code pattern from table_id for matching
    # table_id format: EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx1
    # We want to match cube_mapping_id like: M_C_07_00_a_..._REF_EBA_COREP_4_0
    # Extract the table code portion (e.g., C_07_00_a) from table_id
    # Pattern: after framework prefix, before version
    table_code_match = re.search(r'EBA_\w+_([A-Z]_[\d_]+_[a-z]?)_', table_id)
    if table_code_match:
        table_code_pattern = table_code_match.group(1)
        # Delete MAPPING_TO_CUBE records that match this table code
        mapping_to_cube_deleted = MAPPING_TO_CUBE.objects.filter(
            cube_mapping_id__icontains=f"M_{table_code_pattern}"
        ).delete()
        stats['mapping_to_cube'] = mapping_to_cube_deleted[0]
        logger.info(f"[DELETE_OUTPUT_LAYERS] Deleted {mapping_to_cube_deleted[0]} MAPPING_TO_CUBE records matching M_{table_code_pattern}")

    logger.info(f"[DELETE_OUTPUT_LAYERS] Deletion complete. Stats: {stats}")
    return stats


def detect_mapping_conflicts(table_code, version, all_mappings, variable_groups):
    """
    Detect if any VARIABLE_MAPPING IDs would conflict with existing records.
    This happens when users try to regenerate mappings with the same naming.

    Args:
        table_code: str - The table code (e.g., "F_01_01")
        version: str - The version string (e.g., "3.0")
        all_mappings: dict - Mapping data from session
        variable_groups: dict - Variable groups from session

    Returns:
        list: List of conflict dictionaries with:
            - variable_mapping_id: The conflicting ID
            - mapping_name: Name of the conflicting mapping
            - source_variables: List of source variable IDs
            - target_variables: List of target variable IDs
    """
    conflicts = []

    # Generate the mapping prefix
    version_normalized = version.replace('.', '_')
    mapping_prefix = f"{table_code}_{version_normalized}_MAP"

    # Calculate starting sequence number
    existing_count = MAPPING_DEFINITION.objects.filter(
        code__startswith=mapping_prefix
    ).count()
    mapping_sequence_start = existing_count + 1

    # Check each mapping for conflicts
    mapping_counter = 0
    for group_id, mapping_data in all_mappings.items():
        mapping_name = mapping_data['mapping_name']
        internal_id = mapping_data['internal_id']

        # Calculate the variable_mapping_id that would be generated
        current_sequence = mapping_sequence_start + mapping_counter
        mapping_id_suffix = f"{current_sequence:03d}"
        variable_mapping_id = f"{mapping_prefix}_{mapping_id_suffix}_VAR"
        mapping_counter += 1

        # Check if this VARIABLE_MAPPING already exists
        if VARIABLE_MAPPING.objects.filter(variable_mapping_id=variable_mapping_id).exists():
            # Get the group info to extract variables
            group_info = variable_groups.get(group_id, {})
            source_var_ids = group_info.get('variable_ids', [])
            target_var_ids = group_info.get('targets', [])

            # Get variable names for better display
            source_vars = list(VARIABLE.objects.filter(variable_id__in=source_var_ids).values('variable_id', 'name'))
            target_vars = list(VARIABLE.objects.filter(variable_id__in=target_var_ids).values('variable_id', 'name'))

            conflicts.append({
                'variable_mapping_id': variable_mapping_id,
                'mapping_name': mapping_name,
                'internal_id': internal_id,
                'group_type': mapping_data.get('group_type', 'dimension'),
                'source_variables': source_vars,
                'target_variables': target_vars
            })

    return conflicts


def delete_conflicting_mappings(conflict_ids):
    """
    Delete VARIABLE_MAPPING and related VARIABLE_MAPPING_ITEM records for conflicting IDs.

    Args:
        conflict_ids: List of variable_mapping_id strings to delete

    Returns:
        dict: Statistics about what was deleted
    """
    stats = {
        'variable_mapping_items': 0,
        'variable_mappings': 0,
        'member_mapping_items': 0,
        'member_mappings': 0,
        'mapping_definitions': 0
    }

    logger.info(f"Deleting conflicting mappings: {conflict_ids}")

    # Step 1: Find VARIABLE_MAPPING records
    variable_mappings = VARIABLE_MAPPING.objects.filter(variable_mapping_id__in=conflict_ids)

    # Step 2: Delete VARIABLE_MAPPING_ITEM records first (foreign key dependency)
    for vm in variable_mappings:
        items_deleted = VARIABLE_MAPPING_ITEM.objects.filter(variable_mapping_id=vm).delete()
        stats['variable_mapping_items'] += items_deleted[0] if items_deleted[0] else 0

    # Step 3: Find MAPPING_DEFINITION records that reference these VARIABLE_MAPPING records
    mapping_definitions = MAPPING_DEFINITION.objects.filter(variable_mapping_id__in=variable_mappings)

    # Step 4: Delete MEMBER_MAPPING_ITEM and MEMBER_MAPPING records associated with these MAPPING_DEFINITIONs
    for mapping_def in mapping_definitions:
        if mapping_def.member_mapping_id:
            # Delete MEMBER_MAPPING_ITEM records first
            member_items_deleted = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping_def.member_mapping_id
            ).delete()
            stats['member_mapping_items'] += member_items_deleted[0] if member_items_deleted[0] else 0

            # Delete MEMBER_MAPPING record
            member_mapping_deleted = MEMBER_MAPPING.objects.filter(
                member_mapping_id=mapping_def.member_mapping_id.member_mapping_id
            ).delete()
            stats['member_mappings'] += member_mapping_deleted[0] if member_mapping_deleted[0] else 0

    # Step 5: Delete MAPPING_DEFINITION records
    mapping_defs_deleted = mapping_definitions.delete()
    stats['mapping_definitions'] = mapping_defs_deleted[0] if mapping_defs_deleted[0] else 0

    # Step 6: Delete VARIABLE_MAPPING records
    variable_mappings_deleted = variable_mappings.delete()
    stats['variable_mappings'] = variable_mappings_deleted[0] if variable_mappings_deleted[0] else 0

    logger.info(f"Conflict deletion complete. Stats: {stats}")

    return stats


def check_existing_mappings(request):
    """
    Step 1.5: Check for existing mappings for the selected table.
    Allows user to choose between using existing mapping, creating new, or modifying existing.
    Uses string matching on MAPPING_TO_CUBE.cube_mapping_id based on table code and version.
    """
    # Check if table was selected in previous step
    if 'olmw_table_id' not in request.session:
        messages.error(request, 'Please select a table first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    try:
        table = TABLE.objects.get(table_id=table_id)
    except TABLE.DoesNotExist:
        messages.error(request, 'Selected table not found.')
        return redirect('pybirdai:output_layer_mapping_step1')

    # Query for existing mappings related to this table using string matching on cube_mapping_id
    # Use table.code to construct search patterns that match both old and new formats

    # Extract base table code (without Z-axis suffix) for pattern matching
    # This ensures we find mappings for the core table regardless of Z-axis member
    from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
        extract_base_table_code
    )
    base_table_code = extract_base_table_code(table.table_id, table.code)

    # Build two normalization patterns:
    # - Partial: spaces replaced with underscores (old format: C_07.00.a)
    # - Full: spaces AND dots replaced with underscores (new format: C_07_00_a)
    table_code_partial = base_table_code.replace(" ", "_")  # Old format: C_07.00.a
    table_code_full = base_table_code.replace(" ", "_").replace(".", "_")  # New format: C_07_00_a

    # Build flexible search using Q objects to match multiple patterns:
    # - Old workflow format: M_C_07.00.a_..._REF_EBA_COREP_4_0
    # - New workflow format: M_C_07_00_a_..._REF_EBA_COREP_4_0
    # - DPM format: M_EBA_COREP_C_07_00_a_4_0_..._REF_EBA_COREP_4_0
    mapping_query = MAPPING_TO_CUBE.objects.filter(
        models.Q(cube_mapping_id__icontains=f"M_{table_code_partial}_") |  # Old workflow format
        models.Q(cube_mapping_id__icontains=f"M_{table_code_full}_") |  # New workflow format
        models.Q(cube_mapping_id__icontains=f"_{table_code_full}_")  # DPM format with framework prefix
    )
    logging.info(f"Searching for mappings with patterns: M_{table_code_partial}_, M_{table_code_full}_, _{table_code_full}_")
    logging.info(f"Original table code: {table.code}, Base table code: {base_table_code}")

    # # Add version filter if available (version format: "3.0" may appear as "3_0" or "3.0" in cube_mapping_id)
    # if table.version:
    #     # Try both formats: with dot and with underscore
    #     version_patterns = [
    #         table.version,  # e.g., "3.0"
    #         table.version.replace('.', '_'),  # e.g., "3_0"
    #         table.version.replace('.', '')  # e.g., "30"
    #     ]

    #     # Build Q object for OR condition on version patterns
    #     version_q = models.Q()
    #     for pattern in version_patterns:
    #         version_q |= models.Q(cube_mapping_id__icontains=pattern)

    #     mapping_query = mapping_query.filter(version_q)

    # Get mapping IDs
    mapping_to_cube_links = mapping_query.values_list('mapping_id', flat=True).distinct()

    # Get the actual mapping definitions
    existing_mappings = MAPPING_DEFINITION.objects.filter(
        mapping_id__in=mapping_to_cube_links
    ).distinct().select_related('variable_mapping_id', 'member_mapping_id')

    if request.method == 'POST':
        mapping_mode = request.POST.get('mapping_mode', 'new')
        request.session['olmw_mapping_mode'] = mapping_mode

        if mapping_mode == 'use_existing':
            # User wants to use an existing mapping as-is
            existing_mapping_id = request.POST.get('existing_mapping_id')
            if not existing_mapping_id:
                messages.error(request, 'Please select a mapping to use.')
                return redirect('pybirdai:output_layer_mapping_step2')

            request.session['olmw_existing_mapping_id'] = existing_mapping_id
            messages.success(request, f'Using existing mapping: {MAPPING_DEFINITION.objects.get(mapping_id=existing_mapping_id).name}')
            # Skip to confirmation
            return redirect('pybirdai:output_layer_mapping_step7')

        elif mapping_mode == 'modify_existing':
            # User wants to modify an existing mapping
            existing_mapping_id = request.POST.get('existing_mapping_id')
            if not existing_mapping_id:
                messages.error(request, 'Please select a mapping to modify.')
                return redirect('pybirdai:output_layer_mapping_step2')

            request.session['olmw_existing_mapping_id'] = existing_mapping_id
            # Load existing mapping data into session for modification
            _load_existing_mapping_to_session(request, existing_mapping_id)
            messages.info(request, 'Existing mapping loaded. You can now modify it.')
            return redirect('pybirdai:output_layer_mapping_step3')

        elif mapping_mode == 'regenerate_all':
            # User wants to delete all existing artifacts and regenerate from all mappings
            if not existing_mappings:
                messages.error(request, 'No existing mappings found to regenerate.')
                return redirect('pybirdai:output_layer_mapping_step2')

            # Collect all mapping IDs
            all_mapping_ids = [m.mapping_id for m in existing_mappings]

            # Don't delete here - deletion will happen in Step 7 POST when user confirms
            # Load all mappings' data into session for regeneration
            _load_all_mappings_to_session(request, all_mapping_ids)

            # Set regeneration mode flag so step 7 knows to use existing mappings
            request.session['olmw_regenerate_mode'] = True
            request.session['olmw_existing_mapping_ids'] = all_mapping_ids

            messages.info(
                request,
                f"Ready to regenerate from {len(all_mapping_ids)} mapping(s). "
                f"Confirm in the next step to delete old structures and regenerate."
            )

            # Proceed directly to step 7 (generation)
            return redirect('pybirdai:output_layer_mapping_step7')

        else:  # mapping_mode == 'new'
            # User wants to create a completely new mapping
            # Check if existing mappings should be deleted when generating new ones
            delete_existing = request.POST.get('delete_existing_on_generate') == 'true'
            if delete_existing and existing_mappings:
                # Store existing mapping IDs for deletion in Step 7
                existing_ids = [m.mapping_id for m in existing_mappings]
                request.session['olmw_delete_existing_mapping_ids'] = existing_ids
                logger.info(f"[STEP 2] Will delete {len(existing_ids)} existing mapping(s) when generating new ones")
            else:
                # Clear any previous deletion flag
                request.session.pop('olmw_delete_existing_mapping_ids', None)

            messages.info(request, 'Creating new mapping from scratch.')
            return redirect('pybirdai:output_layer_mapping_step3')

    # Prepare context for template
    mapping_details = []
    for mapping in existing_mappings:
        details = {
            'mapping': mapping,
            'variable_count': 0,
            'member_count': 0,
            'created_date': mapping.maintenance_agency_id.name if mapping.maintenance_agency_id else 'Unknown',
        }

        if mapping.variable_mapping_id:
            details['variable_count'] = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id=mapping.variable_mapping_id
            ).count()

        if mapping.member_mapping_id:
            details['member_count'] = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping.member_mapping_id
            ).count()

        # Find all MAPPING_TO_CUBE records that use this mapping
        # This helps identify if the mapping is shared across multiple table variants
        mtc_records = mapping_query.filter(mapping_id=mapping)
        cube_mapping_ids = list(mtc_records.values_list('cube_mapping_id', flat=True))
        details['cube_mapping_ids'] = cube_mapping_ids
        details['variant_count'] = len(cube_mapping_ids)

        # Extract Z-axis variant info from cube_mapping_ids
        z_axis_variants = []
        for cmi in cube_mapping_ids:
            # Extract Z-axis member from patterns like:
            # - "C_07.00.a_EBA_qEC_EBA_qx2029_4_0_MAP_001" -> "qx2029"
            # - "EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx1_REF_EBA_COREP_4_0" -> "qx1"
            import re
            z_match = re.search(r'_EBA_q[A-Z]_EBA_(q[a-z]\d+)', cmi)
            if z_match:
                z_axis_variants.append(z_match.group(1))

        details['z_axis_variants'] = z_axis_variants if z_axis_variants else None

        logging.info(
            f"Mapping {mapping.mapping_id}: {details['variant_count']} variant(s), "
            f"{details['variable_count']} variables, {details['member_count']} members"
        )
        if z_axis_variants:
            logging.info(f"  Z-axis variants: {', '.join(z_axis_variants)}")

        mapping_details.append(details)

    # Get Z-axis sibling information for deduplicated tables
    from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
        is_deduplicated_table,
        get_z_axis_sibling_tables
    )

    table_is_deduplicated = is_deduplicated_table(table_id)
    z_axis_siblings = []
    if table_is_deduplicated:
        siblings = get_z_axis_sibling_tables(table_id)
        # Extract table_id integers for JavaScript (not TABLE objects)
        z_axis_siblings = [s.table_id for s in siblings]

    context = {
        'table': table,
        'existing_mappings': mapping_details,
        'has_existing': len(mapping_details) > 0,
        'step': 2,
        'total_steps': 7,
        # Z-axis variant info
        'is_deduplicated': table_is_deduplicated,
        'z_axis_siblings': z_axis_siblings
    }

    return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step2_existing_mappings.html', context)


def _load_existing_mapping_to_session(request, mapping_id):
    """
    Helper function to load existing mapping data into session for modification.
    Uses same logic as _load_mapping_data_for_batch_edit() but saves to session.
    """
    try:
        mapping = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)

        variable_groups = {}
        multi_mappings = {}

        # Load variable mappings
        if mapping.variable_mapping_id:
            var_items = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id=mapping.variable_mapping_id
            ).select_related('variable_id')

            # Group by source/target using is_source field
            source_vars = []
            target_vars = []
            for item in var_items:
                if item.variable_id:
                    var_id = item.variable_id.variable_id
                    if item.is_source == 'true':
                        source_vars.append(var_id)
                    else:
                        target_vars.append(var_id)

            # Map database codes to template-expected names
            mapping_type_map = {'E': 'Dimension', 'O': 'Observation', 'A': 'Attribute'}
            group_type = mapping_type_map.get(mapping.mapping_type, 'Dimension')

            # Create a variable group with proper structure
            group_id = f"group_{mapping.mapping_id}"
            variable_groups[group_id] = {
                'name': mapping.name,
                'variable_ids': source_vars,
                'targets': target_vars,
                'mapping_type': 'direct',
                'group_type': group_type
            }

        # Load member mappings
        if mapping.member_mapping_id:
            member_items = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping.member_mapping_id
            ).select_related('variable_id', 'member_id').order_by('member_mapping_row')

            # Group by row to reconstruct dimension combinations
            rows = {}
            for item in member_items:
                row_key = item.member_mapping_row or '1'
                if row_key not in rows:
                    rows[row_key] = {}
                if item.variable_id and item.member_id:
                    rows[row_key][item.variable_id.variable_id] = item.member_id.member_id

            # Map database codes to template-expected names
            mapping_type_map = {'E': 'Dimension', 'O': 'Observation', 'A': 'Attribute'}
            group_type = mapping_type_map.get(mapping.mapping_type, 'Dimension')

            # Create multi_mappings entry
            group_id = f"group_{mapping.mapping_id}"
            multi_mappings[group_id] = {
                'mapping_name': mapping.name,
                'internal_id': mapping.code or mapping.mapping_id,
                'group_type': group_type,
                'dimensions': list(rows.values()),
                'observations': {},
                'attributes': {}
            }

        # Save to correct session keys
        request.session['olmw_variable_groups'] = json.dumps(variable_groups)
        request.session['olmw_multi_mappings'] = json.dumps(multi_mappings)

        logger.info(f"Loaded existing mapping {mapping_id} with {len(variable_groups)} variable group(s)")

    except MAPPING_DEFINITION.DoesNotExist:
        messages.error(request, 'Mapping not found.')


def _load_all_mappings_to_session(request, mapping_ids):
    """
    Helper function to load all mappings' data into session for regeneration.
    This aggregates all variable and member mappings from all provided mapping IDs.

    Args:
        request: Django request object
        mapping_ids: List of mapping_definition_id values
    """
    try:
        all_breakdowns = {}
        all_member_mappings = {}

        for mapping_id in mapping_ids:
            try:
                mapping = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)

                # Load variable mappings
                if mapping.variable_mapping_id:
                    var_items = VARIABLE_MAPPING_ITEM.objects.filter(
                        variable_mapping_id=mapping.variable_mapping_id
                    )
                    for item in var_items:
                        if item.variable_id:
                            var_key = str(item.variable_id.variable_id)
                            # Only add if not already present (first mapping wins)
                            if var_key not in all_breakdowns:
                                all_breakdowns[var_key] = {
                                    'source': var_key,
                                    'rule': 'direct',
                                    'mapping_id': mapping_id
                                }

                # Load member mappings
                if mapping.member_mapping_id:
                    member_items = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mapping.member_mapping_id
                    )
                    for item in member_items:
                        if item.member_id:
                            member_key = str(item.member_id.member_id)
                            if member_key not in all_member_mappings:
                                all_member_mappings[member_key] = {
                                    'source': member_key,
                                    'mapping_id': mapping_id
                                }

            except MAPPING_DEFINITION.DoesNotExist:
                logger.warning(f"Mapping {mapping_id} not found during bulk load")
                continue

        # Store aggregated mappings in session
        request.session['olmw_breakdowns'] = json.dumps(all_breakdowns)
        request.session['olmw_member_mappings'] = json.dumps(all_member_mappings)
        request.session['olmw_regenerate_all_mapping_ids'] = mapping_ids

        logger.info(f"Loaded {len(all_breakdowns)} variable mappings and {len(all_member_mappings)} member mappings from {len(mapping_ids)} mappings")

    except Exception as e:
        logger.error(f"Error loading mappings to session: {str(e)}", exc_info=True)
        raise


def define_variable_breakdown(request):
    """
    Step 2.5: Allow user to define the breakdown of source variables.
    For each dimension in the selected table, show current domain/members
    and allow selection of source variables and transformation rules.
    This step requires ordinates to be selected first (in Step 2).
    """
    # Check if we have table selection in session
    if 'olmw_table_id' not in request.session:
        messages.error(request, 'Please select a table first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    table = TABLE.objects.get(table_id=table_id)

    # Check if ordinates have been selected (required from Step 3)
    if 'olmw_selected_ordinates' not in request.session:
        messages.warning(request, 'Please select ordinates first to determine which dimensions to map.')
        return redirect('pybirdai:output_layer_mapping_step3')

    selected_ordinates = request.session.get('olmw_selected_ordinates', [])
    logging.info(f"[Step 4] Table: {table_id}")
    logging.info(f"[Step 4] Selected ordinates count: {len(selected_ordinates)}")

    # If ordinates were selected, filter variables to only those from selected ordinates
    if selected_ordinates:
        from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM

        # Get variables directly from selected ordinates
        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('variable_id', 'member_id')

        logging.info(f"[Step 4] Found {ordinate_items.count()} ORDINATE_ITEM records for selected ordinates")
        logging.info(f"[Step 4] Ordinate items with variables: {ordinate_items.filter(variable_id__isnull=False).count()}")

        # Build variables dict from ordinate items
        variables = {}
        for item in ordinate_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if var_id not in variables:
                    variables[var_id] = {
                        'variable': item.variable_id,
                        'domain': item.variable_id.domain_id if hasattr(item.variable_id, 'domain_id') else None,
                        'members': set(),
                        'subdomain': None,  # ORDINATE_ITEM doesn't have subdomain_id
                        'suggested_role': suggest_variable_role(item.variable_id)
                    }
                if item.member_id:
                    variables[var_id]['members'].add(item.member_id)

        logging.info(f"[Step 4] Built variables dict from ordinates: {len(variables)} unique variables")
        if len(variables) == 0:
            logging.warning(
                f"[Step 4] WARNING: No variables found in ORDINATE_ITEM records! "
                f"This likely means ORDINATE_ITEM records were not created during DPM import. "
                f"Falling back to TABLE_CELL → COMBINATION method..."
            )
    else:
        logging.info("[Step 4] No ordinates selected, using fallback TABLE_CELL → COMBINATION method")

    # Fallback: Get all variables from table combinations if no variables from ordinates
    if not variables:
        # Get table cells with their combinations
        cells = TABLE_CELL.objects.filter(table_id=table).select_related('table_id', 'table_cell_combination_id')

        # Get unique combinations from cells
        combination_ids = cells.values_list('table_cell_combination_id', flat=True).distinct()
        combinations = COMBINATION.objects.filter(combination_id__in=combination_ids)

        # Get unique variables from combination items
        combination_items = COMBINATION_ITEM.objects.filter(
            combination_id__in=combinations
        ).select_related('variable_id', 'member_id', 'subdomain_id')

        # Group by variable for display
        variables = {}
        for item in combination_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if var_id not in variables:
                    variables[var_id] = {
                        'variable': item.variable_id,
                        'domain': item.variable_id.domain_id if hasattr(item.variable_id, 'domain_id') else None,
                        'members': set(),
                        'subdomain': item.subdomain_id,
                        'suggested_role': suggest_variable_role(item.variable_id)
                    }
                if item.member_id:
                    variables[var_id]['members'].add(item.member_id)

        logging.info(f"[Step 4] Fallback completed: {len(variables)} variables from TABLE_CELL → COMBINATION")
        logging.info(f"[Step 4] Found {cells.count()} table cells, {len(combination_ids)} unique combinations")

    if request.method == 'POST':
        # Process and store variable groups
        groups = {}
        # Extract all group IDs from POST data
        group_ids = set()
        for key in request.POST.keys():
            if key.startswith('group_') and key.endswith('_name'):
                # Extract group ID from key like "group_1_name"
                group_id = key.replace('_name', '')
                group_ids.add(group_id)

        # Build groups dictionary
        for group_id in group_ids:
            group_name = request.POST.get(f'{group_id}_name', '')
            group_variables = request.POST.get(f'{group_id}_variables', '')
            group_mapping_type = request.POST.get(f'{group_id}_mapping_type', 'many_to_one')
            group_targets = request.POST.get(f'{group_id}_targets', '')
            group_type = request.POST.get(f'{group_id}_type', None)  # Can be None, 'dimension', or 'observation'

            # Parse comma-separated lists
            variable_ids = [v.strip() for v in group_variables.split(',') if v.strip()]
            target_ids = [t.strip() for t in group_targets.split(',') if t.strip()]

            if variable_ids:  # Only store non-empty groups
                groups[group_id] = {
                    'name': group_name,
                    'variable_ids': variable_ids,
                    'mapping_type': group_mapping_type,
                    'targets': target_ids,
                    'group_type': group_type  # None if not set, or 'dimension'/'observation'
                }

        # Validate: Check for duplicate source variables across groups
        all_variables = []
        duplicate_vars = []
        for group_id, group_data in groups.items():
            for var_id in group_data['variable_ids']:
                if var_id in all_variables:
                    # Find which group already has this variable
                    for other_group_id, other_group_data in groups.items():
                        if other_group_id != group_id and var_id in other_group_data['variable_ids']:
                            duplicate_vars.append({
                                'var_id': var_id,
                                'group1': other_group_data['name'],
                                'group2': group_data['name']
                            })
                            break
                else:
                    all_variables.append(var_id)

        if duplicate_vars:
            # Build error message
            error_details = []
            for dup in duplicate_vars:
                error_details.append(
                    f"Variable '{dup['var_id']}' appears in both '{dup['group1']}' and '{dup['group2']}'"
                )
            error_message = "Error: The following variable(s) are selected in multiple groups: " + "; ".join(error_details)
            messages.error(request, error_message)
            # Re-render form with error (don't store invalid data)
            all_variables = VARIABLE.objects.all().order_by('name')
            target_variables = VARIABLE.objects.exclude(
                variable_id__contains='EBA_'
            ).exclude(
                variable_id__regex=r'[a-z]'
            ).order_by('name')
            context = {
                'table': table,
                'variables': variables,
                'all_variables': all_variables,
                'target_variables': target_variables,
                'step': 4,
                'total_steps': 7
            }
            return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step4_variable_breakdown.html', context)

        # ========== DOMAIN VALIDATION: REMOVED - Frontend already validates comprehensively ==========
        # The frontend (step4_variable_breakdown.html) implements comprehensive domain validation:
        # - Filters source variable dropdowns by group type (Dimension/Observation/Attribute)
        # - Auto-suggests group type based on first variable's domain
        # - Prevents selection of incompatible variables via isValidVariableForGroupType()
        # - Refreshes dropdowns when group type changes, removing invalid selections
        # Backend validation is redundant and caused false positives (e.g., EBA_Float rejection)
        # If malicious POST manipulation is a concern, add security logging instead of business logic
        # ========== END DOMAIN VALIDATION ==========

        # ========== TARGET VARIABLE CREATION: Create missing target variables ==========
        variables_created = []
        variables_skipped = []

        for group_id, group_data in groups.items():
            group_type = group_data.get('group_type')
            group_name = group_data.get('name', group_id)
            target_ids = group_data.get('targets', [])

            if not group_type or not target_ids:
                continue  # Skip if no group type or no targets

            for target_var_id in target_ids:
                # Check if target variable already exists
                if VARIABLE.objects.filter(variable_id=target_var_id).exists():
                    logger.info(f"[STEP 4 VARIABLE CREATION] Variable '{target_var_id}' already exists")
                    continue

                # Variable doesn't exist - create it
                logger.info(f"[STEP 4 VARIABLE CREATION] Creating variable '{target_var_id}' for group '{group_name}' (type: {group_type})")

                # Get appropriate domain for this group type
                domain = get_domain_for_group_type(group_type)

                if not domain:
                    error_msg = (
                        f"Cannot create target variable '{target_var_id}' for group '{group_name}': "
                        f"No suitable domain found for group type '{group_type}'"
                    )
                    logger.error(f"[STEP 4 VARIABLE CREATION ERROR] {error_msg}")
                    variables_skipped.append({
                        'var_id': target_var_id,
                        'group_name': group_name,
                        'group_type': group_type,
                        'reason': f"No suitable domain found for {group_type}"
                    })
                    continue

                # Get or create maintenance agency for system-created variables
                maintenance_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
                    maintenance_agency_id='PYBIRDAI',
                    defaults={
                        'name': 'PyBIRD AI System',
                        'description': 'System-created maintenance agency for auto-generated variables'
                    }
                )

                try:
                    # Create the variable
                    new_variable = VARIABLE.objects.create(
                        variable_id=target_var_id,
                        name=target_var_id,  # Use variable_id as name (can be customized later)
                        domain_id=domain,
                        maintenance_agency_id=maintenance_agency
                    )

                    logger.info(
                        f"[STEP 4 VARIABLE CREATION SUCCESS] Created variable '{target_var_id}' "
                        f"with domain '{domain.domain_id}' for {group_type} group '{group_name}'"
                    )

                    variables_created.append({
                        'var_id': target_var_id,
                        'group_name': group_name,
                        'group_type': group_type,
                        'domain': domain.domain_id
                    })

                except Exception as e:
                    error_msg = f"Failed to create variable '{target_var_id}': {str(e)}"
                    logger.error(f"[STEP 4 VARIABLE CREATION ERROR] {error_msg}", exc_info=True)
                    variables_skipped.append({
                        'var_id': target_var_id,
                        'group_name': group_name,
                        'group_type': group_type,
                        'reason': str(e)
                    })

        # Report results
        if variables_created:
            created_summary = "\n".join([
                f"  ✓ {v['var_id']} ({v['group_type']}, domain: {v['domain']}) in group '{v['group_name']}'"
                for v in variables_created
            ])
            logger.info(f"\n[STEP 4 VARIABLE CREATION] Created {len(variables_created)} new variable(s):\n{created_summary}\n")

            success_message = (
                f"✅ Created {len(variables_created)} new target variable(s) in the database. "
                f"These variables are now available for mapping."
            )
            messages.success(request, success_message)

        if variables_skipped:
            skipped_summary = "\n".join([
                f"  ✗ {v['var_id']} in group '{v['group_name']}' ({v['group_type']}): {v['reason']}"
                for v in variables_skipped
            ])
            logger.warning(f"\n[STEP 4 VARIABLE CREATION] Skipped {len(variables_skipped)} variable(s):\n{skipped_summary}\n")

            error_message = (
                f"⚠️ Could not create {len(variables_skipped)} target variable(s). "
                f"See logs for details. The workflow may fail in Step 7 if these variables are required."
            )
            messages.warning(request, error_message)
        # ========== END TARGET VARIABLE CREATION ==========

        # ========== STRICT VALIDATION: ALL groups MUST have group_type set ==========
        groups_missing_type = []
        for group_id, group_data in groups.items():
            if not group_data.get('group_type'):
                groups_missing_type.append(group_data.get('name', group_id))

        if groups_missing_type:
            # Reject submission - user MUST select Dimension, Observation, or Attribute for ALL groups
            error_message = (
                f"❌ Cannot proceed to Step 5. The following variable group(s) are missing "
                f"variable type selection: {', '.join(groups_missing_type)}. "
                f"Please select either 'Dimension', 'Observation', or 'Attribute' for each group before submitting."
            )
            messages.error(request, error_message)

            # Re-render form with error (don't store invalid data)
            all_variables = VARIABLE.objects.all().order_by('name')
            target_variables = VARIABLE.objects.exclude(
                variable_id__contains='EBA_'
            ).exclude(
                variable_id__regex=r'[a-z]'
            ).order_by('name')
            context = {
                'table': table,
                'variables': variables,
                'all_variables': all_variables,
                'target_variables': target_variables,
                'step': 4,
                'total_steps': 7
            }
            return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step4_variable_breakdown.html', context)
        # ========== END STRICT VALIDATION ==========

        # Normalize group_type to lowercase for step5 compatibility
        for group_id, group_data in groups.items():
            if group_data.get('group_type'):
                group_data['group_type'] = group_data['group_type'].lower()

        request.session['olmw_variable_groups'] = json.dumps(groups)

        return redirect('pybirdai:output_layer_mapping_step5')

    # GET request - show breakdown form
    # Get all variables for dropdown
    all_variables = VARIABLE.objects.all().order_by('name')

    # Get filtered target variables (uppercase only, no EBA_)
    target_variables = VARIABLE.objects.exclude(
        variable_id__contains='EBA_'
    ).exclude(
        variable_id__regex=r'[a-z]'
    ).order_by('name')

    # Serialize variables with members for JavaScript consumption
    # This allows the frontend to display EBA_ATY members when user selects Observation type
    variables_with_members = {}
    for var_id, var_data in variables.items():
        variables_with_members[var_id] = {
            'variable': {
                'variable_id': var_data['variable'].variable_id,
                'name': var_data['variable'].name,
            },
            'domain': {
                'domain_id': var_data['domain'].domain_id if var_data['domain'] else None,
                'name': var_data['domain'].name if var_data['domain'] else None,
                'is_enumerated': var_data['domain'].is_enumerated if var_data['domain'] else False,
            } if var_data['domain'] else None,
            'members': [
                {
                    'member_id': m.member_id,
                    'name': m.name,
                    'code': m.code
                }
                for m in var_data['members']
            ],
            'suggested_role': var_data.get('suggested_role', 'Dimension')
        }

    context = {
        'table': table,
        'variables': variables,
        'all_variables': all_variables,
        'target_variables': target_variables,
        'variables_with_members_json': json.dumps(variables_with_members),
        'step': 4,
        'total_steps': 7
    }

    # ========== EDIT MODE CONTEXT ==========
    batch_edit_mode = request.session.get('olmw_batch_edit_mode', False)
    mapping_mode = request.session.get('olmw_mapping_mode', 'new')
    context['batch_edit_mode'] = batch_edit_mode
    context['mapping_mode'] = mapping_mode

    if batch_edit_mode:
        # Load batch edit data for tabbed display
        batch_edit_data_str = request.session.get('olmw_batch_edit_data', '{}')
        try:
            batch_edit_data = json.loads(batch_edit_data_str)
        except json.JSONDecodeError:
            batch_edit_data = {}

        context['batch_edit_data'] = batch_edit_data
        context['batch_edit_data_json'] = json.dumps(batch_edit_data)
        context['batch_edit_mapping_count'] = len(batch_edit_data)
        logger.info(f"[STEP 4] Batch edit mode: {len(batch_edit_data)} mappings")

    elif mapping_mode == 'modify_existing':
        # Single edit mode - load existing variable groups from session
        existing_groups_str = request.session.get('olmw_variable_groups', '{}')
        existing_multi_mappings_str = request.session.get('olmw_multi_mappings', '{}')
        try:
            existing_groups = json.loads(existing_groups_str)
            existing_multi_mappings = json.loads(existing_multi_mappings_str)
        except json.JSONDecodeError:
            existing_groups = {}
            existing_multi_mappings = {}

        context['existing_variable_groups'] = existing_groups
        context['existing_variable_groups_json'] = json.dumps(existing_groups)
        context['existing_multi_mappings'] = existing_multi_mappings
        context['existing_multi_mappings_json'] = json.dumps(existing_multi_mappings)
        context['existing_mapping_id'] = request.session.get('olmw_existing_mapping_id', '')
        logger.info(f"[STEP 4] Single edit mode: {len(existing_groups)} existing variable group(s)")

    # ========== QUICK START GROUPS ==========
    # Check if quick start groups were created in step 3
    quick_start_groups_str = request.session.pop('olmw_quick_start_groups', None)
    if quick_start_groups_str:
        try:
            quick_start_groups = json.loads(quick_start_groups_str)
            context['initial_variable_groups_json'] = json.dumps(quick_start_groups)
            logger.info(f"[STEP 4] Quick Start mode: {len(quick_start_groups)} pre-populated group(s)")
        except json.JSONDecodeError:
            logger.warning("[STEP 4] Failed to parse quick start groups JSON")

    return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step4_variable_breakdown.html', context)


def select_axis_ordinates(request):
    """
    Step 2.5: Allow user to select which axis ordinates should be mapped.
    These ordinates will be used to filter which cells are processed.

    For deduplicated tables, we traverse directly from the table's axes:
    TABLE → AXIS → AXIS_ORDINATE → CELL_POSITION → TABLE_CELL
    """
    from pybirdai.models.bird_meta_data_model import (
        CELL_POSITION, AXIS_ORDINATE, AXIS
    )

    # Check if table was selected in previous step
    if 'olmw_table_id' not in request.session:
        messages.error(request, 'Please select a table first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    try:
        table = TABLE.objects.get(table_id=table_id)
    except TABLE.DoesNotExist:
        messages.error(request, 'Selected table not found.')
        return redirect('pybirdai:output_layer_mapping_step1')

    logging.info(f"[Step 3] Loading ordinates for table: {table_id}")
    logging.info(f"[Step 3] Table code: {table.code}, version: {table.version}")

    # Get axes directly from the table (works for both original and deduplicated tables)
    table_axes = AXIS.objects.filter(table_id=table)
    logging.info(f"[Step 3] Found {table_axes.count()} axes for table")

    # Get all ordinates from those axes
    table_ordinates = AXIS_ORDINATE.objects.filter(
        axis_id__in=table_axes
    ).select_related('axis_id')
    logging.info(f"[Step 3] Found {table_ordinates.count()} ordinates across all axes")

    # Get all cell positions for those ordinates
    cell_positions = CELL_POSITION.objects.filter(
        axis_ordinate_id__in=table_ordinates
    ).select_related('axis_ordinate_id', 'axis_ordinate_id__axis_id', 'cell_id')

    # Get unique cells from cell positions
    cell_ids = cell_positions.values_list('cell_id', flat=True).distinct()
    table_cells = TABLE_CELL.objects.filter(cell_id__in=cell_ids)

    # Build unique axis ordinates with their details
    ordinates_data = {}
    for ordinate in table_ordinates:
        ordinates_data[ordinate.axis_ordinate_id] = {
            'ordinate': ordinate,
            'axis': ordinate.axis_id,
            'axis_name': ordinate.axis_id.name if ordinate.axis_id else 'Unknown',
            'axis_orientation': ordinate.axis_id.orientation if ordinate.axis_id else 'Unknown',
            'name': ordinate.name or ordinate.axis_ordinate_id,
            'is_abstract': ordinate.is_abstract_header,
            'level': ordinate.level or 0,
            'order': ordinate.order or 0,
            'cell_count': 0,
            'parent': ordinate.parent_axis_ordinate_id
        }

    # Count cells for each ordinate
    for cp in cell_positions:
        if cp.axis_ordinate_id and cp.axis_ordinate_id.axis_ordinate_id in ordinates_data:
            ordinates_data[cp.axis_ordinate_id.axis_ordinate_id]['cell_count'] += 1

    if request.method == 'POST':
        # Get selected ordinates from form
        selected_ordinates = request.POST.getlist('selected_ordinates')

        if not selected_ordinates:
            messages.warning(request, 'No ordinates selected. All cells will be mapped.')
            request.session['olmw_selected_ordinates'] = []
        else:
            request.session['olmw_selected_ordinates'] = selected_ordinates
            messages.success(request, f'Selected {len(selected_ordinates)} ordinate(s) for mapping.')

        # Continue to Step 4 (Variable Breakdown)
        return redirect('pybirdai:output_layer_mapping_step4')

    # Prepare data for template
    # Sort ordinates by axis orientation (row/column) and then by order
    sorted_ordinates = sorted(
        ordinates_data.values(),
        key=lambda x: (
            x['axis_orientation'] if x['axis_orientation'] else 'Z',
            x['level'],
            x['order']
        )
    )

    # Group by axis for better display
    axes_groups = {}
    for ord_data in sorted_ordinates:
        axis_name = ord_data['axis_name']
        if axis_name not in axes_groups:
            axes_groups[axis_name] = {
                'name': axis_name,
                'orientation': ord_data['axis_orientation'],
                'ordinates': []
            }
        axes_groups[axis_name]['ordinates'].append(ord_data)

    context = {
        'table': table,
        'axes_groups': axes_groups,
        'total_ordinates': len(ordinates_data),
        'total_cells': table_cells.count(),
        'step': 3,
        'total_steps': 7
    }

    # Generate embedded table HTML for visual selection
    table_html = generate_table_html(table, ordinates_data)
    context['embedded_table_html'] = table_html
    context['has_table_preview'] = True

    # ========== BATCH EDIT MODE CONTEXT ==========
    batch_edit_mode = request.session.get('olmw_batch_edit_mode', False)
    context['batch_edit_mode'] = batch_edit_mode

    if batch_edit_mode:
        # Load batch edit data to show which mappings use which ordinates
        batch_edit_data_str = request.session.get('olmw_batch_edit_data', '{}')
        try:
            batch_edit_data = json.loads(batch_edit_data_str)
        except json.JSONDecodeError:
            batch_edit_data = {}

        context['batch_edit_data'] = batch_edit_data
        context['batch_edit_mapping_count'] = len(batch_edit_data)

        # Build ordinate-to-mappings lookup for showing indicators
        ordinate_to_mappings = {}
        for mapping_id, mapping_data in batch_edit_data.items():
            for ordinate_id in mapping_data.get('ordinates', []):
                if ordinate_id not in ordinate_to_mappings:
                    ordinate_to_mappings[ordinate_id] = []
                ordinate_to_mappings[ordinate_id].append({
                    'mapping_id': mapping_id,
                    'mapping_name': mapping_data.get('name', mapping_id)
                })
        context['ordinate_to_mappings'] = ordinate_to_mappings

        # Pre-selected ordinates (union of all mappings' ordinates)
        pre_selected_ordinates = request.session.get('olmw_selected_ordinates', [])
        context['pre_selected_ordinates'] = pre_selected_ordinates

        logger.info(f"[STEP 3] Batch edit mode: {len(batch_edit_data)} mappings, "
                   f"{len(ordinate_to_mappings)} unique ordinates with mapping links, "
                   f"{len(pre_selected_ordinates)} pre-selected")

    return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step3_select_ordinates.html', context)


def quick_start_variable_groups(request):
    """
    Quick Start: Automatically create variable groups by axis orientation.
    Creates one group per axis (Row, Column, Z) with all variables from that axis's ordinates.
    """
    from pybirdai.models.bird_meta_data_model import (
        ORDINATE_ITEM, AXIS_ORDINATE, AXIS
    )

    # Check prerequisites
    if 'olmw_table_id' not in request.session:
        messages.error(request, 'Please select a table first.')
        return redirect('pybirdai:output_layer_mapping_step1')

    if request.method != 'POST':
        return redirect('pybirdai:output_layer_mapping_step3')

    table_id = request.session['olmw_table_id']
    try:
        table = TABLE.objects.get(table_id=table_id)
    except TABLE.DoesNotExist:
        messages.error(request, 'Selected table not found.')
        return redirect('pybirdai:output_layer_mapping_step1')

    # Get selected ordinates from POST (same as regular step 3 submission)
    selected_ordinates = request.POST.getlist('selected_ordinates')

    if not selected_ordinates:
        messages.warning(request, 'Please select at least one ordinate for Quick Start.')
        return redirect('pybirdai:output_layer_mapping_step3')

    # Store selected ordinates in session
    request.session['olmw_selected_ordinates'] = selected_ordinates

    # Get ordinate items with axis information
    ordinate_items = ORDINATE_ITEM.objects.filter(
        axis_ordinate_id__in=selected_ordinates
    ).select_related(
        'variable_id',
        'axis_ordinate_id',
        'axis_ordinate_id__axis_id'
    )

    # Group variables by axis orientation
    axis_variables = {}  # orientation -> set of variable_ids
    variable_info = {}   # variable_id -> variable data for role suggestion

    for item in ordinate_items:
        if item.variable_id and item.axis_ordinate_id and item.axis_ordinate_id.axis_id:
            var_id = item.variable_id.variable_id
            orientation = item.axis_ordinate_id.axis_id.orientation or 'Unknown'

            if orientation not in axis_variables:
                axis_variables[orientation] = set()
            axis_variables[orientation].add(var_id)

            # Store variable info for role suggestion
            if var_id not in variable_info:
                variable_info[var_id] = {
                    'variable': item.variable_id,
                    'suggested_role': suggest_variable_role(item.variable_id)
                }

    # Separate variables by role: Dimension vs Observation
    # Dimensions stay grouped by axis, Observations go to a single group
    dimension_vars_by_axis = {}  # orientation -> [var_ids with Dimension role]
    observation_vars = []         # All observation variables (any axis)
    attribute_vars = []           # All attribute variables (any axis)

    logger.info(f"[Quick Start] Found orientations in data: {list(axis_variables.keys())}")

    for orientation, var_ids_set in axis_variables.items():
        for var_id in var_ids_set:
            role = variable_info.get(var_id, {}).get('suggested_role', 'Dimension')
            if role == 'Observation':
                observation_vars.append(var_id)
            elif role == 'Attribute':
                attribute_vars.append(var_id)
            else:  # Dimension
                if orientation not in dimension_vars_by_axis:
                    dimension_vars_by_axis[orientation] = []
                dimension_vars_by_axis[orientation].append(var_id)

    # Create variable groups
    quick_start_groups = {}
    group_counter = 0

    # 1. Create axis groups for Dimension variables
    for orientation, var_ids in dimension_vars_by_axis.items():
        if var_ids:
            group_counter += 1
            group_id = f'group_{group_counter}'
            orientation_display = orientation.capitalize() if orientation else 'Unknown'

            quick_start_groups[group_id] = {
                'name': f'{orientation_display} Variables',
                'variable_ids': var_ids,
                'group_type': 'Dimension',
                'targets': [],
                'mapping_type': 'many_to_one'
            }
            logger.info(f"[Quick Start] Created Dimension group '{orientation_display} Variables' with {len(var_ids)} variables")

    # 2. Create single Observation group (if any observation variables exist)
    if observation_vars:
        group_counter += 1
        group_id = f'group_{group_counter}'
        quick_start_groups[group_id] = {
            'name': 'Observation Variables',
            'variable_ids': observation_vars,
            'group_type': 'Observation',
            'targets': [],
            'mapping_type': 'many_to_one'
        }
        logger.info(f"[Quick Start] Created Observation group with {len(observation_vars)} variables")

    # 3. Create single Attribute group (if any attribute variables exist)
    if attribute_vars:
        group_counter += 1
        group_id = f'group_{group_counter}'
        quick_start_groups[group_id] = {
            'name': 'Attribute Variables',
            'variable_ids': attribute_vars,
            'group_type': 'Attribute',
            'targets': [],
            'mapping_type': 'many_to_one'
        }
        logger.info(f"[Quick Start] Created Attribute group with {len(attribute_vars)} variables")

    # Store quick start groups in session for step 4
    request.session['olmw_quick_start_groups'] = json.dumps(quick_start_groups)

    # Success message
    group_count = len(quick_start_groups)
    total_vars = sum(len(g['variable_ids']) for g in quick_start_groups.values())
    messages.success(
        request,
        f'Quick Start: Created {group_count} variable group(s) with {total_vars} variable(s). '
        f'You can now review and modify the groups.'
    )

    logger.info(f"[Quick Start] Created {group_count} groups with {total_vars} variables for table {table_id}")

    return redirect('pybirdai:output_layer_mapping_step4')


def generate_table_html(table, ordinates_data):
    """
    Generate HTML representation of TABLE with selectable ordinates.
    Creates an interactive table matching the actual table structure.
    """
    from pybirdai.models.bird_meta_data_model import (
        TABLE_CELL, CELL_POSITION, AXIS_ORDINATE, AXIS
    )

    # Get all cells for this table via CELL_POSITION traversal
    # This works for deduplicated tables where TABLE_CELL.table_id points to original table
    # Path: TABLE -> AXIS -> AXIS_ORDINATE -> CELL_POSITION -> TABLE_CELL
    table_axes = AXIS.objects.filter(table_id=table)
    table_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=table_axes)
    table_ordinate_ids = set(table_ordinates.values_list('axis_ordinate_id', flat=True))

    # OPTIMIZATION: Fetch all cell positions at once instead of per-cell queries
    cell_positions = CELL_POSITION.objects.filter(
        axis_ordinate_id__in=table_ordinates
    ).select_related('axis_ordinate_id', 'axis_ordinate_id__axis_id')

    cell_ids = cell_positions.values_list('cell_id', flat=True).distinct()
    cells = TABLE_CELL.objects.filter(cell_id__in=cell_ids).select_related('table_id')

    # Build cell_id -> positions mapping for O(1) lookup
    cell_to_positions = {}
    for pos in cell_positions:
        cell_id = pos.cell_id_id if hasattr(pos, 'cell_id_id') else (pos.cell_id.cell_id if pos.cell_id else None)
        if cell_id:
            if cell_id not in cell_to_positions:
                cell_to_positions[cell_id] = []
            cell_to_positions[cell_id].append(pos)

    # Build structure: (row_ordinate_id, col_ordinate_id) -> cell
    cell_matrix = {}
    row_ordinates_set = set()
    col_ordinates_set = set()

    for cell in cells:
        # Use pre-fetched positions instead of per-cell query
        positions = cell_to_positions.get(cell.cell_id, [])

        row_ord_id = None
        col_ord_id = None

        for pos in positions:
            if pos.axis_ordinate_id and pos.axis_ordinate_id.axis_id:
                orientation = pos.axis_ordinate_id.axis_id.orientation
                ord_id = pos.axis_ordinate_id.axis_ordinate_id

                if orientation in ['Y', '2']:  # Row
                    row_ord_id = ord_id
                    row_ordinates_set.add(ord_id)
                elif orientation in ['X', '1']:  # Column
                    col_ord_id = ord_id
                    col_ordinates_set.add(ord_id)

        if row_ord_id and col_ord_id:
            cell_matrix[(row_ord_id, col_ord_id)] = {
                'cell': cell,
                'is_shaded': cell.is_shaded,
                'name': cell.name or ''
            }

    # Get ordered lists of ordinates
    row_ordinates = []
    col_ordinates = []

    for ord_id, ord_data in ordinates_data.items():
        orientation = ord_data.get('axis_orientation', '')
        if orientation in ['Y', '2'] and ord_id in row_ordinates_set:
            row_ordinates.append({
                'id': ord_id,
                'data': ord_data,
                'order': ord_data.get('order', 0),
                'level': ord_data.get('level', 0)
            })
        elif orientation in ['X', '1'] and ord_id in col_ordinates_set:
            col_ordinates.append({
                'id': ord_id,
                'data': ord_data,
                'order': ord_data.get('order', 0),
                'level': ord_data.get('level', 0)
            })

    # Sort by level and order
    row_ordinates.sort(key=lambda x: (x['level'], x['order']))
    col_ordinates.sort(key=lambda x: (x['level'], x['order']))

    # Generate HTML
    html = ['<table class="embedded-table table-bordered">']

    # Header row with column ordinates
    html.append('<thead><tr>')
    html.append('<th class="corner-cell">Select Ordinates</th>')  # Corner cell

    for col_ord in col_ordinates:
        name = col_ord['data'].get('name', 'N/A')

        html.append(
            f'<th class="col-header" data-ordinate-id="{col_ord["id"]}" '
            f'title="Click to select this column\n{name}">'
            f'{name}</th>'
        )

    html.append('</tr></thead>')

    # Body rows
    html.append('<tbody>')

    for row_ord in row_ordinates:
        html.append('<tr>')

        # Row header
        name = row_ord['data'].get('name', 'N/A')

        # Add indentation for hierarchy
        level = row_ord.get('level', 0)
        indent_style = f'padding-left: {level * 20 + 8}px;' if level > 0 else ''

        html.append(
            f'<th class="row-header" data-ordinate-id="{row_ord["id"]}" '
            f'style="{indent_style}" '
            f'title="Click to select this row\n{name}">'
            f'{name}</th>'
        )

        # Data cells
        for col_ord in col_ordinates:
            cell_key = (row_ord['id'], col_ord['id'])

            if cell_key in cell_matrix:
                cell_info = cell_matrix[cell_key]
                cell_name = cell_info['name']
                is_shaded = cell_info['is_shaded']

                shade_class = 'cell-shaded' if is_shaded else ''
                html.append(
                    f'<td class="data-cell {shade_class}" '
                    f'data-row-ordinate="{row_ord["id"]}" '
                    f'data-col-ordinate="{col_ord["id"]}" '
                    f'title="Cell: {cell_name}\nClick to select both ordinates">'
                    f'<span class="cell-indicator">●</span></td>'
                )
            else:
                # Empty cell - no data
                html.append(
                    f'<td class="data-cell cell-empty" '
                    f'data-row-ordinate="{row_ord["id"]}" '
                    f'data-col-ordinate="{col_ord["id"]}" '
                    f'title="No data in this cell">'
                    f'<span class="empty-indicator">—</span></td>'
                )

        html.append('</tr>')

    html.append('</tbody>')
    html.append('</table>')

    return '\n'.join(html)


def edit_mappings_tabbed(request):
    """
    Step 5: Multi-mapping tabbed interface - one mapping per variable group.
    Each group gets its own auto-generated mapping with separate navigation.
    """
    # ========== 1. VALIDATE SESSION DATA ==========
    if 'olmw_table_id' not in request.session or 'olmw_variable_groups' not in request.session:
        messages.error(request, 'Session expired or invalid. Please start from Step 1.')
        return redirect('pybirdai:output_layer_mapping_step1')

    table_id = request.session['olmw_table_id']
    table = TABLE.objects.get(table_id=table_id)
    variable_groups = json.loads(request.session['olmw_variable_groups'])

    # ========== BATCH EDIT MODE: Pre-select Z-tables from existing mappings ==========
    batch_edit_mode = request.session.get('olmw_batch_edit_mode', False)
    if batch_edit_mode and not request.session.get('olmw_selected_z_tables'):
        # Get table IDs from mapping ordinates via: MAPPING_ORDINATE_LINK → AXIS_ORDINATE → AXIS → table_id
        batch_edit_data_str = request.session.get('olmw_batch_edit_data', '{}')
        try:
            batch_edit_data = json.loads(batch_edit_data_str)
            mapping_ids = list(batch_edit_data.keys())

            if mapping_ids:
                z_table_ids = set()

                # Query ordinate links for all mappings
                ordinate_links = MAPPING_ORDINATE_LINK.objects.filter(
                    mapping_id__in=mapping_ids
                ).select_related('axis_ordinate_id__axis_id__table_id')

                for link in ordinate_links:
                    if (link.axis_ordinate_id and
                        link.axis_ordinate_id.axis_id and
                        link.axis_ordinate_id.axis_id.table_id):
                        z_table_ids.add(link.axis_ordinate_id.axis_id.table_id.table_id)
                        logger.info(f"[BATCH EDIT Z-TABLE] Found table from ordinate link: {link.axis_ordinate_id.axis_id.table_id.table_id}")

                if z_table_ids:
                    request.session['olmw_selected_z_tables'] = list(z_table_ids)
                    logger.info(f"[BATCH EDIT Z-TABLE] Pre-selected {len(z_table_ids)} Z-tables from existing mappings: {z_table_ids}")

        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"[BATCH EDIT Z-TABLE] Error loading batch edit data: {e}")

    # ========== 2. STRICT VALIDATION ==========
    invalid_groups = [
        g.get('name', gid)
        for gid, g in variable_groups.items()
        if not g.get('group_type') or g.get('group_type') not in ['dimension', 'observation', 'attribute']
    ]

    if invalid_groups:
        error_msg = (
            f"Data integrity error: {len(invalid_groups)} group(s) have invalid or missing type: "
            f"{', '.join(invalid_groups)}. Please go back and resubmit Step 4."
        )
        messages.error(request, error_msg)
        return redirect('pybirdai:output_layer_mapping_step4')

    # ========== 2.5. HELPER FUNCTION TO DETERMINE DOMAIN TYPE ==========
    def get_domain_type(variable):
        """Determine if a variable has a numeric (observation) or enumerated (dimension) domain."""
        if not variable or not hasattr(variable, 'domain_id') or not variable.domain_id:
            return 'unknown'

        domain_id = variable.domain_id.domain_id

        # Check for numeric domain patterns
        numeric_patterns = ['Integer', 'Float', 'Decimal', 'Monetary', 'INTGR', 'MNTRY', 'EBA_Integer', 'EBA_Float', 'EBA_Decimal', 'EBA_Monetary']
        if any(pattern in domain_id for pattern in numeric_patterns):
            return 'numeric'

        # Check if domain is enumerated
        if hasattr(variable.domain_id, 'is_enumerated') and variable.domain_id.is_enumerated:
            return 'enumerated'

        # Default to enumerated for dimensions, numeric for observations based on variable name patterns
        if 'AMT' in variable.variable_id or 'VAL' in variable.variable_id or 'QTY' in variable.variable_id:
            return 'numeric'

        return 'enumerated'

    # ========== 3. EXTRACT ALL ORDINATE MEMBERS ==========
    from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM

    selected_ordinates = request.session.get('olmw_selected_ordinates', [])
    all_ordinate_members = {}  # {variable_id: [member objects]}

    if selected_ordinates:
        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('variable_id', 'member_id')

        for item in ordinate_items:
            if item.variable_id and item.member_id:
                var_id = item.variable_id.variable_id
                if var_id not in all_ordinate_members:
                    all_ordinate_members[var_id] = []
                if item.member_id not in all_ordinate_members[var_id]:
                    all_ordinate_members[var_id].append(item.member_id)

    # ========== 4. PROCESS EACH VARIABLE GROUP AS A SEPARATE MAPPING ==========
    from itertools import product

    mappings_data = []  # List of mapping objects, one per group

    for group_id, group_data in variable_groups.items():
        group_name = group_data.get('name', group_id)
        group_type = group_data['group_type']
        source_var_ids = group_data.get('variable_ids', [])
        target_var_ids = group_data.get('targets', [])

        # Skip groups with no targets
        if not target_var_ids:
            continue

        # Generate auto-name for this mapping (use base table code from session, not Z-axis variant)
        base_table_code = request.session.get('olmw_table_code', table.code)

        # Clean variable IDs: remove "EBA_" prefix and use underscore separator
        cleaned_var_ids = [var_id.replace('EBA_', '') for var_id in source_var_ids]
        auto_name = f"DPM_{base_table_code}_{table.version}__{'_'.join(cleaned_var_ids)}"

        # Get VARIABLE objects for this group
        source_vars = list(VARIABLE.objects.filter(variable_id__in=source_var_ids))
        target_vars = list(VARIABLE.objects.filter(variable_id__in=target_var_ids))

        # Categorize variables by type within this group
        dimension_source_vars = []
        dimension_target_vars = []
        observation_source_vars = []
        observation_target_vars = []
        attribute_source_vars = []
        attribute_target_vars = []

        if group_type == 'dimension':
            dimension_source_vars = source_vars
            dimension_target_vars = target_vars
        elif group_type == 'observation':
            observation_source_vars = source_vars
            observation_target_vars = target_vars
        elif group_type == 'attribute':
            attribute_source_vars = source_vars
            attribute_target_vars = target_vars

        # Filter ordinate members for this group's variables
        group_ordinate_members = {
            var_id: members
            for var_id, members in all_ordinate_members.items()
            if var_id in source_var_ids or var_id in target_var_ids
        }

        # Override Z-axis member for deduplicated tables
        # ORDINATE_ITEM records point to "Total" member (x0) instead of specific Z-axis member (qx50)
        # If multiple Z-tables are selected, merge all their Z-axis members
        from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
            is_deduplicated_table,
            extract_z_axis_member_from_table_id,
            resolve_full_member_id
        )

        # Initialize Z-axis variables (available for serialization even if table isn't deduplicated)
        z_members_to_merge = []
        z_axis_variable_id = None
        z_axis_domain_id = None

        if is_deduplicated_table(table_id):
            # Get selected Z-tables from session (defaults to current table only on first load)
            selected_z_tables = request.session.get('olmw_selected_z_tables')
            if not selected_z_tables:
                # Only default to current table if session is empty (first load)
                # Otherwise, respect user's selection - they can choose ANY Z-variant tables
                selected_z_tables = [table_id]

            for selected_table_id in selected_z_tables:
                z_member_suffix = extract_z_axis_member_from_table_id(selected_table_id)

                if z_member_suffix:
                    # Find which variable is the Z-axis dimension (typically Exposure Class domain)
                    for var_id in source_var_ids:
                        try:
                            variable = VARIABLE.objects.get(variable_id=var_id)
                            if variable.domain_id and hasattr(variable.domain_id, 'domain_id'):
                                domain_id_str = variable.domain_id.domain_id

                                # Check if this is a Z-axis domain (Exposure Class)
                                if 'EC' in domain_id_str or 'XPSR' in domain_id_str:
                                    # Resolve full member_id from suffix
                                    full_member_id = resolve_full_member_id(z_member_suffix, domain_id_str)

                                    # Get the correct member object
                                    try:
                                        correct_member = MEMBER.objects.get(member_id=full_member_id)
                                        z_members_to_merge.append(correct_member)
                                        z_axis_variable_id = var_id
                                        z_axis_domain_id = domain_id_str  # Save domain ID for use in serialization
                                        break

                                    except MEMBER.DoesNotExist:
                                        logger.warning(f"[STEP 5 Z-AXIS] Z-axis member not found: {full_member_id}")

                        except VARIABLE.DoesNotExist:
                            continue

            # Merge all Z-axis members into the ordinate members for the Z-axis variable
            if z_members_to_merge and z_axis_variable_id:
                group_ordinate_members[z_axis_variable_id] = z_members_to_merge
                logger.info(f"[STEP 5 Z-AXIS] Merged Z-axis members: {z_axis_variable_id} = {[m.code for m in z_members_to_merge]}")

                # CRITICAL FIX: Filter out any other exposure class members from group_ordinate_members
                # that aren't in our explicitly extracted Z-member set
                # This prevents extra exposure class members from appearing in the UI
                try:
                    z_axis_var = VARIABLE.objects.get(variable_id=z_axis_variable_id)
                    if z_axis_var.domain_id:
                        z_axis_domain_id = z_axis_var.domain_id.domain_id
                        z_member_ids_set = {m.member_id for m in z_members_to_merge}

                        # Filter all variables to remove exposure class members not in our set
                        filtered_count = 0
                        for var_id, members in list(group_ordinate_members.items()):
                            if var_id == z_axis_variable_id:
                                # Already set correctly above
                                continue

                            # Check if this variable also uses the exposure class domain
                            try:
                                var = VARIABLE.objects.get(variable_id=var_id)
                                if var.domain_id and var.domain_id.domain_id == z_axis_domain_id:
                                    # This variable uses the same exposure class domain
                                    # Filter its members to only include our extracted Z-members
                                    original_count = len(members)
                                    filtered_members = [m for m in members if m.member_id in z_member_ids_set]

                                    if len(filtered_members) != original_count:
                                        group_ordinate_members[var_id] = filtered_members
                                        filtered_count += (original_count - len(filtered_members))
                                        logger.info(f"[STEP 5 Z-AXIS FILTER] Removed {original_count - len(filtered_members)} extra exposure class members from {var_id}")
                            except VARIABLE.DoesNotExist:
                                continue

                        if filtered_count > 0:
                            logger.info(f"[STEP 5 Z-AXIS FILTER] Removed total of {filtered_count} extra exposure class members across all variables")

                except VARIABLE.DoesNotExist:
                    logger.warning(f"[STEP 5 Z-AXIS FILTER] Could not load Z-axis variable {z_axis_variable_id} for domain filtering")

        # Serialize ordinate members for JavaScript
        # Prioritize ordinate-filtered members, especially for Z-axis dimensions
        ordinate_members_json = {}
        for var_id in list(source_var_ids) + list(target_var_ids):
            try:
                variable = VARIABLE.objects.get(variable_id=var_id)

                # Check if we have ordinate-filtered members for this variable
                ordinate_filtered_members = group_ordinate_members.get(var_id, [])

                if ordinate_filtered_members:
                    # Use ordinate-filtered members (preferred for Z-axis and other dimensions)
                    # This ensures deduplicated tables show only the correct Z-axis member
                    ordinate_members_json[var_id] = [
                        {
                            'member_id': m.member_id,
                            'name': m.name if hasattr(m, 'name') else '',
                            'code': m.code if hasattr(m, 'code') else ''
                        }
                        for m in ordinate_filtered_members
                    ]
                elif z_members_to_merge and z_axis_domain_id and variable.domain_id and variable.domain_id.domain_id == z_axis_domain_id:
                    # CRITICAL: For Z-axis domain variables without ordinate members (typically target variables),
                    # use the extracted Z-members instead of loading all domain members
                    # This prevents showing extra exposure class members
                    ordinate_members_json[var_id] = [
                        {
                            'member_id': m.member_id,
                            'name': m.name if hasattr(m, 'name') else '',
                            'code': m.code if hasattr(m, 'code') else ''
                        }
                        for m in z_members_to_merge
                    ]
                    logger.info(f"[STEP 5 Z-AXIS FALLBACK] Applied Z-axis filter to target variable {var_id}")
                elif variable.domain_id and hasattr(variable.domain_id, 'domain_id'):
                    # Fallback: Load all domain members if no ordinate members exist
                    # (Only for variables not filtered by ordinates and NOT using Z-axis domain)
                    all_domain_members = MEMBER.objects.filter(
                        domain_id=variable.domain_id
                    ).order_by('code')

                    ordinate_members_json[var_id] = [
                        {
                            'member_id': m.member_id,
                            'name': m.name if hasattr(m, 'name') else '',
                            'code': m.code if hasattr(m, 'code') else ''
                        }
                        for m in all_domain_members
                    ]
                else:
                    # No members available
                    ordinate_members_json[var_id] = []

            except VARIABLE.DoesNotExist:
                # Fallback to ordinate members
                ordinate_members_json[var_id] = [
                    {
                        'member_id': m.member_id,
                        'name': m.name if hasattr(m, 'name') else '',
                        'code': m.code if hasattr(m, 'code') else ''
                    }
                    for m in group_ordinate_members.get(var_id, [])
                ]

        # Merge newly created members from session into ordinate_members_json
        # This ensures members created via the modal persist across page reloads
        newly_created = request.session.get('olmw_newly_created_members', {})
        if newly_created:
            for var_id, new_members in newly_created.items():
                if var_id in ordinate_members_json:
                    # Get existing member IDs to avoid duplicates
                    existing_member_ids = {m['member_id'] for m in ordinate_members_json[var_id]}

                    # Add newly created members that aren't already in the list
                    for new_member in new_members:
                        if new_member['member_id'] not in existing_member_ids:
                            ordinate_members_json[var_id].append({
                                'member_id': new_member['member_id'],
                                'name': new_member['name'],
                                'code': new_member['code']
                            })
                            logger.info(f"[STEP 5 MERGE] Added newly created member {new_member['member_id']} to {var_id}")
                else:
                    # Variable not in ordinate_members_json yet, add all newly created members
                    ordinate_members_json[var_id] = [
                        {
                            'member_id': m['member_id'],
                            'name': m['name'],
                            'code': m['code']
                        }
                        for m in new_members
                    ]
                    logger.info(f"[STEP 5 MERGE] Created new member list for {var_id} with {len(new_members)} newly created members")

        # Create ordinate_selected_members_json with ONLY ordinate-filtered members
        # (For measure rows that should show only ordinate-selected EBA_ATY members)
        ordinate_selected_members_json = {}
        for var_id in list(source_var_ids) + list(target_var_ids):
            ordinate_selected_members_json[var_id] = [
                {
                    'member_id': m.member_id,
                    'name': m.name if hasattr(m, 'name') else '',
                    'code': m.code if hasattr(m, 'code') else ''
                }
                for m in group_ordinate_members.get(var_id, [])
            ]

        # Compute Cartesian product for dimension combinations (if group is dimension type)
        dimension_combinations = []
        skipped_vars = []
        existing_dimensions_loaded = False

        # ========== BATCH EDIT MODE: Load existing dimension combinations ==========
        if batch_edit_mode and group_type == 'dimension':
            # Try to find existing dimensions from batch edit data
            batch_edit_data_str = request.session.get('olmw_batch_edit_data', '{}')
            try:
                batch_edit_data = json.loads(batch_edit_data_str)

                # Look through all mappings to find matching multi_mappings by group name
                for mapping_id, mapping_batch_data in batch_edit_data.items():
                    multi_mappings = mapping_batch_data.get('multi_mappings', {})

                    for mm_group_id, mm_data in multi_mappings.items():
                        # Match by name
                        if mm_data.get('mapping_name') == group_name or mm_data.get('mapping_name') == group_data.get('name'):
                            existing_dims = mm_data.get('dimensions', [])
                            if existing_dims:
                                # Convert existing dimensions to the expected format
                                for dim_row in existing_dims:
                                    combo_dict = {}
                                    for var_id, member_id in dim_row.items():
                                        # Get member details
                                        try:
                                            member = MEMBER.objects.get(member_id=member_id)
                                            combo_dict[var_id] = {
                                                'member_id': member.member_id,
                                                'name': member.name if hasattr(member, 'name') else '',
                                                'code': member.code if hasattr(member, 'code') else ''
                                            }
                                        except MEMBER.DoesNotExist:
                                            combo_dict[var_id] = {
                                                'member_id': member_id,
                                                'name': member_id,
                                                'code': ''
                                            }
                                    if combo_dict:
                                        dimension_combinations.append(combo_dict)

                                existing_dimensions_loaded = True
                                logger.info(f"[BATCH EDIT] Loaded {len(dimension_combinations)} existing dimension combinations for group '{group_name}'")
                                break

                    if existing_dimensions_loaded:
                        break

            except (json.JSONDecodeError, Exception) as e:
                logger.warning(f"[BATCH EDIT] Error loading existing dimensions: {e}")

        # If no existing dimensions loaded, compute Cartesian product
        if group_type == 'dimension' and dimension_source_vars and not existing_dimensions_loaded:
            source_member_lists = []
            vars_with_members = []

            for var in dimension_source_vars:
                var_id = var.variable_id
                var_members = group_ordinate_members.get(var_id, [])

                if var_members:
                    members_data = [
                        {
                            'member_id': m.member_id,
                            'name': m.name if hasattr(m, 'name') else '',
                            'code': m.code if hasattr(m, 'code') else ''
                        }
                        for m in var_members
                    ]
                    source_member_lists.append(members_data)
                    vars_with_members.append(var)
                else:
                    skipped_vars.append(var_id)

            # Generate Cartesian product only from variables with members
            if source_member_lists and vars_with_members:
                for combination in product(*source_member_lists):
                    combo_dict = {}
                    for i, member in enumerate(combination):
                        var_id = vars_with_members[i].variable_id
                        combo_dict[var_id] = member
                    dimension_combinations.append(combo_dict)

        # Serialize variables for JavaScript
        mapping_obj = {
            'group_id': group_id,
            'group_name': group_name,
            'group_type': group_type,
            'auto_name': auto_name,
            'skipped_dimension_vars': skipped_vars,  # Variables with no ordinate members
            'dimension_source_vars': dimension_source_vars,
            'dimension_target_vars': dimension_target_vars,
            'observation_source_vars': observation_source_vars,
            'observation_target_vars': observation_target_vars,
            'attribute_source_vars': attribute_source_vars,
            'attribute_target_vars': attribute_target_vars,
            'dimension_source_vars_json': json.dumps([
                {'variable_id': v.variable_id, 'name': v.name, 'domain_id': v.domain_id.domain_id if v.domain_id else None, 'domain_type': get_domain_type(v)}
                for v in dimension_source_vars
            ]),
            'dimension_target_vars_json': json.dumps([
                {'variable_id': v.variable_id, 'name': v.name, 'domain_id': v.domain_id.domain_id if v.domain_id else None, 'domain_type': get_domain_type(v)}
                for v in dimension_target_vars
            ]),
            'observation_source_vars_json': json.dumps([
                {'variable_id': v.variable_id, 'name': v.name, 'domain_id': v.domain_id.domain_id if v.domain_id else None, 'domain_type': get_domain_type(v)}
                for v in observation_source_vars
            ]),
            'observation_target_vars_json': json.dumps([
                {'variable_id': v.variable_id, 'name': v.name, 'domain_id': v.domain_id.domain_id if v.domain_id else None, 'domain_type': get_domain_type(v)}
                for v in observation_target_vars
            ]),
            'attribute_source_vars_json': json.dumps([
                {'variable_id': v.variable_id, 'name': v.name, 'domain_id': v.domain_id.domain_id if v.domain_id else None, 'domain_type': get_domain_type(v)}
                for v in attribute_source_vars
            ]),
            'attribute_target_vars_json': json.dumps([
                {'variable_id': v.variable_id, 'name': v.name, 'domain_id': v.domain_id.domain_id if v.domain_id else None, 'domain_type': get_domain_type(v)}
                for v in attribute_target_vars
            ]),
            'ordinate_members_json': json.dumps(ordinate_members_json),
            'ordinate_selected_members_json': json.dumps(ordinate_selected_members_json),
            'dimension_combinations_json': json.dumps(dimension_combinations),
        }

        mappings_data.append(mapping_obj)

    # ========== 5. VALIDATION ==========
    if not mappings_data:
        messages.warning(
            request,
            "No mappings were created. This usually means groups have no target variables defined. "
            "Go back to Step 4 and ensure each group has at least one target variable selected."
        )

    # ========== 6. HANDLE FORM SUBMISSION ==========
    if request.method == 'POST':
        # Collect data for all mappings keyed by group_id
        all_mappings = {}

        for mapping in mappings_data:
            group_id = mapping['group_id']
            mapping_name = request.POST.get(f'mapping_name_{group_id}', mapping['auto_name'])

            # Collect mapping rows submitted via AJAX/form data
            # Expected format: mapping_data_{group_id} as JSON string
            mapping_data_json = request.POST.get(f'mapping_data_{group_id}', '{}')
            mapping_data = json.loads(mapping_data_json)

            all_mappings[group_id] = {
                'mapping_name': mapping_name,
                'auto_name': mapping['auto_name'],
                'group_type': mapping['group_type'],
                'dimensions': mapping_data.get('dimensions', []),
                'observations': mapping_data.get('observations', []),
                'attributes': mapping_data.get('attributes', [])
            }

        request.session['olmw_multi_mappings'] = json.dumps(all_mappings)
        return redirect('pybirdai:output_layer_mapping_step6')

    # ========== 7. RENDER TEMPLATE ==========
    # Check if this is a Z-axis deduplicated table and get siblings
    from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
        is_deduplicated_table,
        get_z_axis_sibling_tables
    )

    is_dedup = is_deduplicated_table(table_id)
    z_siblings = []
    if is_dedup:
        z_siblings = list(get_z_axis_sibling_tables(table_id))

    # Ensure selected_z_tables is defined (it's set inside the for loop)
    try:
        selected_z_for_template = selected_z_tables if is_dedup else []
    except NameError:
        # Variable not defined (loop didn't execute or table not deduplicated)
        selected_z_for_template = []

    # Ensure all session-selected tables are in the dropdown options
    # This prevents Tom Select from dropping tables that aren't in z_siblings
    if selected_z_for_template and is_dedup:
        z_sibling_ids = {t.table_id for t in z_siblings}
        for session_table_id in selected_z_for_template:
            if session_table_id not in z_sibling_ids:
                # Add missing table to dropdown options
                try:
                    missing_table = TABLE.objects.get(table_id=session_table_id)
                    z_siblings.append(missing_table)
                except TABLE.DoesNotExist:
                    pass

    context = {
        'table': table,
        'mappings': mappings_data,
        'mappings_count': len(mappings_data),
        'all_variables': VARIABLE.objects.all().order_by('name'),
        'is_deduplicated': is_dedup,
        'z_axis_siblings': z_siblings,
        'selected_z_table_ids': selected_z_for_template,
        'step': 5,
        'total_steps': 7
    }

    # ========== BATCH EDIT MODE CONTEXT ==========
    batch_edit_mode = request.session.get('olmw_batch_edit_mode', False)
    context['batch_edit_mode'] = batch_edit_mode

    if batch_edit_mode:
        batch_edit_data_str = request.session.get('olmw_batch_edit_data', '{}')
        try:
            batch_edit_data = json.loads(batch_edit_data_str)
        except json.JSONDecodeError:
            batch_edit_data = {}

        context['batch_edit_data'] = batch_edit_data
        context['batch_edit_mapping_count'] = len(batch_edit_data)
        context['batch_edit_mapping_names'] = [m.get('name', 'Unnamed') for m in batch_edit_data.values()]
        logger.info(f"[STEP 5] Batch edit mode: {len(batch_edit_data)} mappings being edited")

    return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step5_mapping_editor.html', context)


def create_member(request):
    """
    AJAX endpoint to create a new MEMBER.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'Method not allowed'}, status=405)

    try:
        # Read from FormData instead of JSON to avoid stream consumption issue
        name = request.POST.get('name', '').strip()
        code = request.POST.get('code', '').strip()
        description = request.POST.get('description', '').strip()
        domain_id = request.POST.get('domain_id', '').strip()
        variable_id = request.POST.get('variable_id', '').strip()

        # Validate required fields
        if not name or not code:
            return JsonResponse({'success': False, 'error': 'Name and code are required'}, status=400)

        if not domain_id:
            return JsonResponse({'success': False, 'error': 'Domain ID is required'}, status=400)

        # Get domain object
        try:
            domain = DOMAIN.objects.get(domain_id=domain_id)
        except DOMAIN.DoesNotExist:
            return JsonResponse({'success': False, 'error': f'Domain not found: {domain_id}'}, status=404)

        # Generate member_id (using domain_id.code format for clearer separation)
        member_id = f"{domain_id}.{code}"

        # Check if member already exists
        if MEMBER.objects.filter(member_id=member_id).exists():
            existing_member = MEMBER.objects.get(member_id=member_id)
            return JsonResponse({
                'success': False,
                'error': f'Member "{existing_member.name}" (code: {existing_member.code}) already exists in domain {domain_id}.',
                'existing_member': {
                    'member_id': existing_member.member_id,
                    'name': existing_member.name,
                    'code': existing_member.code,
                    'description': existing_member.description if hasattr(existing_member, 'description') else ''
                },
                'suggestion': 'This member already exists. Would you like to use it instead?'
            }, status=400)

        # Get maintenance agency (use first one if available, or None)
        maintenance_agency = MAINTENANCE_AGENCY.objects.first()

        # Create the member
        member = MEMBER.objects.create(
            member_id=member_id,
            code=code,
            name=name,
            description=description if description else None,
            domain_id=domain,
            maintenance_agency_id=maintenance_agency
        )

        # Track newly created member in session for persistence across page reloads
        # This ensures the member appears in dropdowns even after "Regenerate Mappings"
        if 'olmw_newly_created_members' not in request.session:
            request.session['olmw_newly_created_members'] = {}

        if variable_id:
            # Store member info keyed by variable_id for easy lookup
            if variable_id not in request.session['olmw_newly_created_members']:
                request.session['olmw_newly_created_members'][variable_id] = []

            request.session['olmw_newly_created_members'][variable_id].append({
                'member_id': member.member_id,
                'code': member.code,
                'name': member.name,
                'domain_id': domain_id
            })
            request.session.modified = True
            logger.info(f"[OLMW] Tracked newly created member {member_id} for variable {variable_id} in session")

        return JsonResponse({
            'success': True,
            'member': {
                'member_id': member.member_id,
                'code': member.code,
                'name': member.name,
                'description': member.description
            }
        })

    except Exception as e:
        logger.error(f"Error creating member: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def review_and_name_mapping(request):
    """
    Step 6: Review all mappings and edit names.
    Shows summary of all mappings with auto-generated names (editable).
    """
    # Check session
    if 'olmw_multi_mappings' not in request.session:
        messages.error(request, 'Please complete mapping configuration first.')
        return redirect('pybirdai:output_layer_mapping_step5')

    table_id = request.session['olmw_table_id']
    table = TABLE.objects.get(table_id=table_id)
    all_mappings = json.loads(request.session['olmw_multi_mappings'])
    variable_groups = json.loads(request.session['olmw_variable_groups'])

    # Prepare summaries for all mappings
    mapping_summaries = []
    for group_id, mapping_data in all_mappings.items():
        # Get dimension variable details from variable_groups (not from dimensions array)
        dimension_variables = {'source': [], 'target': []}
        group_data = variable_groups.get(group_id, {})
        group_type = group_data.get('group_type', 'dimension')

        if group_type == 'dimension':
            # Get source and target variable IDs from variable_groups
            source_var_ids = group_data.get('variable_ids', [])
            target_var_ids = group_data.get('targets', [])

            if source_var_ids:
                source_vars = VARIABLE.objects.filter(variable_id__in=source_var_ids)
                dimension_variables['source'] = [
                    {'code': var.variable_id, 'name': var.name}
                    for var in source_vars
                ]

            if target_var_ids:
                target_vars = VARIABLE.objects.filter(variable_id__in=target_var_ids)
                dimension_variables['target'] = [
                    {'code': var.variable_id, 'name': var.name}
                    for var in target_vars
                ]

        # Get observation variable details
        observation_variables = {'source': [], 'target': []}
        observations = mapping_data.get('observations', {})
        if observations:
            source_var_ids = observations.get('source_vars', [])
            target_var_ids = observations.get('target_vars', [])

            if source_var_ids:
                source_vars = VARIABLE.objects.filter(variable_id__in=source_var_ids)
                observation_variables['source'] = [
                    {'code': var.variable_id, 'name': var.name}
                    for var in source_vars
                ]

            if target_var_ids:
                target_vars = VARIABLE.objects.filter(variable_id__in=target_var_ids)
                observation_variables['target'] = [
                    {'code': var.variable_id, 'name': var.name}
                    for var in target_vars
                ]

        # Get attribute variable details
        attribute_variables = {'source': [], 'target': []}
        attributes = mapping_data.get('attributes', {})
        if attributes:
            source_var_ids = attributes.get('source_vars', [])
            target_var_ids = attributes.get('target_vars', [])

            if source_var_ids:
                source_vars = VARIABLE.objects.filter(variable_id__in=source_var_ids)
                attribute_variables['source'] = [
                    {'code': var.variable_id, 'name': var.name}
                    for var in source_vars
                ]

            if target_var_ids:
                target_vars = VARIABLE.objects.filter(variable_id__in=target_var_ids)
                attribute_variables['target'] = [
                    {'code': var.variable_id, 'name': var.name}
                    for var in target_vars
                ]

        summary = {
            'group_id': group_id,
            'group_name': variable_groups[group_id].get('name', group_id),
            'auto_name': mapping_data['auto_name'],
            'mapping_name': mapping_data.get('mapping_name', mapping_data['auto_name']),
            'group_type': mapping_data['group_type'],
            'dimension_count': len(mapping_data.get('dimensions', [])),
            # Observations and attributes are non-enumerated, so they have no member mapping rows
            'observation_count': 0,
            'attribute_count': 0,
            'dimension_variables': dimension_variables,
            'observation_variables': observation_variables,
            'attribute_variables': attribute_variables
        }
        mapping_summaries.append(summary)

    if request.method == 'POST':
        # Collect updated names for all mappings
        for summary in mapping_summaries:
            group_id = summary['group_id']
            new_name = request.POST.get(f'mapping_name_{group_id}', summary['auto_name'])
            new_description = request.POST.get(f'mapping_description_{group_id}', '')

            # Update mapping data with new name/description
            all_mappings[group_id]['mapping_name'] = new_name
            all_mappings[group_id]['mapping_description'] = new_description
            all_mappings[group_id]['internal_id'] = NamingUtils.generate_internal_id(new_name)

        # Save updated mappings back to session
        request.session['olmw_multi_mappings'] = json.dumps(all_mappings)

        return redirect('pybirdai:output_layer_mapping_step7')

    # GET request - show review form
    context = {
        'table': table,
        'mapping_summaries': mapping_summaries,
        'mappings_count': len(mapping_summaries),
        'step': 6,
        'total_steps': 7
    }

    return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step6_review_and_name.html', context)


def map_ordinates_between_tables(source_table_id, target_table_id, source_ordinate_ids):
    """
    Map ordinate IDs from a source deduplicated table to a target deduplicated table.

    For deduplicated tables sharing the same base, ordinates have similar structures
    but different IDs. This function maps selected ordinates from source to target.

    Args:
        source_table_id: str - Source table ID
        target_table_id: str - Target table ID
        source_ordinate_ids: list - List of ordinate IDs from source table

    Returns:
        list - Mapped ordinate IDs for target table
    """
    from pybirdai.models.bird_meta_data_model import AXIS, AXIS_ORDINATE

    logger.info(f"[ORDINATE_MAPPING] Mapping {len(source_ordinate_ids)} ordinates from {source_table_id} to {target_table_id}")

    if not source_ordinate_ids:
        return []

    # Get source ordinates with their axis information
    source_ordinates = AXIS_ORDINATE.objects.filter(
        axis_ordinate_id__in=source_ordinate_ids
    ).select_related('axis_id')

    # Get target table axes
    target_table = TABLE.objects.get(table_id=target_table_id)
    target_axes = AXIS.objects.filter(table_id=target_table)

    mapped_ordinates = []

    for source_ordinate in source_ordinates:
        source_axis = source_ordinate.axis_id

        # Find corresponding axis in target table by comparing axis codes or orientations
        target_axis = target_axes.filter(
            code=source_axis.code,
            orientation=source_axis.orientation
        ).first()

        if not target_axis:
            # Fallback: try matching by orientation only
            target_axis = target_axes.filter(
                orientation=source_axis.orientation
            ).first()

        if target_axis:
            # Find ordinate in target axis with same ordinate code
            target_ordinate = AXIS_ORDINATE.objects.filter(
                axis_id=target_axis,
                code=source_ordinate.code
            ).first()

            if not target_ordinate:
                # Fallback: try matching by order
                target_ordinate = AXIS_ORDINATE.objects.filter(
                    axis_id=target_axis,
                    order=source_ordinate.order
                ).first()

            if target_ordinate:
                mapped_ordinates.append(target_ordinate.axis_ordinate_id)
                logger.debug(f"[ORDINATE_MAPPING] Mapped {source_ordinate.axis_ordinate_id} -> {target_ordinate.axis_ordinate_id}")
            else:
                logger.warning(f"[ORDINATE_MAPPING] Could not find matching ordinate for {source_ordinate.axis_ordinate_id} in target table")
        else:
            logger.warning(f"[ORDINATE_MAPPING] Could not find matching axis for source axis {source_axis.axis_id} in target table")

    logger.info(f"[ORDINATE_MAPPING] Successfully mapped {len(mapped_ordinates)} out of {len(source_ordinate_ids)} ordinates")

    return mapped_ordinates


def update_z_axis_member_in_mappings(all_mappings, variable_groups, z_axis_variable_id, new_member_id):
    """
    Update Z-axis member ID in dimension mappings for table replication.

    When replicating mappings to a different deduplicated table, the Z-axis member
    needs to be updated to reflect the target table's Z-axis value.

    Args:
        all_mappings: dict - Mapping configuration with dimensions/observations/attributes
        variable_groups: dict - Variable group configuration
        z_axis_variable_id: str - Variable ID for the Z-axis dimension
        new_member_id: str - New member ID to use for Z-axis (e.g., 'EBA_qx51')

    Returns:
        dict - Updated mappings with new Z-axis member ID
    """
    import copy

    logger.info(f"[Z_MEMBER_UPDATE] Updating Z-axis member to {new_member_id} for variable {z_axis_variable_id}")

    # Deep copy to avoid modifying original
    updated_mappings = copy.deepcopy(all_mappings)

    # Iterate through all mappings and update Z-axis member
    for group_id, mapping_data in updated_mappings.items():
        dimensions = mapping_data.get('dimensions', [])

        if not dimensions:
            continue

        # Update each dimension row that contains the Z-axis variable
        for row_idx, row in enumerate(dimensions):
            if z_axis_variable_id in row:
                old_member = row[z_axis_variable_id]
                row[z_axis_variable_id] = new_member_id
                logger.debug(f"[Z_MEMBER_UPDATE] Updated row {row_idx} in {mapping_data.get('mapping_name')}: {old_member} -> {new_member_id}")

    logger.info(f"[Z_MEMBER_UPDATE] Successfully updated Z-axis member in {len(updated_mappings)} mapping(s)")
    return updated_mappings


def generate_structures_for_table(table_id, table_code, framework, version,
                                  variable_groups, all_mappings, selected_ordinates,
                                  maintenance_agency, regenerate_mode=False,
                                  existing_mapping_definitions=None):
    """
    Helper function to generate output layer structures for a single table.

    Args:
        table_id: str - Table ID (e.g., 'F_01_00_EBA_EC_EBA_qx50')
        table_code: str - Table code (e.g., 'F_01_00')
        framework: str - Framework ID (e.g., 'FINREP_REF')
        version: str - Version (e.g., '3.0')
        variable_groups: dict - Variable group configuration
        all_mappings: dict - Mapping definitions configuration
        selected_ordinates: list - Selected ordinate IDs
        maintenance_agency: MAINTENANCE_AGENCY - Maintenance agency object
        regenerate_mode: bool - Whether in regenerate mode
        existing_mapping_definitions: list - Pre-existing mapping definitions to reuse
            (if provided, skip creating new mappings and reuse these instead)

    Returns:
        dict with keys:
            - success: bool
            - mapping_definitions: list
            - cube_structure: CUBE_STRUCTURE object
            - cube: CUBE object
            - combinations: list of COMBINATION objects
            - error: str (if success=False)
    """
    try:
        logger.info(f"[GENERATE_FOR_TABLE] Starting generation for table {table_id}")

        # Initialize orchestrator
        orchestrator = OutputLayerMappingOrchestrator()

        # Track created objects
        dimension_target_vars = []
        observation_target_vars = []
        attribute_target_vars = []
        created_mapping_definitions = []
        created_member_mappings = []  # Track MEMBER_MAPPING objects for debug_data collection

        # Extract target variables from variable_groups
        for group_id, group_data in variable_groups.items():
            group_type = group_data.get('group_type', 'dimension').lower()
            target_var_ids = group_data.get('targets', [])

            if target_var_ids:
                target_vars = list(VARIABLE.objects.filter(variable_id__in=target_var_ids))

                if group_type == 'dimension':
                    dimension_target_vars.extend(target_vars)
                elif group_type == 'observation':
                    observation_target_vars.extend(target_vars)
                elif group_type == 'attribute':
                    attribute_target_vars.extend(target_vars)
            else:
                logger.warning(f"[GENERATE_FOR_TABLE] No target_var_ids for group {group_id}")

        logger.info(f"[GENERATE_FOR_TABLE] Target variables: {len(dimension_target_vars)} dims, "
                   f"{len(observation_target_vars)} obs, {len(attribute_target_vars)} attrs")

        # Normalize version for use in IDs
        version_normalized = version.replace('.', '_')

        # Check if we should reuse existing mappings (for Z-axis variants)
        # Use explicit None check to handle empty lists correctly
        if existing_mapping_definitions is not None:
            # Reuse provided mappings instead of creating new ones
            created_mapping_definitions = existing_mapping_definitions
            # Extract member mappings from existing mapping definitions for debug_data
            for mapping_info in existing_mapping_definitions:
                mapping_def = mapping_info['mapping_definition']
                if mapping_def.member_mapping_id:
                    created_member_mappings.append(mapping_def.member_mapping_id)
            logger.info(f"[GENERATE_FOR_TABLE] Reusing {len(created_mapping_definitions)} existing mapping definitions (Z-axis variant)")
        else:
            # Create new MAPPING_DEFINITIONs
            # Generate mapping IDs
            mapping_prefix = f"{table_code}_{version_normalized}_MAP"
            existing_count = MAPPING_DEFINITION.objects.filter(
                code__startswith=mapping_prefix
            ).count()
            mapping_sequence_start = existing_count + 1

            # ========== OPTIMIZATION: BULK PRE-FETCH VARIABLES AND MEMBERS ==========
            # Collect all variable IDs and member IDs needed across all mappings
            all_var_ids_to_fetch = set()
            all_member_ids_to_fetch = set()

            for group_id, mapping_data in all_mappings.items():
                # Collect variable IDs from variable_groups
                group_info = variable_groups.get(group_id, {})
                all_var_ids_to_fetch.update(group_info.get('variable_ids', []))
                all_var_ids_to_fetch.update(group_info.get('targets', []))

                # Collect member IDs from dimensions
                for row in mapping_data.get('dimensions', []):
                    all_var_ids_to_fetch.update(row.keys())
                    all_member_ids_to_fetch.update(v for v in row.values() if v)

            # Bulk fetch all variables and members
            variables_cache = {v.variable_id: v for v in VARIABLE.objects.filter(
                variable_id__in=all_var_ids_to_fetch
            ).select_related('domain_id')}
            members_cache = {m.member_id: m for m in MEMBER.objects.filter(
                member_id__in=all_member_ids_to_fetch
            )}

            logger.info(f"[GENERATE_FOR_TABLE OPTIMIZATION] Pre-fetched {len(variables_cache)} variables and {len(members_cache)} members")
            # ========== END OPTIMIZATION ==========

            # Create MAPPING_DEFINITIONs for each mapping
            mapping_counter = 0
            logger.info(f"[GENERATE_FOR_TABLE] Processing {len(all_mappings)} mappings")
            for group_id, mapping_data in all_mappings.items():
                mapping_name = mapping_data['mapping_name']
                internal_id = mapping_data['internal_id']
                group_type = mapping_data.get('group_type', 'dimension').lower()
                dimensions = mapping_data.get('dimensions', [])
                observations = mapping_data.get('observations', [])
                attributes = mapping_data.get('attributes', [])

                current_sequence = mapping_sequence_start + mapping_counter
                mapping_id_suffix = f"{current_sequence:03d}"
                mapping_counter += 1

                logger.info(f"[GENERATE_FOR_TABLE] Creating mapping '{mapping_name}'")

                # Create VARIABLE_MAPPING
                variable_mapping = VARIABLE_MAPPING.objects.create(
                    variable_mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_VAR",
                    maintenance_agency_id=maintenance_agency,
                    name=mapping_name,
                    code=internal_id
                )

                # Create VARIABLE_MAPPING_ITEMs (using pre-fetched cache)
                group_info = variable_groups.get(group_id, {})
                source_var_ids = set(group_info.get('variable_ids', []))
                target_var_ids = set(group_info.get('targets', []))
                created_var_ids = set()
                all_var_ids = source_var_ids | target_var_ids

                # Collect items for bulk_create
                var_mapping_items_to_create = []
                for var_id in all_var_ids:
                    if var_id not in created_var_ids:
                        variable = variables_cache.get(var_id)  # Use cache instead of DB query
                        if variable:
                            is_source = "true" if var_id in source_var_ids else "false"
                            var_mapping_items_to_create.append(
                                VARIABLE_MAPPING_ITEM(
                                    variable_mapping_id=variable_mapping,
                                    variable_id=variable,
                                    is_source=is_source
                                )
                            )
                            created_var_ids.add(var_id)

                # Bulk create all variable mapping items
                if var_mapping_items_to_create:
                    VARIABLE_MAPPING_ITEM.objects.bulk_create(var_mapping_items_to_create)

                # Create MEMBER_MAPPING if needed
                logger.info(f"[GENERATE_FOR_TABLE MEMBER_MAPPING DEBUG] Processing mapping {group_id} ({mapping_name})")
                logger.info(f"[GENERATE_FOR_TABLE MEMBER_MAPPING DEBUG] dimensions type: {type(dimensions)}, length: {len(dimensions) if dimensions else 0}")
                if dimensions:
                    logger.info(f"[GENERATE_FOR_TABLE MEMBER_MAPPING DEBUG] Sample dimension row: {dimensions[0] if dimensions else 'None'}")
                else:
                    logger.warning(f"[GENERATE_FOR_TABLE MEMBER_MAPPING DEBUG] No dimensions for mapping {group_id} - will NOT create MEMBER_MAPPING")

                member_mapping = None
                if dimensions:
                    member_mapping = MEMBER_MAPPING.objects.create(
                        member_mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_MEM",
                        maintenance_agency_id=maintenance_agency,
                        name=f"{mapping_name} - Member Mappings",
                        code=f"{internal_id}_MEM"
                    )
                    created_member_mappings.append(member_mapping)  # Track for debug_data collection
                    logger.info(f"[GENERATE_FOR_TABLE MEMBER_MAPPING] Created MEMBER_MAPPING: {member_mapping.member_mapping_id}")

                    # Create member mapping items (using pre-fetched cache with bulk_create)
                    member_mapping_items_to_create = []
                    missing_member_fallback_ids = set()  # Track member IDs that need fallback lookup

                    # First pass: use cache and collect fallback IDs
                    for row_idx, row in enumerate(dimensions):
                        for var_id, member_id in row.items():
                            if member_id:
                                variable = variables_cache.get(var_id)  # Use cache
                                member = members_cache.get(member_id)  # Use cache

                                # Fallback: Try alternative member_id format if not found
                                if not member and variable and variable.domain_id:
                                    domain_id_str = variable.domain_id.domain_id if hasattr(variable.domain_id, 'domain_id') else str(variable.domain_id)

                                    # If member_id looks like "EBA_EC_EBA_qx50", try "EBA_EC_qx50"
                                    if '_EBA_' in member_id and member_id.startswith(f"{domain_id_str}_EBA_"):
                                        code_part = member_id.split('_EBA_')[-1]
                                        alt_member_id = f"{domain_id_str}_{code_part}"
                                        missing_member_fallback_ids.add(alt_member_id)
                                    # If member_id looks like "EBA_EC_qx50", try "EBA_EC_EBA_qx50"
                                    elif member_id.startswith(f"{domain_id_str}_") and '_EBA_' not in member_id:
                                        code_part = member_id.split(f"{domain_id_str}_")[1]
                                        alt_member_id = f"{domain_id_str}_EBA_{code_part}"
                                        missing_member_fallback_ids.add(alt_member_id)

                    # Bulk fetch fallback members if any
                    fallback_members_cache = {}
                    if missing_member_fallback_ids:
                        fallback_members_cache = {m.member_id: m for m in MEMBER.objects.filter(
                            member_id__in=missing_member_fallback_ids
                        )}

                    # Second pass: create items using all caches
                    items_created = 0
                    for row_idx, row in enumerate(dimensions):
                        for var_id, member_id in row.items():
                            if member_id:
                                variable = variables_cache.get(var_id)
                                member = members_cache.get(member_id)

                                # Try fallback formats if not found
                                if not member and variable and variable.domain_id:
                                    domain_id_str = variable.domain_id.domain_id if hasattr(variable.domain_id, 'domain_id') else str(variable.domain_id)

                                    if '_EBA_' in member_id and member_id.startswith(f"{domain_id_str}_EBA_"):
                                        code_part = member_id.split('_EBA_')[-1]
                                        alt_member_id = f"{domain_id_str}_{code_part}"
                                        member = fallback_members_cache.get(alt_member_id)
                                        if member:
                                            logger.info(f"[MEMBER_LOOKUP] Found member using alternative format: {alt_member_id}")
                                    elif member_id.startswith(f"{domain_id_str}_") and '_EBA_' not in member_id:
                                        code_part = member_id.split(f"{domain_id_str}_")[1]
                                        alt_member_id = f"{domain_id_str}_EBA_{code_part}"
                                        member = fallback_members_cache.get(alt_member_id)
                                        if member:
                                            logger.info(f"[MEMBER_LOOKUP] Found member using DPM format: {alt_member_id}")

                                if variable and member:
                                    member_mapping_items_to_create.append(
                                        MEMBER_MAPPING_ITEM(
                                            member_mapping_id=member_mapping,
                                            member_mapping_row=str(row_idx + 1),
                                            variable_id=variable,
                                            is_source="false",
                                            member_id=member
                                        )
                                    )
                                    items_created += 1
                                elif variable:
                                    logger.warning(f"[MEMBER_LOOKUP] Could not find member: {member_id} for variable {var_id}")

                    # Bulk create all member mapping items
                    if member_mapping_items_to_create:
                        MEMBER_MAPPING_ITEM.objects.bulk_create(member_mapping_items_to_create, batch_size=500)

                    logger.info(f"[GENERATE_FOR_TABLE MEMBER_MAPPING] Created {items_created} MEMBER_MAPPING_ITEMs for {member_mapping.member_mapping_id}")

                # Create MAPPING_DEFINITION
                mapping_type = 'E' if group_type == 'dimension' else ('O' if group_type == 'observation' else 'A')
                mapping_def_id = f"{mapping_prefix}_{mapping_id_suffix}"

                mapping_def = MAPPING_DEFINITION.objects.create(
                    mapping_id=mapping_def_id,
                    maintenance_agency_id=maintenance_agency,
                    name=mapping_name,
                    code=internal_id,
                    mapping_type=mapping_type,
                    variable_mapping_id=variable_mapping,
                    member_mapping_id=member_mapping
                )

                created_mapping_definitions.append({
                    'mapping_definition': mapping_def,
                    'name': mapping_name,
                    'internal_id': internal_id
                })

                logger.info(f"[GENERATE_FOR_TABLE] Created MAPPING_DEFINITION: {mapping_def_id}")

        # ========== DELETE EXISTING OUTPUT LAYERS BEFORE REGENERATING ==========
        # This ensures we don't have duplicate CUBE_STRUCTURE_ITEMs, COMBINATIONs, etc.
        logger.info(f"[GENERATE_FOR_TABLE] Deleting existing output layers for table {table_id} before generating new ones")
        deletion_stats = delete_output_layers_by_table(table_id)
        if any(v > 0 for v in deletion_stats.values()):
            logger.info(f"[GENERATE_FOR_TABLE] Deleted old output layers: {deletion_stats}")

        # Create CUBE_STRUCTURE
        # Deduplicate by variable_id (not object identity)
        all_target_vars = dimension_target_vars + observation_target_vars + attribute_target_vars
        unique_target_vars = list({v.variable_id: v for v in all_target_vars}.values())

        # ========== PHASE 3: EXTRACT AND CREATE MEMBERS (HELPER FUNCTION) ==========
        logger.info(f"[GENERATE_FOR_TABLE PHASE 3 START] Beginning member extraction for table {table_id}")
        logger.info(f"[GENERATE_FOR_TABLE PHASE 3 START] all_mappings type: {type(all_mappings)}, is None: {all_mappings is None}")
        if all_mappings is not None:
            logger.info(f"[GENERATE_FOR_TABLE PHASE 3 START] all_mappings has {len(all_mappings)} entries")

        # Build a mapping of variable_id -> [member_ids] to ensure members exist
        variable_to_members_map = {}

        # Extract member codes from all_mappings dimensions
        logger.info("[GENERATE_FOR_TABLE] Extracting member codes from mappings")
        for group_id, mapping_data in all_mappings.items():
            dimensions = mapping_data.get('dimensions', [])

            # Each dimension row is a dict: {var_id: member_id, var_id: member_id, ...}
            for row in dimensions:
                for var_id, member_id in row.items():
                    if var_id and member_id:
                        if var_id not in variable_to_members_map:
                            variable_to_members_map[var_id] = []
                        if member_id not in variable_to_members_map[var_id]:
                            variable_to_members_map[var_id].append(member_id)

        total_members_to_ensure = sum(len(members) for members in variable_to_members_map.values())
        logger.info(f"[GENERATE_FOR_TABLE] Extracted member codes for {len(variable_to_members_map)} variables, {total_members_to_ensure} total unique member references")

        # ========== DIAGNOSTIC LOGGING (HELPER FUNCTION) ==========
        logger.info(f"[GENERATE_FOR_TABLE DEBUG] all_mappings keys: {list(all_mappings.keys()) if all_mappings else 'None'}")
        for group_id, mapping_data in (all_mappings.items() if all_mappings else []):
            dims = mapping_data.get('dimensions', [])
            logger.info(f"[GENERATE_FOR_TABLE DEBUG] Mapping {group_id}: {len(dims)} dimension rows")
            if dims:
                # Show sample of first dimension row
                sample_row = dims[0] if dims else {}
                logger.info(f"[GENERATE_FOR_TABLE DEBUG]   Sample dimension row: {sample_row}")

        logger.info(f"[GENERATE_FOR_TABLE DEBUG] variable_to_members_map has {len(variable_to_members_map)} variables:")
        for var_id, member_ids in list(variable_to_members_map.items())[:5]:  # Show first 5
            logger.info(f"[GENERATE_FOR_TABLE DEBUG]   {var_id}: {len(member_ids)} members - {member_ids[:3]}")  # Show first 3
        if len(variable_to_members_map) > 5:
            logger.info(f"[GENERATE_FOR_TABLE DEBUG]   ... and {len(variable_to_members_map) - 5} more variables")

        # Handle empty extraction
        if total_members_to_ensure == 0:
            logger.warning("[GENERATE_FOR_TABLE WARNING] No members found in mappings - subdomain enumeration may fail")
            logger.warning(f"[GENERATE_FOR_TABLE WARNING] This may cause FK violations if subdomains require member enumerations")
            logger.warning(f"[GENERATE_FOR_TABLE WARNING] Checked {len(all_mappings)} mappings for table {table_id}")

        # Create missing members BEFORE creating subdomains (OPTIMIZED: bulk operations)
        logger.info(f"[GENERATE_FOR_TABLE PHASE 3] Ensuring domains and members exist for {len(unique_target_vars)} target variables")
        domain_manager = DomainManager()

        # ========== OPTIMIZATION: Bulk check existing members ==========
        # Collect all member_ids that need to be checked
        all_member_ids_to_check = set()
        for member_ids in variable_to_members_map.values():
            all_member_ids_to_check.update(member_ids)

        # Bulk fetch existing members
        existing_member_ids = set(MEMBER.objects.filter(
            member_id__in=all_member_ids_to_check
        ).values_list('member_id', flat=True))

        members_validated_count = len(existing_member_ids & all_member_ids_to_check)
        missing_member_ids = all_member_ids_to_check - existing_member_ids

        logger.info(f"[GENERATE_FOR_TABLE PHASE 3 OPTIMIZATION] {len(existing_member_ids)} members exist, {len(missing_member_ids)} need creation")
        # ========== END OPTIMIZATION ==========

        # Build variable -> domain mapping for member creation
        var_to_domain = {}
        for variable in unique_target_vars:
            domain = domain_manager.ensure_domain_and_members(variable, maintenance_agency)
            var_to_domain[variable.variable_id] = domain

        # Collect members to create in bulk
        members_to_create = []
        for variable in unique_target_vars:
            domain = var_to_domain.get(variable.variable_id)
            member_ids_for_var = variable_to_members_map.get(variable.variable_id, [])

            for member_id in member_ids_for_var:
                if member_id in missing_member_ids:
                    # Extract code from member_id
                    if domain and domain.domain_id in member_id:
                        code = member_id.replace(f"{domain.domain_id}_", "")
                    else:
                        code = member_id.split('_')[-1] if '_' in member_id else member_id

                    members_to_create.append(MEMBER(
                        member_id=member_id,
                        maintenance_agency_id=maintenance_agency,
                        code=code,
                        name=code,
                        domain_id=domain,
                        description=f"Member {code} for domain {domain.name if domain else 'Unknown'}"
                    ))

        # Bulk create missing members
        members_created_count = 0
        if members_to_create:
            try:
                # Use ignore_conflicts to handle potential race conditions
                created = MEMBER.objects.bulk_create(members_to_create, batch_size=500, ignore_conflicts=True)
                members_created_count = len(created)
                logger.info(f"[GENERATE_FOR_TABLE PHASE 3] Bulk created {members_created_count} members")
            except Exception as e:
                logger.error(f"[GENERATE_FOR_TABLE PHASE 3] Bulk create failed, falling back to individual creates: {str(e)}")
                # Fallback to individual creates
                for member in members_to_create:
                    try:
                        member.save()
                        members_created_count += 1
                    except Exception as inner_e:
                        logger.error(f"[GENERATE_FOR_TABLE PHASE 3] Failed to create member {member.member_id}: {str(inner_e)}")

        logger.info(f"[GENERATE_FOR_TABLE PHASE 3] Completed domain and member validation/creation: {members_created_count} members created, {members_validated_count} members validated")

        # Manually create CUBE_STRUCTURE using Django ORM (matching pattern from main generate_structures)
        # Use full table_id to ensure uniqueness across variants (not just table_code)
        # Use get_or_create() to make this idempotent (handles regeneration)
        cube_structure_id = f"{table_id}_STRUCTURE"
        cube_structure, cs_created = CUBE_STRUCTURE.objects.get_or_create(
            cube_structure_id=cube_structure_id,
            defaults={
                'maintenance_agency_id': maintenance_agency,
                'name': f"Reference structure for {table_code}",
                'code': f"{table_code}_CS",
                'description': f"Cube structure for table {table_id}",
                'version': version
            }
        )

        if cs_created:
            logger.info(f"[GENERATE_FOR_TABLE] Created NEW CUBE_STRUCTURE: {cube_structure_id}")
        else:
            logger.info(f"[GENERATE_FOR_TABLE] Reusing existing CUBE_STRUCTURE: {cube_structure_id}")
            # Update description to reflect current generation
            cube_structure.description = f"Cube structure for table {table_id}"
            cube_structure.save()

        # Initialize CubeStructureGenerator (no arguments!)
        csi_generator = CubeStructureGenerator()

        # Create CUBE_STRUCTURE_ITEMs
        # Deduplicate by variable_id (not object identity)
        unique_dimension_vars = list({v.variable_id: v for v in dimension_target_vars}.values())
        unique_observation_vars = list({v.variable_id: v for v in observation_target_vars}.values())
        unique_attribute_vars = list({v.variable_id: v for v in attribute_target_vars}.values())

        logger.info(f"[GENERATE_FOR_TABLE CUBE_STRUCTURE_ITEM] Creating items for: {len(unique_dimension_vars)} dims, {len(unique_observation_vars)} obs, {len(unique_attribute_vars)} attrs")

        order_counter = 1
        items_created_count = 0

        # Dimension items (with subdomain and dimension_type)
        for variable in unique_dimension_vars:
            # Log domain and member count before subdomain creation
            if hasattr(variable, 'domain_id') and variable.domain_id:
                domain = variable.domain_id
                member_count = MEMBER.objects.filter(domain_id=domain).count()
                logger.info(f"[GENERATE_FOR_TABLE PRE-SUBDOMAIN] Variable {variable.variable_id} has domain {domain.domain_id} with {member_count} members in database")
            else:
                logger.warning(f"[GENERATE_FOR_TABLE PRE-SUBDOMAIN] Variable {variable.variable_id} has NO domain!")

            # Create or get subdomain (returns tuple: subdomain, single_member)
            subdomain, single_member = csi_generator.create_or_get_subdomain(
                variable, cube_structure.cube_structure_id
            )

            # Determine dimension_type based on variable name patterns
            dimension_type = "B"  # Default: Business
            var_id_upper = variable.variable_id.upper()
            if "TIME" in var_id_upper or "DATE" in var_id_upper or "PERIOD" in var_id_upper:
                dimension_type = "T"  # Temporal
            elif "METHOD" in var_id_upper or "APPROACH" in var_id_upper:
                dimension_type = "M"  # Methodological
            elif "UNIT" in var_id_upper or "CURRENCY" in var_id_upper:
                dimension_type = "U"  # Unit

            cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"
            # IMPORTANT: Using create() instead of get_or_create() to force new item creation
            item = CUBE_STRUCTURE_ITEM.objects.create(
                cube_structure_id=cube_structure,
                cube_variable_code=cube_variable_code,
                variable_id=variable,
                role="D",
                order=order_counter,
                subdomain_id=subdomain,
                member_id=single_member,
                dimension_type=dimension_type,
                is_mandatory=True,
                is_implemented=True,
                description=f"Dimension: {variable.name}"
            )
            order_counter += 1
            items_created_count += 1

        # Observation items
        for variable in unique_observation_vars:
            cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"
            # IMPORTANT: Using create() instead of get_or_create() to force new item creation
            item = CUBE_STRUCTURE_ITEM.objects.create(
                cube_structure_id=cube_structure,
                cube_variable_code=cube_variable_code,
                variable_id=variable,
                role="O",
                order=order_counter,
                is_mandatory=True,
                is_implemented=True,
                is_flow=True,
                description=f"Observation: {variable.name}"
            )
            order_counter += 1
            items_created_count += 1

        # Attribute items
        for variable in unique_attribute_vars:
            cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"
            # IMPORTANT: Using create() instead of get_or_create() to force new item creation
            item = CUBE_STRUCTURE_ITEM.objects.create(
                cube_structure_id=cube_structure,
                cube_variable_code=cube_variable_code,
                variable_id=variable,
                role="A",
                order=order_counter,
                is_mandatory=False,
                is_implemented=True,
                description=f"Attribute: {variable.name}"
            )
            order_counter += 1
            items_created_count += 1

        logger.info(f"[GENERATE_FOR_TABLE CUBE_STRUCTURE_ITEM] Created {items_created_count} CUBE_STRUCTURE_ITEMs for {cube_structure_id}")

        # Create CUBE
        framework_obj = FRAMEWORK.objects.filter(framework_id=framework).first()
        if not framework_obj:
            efbt_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
                maintenance_agency_id='EFBT',
                defaults={'name': 'EFBT System', 'code': 'EFBT'}
            )
            framework_obj, _ = FRAMEWORK.objects.get_or_create(
                framework_id=framework,
                defaults={
                    'name': framework,
                    'code': framework,
                    'maintenance_agency_id': efbt_agency,
                    'description': f'Auto-generated framework for {framework}'
                }
            )

        # Use full table_id to ensure uniqueness across variants
        # Extract framework short name (e.g., 'EBA_COREP' -> 'COREP')
        framework_short = framework.replace('EBA_', '') if framework.startswith('EBA_') else framework
        # New naming: {FRAMEWORK_SHORT}_REF_{table_id}_CUBE
        cube_id = f"{framework_short}_REF_{table_id}_CUBE"
        cube, cube_created = CUBE.objects.get_or_create(
            cube_id=cube_id,
            defaults={
                'maintenance_agency_id': maintenance_agency,
                'name': f"{table_id}",
                'code': f"{framework_short}_REF_{table_code}_CUBE",
                'framework_id': framework_obj,
                'cube_structure_id': cube_structure,
                'cube_type': "RC",
                'is_allowed': True,
                'published': False,
                'version': version,
                'description': f"Cube for {len(created_mapping_definitions)} mapping definitions"
            }
        )

        if not cube_created:
            cube.cube_structure_id = cube_structure
            cube.description = f"Cube for {len(created_mapping_definitions)} mapping definitions"
            cube.save()

        logger.info(f"[GENERATE_FOR_TABLE] Created/updated CUBE: {cube_id}")

        # Link mappings to cube via MAPPING_TO_CUBE
        today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        far_future = datetime.datetime(9999, 12, 31, 0, 0, 0)

        # Normalize version for use in IDs (replace dots with underscores)
        version_normalized = version.replace('.', '_')

        # Get table to check if it has Z-axis suffix
        table = TABLE.objects.get(table_id=table_id)
        from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
            extract_z_axis_suffix, extract_base_table_code
        )
        z_axis_suffix = extract_z_axis_suffix(table_id)

        # Build table_code_with_suffix for consistent cube_mapping_id generation
        # This ensures both primary tables and replicated Z-axis variants use the same pattern
        # First, extract the base table code (without any existing Z-axis suffix)
        # This prevents double-suffix issues when table_code already contains the suffix
        base_table_code = extract_base_table_code(table_id, table_code)
        # Normalize: replace spaces AND dots with underscores for consistent IDs
        table_code_normalized = base_table_code.replace(" ", "_").replace(".", "_")
        table_code_with_suffix = table_code_normalized + z_axis_suffix if z_axis_suffix else table_code_normalized

        logger.info(f"[GENERATE_STRUCTURES] Primary table cube_mapping_id generation:")
        logger.info(f"  table_id: {table_id}")
        logger.info(f"  table_code from session: {table_code}")
        logger.info(f"  z_axis_suffix: {z_axis_suffix}")
        logger.info(f"  final table_code_with_suffix: {table_code_with_suffix}")

        for mapping_info in created_mapping_definitions:
            mapping_def = mapping_info['mapping_definition']
            # Generate cube_mapping_id: M_{table_code}_REF_{framework}_{version_normalized}
            # Now includes Z-axis suffix if table is a Z-axis variant
            cube_mapping_id = f"M_{table_code_with_suffix}_REF_{framework}_{version_normalized}"

            logger.info(f"  Creating MAPPING_TO_CUBE: {cube_mapping_id} -> {mapping_def.mapping_id}")

            MAPPING_TO_CUBE.objects.create(
                cube_mapping_id=cube_mapping_id,
                mapping_id=mapping_def,
                valid_from=today_start,
                valid_to=far_future
            )

        # Create COMBINATIONs (table already loaded above)
        from pybirdai.models.bird_meta_data_model import AXIS, AXIS_ORDINATE, CELL_POSITION

        table_axes = AXIS.objects.filter(table_id=table)
        table_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=table_axes)
        all_cell_positions = CELL_POSITION.objects.filter(axis_ordinate_id__in=table_ordinates)
        cell_ids = all_cell_positions.values_list('cell_id', flat=True).distinct()
        cells = TABLE_CELL.objects.filter(cell_id__in=cell_ids)

        logger.info(f"[GENERATE_FOR_TABLE] Found {cells.count()} cells for table {table_id}")

        # Filter cells by selected ordinates if provided
        # Use AND logic across axes: cells must match selected ordinates in EACH axis
        if selected_ordinates:
            # Group selected ordinates by their axis
            from collections import defaultdict
            ordinates_by_axis = defaultdict(list)
            selected_ordinate_objs = AXIS_ORDINATE.objects.filter(
                axis_ordinate_id__in=selected_ordinates
            )
            for ordinate in selected_ordinate_objs:
                if ordinate.axis_id:
                    ordinates_by_axis[ordinate.axis_id_id].append(ordinate.axis_ordinate_id)

            logger.info(f'[GENERATE_FOR_TABLE] Selected ordinates grouped by {len(ordinates_by_axis)} axes')

            # For each axis, find cells with matching ordinates, then intersect
            filtered_cell_sets = []
            for axis_id, axis_ordinates in ordinates_by_axis.items():
                cells_for_axis = set(CELL_POSITION.objects.filter(
                    axis_ordinate_id__in=axis_ordinates,
                    cell_id__in=cells
                ).values_list('cell_id', flat=True).distinct())
                filtered_cell_sets.append(cells_for_axis)
                logger.info(f'[GENERATE_FOR_TABLE] Axis {axis_id}: {len(axis_ordinates)} ordinates -> {len(cells_for_axis)} cells')

            # Intersect all sets - cells must match in ALL axes
            if filtered_cell_sets:
                final_cell_ids = filtered_cell_sets[0]
                for cell_set in filtered_cell_sets[1:]:
                    final_cell_ids = final_cell_ids.intersection(cell_set)
                cells = cells.filter(cell_id__in=final_cell_ids)
            logger.info(f"[GENERATE_FOR_TABLE] Filtered to {cells.count()} cells (intersection of {len(ordinates_by_axis)} axes)")

        # Generate combinations
        generation_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        combination_creator = CombinationCreator(table_code, version_normalized)
        created_combinations = []
        created_cube_to_combinations = []

        for cell in cells:
            combination = combination_creator.create_combination_for_cell(
                cell, cube, generation_timestamp
            )
            if combination:
                created_combinations.append(combination)
                cube_to_combo, _ = CUBE_TO_COMBINATION.objects.get_or_create(
                    cube_id=cube,
                    combination_id=combination
                )
                created_cube_to_combinations.append(cube_to_combo)

        logger.info(f"[GENERATE_FOR_TABLE] Created {len(created_combinations)} combinations")

        # Domains and members were already ensured in Phase 3 (before cube structure items)
        # No need to validate them again here

        logger.info(f"[GENERATE_FOR_TABLE] Successfully completed generation for {table_id}")

        return {
            'success': True,
            'mapping_definitions': created_mapping_definitions,
            'member_mappings': created_member_mappings,  # Return for debug_data tracking
            'cube_structure': cube_structure,
            'cube': cube,
            'combinations': created_combinations,
            'cube_to_combinations': created_cube_to_combinations
        }

    except Exception as e:
        logger.error(f"[GENERATE_FOR_TABLE] Error generating structures for {table_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }


@transaction.atomic
def generate_structures(request):
    """
    Step 7: Generate all the required structures for multiple mappings.
    Creates multiple MAPPING_DEFINITIONs, one shared CUBE, and all related structures.
    Supports replication to multiple deduplicated tables.
    """
    # ========== DEBUG: Entry Point Logging ==========
    logger.info("[STEP 7 ENTRY] Received request to generate_structures")
    logger.info(f"[STEP 7 ENTRY] Session keys on entry:")
    logger.info(f"  olmw_regenerate_mode = {request.session.get('olmw_regenerate_mode')}")
    logger.info(f"  olmw_existing_mapping_ids = {request.session.get('olmw_existing_mapping_ids')}")
    logger.info(f"  olmw_table_id = {request.session.get('olmw_table_id')}")
    logger.info(f"  olmw_framework = {request.session.get('olmw_framework')}")
    logger.info(f"  olmw_version = {request.session.get('olmw_version')}")
    logger.info(f"  olmw_table_code = {request.session.get('olmw_table_code')}")
    logger.info(f"  olmw_replicate_to_variants = {request.session.get('olmw_replicate_to_variants')}")
    logger.info(f"  olmw_table_variants = {request.session.get('olmw_table_variants')}")
    logger.info(f"  olmw_mapping_mode = {request.session.get('olmw_mapping_mode')}")

    # Initialize debug data tracking (conditionally based on settings)
    from django.conf import settings
    debug_export_enabled = getattr(settings, 'DEBUG_EXPORT_ENABLED', False)

    if debug_export_enabled:
        debug_data = {
            'MAPPING_DEFINITION': [],
            'VARIABLE_MAPPING': [],
            'VARIABLE_MAPPING_ITEM': [],
            'MEMBER_MAPPING': [],
            'MEMBER_MAPPING_ITEM': [],
            'MAPPING_TO_CUBE': [],
            'CUBE_STRUCTURE': [],
            'CUBE_STRUCTURE_ITEM': [],
            'CUBE': [],
            'COMBINATION': [],
            'COMBINATION_ITEM': [],
            'CUBE_TO_COMBINATION': [],
            'SUBDOMAIN': [],
            'SUBDOMAIN_ENUMERATION': [],
            'FRAMEWORK': [],
            'MAINTENANCE_AGENCY': [],
            'MEMBER': [],
        }
        logger.info("[DEBUG TRACKING] Debug export ENABLED - tracking all created objects")
    else:
        debug_data = None  # Skip debug data collection to save memory
        logger.info("[DEBUG TRACKING] Debug export DISABLED - skipping object tracking for memory optimization")

    # Check if we're in regenerate mode or batch edit mode
    regenerate_mode = request.session.get('olmw_regenerate_mode', False)
    batch_edit_mode = request.session.get('olmw_batch_edit_mode', False)
    logger.info(f"[STEP 7 MODE CHECK] regenerate_mode = {regenerate_mode}, batch_edit_mode = {batch_edit_mode}")

    # ========== BATCH EDIT MODE: DELETE OLD MAPPINGS BEFORE PROCEEDING ==========
    if batch_edit_mode and request.method == 'POST' and request.POST.get('confirm') == 'true':
        batch_edit_mapping_ids = request.session.get('olmw_batch_edit_mapping_ids', [])
        if batch_edit_mapping_ids:
            logger.info(f"[STEP 7 BATCH EDIT] Deleting {len(batch_edit_mapping_ids)} old mapping(s) and related records")

            # First, delete cube artifacts (CUBE_STRUCTURE_ITEM, CUBE, etc.) using shared function
            deletion_stats = delete_mapping_artifacts(batch_edit_mapping_ids)
            logger.info(f"[STEP 7 BATCH EDIT] Deleted cube artifacts: {deletion_stats}")

            # Delete old mappings and all related records
            deleted_counts = {
                'MAPPING_ORDINATE_LINK': 0,
                'MAPPING_TO_CUBE': 0,
                'MEMBER_MAPPING_ITEM': 0,
                'MEMBER_MAPPING': 0,
                'VARIABLE_MAPPING_ITEM': 0,
                'VARIABLE_MAPPING': 0,
                'MAPPING_DEFINITION': 0
            }

            for mapping_id in batch_edit_mapping_ids:
                try:
                    mapping = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)

                    # Delete MAPPING_ORDINATE_LINK records
                    mol_count = MAPPING_ORDINATE_LINK.objects.filter(mapping_id=mapping).delete()[0]
                    deleted_counts['MAPPING_ORDINATE_LINK'] += mol_count

                    # Delete MAPPING_TO_CUBE records
                    mtc_count = MAPPING_TO_CUBE.objects.filter(mapping_id=mapping).delete()[0]
                    deleted_counts['MAPPING_TO_CUBE'] += mtc_count

                    # Delete MEMBER_MAPPING_ITEM and MEMBER_MAPPING
                    if mapping.member_mapping_id:
                        mmi_count = MEMBER_MAPPING_ITEM.objects.filter(
                            member_mapping_id=mapping.member_mapping_id
                        ).delete()[0]
                        deleted_counts['MEMBER_MAPPING_ITEM'] += mmi_count

                        mm_count = MEMBER_MAPPING.objects.filter(
                            member_mapping_id=mapping.member_mapping_id.member_mapping_id
                        ).delete()[0]
                        deleted_counts['MEMBER_MAPPING'] += mm_count

                    # Delete VARIABLE_MAPPING_ITEM and VARIABLE_MAPPING
                    if mapping.variable_mapping_id:
                        vmi_count = VARIABLE_MAPPING_ITEM.objects.filter(
                            variable_mapping_id=mapping.variable_mapping_id
                        ).delete()[0]
                        deleted_counts['VARIABLE_MAPPING_ITEM'] += vmi_count

                        vm_count = VARIABLE_MAPPING.objects.filter(
                            variable_mapping_id=mapping.variable_mapping_id.variable_mapping_id
                        ).delete()[0]
                        deleted_counts['VARIABLE_MAPPING'] += vm_count

                    # Delete MAPPING_DEFINITION
                    mapping.delete()
                    deleted_counts['MAPPING_DEFINITION'] += 1

                except MAPPING_DEFINITION.DoesNotExist:
                    logger.warning(f"[STEP 7 BATCH EDIT] Mapping {mapping_id} not found, skipping deletion")

            logger.info(f"[STEP 7 BATCH EDIT] Deleted old records: {deleted_counts}")

            # Batch edit mode now proceeds as normal mode (create new mappings)
            # Clear the batch_edit_mode flag so it doesn't interfere with future runs
            # (The new mappings will be created in Phase 3)

    # ========== CREATE NEW MODE: DELETE OLD MAPPINGS BEFORE PROCEEDING ==========
    # When user chose "Create New Mappings" in Step 2, delete existing mappings first
    delete_existing_mapping_ids = request.session.get('olmw_delete_existing_mapping_ids', [])
    if delete_existing_mapping_ids and request.method == 'POST' and request.POST.get('confirm') == 'true':
        logger.info(f"[STEP 7 CREATE NEW] Deleting {len(delete_existing_mapping_ids)} existing mapping(s) before creating new ones")

        deleted_counts = {
            'MAPPING_ORDINATE_LINK': 0,
            'MAPPING_TO_CUBE': 0,
            'MEMBER_MAPPING_ITEM': 0,
            'MEMBER_MAPPING': 0,
            'VARIABLE_MAPPING_ITEM': 0,
            'VARIABLE_MAPPING': 0,
            'MAPPING_DEFINITION': 0
        }

        for mapping_id in delete_existing_mapping_ids:
            try:
                mapping = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)

                # Delete MAPPING_ORDINATE_LINK records
                mol_count = MAPPING_ORDINATE_LINK.objects.filter(mapping_id=mapping).delete()[0]
                deleted_counts['MAPPING_ORDINATE_LINK'] += mol_count

                # Delete MAPPING_TO_CUBE records
                mtc_count = MAPPING_TO_CUBE.objects.filter(mapping_id=mapping).delete()[0]
                deleted_counts['MAPPING_TO_CUBE'] += mtc_count

                # Delete MEMBER_MAPPING_ITEM and MEMBER_MAPPING
                if mapping.member_mapping_id:
                    mmi_count = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mapping.member_mapping_id
                    ).delete()[0]
                    deleted_counts['MEMBER_MAPPING_ITEM'] += mmi_count

                    mm_count = MEMBER_MAPPING.objects.filter(
                        member_mapping_id=mapping.member_mapping_id.member_mapping_id
                    ).delete()[0]
                    deleted_counts['MEMBER_MAPPING'] += mm_count

                # Delete VARIABLE_MAPPING_ITEM and VARIABLE_MAPPING
                if mapping.variable_mapping_id:
                    vmi_count = VARIABLE_MAPPING_ITEM.objects.filter(
                        variable_mapping_id=mapping.variable_mapping_id
                    ).delete()[0]
                    deleted_counts['VARIABLE_MAPPING_ITEM'] += vmi_count

                    vm_count = VARIABLE_MAPPING.objects.filter(
                        variable_mapping_id=mapping.variable_mapping_id.variable_mapping_id
                    ).delete()[0]
                    deleted_counts['VARIABLE_MAPPING'] += vm_count

                # Delete MAPPING_DEFINITION
                mapping.delete()
                deleted_counts['MAPPING_DEFINITION'] += 1

            except MAPPING_DEFINITION.DoesNotExist:
                logger.warning(f"[STEP 7 CREATE NEW] Mapping {mapping_id} not found, skipping deletion")

        logger.info(f"[STEP 7 CREATE NEW] Deleted old mappings: {deleted_counts}")

        # Clear the session flag
        request.session.pop('olmw_delete_existing_mapping_ids', None)

    if regenerate_mode:
        # Regenerate mode: use existing mappings, skip creation of new MAPPING_DEFINITION
        existing_mapping_ids = request.session.get('olmw_existing_mapping_ids', [])
        logger.info(f"[STEP 7 REGENERATE] Retrieved existing_mapping_ids: {existing_mapping_ids}")

        if not existing_mapping_ids:
            logger.error("[STEP 7 REGENERATE] No existing_mapping_ids found in session - REDIRECTING TO STEP 2")
            messages.error(request, 'No mappings found for regeneration.')
            return redirect('pybirdai:output_layer_mapping_step2')

        # Get existing mappings
        existing_mappings = MAPPING_DEFINITION.objects.filter(mapping_id__in=existing_mapping_ids)

        # Retrieve session data
        table_id = request.session['olmw_table_id']
        framework = request.session['olmw_framework']
        version = request.session['olmw_version']
        table_code = request.session['olmw_table_code']
        version_normalized = version.replace('.', '_')

        # We'll use a simplified regeneration flow
        messages.info(request, f'Regeneration mode detected for {len(existing_mappings)} mapping(s). Click confirm to regenerate output layer.')

        # Use existing mappings list for now
        created_mapping_definitions = [
            {'mapping_definition': m, 'name': m.name, 'internal_id': m.code}
            for m in existing_mappings
        ]

    else:
        # Normal mode: Check session
        if 'olmw_multi_mappings' not in request.session:
            messages.error(request, 'Please complete the review step first.')
            return redirect('pybirdai:output_layer_mapping_step6')

        # Retrieve all session data
        table_id = request.session['olmw_table_id']
        framework = request.session['olmw_framework']
        version = request.session['olmw_version']
        table_code = request.session['olmw_table_code']
        all_mappings = json.loads(request.session['olmw_multi_mappings'])

        # Initialize created_mapping_definitions for normal mode
        # Will be populated after Phase 3 execution
        created_mapping_definitions = []

    if request.method == 'POST' and request.POST.get('confirm') == 'true':
        # Check if table replication is requested (from form submission or from session)
        replicate_to_tables = request.POST.getlist('replicate_to_tables[]')

        # Also check for selected Z-tables from session (set in Step 5)
        selected_z_tables = request.session.get('olmw_selected_z_tables', [])
        if selected_z_tables:
            # Add Z-tables that aren't already in replicate_to_tables and aren't the current table
            for z_table_id in selected_z_tables:
                if z_table_id != table_id and z_table_id not in replicate_to_tables:
                    replicate_to_tables.append(z_table_id)
            logger.info(f"[STEP 7 Z-AXIS] Added {len(selected_z_tables)} selected Z-variant tables to replication list")

        # AUTO-SELECT: If no Z-tables selected by user, automatically select ALL Z-axis siblings
        # This ensures MAPPING_TO_CUBE records are created for all variants
        if not selected_z_tables:
            from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
                is_deduplicated_table,
                get_z_axis_sibling_tables
            )
            if is_deduplicated_table(table_id):
                siblings = get_z_axis_sibling_tables(table_id)
                auto_selected = [s.table_id for s in siblings if s.table_id != table_id]
                for sibling_id in auto_selected:
                    if sibling_id not in replicate_to_tables:
                        replicate_to_tables.append(sibling_id)
                logger.info(f"[STEP 7 AUTO-SELECT] Automatically selected {len(auto_selected)} Z-axis siblings for replication")

        # REGENERATE MODE: Check if Step 2 bulk apply requested replication to variants
        # This handles the "Apply to All Variants" slider from Step 2
        if regenerate_mode and request.session.get('olmw_replicate_to_variants'):
            table_variants = request.session.get('olmw_table_variants', [])
            variants_added = 0
            for variant_id in table_variants:
                if variant_id != table_id and variant_id not in replicate_to_tables:
                    replicate_to_tables.append(variant_id)
                    variants_added += 1
            if variants_added > 0:
                logger.info(f"[STEP 7 REGENERATE] Added {variants_added} table variants from Step 2 'Apply to All Variants' slider")

        all_tables_to_process = [table_id]  # Start with current table

        if replicate_to_tables:
            all_tables_to_process.extend(replicate_to_tables)
            logger.info(f"[STEP 7 REPLICATION] Will process {len(all_tables_to_process)} tables total: {all_tables_to_process}")

        try:
            # In regenerate mode, delete old artifacts BEFORE creating new ones
            if regenerate_mode:
                existing_mapping_ids = request.session.get('olmw_existing_mapping_ids', [])
                if existing_mapping_ids:
                    deletion_stats = delete_mapping_artifacts(existing_mapping_ids)
                    logger.info(f"[STEP 7 REGENERATE] Deleted artifacts: {deletion_stats}")

            # Initialize orchestrator (kept for potential future use)
            orchestrator = OutputLayerMappingOrchestrator()

            # ========================================================================
            # PHASE-BASED TRANSACTION EXECUTION WITH SAVEPOINTS
            # Each phase is executed with its own savepoint and FK validation
            # ========================================================================

            # Initialize phase executor
            executor = PhaseExecutor()
            logger.info("[STEP 7] Initialized PhaseExecutor for savepoint-based execution")

            # ========== EXTRACT TARGET VARIABLES EARLY (needed for Phase 2) ==========
            dimension_target_vars = []
            observation_target_vars = []
            attribute_target_vars = []

            if regenerate_mode:
                # Extract from existing mappings
                for mapping_info in created_mapping_definitions:
                    mapping_def = mapping_info['mapping_definition']
                    if mapping_def.variable_mapping_id:
                        vm_items = VARIABLE_MAPPING_ITEM.objects.filter(
                            variable_mapping_id=mapping_def.variable_mapping_id,
                            is_source='false'
                        ).select_related('variable_id')

                        for item in vm_items:
                            if item.variable_id:
                                var = item.variable_id
                                # Determine type from variable or mapping context
                                # For now, assume all are dimensions (can be refined)
                                dimension_target_vars.append(var)
            else:
                # Extract from session variable_groups
                variable_groups = json.loads(request.session['olmw_variable_groups'])
                for group_id, group_data in variable_groups.items():
                    group_type = group_data.get('group_type', 'dimension').lower()
                    target_var_ids = group_data.get('targets', [])

                    if target_var_ids:
                        target_vars = list(VARIABLE.objects.filter(variable_id__in=target_var_ids))

                        if group_type == 'dimension':
                            dimension_target_vars.extend(target_vars)
                        elif group_type == 'observation':
                            observation_target_vars.extend(target_vars)
                        elif group_type == 'attribute':
                            attribute_target_vars.extend(target_vars)

            # Get unique target variables for Phase 2
            unique_dimension_vars = {v.variable_id: v for v in dimension_target_vars}.values()
            unique_observation_vars = {v.variable_id: v for v in observation_target_vars}.values()
            unique_attribute_vars = {v.variable_id: v for v in attribute_target_vars}.values()
            unique_target_vars = list(unique_dimension_vars) + list(unique_observation_vars) + list(unique_attribute_vars)

            logger.info(f"[STEP 7] Extracted {len(unique_target_vars)} unique target variables "
                       f"({len(unique_dimension_vars)} dims, {len(unique_observation_vars)} obs, "
                       f"{len(unique_attribute_vars)} attrs)")

            # ========== PHASE 1: BASE SETUP ==========
            phase1_result = executor.execute_phase(
                "Phase 1: Base Setup",
                lambda: execute_phase1_base_setup(framework, debug_data),
                debug_data
            )
            framework_obj = phase1_result['framework']
            maintenance_agency = phase1_result['maintenance_agencies']['USER']

            # ========== PHASE 2: DOMAINS & MEMBERS ==========
            # Extract all_mappings for Phase 2 and 3
            all_mappings = json.loads(request.session['olmw_multi_mappings']) if not regenerate_mode else {}

            phase2_result = executor.execute_phase(
                "Phase 2: Domains & Members",
                lambda: execute_phase2_domains_members(
                    all_mappings,
                    created_mapping_definitions,
                    unique_target_vars,
                    maintenance_agency,
                    regenerate_mode,
                    debug_data
                ),
                debug_data
            )
            variable_to_members_map = phase2_result['variable_to_members_map']

            # ========== PHASE 3: MAPPINGS (Normal mode only) ==========
            if not regenerate_mode:
                # Version normalization for Phase 3
                version_normalized = version.replace('.', '_')

                # Phase 3 will create mapping definitions and return them
                created_mapping_definitions = executor.execute_phase(
                    "Phase 3: Mappings",
                    lambda: execute_phase3_mappings(
                        request,
                        table_code,
                        version,
                        None,  # cube not created yet, will be handled in Phase 3
                        maintenance_agency,
                        list(dimension_target_vars),
                        list(observation_target_vars),
                        list(attribute_target_vars),
                        debug_data
                    ),
                    debug_data
                )
            else:
                # Regenerate mode: Skip Phase 3, use existing mappings
                version_normalized = version.replace('.', '_')
                logger.info(f"[STEP 7] Skipping Phase 3 in regenerate mode - using {len(created_mapping_definitions)} existing mappings")

            # ========== DELETE EXISTING OUTPUT LAYERS BEFORE REGENERATING ==========
            # This ensures we don't have duplicate CUBE_STRUCTURE_ITEMs, COMBINATIONs, etc.
            logger.info(f"[STEP 7] Deleting existing output layers for table {table_id} before generating new ones")
            deletion_stats = delete_output_layers_by_table(table_id)
            if any(v > 0 for v in deletion_stats.values()):
                logger.info(f"[STEP 7] Deleted old output layers: {deletion_stats}")
            else:
                logger.info(f"[STEP 7] No existing output layers to delete for {table_id}")

            # ========== PHASE 4: CUBE STRUCTURES & SUBDOMAINS ==========
            phase4_result = executor.execute_phase(
                "Phase 4: Cube Structures",
                lambda: execute_phase4_cube_structures(
                    table_id,
                    table_code,
                    version,
                    framework_obj,
                    maintenance_agency,
                    list(dimension_target_vars),
                    list(observation_target_vars),
                    list(attribute_target_vars),
                    created_mapping_definitions,
                    debug_data
                ),
                debug_data
            )
            cube_structure = phase4_result['cube_structure']
            cube = phase4_result['cube']

            # ========== PHASE 3.5: MAPPING_TO_CUBE LINKS (if in normal mode) ==========
            # This was part of Phase 3 but needs cube, so we do it here
            if not regenerate_mode:
                from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
                    extract_z_axis_suffix, extract_base_table_code
                )

                # Get Z-axis suffix for consistent naming
                z_axis_suffix = extract_z_axis_suffix(table_id)
                # First, extract the base table code (without any existing Z-axis suffix)
                # This prevents double-suffix issues when table_code already contains the suffix
                base_table_code = extract_base_table_code(table_id, table_code)
                # Normalize: replace spaces AND dots with underscores for consistent IDs
                table_code_normalized = base_table_code.replace(" ", "_").replace(".", "_")
                table_code_with_suffix = table_code_normalized + z_axis_suffix if z_axis_suffix else table_code_normalized

                # Delete old MAPPING_TO_CUBE records (use pattern matching to catch variations)
                # Search for both old (partial normalization) and new (full normalization) patterns
                table_code_partial = table_code.replace(" ", "_")  # Old format: C_07.00.a
                cube_mapping_id_pattern = f"M_{table_code_partial}"  # Match old format
                cube_mapping_id_pattern_new = f"M_{table_code_normalized}"  # Match new format

                # Delete old records matching either pattern
                old_mapping_to_cube = MAPPING_TO_CUBE.objects.filter(
                    models.Q(cube_mapping_id__icontains=cube_mapping_id_pattern) |
                    models.Q(cube_mapping_id__icontains=cube_mapping_id_pattern_new)
                )
                old_count = old_mapping_to_cube.count()
                if old_count > 0:
                    old_mapping_to_cube.delete()
                    logger.info(f"[STEP 7] Deleted {old_count} old MAPPING_TO_CUBE record(s)")

                # Create new MAPPING_TO_CUBE records
                today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                far_future = datetime.datetime(9999, 12, 31, 0, 0, 0)

                # Normalize version for use in IDs (replace dots with underscores)
                version_normalized = version.replace('.', '_')

                for mapping_info in created_mapping_definitions:
                    mapping_def = mapping_info['mapping_definition']
                    # Generate cube_mapping_id: M_{table_code}_REF_{framework}_{version_normalized}
                    # Now includes Z-axis suffix if table is a Z-axis variant
                    cube_mapping_id = f"M_{table_code_with_suffix}_REF_{framework}_{version_normalized}"

                    logger.info(f"[STEP 7] Creating MAPPING_TO_CUBE: {cube_mapping_id} -> {mapping_def.mapping_id}")

                    mtc = MAPPING_TO_CUBE.objects.create(
                        cube_mapping_id=cube_mapping_id,
                        mapping_id=mapping_def,
                        valid_from=today_start,
                        valid_to=far_future
                    )
                    if debug_data is not None:
                        debug_data['MAPPING_TO_CUBE'].append(mtc)

            # ========== PHASE 5: COMBINATIONS ==========
            phase5_result = executor.execute_phase(
                "Phase 5: Combinations",
                lambda: execute_phase5_combinations(
                    request,
                    table_id,
                    table_code,
                    version_normalized,
                    cube,
                    debug_data
                ),
                debug_data
            )
            created_combinations = phase5_result['created_combinations']

            logger.info(f"[STEP 7] All 5 phases completed successfully")
            logger.info(f"[STEP 7] Completed phases: {executor.get_completed_phases()}")

            # Save table_id before clearing session (needed for regenerate feature)
            saved_table_id = table_id

            # ========== GENERATE REFERENCE TABLE ARTIFACTS ==========
            # Create a reference table with only the selected ordinates
            # Pass mapping definitions so reference table shows mapped variables/members
            selected_ordinates = request.session.get('olmw_selected_ordinates', [])
            reference_table_result = generate_reference_table_artifacts(
                source_table_id=table_id,
                selected_ordinates=selected_ordinates,
                framework=framework,
                version=version,
                maintenance_agency=maintenance_agency,
                mapping_definitions=created_mapping_definitions
            )

            if reference_table_result['success']:
                reference_table_id = reference_table_result['reference_table_id']
                logger.info(f"[STEP 7] Generated reference table: {reference_table_id}")
            else:
                reference_table_id = saved_table_id  # Fallback to DPM table
                logger.warning(f"[STEP 7] Failed to generate reference table: {reference_table_result.get('error', 'Unknown error')}")

            # Prepare success context
            context = {
                'success': True,
                'mappings_created': len(created_mapping_definitions),
                'mapping_names': [m['name'] for m in created_mapping_definitions],
                'generated': {
                    'mapping_definitions': [
                        {
                            'id': m['mapping_definition'].code,
                            'name': m['name'],
                            'type': m['mapping_definition'].mapping_type,
                            'member_mapping_id': m['mapping_definition'].member_mapping_id
                        }
                        for m in created_mapping_definitions
                    ],
                    'cube_structure_id': cube_structure.cube_structure_id,
                    'cube_id': cube.cube_id,
                    'combinations_created': len(created_combinations),
                    'table_id': saved_table_id,  # For regenerate feature
                    'reference_table_id': reference_table_id  # For annotated template viewer
                },
                'conflicts': [],
                'conflicts_json': '[]',
                'step': 7,
                'total_steps': 7,
                'batch_edit_mode': batch_edit_mode,
                'batch_edit_completed': batch_edit_mode  # Flag to show special success message
            }

            # Handle table replication if requested
            replication_results = []
            if replicate_to_tables:
                logger.info(f"[STEP 7 REPLICATION] Starting replication to {len(replicate_to_tables)} additional tables")

                # Store the original configuration
                original_config = {
                    'variable_groups': json.loads(request.session.get('olmw_variable_groups', '{}')),
                    'multi_mappings': json.loads(request.session.get('olmw_multi_mappings', '{}')),
                    'selected_ordinates': request.session.get('olmw_selected_ordinates', [])
                }

                # Parse Z-axis member mappings from form data
                z_member_mappings = {}
                for key in request.POST.keys():
                    if key.startswith('z_member_mapping['):
                        # Extract table_id from key like 'z_member_mapping[F_01_00_EBA_EC_EBA_qx51]'
                        table_id_match = key[len('z_member_mapping['):-1]
                        member_id = request.POST[key]
                        z_member_mappings[table_id_match] = member_id
                        logger.info(f"[STEP 7 REPLICATION] Parsed Z-axis member mapping: {table_id_match} -> {member_id}")

                # Detect Z-axis variable and domain from variable_groups
                # The Z-axis variable is typically in a dimension group with axis_orientation='Z' or '3'
                z_axis_variable_id = None
                z_axis_domain_id = None
                variable_groups = original_config['variable_groups']

                for group_id, group_info in variable_groups.items():
                    # Check if this group contains Z-axis variables
                    variable_ids = group_info.get('variable_ids', [])
                    for var_id in variable_ids:
                        try:
                            variable = VARIABLE.objects.get(variable_id=var_id)
                            # Check if this variable is associated with Z-axis ordinates
                            # by examining the ordinate data or axis orientation
                            # For now, we'll check if the variable domain matches typical Z-axis patterns
                            if variable.domain_id:
                                domain = variable.domain_id
                                # Z-axis domains typically have codes like 'EBA_EC' (Exposure Class)
                                if 'EC' in domain.domain_id or 'XPSR' in domain.domain_id:
                                    z_axis_variable_id = var_id
                                    z_axis_domain_id = domain.domain_id
                                    logger.info(f"[STEP 7 REPLICATION] Detected Z-axis variable: {z_axis_variable_id} (domain: {z_axis_domain_id})")
                                    break
                        except VARIABLE.DoesNotExist:
                            continue
                    if z_axis_variable_id:
                        break

                # If we couldn't detect Z-axis variable automatically, try to extract from source table
                if not z_axis_variable_id and table_id:
                    from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
                        extract_z_axis_member_from_table_id
                    )
                    z_member = extract_z_axis_member_from_table_id(table_id)
                    if z_member:
                        # Look for variable with member matching this Z-axis member
                        for group_id, group_info in variable_groups.items():
                            variable_ids = group_info.get('variable_ids', [])
                            for var_id in variable_ids:
                                # Check if this variable has the Z-axis member in its mappings
                                for mapping_data in original_config['multi_mappings'].values():
                                    for dim_row in mapping_data.get('dimensions', []):
                                        if var_id in dim_row and dim_row[var_id] == z_member:
                                            z_axis_variable_id = var_id
                                            # Also get the domain_id
                                            try:
                                                var_obj = VARIABLE.objects.get(variable_id=var_id)
                                                if var_obj.domain_id:
                                                    z_axis_domain_id = var_obj.domain_id.domain_id
                                            except VARIABLE.DoesNotExist:
                                                pass
                                            logger.info(f"[STEP 7 REPLICATION] Detected Z-axis variable by member: {z_axis_variable_id} (domain: {z_axis_domain_id})")
                                            break
                                    if z_axis_variable_id:
                                        break
                                if z_axis_variable_id:
                                    break
                            if z_axis_variable_id:
                                break

                if not z_axis_variable_id:
                    logger.warning("[STEP 7 REPLICATION] Could not automatically detect Z-axis variable. Member replacement may not work correctly.")
                elif not z_axis_domain_id:
                    logger.warning("[STEP 7 REPLICATION] Z-axis variable detected but domain_id not found. Member ID resolution may fail.")

                # Get maintenance agency (reuse from main generation)
                repl_maintenance_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
                    maintenance_agency_id='USER',
                    defaults={'name': 'User Defined', 'code': 'USER'}
                )

                for repl_table_id in replicate_to_tables:
                    try:
                        # Get the target table and its code
                        repl_table = TABLE.objects.get(table_id=repl_table_id)

                        # Build table_code for cube_mapping_id generation
                        # Use simple table.code WITHOUT Z-axis suffix here - the suffix will be extracted
                        # from table_id and added inside generate_structures_for_table to avoid duplication
                        repl_table_code = repl_table.code

                        logger.info(f"[STEP 7 REPLICATION] Processing table {repl_table_id}")
                        logger.info(f"[STEP 7 REPLICATION] Using table_code: {repl_table_code} (base code only, suffix added by generate_structures_for_table)")

                        # Map ordinates from source table to target table
                        mapped_ordinates = map_ordinates_between_tables(
                            table_id,  # source table
                            repl_table_id,  # target table
                            original_config['selected_ordinates']
                        )

                        logger.info(f"[STEP 7 REPLICATION] Mapped {len(mapped_ordinates)} ordinates for {repl_table_id}")

                        # Get Z-axis member mapping for this table (if provided by user)
                        new_member_suffix = z_member_mappings.get(repl_table_id)

                        # Prepare mappings for this table
                        table_mappings = original_config['multi_mappings']

                        # If Z-axis member mapping was provided and we detected the Z-axis variable, update it
                        if new_member_suffix and z_axis_variable_id and z_axis_domain_id:
                            # Resolve full member_id from suffix and domain
                            from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
                                resolve_full_member_id
                            )
                            new_member_id = resolve_full_member_id(new_member_suffix, z_axis_domain_id)
                            logger.info(f"[STEP 7 REPLICATION] Resolved member ID: {new_member_suffix} + {z_axis_domain_id} -> {new_member_id}")
                            logger.info(f"[STEP 7 REPLICATION] Updating Z-axis member for {repl_table_id} to {new_member_id}")

                            table_mappings = update_z_axis_member_in_mappings(
                                original_config['multi_mappings'],
                                original_config['variable_groups'],
                                z_axis_variable_id,
                                new_member_id
                            )
                        elif new_member_suffix and not (z_axis_variable_id and z_axis_domain_id):
                            logger.warning(f"[STEP 7 REPLICATION] Z-axis member provided ({new_member_suffix}) but Z-axis variable/domain not detected. Using original mappings.")
                        else:
                            logger.info(f"[STEP 7 REPLICATION] No Z-axis member mapping provided for {repl_table_id}, using original mappings")

                        # Call the helper function to generate structures for this table
                        # Pass existing_mapping_definitions to REUSE mappings from primary table
                        # (instead of creating new ones for each variant)
                        result = generate_structures_for_table(
                            table_id=repl_table_id,
                            table_code=repl_table_code,
                            framework=framework,
                            version=version,
                            variable_groups=original_config['variable_groups'],
                            all_mappings=table_mappings,  # Use updated mappings
                            selected_ordinates=mapped_ordinates,
                            maintenance_agency=repl_maintenance_agency,
                            regenerate_mode=False,
                            existing_mapping_definitions=created_mapping_definitions  # Reuse primary table's mappings
                        )

                        if result['success']:
                            # Add replicated combinations to main tracking
                            created_combinations.extend(result['combinations'])
                            # Add replicated objects to debug_data (if enabled)
                            if debug_data is not None:
                                debug_data['CUBE_TO_COMBINATION'].extend(result.get('cube_to_combinations', []))
                                if result.get('cube') and result['cube'] not in debug_data['CUBE']:
                                    debug_data['CUBE'].append(result['cube'])
                                if result.get('cube_structure') and result['cube_structure'] not in debug_data['CUBE_STRUCTURE']:
                                    debug_data['CUBE_STRUCTURE'].append(result['cube_structure'])
                                for member_mapping in result.get('member_mappings', []):
                                    if member_mapping not in debug_data['MEMBER_MAPPING']:
                                        debug_data['MEMBER_MAPPING'].append(member_mapping)

                            replication_results.append({
                                'table_id': repl_table_id,
                                'table_code': repl_table_code,
                                'status': 'success',
                                'message': f'Generated {len(result["mapping_definitions"])} mappings, {len(result["combinations"])} combinations',
                                'mapping_count': len(result['mapping_definitions']),
                                'combination_count': len(result['combinations']),
                                'cube_id': result['cube'].cube_id
                            })
                            logger.info(f"[STEP 7 REPLICATION] Successfully generated structures for {repl_table_id}")
                        else:
                            replication_results.append({
                                'table_id': repl_table_id,
                                'table_code': repl_table_code,
                                'status': 'error',
                                'message': result.get('error', 'Unknown error')
                            })

                    except Exception as repl_error:
                        logger.error(f"[STEP 7 REPLICATION] Error processing {repl_table_id}: {str(repl_error)}")
                        import traceback
                        traceback.print_exc()
                        replication_results.append({
                            'table_id': repl_table_id,
                            'status': 'error',
                            'message': str(repl_error)
                        })

                # Add replication info to context
                context['replication_results'] = replication_results

                successful_replications = sum(1 for r in replication_results if r['status'] == 'success')
                if successful_replications > 0:
                    total_mappings = sum(r.get('mapping_count', 0) for r in replication_results if r['status'] == 'success')
                    total_combinations = sum(r.get('combination_count', 0) for r in replication_results if r['status'] == 'success')
                    messages.success(request, f'Successfully replicated to {successful_replications} additional table(s): {total_mappings} mappings, {total_combinations} combinations created')

                failed_replications = sum(1 for r in replication_results if r['status'] == 'error')
                if failed_replications > 0:
                    messages.warning(request, f'{failed_replications} table(s) failed during replication. Check logs for details.')

            # Collect debug data for all created objects (only if debug export is enabled)
            if debug_data is not None:
                # Collect CUBE_STRUCTURE_ITEMs for ALL cube_structures (original + replicated)
                existing_item_ids = {item.pk for item in debug_data['CUBE_STRUCTURE_ITEM']}
                for cube_struct in debug_data['CUBE_STRUCTURE']:
                    cube_structure_items = CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_id=cube_struct)
                    for item in cube_structure_items:
                        if item.pk not in existing_item_ids:
                            debug_data['CUBE_STRUCTURE_ITEM'].append(item)
                            existing_item_ids.add(item.pk)
                logger.info(f"[DEBUG TRACKING] Collected {len(debug_data['CUBE_STRUCTURE_ITEM'])} CUBE_STRUCTURE_ITEMs from {len(debug_data['CUBE_STRUCTURE'])} cube structures")

                # Collect all SUBDOMAINs and SUBDOMAIN_ENUMERATIONs created in this session
                session_subdomains = SUBDOMAIN.objects.filter(subdomain_id__contains=cube_structure.cube_structure_id)
                debug_data['SUBDOMAIN'].extend(list(session_subdomains))

                # Collect SUBDOMAIN_ENUMERATIONs for all subdomains in debug_data
                for subdomain_obj in debug_data['SUBDOMAIN']:
                    subdomain_enums = SUBDOMAIN_ENUMERATION.objects.filter(subdomain_id=subdomain_obj)
                    debug_data['SUBDOMAIN_ENUMERATION'].extend(list(subdomain_enums))

                # Collect VARIABLE_MAPPING_ITEMs for all VARIABLE_MAPPINGs
                existing_vmi_ids = {vmi.pk for vmi in debug_data['VARIABLE_MAPPING_ITEM']}
                for vm_obj in debug_data['VARIABLE_MAPPING']:
                    vmi_items = VARIABLE_MAPPING_ITEM.objects.filter(variable_mapping_id=vm_obj)
                    for vmi in vmi_items:
                        if vmi.pk not in existing_vmi_ids:
                            debug_data['VARIABLE_MAPPING_ITEM'].append(vmi)
                            existing_vmi_ids.add(vmi.pk)

                # Collect MEMBER_MAPPING_ITEMs for all MEMBER_MAPPINGs
                logger.info(f"[DEBUG TRACKING MEMBER_MAPPING_ITEM] Starting collection from {len(debug_data['MEMBER_MAPPING'])} MEMBER_MAPPINGs")
                existing_mmi_ids = {mmi.pk for mmi in debug_data['MEMBER_MAPPING_ITEM']}

                for mm_obj in debug_data['MEMBER_MAPPING']:
                    mmi_items = MEMBER_MAPPING_ITEM.objects.filter(member_mapping_id=mm_obj)
                    for mmi in mmi_items:
                        if mmi.pk not in existing_mmi_ids:
                            debug_data['MEMBER_MAPPING_ITEM'].append(mmi)
                            existing_mmi_ids.add(mmi.pk)

                logger.info(f"[DEBUG TRACKING] Collected {len(debug_data['VARIABLE_MAPPING_ITEM'])} VARIABLE_MAPPING_ITEMs")
                logger.info(f"[DEBUG TRACKING] Collected {len(debug_data['MEMBER_MAPPING_ITEM'])} MEMBER_MAPPING_ITEMs")
                logger.info(f"[DEBUG TRACKING] Tracked {len(debug_data['MAPPING_TO_CUBE'])} MAPPING_TO_CUBE records")

                # Collect COMBINATIONs
                for combo_info in created_combinations:
                    if isinstance(combo_info, dict):
                        combo = combo_info.get('combination')
                    else:
                        combo = combo_info
                    if combo:
                        debug_data['COMBINATION'].append(combo)

                # Collect COMBINATION_ITEMs for all combinations
                for combo in debug_data['COMBINATION']:
                    items = COMBINATION_ITEM.objects.filter(combination_id=combo)
                    debug_data['COMBINATION_ITEM'].extend(list(items))

                logger.info(f"[DEBUG TRACKING] Collected {len(debug_data['COMBINATION_ITEM'])} COMBINATION_ITEMs from {len(debug_data['COMBINATION'])} combinations")

            # Export debug data before transaction commit (if enabled)
            if debug_data is not None:
                try:
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    debug_folder = Path("debug_exports") / f"step7_generation_{timestamp}"
                    logger.info(f"[DEBUG EXPORT] Exporting {sum(len(v) for v in debug_data.values())} objects to {debug_folder}")

                    export_success = export_debug_data(debug_data, str(debug_folder))

                    if export_success:
                        logger.info(f"[DEBUG EXPORT] Debug data successfully exported to {debug_folder}")
                        messages.info(request, f'Debug data exported to {debug_folder}')
                    else:
                        logger.warning("[DEBUG EXPORT] Debug export encountered errors - check logs")

                except Exception as export_error:
                    logger.error(f"[DEBUG EXPORT] Failed to export debug data: {str(export_error)}")
                    # Don't fail the transaction if debug export fails

            # ========== PRE-COMMIT FK VALIDATION ==========
            # Validate all FK references before committing to catch specific FK errors
            fk_validation_errors = []

            logger.info("[STEP 7 PRE-COMMIT FK VALIDATION] Validating all foreign key references...")

            # Validate COMBINATION.metric FKs
            for combo_info in created_combinations:
                # Handle both dictionary format {'combination': combo} and raw COMBINATION objects
                if isinstance(combo_info, dict):
                    combo = combo_info.get('combination')
                else:
                    combo = combo_info  # Raw COMBINATION object from replication

                if combo and hasattr(combo, 'metric') and combo.metric:
                    if not VARIABLE.objects.filter(variable_id=combo.metric.variable_id).exists():
                        fk_validation_errors.append(
                            f"COMBINATION {combo.combination_id}: metric variable '{combo.metric.variable_id}' does not exist"
                        )

            # Validate COMBINATION_ITEM FKs
            for combo_info in created_combinations:
                # Handle both dictionary format {'combination': combo} and raw COMBINATION objects
                if isinstance(combo_info, dict):
                    combo = combo_info.get('combination')
                else:
                    combo = combo_info  # Raw COMBINATION object from replication

                if combo:
                    items = COMBINATION_ITEM.objects.filter(combination_id=combo)
                    for item in items:
                        if item.variable_id and not VARIABLE.objects.filter(variable_id=item.variable_id.variable_id).exists():
                            fk_validation_errors.append(
                                f"COMBINATION_ITEM in {combo.combination_id}: variable '{item.variable_id.variable_id}' does not exist"
                            )
                        if item.member_id and not MEMBER.objects.filter(member_id=item.member_id.member_id).exists():
                            fk_validation_errors.append(
                                f"COMBINATION_ITEM in {combo.combination_id}: member '{item.member_id.member_id}' does not exist"
                            )
                        if item.subdomain_id and not SUBDOMAIN.objects.filter(subdomain_id=item.subdomain_id.subdomain_id).exists():
                            fk_validation_errors.append(
                                f"COMBINATION_ITEM in {combo.combination_id}: subdomain '{item.subdomain_id.subdomain_id}' does not exist"
                            )

            # Validate CUBE_STRUCTURE_ITEM FKs
            csi_items = CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_id=cube_structure)
            for csi in csi_items:
                if csi.variable_id and not VARIABLE.objects.filter(variable_id=csi.variable_id.variable_id).exists():
                    fk_validation_errors.append(
                        f"CUBE_STRUCTURE_ITEM {csi.cube_structure_item_id}: variable '{csi.variable_id.variable_id}' does not exist"
                    )
                if csi.subdomain_id and not SUBDOMAIN.objects.filter(subdomain_id=csi.subdomain_id.subdomain_id).exists():
                    fk_validation_errors.append(
                        f"CUBE_STRUCTURE_ITEM {csi.cube_structure_item_id}: subdomain '{csi.subdomain_id.subdomain_id}' does not exist"
                    )

            # Validate CUBE.framework_id FK
            if cube.framework_id:
                if not FRAMEWORK.objects.filter(framework_id=cube.framework_id.framework_id).exists():
                    fk_validation_errors.append(
                        f"CUBE {cube.cube_id}: framework '{cube.framework_id.framework_id}' does not exist"
                    )
            else:
                fk_validation_errors.append(
                    f"CUBE {cube.cube_id}: framework_id is NULL (required)"
                )

            # Validate CUBE.cube_structure_id FK
            if cube.cube_structure_id:
                if not CUBE_STRUCTURE.objects.filter(cube_structure_id=cube.cube_structure_id.cube_structure_id).exists():
                    fk_validation_errors.append(
                        f"CUBE {cube.cube_id}: cube_structure '{cube.cube_structure_id.cube_structure_id}' does not exist"
                    )
            else:
                fk_validation_errors.append(
                    f"CUBE {cube.cube_id}: cube_structure_id is NULL (required)"
                )

            # Validate SUBDOMAIN_ENUMERATION FKs (only if debug tracking is enabled)
            if debug_data is not None:
                for sd_obj in debug_data.get('SUBDOMAIN', []):
                    if hasattr(sd_obj, 'subdomain_id'):
                        enums = SUBDOMAIN_ENUMERATION.objects.filter(subdomain_id=sd_obj)
                        for enum in enums:
                            if enum.member_id and not MEMBER.objects.filter(member_id=enum.member_id.member_id).exists():
                                fk_validation_errors.append(
                                    f"SUBDOMAIN_ENUMERATION in subdomain {sd_obj.subdomain_id}: member '{enum.member_id.member_id}' does not exist"
                                )

            if fk_validation_errors:
                logger.error("[STEP 7 PRE-COMMIT FK VALIDATION] Found FK validation errors:")
                for error in fk_validation_errors:
                    logger.error(f"  ❌ {error}")
                raise ValueError(f"FK validation failed with {len(fk_validation_errors)} error(s): " + "; ".join(fk_validation_errors[:3]))

            logger.info("[STEP 7 PRE-COMMIT FK VALIDATION] All FK references validated successfully ✓")
            # ========== END PRE-COMMIT FK VALIDATION ==========

            # Diagnostic logging before transaction commit
            logger.info("[STEP 7 PRE-COMMIT] Summary of structures created in this transaction:")
            logger.info(f"  - {len(created_mapping_definitions)} MAPPING_DEFINITIONs")
            if debug_data is not None:
                logger.info(f"  - {len(debug_data['CUBE_STRUCTURE'])} CUBE_STRUCTURE(s):")
                for cs in debug_data['CUBE_STRUCTURE']:
                    logger.info(f"      {cs.cube_structure_id}")
                logger.info(f"  - {len(debug_data['CUBE_STRUCTURE_ITEM'])} CUBE_STRUCTURE_ITEMs")
                logger.info(f"  - {len(debug_data['CUBE'])} CUBE(s):")
                for c in debug_data['CUBE']:
                    logger.info(f"      {c.cube_id}")
                logger.info(f"  - {len(created_combinations)} COMBINATIONs")
                logger.info(f"  - {len(debug_data['SUBDOMAIN'])} SUBDOMAINs created in this session")
                logger.info(f"  - {len(debug_data['SUBDOMAIN_ENUMERATION'])} SUBDOMAIN_ENUMERATIONs")
            else:
                logger.info(f"  - {len(created_combinations)} COMBINATIONs")
                logger.info("  - Debug data tracking disabled (DEBUG_EXPORT_ENABLED=False)")
            logger.info("[STEP 7 PRE-COMMIT] Transaction will now attempt to commit...")

            # Note: Session data is NOT cleared here anymore to avoid redirect loops.
            # The session will be overwritten when the user starts a new workflow (Step 1 POST).
            # This prevents the browser from cascading redirects if the user refreshes
            # or navigates after completion, which was causing SQLite locking issues.

            messages.success(request, f'Successfully created {len(created_mapping_definitions)} output layer mappings with shared cube')

        except Exception as e:
            # Rollback is automatic with @transaction.atomic
            logger.error(f"[STEP 7 ERROR] Exception during structure creation: {str(e)}", exc_info=True)

            # Export debug data even on failure to help diagnose the issue (if enabled)
            if debug_data is not None:
                try:
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    debug_folder = Path("debug_exports") / f"step7_ERROR_{timestamp}"
                    logger.info(f"[DEBUG EXPORT ERROR] Exporting {sum(len(v) for v in debug_data.values())} objects to {debug_folder}")

                    export_success = export_debug_data(debug_data, str(debug_folder))

                    if export_success:
                        logger.info(f"[DEBUG EXPORT ERROR] Debug data exported to {debug_folder}")
                        messages.warning(request, f'Error occurred. Debug data exported to {debug_folder}')
                except Exception as export_error:
                    logger.error(f"[DEBUG EXPORT ERROR] Failed to export error debug data: {str(export_error)}")

            messages.error(request, f'Error creating structures: {str(e)}')
            context = {
                'success': False,
                'error': str(e),
                'conflicts': [],
                'conflicts_json': '[]',
                'step': 7,
                'total_steps': 7
            }

    else:
        # GET request - show confirmation page for all mappings
        mapping_summaries = []
        conflicts = []

        if regenerate_mode:
            # Regenerate mode: build summaries from existing mappings
            for mapping_info in created_mapping_definitions:
                mapping_def = mapping_info['mapping_definition']
                # Count variables and members from existing mapping
                var_count = 0
                member_count = 0
                if mapping_def.variable_mapping_id:
                    var_count = VARIABLE_MAPPING_ITEM.objects.filter(
                        variable_mapping_id=mapping_def.variable_mapping_id
                    ).count()
                if mapping_def.member_mapping_id:
                    member_count = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mapping_def.member_mapping_id
                    ).count()

                mapping_summaries.append({
                    'name': mapping_info['name'],
                    'internal_id': mapping_info['internal_id'],
                    'dimension_count': member_count,
                    'observation_count': var_count - member_count,  # Approximation
                    'attribute_count': 0  # Not tracked in old format
                })
        else:
            # Normal mode: build summaries from session data
            for group_id, mapping_data in all_mappings.items():
                # Count unique variable IDs for each category
                # dimensions: list of dicts [{var_id: member_id, ...}, ...]
                # observations/attributes: dict {'source_vars': [...], 'target_vars': [...]}
                dims = mapping_data.get('dimensions', [])
                obs = mapping_data.get('observations', {})
                attrs = mapping_data.get('attributes', {})

                # dimensions is a list of dicts - count unique keys
                unique_dim_vars = set()
                for row in dims:
                    if isinstance(row, dict):
                        unique_dim_vars.update(row.keys())

                # observations is a dict with source_vars and target_vars lists
                unique_obs_vars = set()
                if isinstance(obs, dict):
                    unique_obs_vars.update(obs.get('source_vars', []))
                    unique_obs_vars.update(obs.get('target_vars', []))

                # attributes is a dict with source_vars and target_vars lists
                unique_attr_vars = set()
                if isinstance(attrs, dict):
                    unique_attr_vars.update(attrs.get('source_vars', []))
                    unique_attr_vars.update(attrs.get('target_vars', []))

                mapping_summaries.append({
                    'name': mapping_data['mapping_name'],
                    'internal_id': mapping_data.get('internal_id', ''),
                    'dimension_count': len(unique_dim_vars),
                    'observation_count': len(unique_obs_vars),
                    'attribute_count': len(unique_attr_vars)
                })

            # Check for conflicts (only in normal mode, not regenerate mode)
            variable_groups = json.loads(request.session['olmw_variable_groups'])
            conflicts = detect_mapping_conflicts(table_code, version, all_mappings, variable_groups)

            if conflicts:
                logger.warning(f"[STEP 7] Detected {len(conflicts)} mapping conflicts")
                for conflict in conflicts:
                    logger.warning(f"  - {conflict['variable_mapping_id']}: {conflict['mapping_name']}")

        # Detect if this is a deduplicated table and find siblings
        from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
            is_deduplicated_table,
            get_z_axis_sibling_tables
        )

        related_duplicates = []
        is_duplicated = is_deduplicated_table(table_id)

        if is_duplicated:
            sibling_tables = get_z_axis_sibling_tables(table_id)
            related_duplicates = [
                {
                    'table_id': t.table_id,
                    'table_code': t.table_code if hasattr(t, 'table_code') else t.table_id,
                    'name': t.name if t.name else t.table_id
                }
                for t in sibling_tables
            ]
            logger.info(f"[STEP 7] Found {len(related_duplicates)} related duplicate tables for {table_id}")

        # Compute preview IDs matching actual generation format
        framework_short = framework.replace('EBA_', '') if framework.startswith('EBA_') else framework
        clean_table_id = NamingUtils.strip_z_ordinate_suffix(table_id)
        preview_cube_id = f"{framework_short}_REF_{clean_table_id}_CUBE"
        preview_structure_id = f"{framework_short}_REF_{clean_table_id}_STRUCTURE"

        # Read tables selected in Step 5 (Z-variant table selector)
        selected_z_table_ids = request.session.get('olmw_selected_z_tables', [])
        selected_z_tables = []
        if selected_z_table_ids:
            # Get full table objects for display with preview IDs
            for z_table_id in selected_z_table_ids:
                try:
                    t = TABLE.objects.get(table_id=z_table_id)
                    z_clean_table_id = NamingUtils.strip_z_ordinate_suffix(z_table_id)
                    z_preview_cube_id = f"{framework_short}_REF_{z_clean_table_id}_CUBE"
                    selected_z_tables.append({
                        'table_id': t.table_id,
                        'table_code': t.table_code if hasattr(t, 'table_code') else t.table_id,
                        'name': t.name if t.name else t.table_id,
                        'preview_cube_id': z_preview_cube_id
                    })
                except TABLE.DoesNotExist:
                    logger.warning(f"[STEP 7] Selected Z-table {z_table_id} not found in database")
            logger.info(f"[STEP 7] Loaded {len(selected_z_tables)} selected Z-variant tables from Step 5")

        context = {
            'mappings': mapping_summaries,
            'mappings_count': len(mapping_summaries),
            'table_code': table_code,
            'table_id': table_id,
            'framework': framework,
            'version': version,
            'step': 7,
            'total_steps': 7,
            'preview': True,
            'regenerate_mode': regenerate_mode,
            'batch_edit_mode': batch_edit_mode,
            'batch_edit_mapping_count': len(request.session.get('olmw_batch_edit_mapping_ids', [])),
            'conflicts': conflicts,
            'conflicts_json': json.dumps(conflicts) if conflicts else '[]',
            'is_duplicated_table': is_duplicated,
            'related_duplicates': related_duplicates,
            'related_duplicates_json': json.dumps(related_duplicates),
            'selected_z_tables': selected_z_tables,  # Tables selected in Step 5
            'preview_cube_id': preview_cube_id,
            'preview_structure_id': preview_structure_id
        }

    return render(request, 'pybirdai/workflow/dpm_workflow/output_layer_mapping/step7_confirmation.html', context)


@require_http_methods(["POST"])
@transaction.atomic
def delete_mapping_conflicts(request):
    """
    API endpoint to delete conflicting VARIABLE_MAPPING records and proceed with generation.
    Called from the conflict resolution modal when user confirms deletion.
    """
    try:
        # Parse the conflict IDs from request body
        data = json.loads(request.body)
        conflict_ids = data.get('conflict_ids', [])

        if not conflict_ids:
            return JsonResponse({'error': 'No conflict IDs provided'}, status=400)

        logger.info(f"[CONFLICT RESOLUTION] Deleting {len(conflict_ids)} conflicting mappings")

        # Delete the conflicting mappings
        deletion_stats = delete_conflicting_mappings(conflict_ids)

        logger.info(f"[CONFLICT RESOLUTION] Deletion complete: {deletion_stats}")

        return JsonResponse({
            'success': True,
            'deleted': deletion_stats,
            'message': f"Successfully deleted {deletion_stats['variable_mappings']} conflicting mapping(s)"
        })

    except Exception as e:
        logger.error(f"[CONFLICT RESOLUTION] Error deleting conflicts: {str(e)}")
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def get_table_cells_api(request):
    """
    API endpoint to get table cells and their combinations for AJAX requests.

    For deduplicated tables (Z-axis variants), cells are found via CELL_POSITION
    traversal since TABLE_CELL.table_id still references the original table.
    """
    from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
        get_table_cells_via_cell_position,
        is_deduplicated_table
    )

    table_id = request.GET.get('table_id')
    if not table_id:
        return JsonResponse({'error': 'Table ID required'}, status=400)

    try:
        table = TABLE.objects.get(table_id=table_id)

        # Use CELL_POSITION traversal to find cells (works for deduplicated tables)
        cells = get_table_cells_via_cell_position(table)

        # Check if this is a deduplicated table
        is_dedup = is_deduplicated_table(table_id)

        cell_data = []
        for cell in cells:
            cell_info = {
                'cell_id': cell.cell_id,
                'name': cell.name,
                'is_shaded': cell.is_shaded,
                'combination_id': cell.table_cell_combination_id
            }

            # Get combination details if exists
            if cell.table_cell_combination_id:
                try:
                    combination = COMBINATION.objects.get(combination_id=cell.table_cell_combination_id)
                    cell_info['combination'] = {
                        'id': combination.combination_id,
                        'name': combination.name,
                        'code': combination.code
                    }
                except COMBINATION.DoesNotExist:
                    pass

            cell_data.append(cell_info)

        return JsonResponse({
            'table': {
                'id': table.table_id,
                'name': table.name,
                'code': table.code,
                'is_deduplicated': is_dedup
            },
            'cells': cell_data
        })

    except TABLE.DoesNotExist:
        return JsonResponse({'error': 'Table not found'}, status=404)


@require_http_methods(["GET"])
def get_variable_domain_api(request):
    """
    API endpoint to get domain and members for a variable.
    """
    variable_id = request.GET.get('variable_id')
    if not variable_id:
        return JsonResponse({'error': 'Variable ID required'}, status=400)

    try:
        variable = VARIABLE.objects.get(variable_id=variable_id)

        response_data = {
            'variable': {
                'id': variable.variable_id,
                'name': variable.name,
                'code': variable.code
            }
        }

        # Get domain if exists
        if hasattr(variable, 'domain_id') and variable.domain_id:
            domain = variable.domain_id
            response_data['domain'] = {
                'id': domain.domain_id,
                'name': domain.name,
                'code': domain.code,
                'is_enumerated': domain.is_enumerated
            }

            # Get members if domain is enumerated
            if domain.is_enumerated:
                members = MEMBER.objects.filter(domain_id=domain).order_by('name')
                response_data['members'] = [
                    {
                        'id': member.member_id,
                        'name': member.name,
                        'code': member.code
                    }
                    for member in members
                ]

        return JsonResponse(response_data)

    except VARIABLE.DoesNotExist:
        return JsonResponse({'error': 'Variable not found'}, status=404)


@require_http_methods(["GET"])
def get_filter_options_api(request):
    """
    API endpoint for cascading filter options.
    Returns frameworks, versions filtered by framework, or tables filtered by framework+version.

    Query Parameters:
    - framework_code: Optional. If provided, filters versions and tables by this framework.
    - version: Optional. If provided (with framework_code), filters tables by both.

    Returns:
    - If no params: all frameworks
    - If framework_code only: versions that have tables matching that framework
    - If framework_code + version: tables matching both criteria
    """
    framework_code = request.GET.get('framework_code')
    version = request.GET.get('version')

    try:
        # Case 1: No parameters - return all frameworks
        if not framework_code:
            frameworks = FRAMEWORK.objects.all().order_by('framework_id')
            return JsonResponse({
                'status': 'success',
                'frameworks': [
                    {
                        'id': fw.framework_id,
                        'name': fw.name,
                        'code': fw.code
                    }
                    for fw in frameworks
                ]
            })

        # Case 2: Framework provided - return filtered versions
        if framework_code and not version:
            # Get tables using proper FRAMEWORK_TABLE junction table
            try:
                framework = FRAMEWORK.objects.get(
                    models.Q(framework_id=framework_code) |
                    models.Q(code=framework_code)
                )

                # Get table IDs associated with this framework via FRAMEWORK_TABLE
                table_ids = FRAMEWORK_TABLE.objects.filter(
                    framework_id=framework
                ).values_list('table_id__table_id', flat=True)

                # Get distinct versions from these tables
                versions_list = TABLE.objects.filter(
                    table_id__in=table_ids
                ).values_list('version', flat=True).distinct().order_by('version')
                versions_list = [v for v in versions_list if v]  # Filter out None/empty values

            except FRAMEWORK.DoesNotExist:
                versions_list = []

            return JsonResponse({
                'status': 'success',
                'versions': versions_list
            })

        # Case 3: Framework and version provided - return filtered tables
        if framework_code and version:
            # Get tables using proper FRAMEWORK_TABLE junction table
            try:
                framework = FRAMEWORK.objects.get(
                    models.Q(framework_id=framework_code) |
                    models.Q(code=framework_code)
                )

                # Get table IDs associated with this framework via FRAMEWORK_TABLE
                table_ids = FRAMEWORK_TABLE.objects.filter(
                    framework_id=framework
                ).values_list('table_id__table_id', flat=True)

                # Filter by version and order by name
                tables = TABLE.objects.filter(
                    table_id__in=table_ids,
                    version=version
                ).order_by('name')

            except FRAMEWORK.DoesNotExist:
                tables = TABLE.objects.none()

            return JsonResponse({
                'status': 'success',
                'tables': [
                    {
                        'id': table.table_id,
                        'code': table.code,
                        'name': table.name,
                        'description': table.description or table.name,  # Use description for display
                        'version': table.version
                    }
                    for table in tables
                ]
            })

        return JsonResponse({'error': 'Invalid parameters'}, status=400)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def get_domains(request):
    """
    API endpoint to get all domains for variable creation modal
    """
    try:
        from pybirdai.models.bird_meta_data_model import DOMAIN
        domains = DOMAIN.objects.all().values('domain_id', 'name')
        return JsonResponse({'domains': list(domains)})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def create_variable(request):
    """
    API endpoint to create a new variable
    """
    if request.method == 'POST':
        try:
            from pybirdai.models.bird_meta_data_model import VARIABLE, DOMAIN
            data = json.loads(request.body)

            variable_id = data.get('variable_id')

            # Check if variable already exists - return it instead of error
            if VARIABLE.objects.filter(variable_id=variable_id).exists():
                existing_variable = VARIABLE.objects.get(variable_id=variable_id)
                return JsonResponse({
                    'success': True,
                    'existing': True,
                    'variable': {
                        'variable_id': existing_variable.variable_id,
                        'name': existing_variable.name,
                        'code': existing_variable.code or '',
                        'description': existing_variable.description or '',
                        'domain_id': existing_variable.domain_id.domain_id if existing_variable.domain_id else None
                    }
                })

            # Create variable
            variable = VARIABLE()
            variable.variable_id = variable_id
            variable.name = data.get('name')
            variable.code = data.get('code', '')
            variable.description = data.get('description', '')

            # Set domain if provided
            if data.get('domain_id'):
                try:
                    variable.domain_id = DOMAIN.objects.get(domain_id=data['domain_id'])
                except DOMAIN.DoesNotExist:
                    return JsonResponse({'success': False, 'error': 'Domain not found'})

            variable.save()

            return JsonResponse({
                'success': True,
                'variable': {
                    'variable_id': variable.variable_id,
                    'name': variable.name,
                    'code': variable.code,
                    'description': variable.description,
                    'domain_id': variable.domain_id.domain_id if variable.domain_id else None
                }
            })

        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    return JsonResponse({'success': False, 'error': 'Method not allowed'}, status=405)


def update_variable_domain(request):
    """
    API endpoint to update a variable's domain
    """
    if request.method == 'POST':
        try:
            from pybirdai.models.bird_meta_data_model import VARIABLE, DOMAIN
            data = json.loads(request.body)

            variable_id = data.get('variable_id')
            domain_id = data.get('domain_id')

            if not variable_id or not domain_id:
                return JsonResponse({'success': False, 'error': 'Variable ID and Domain ID are required'})

            # Get the variable
            try:
                variable = VARIABLE.objects.get(variable_id=variable_id)
            except VARIABLE.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Variable not found'})

            # Get the domain
            try:
                domain = DOMAIN.objects.get(domain_id=domain_id)
            except DOMAIN.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Domain not found'})

            # Update the variable's domain
            variable.domain_id = domain
            variable.save()

            logger.info(f"[API] Updated variable {variable_id} domain to {domain_id}")

            return JsonResponse({
                'success': True,
                'variable': {
                    'variable_id': variable.variable_id,
                    'name': variable.name,
                    'domain_id': domain.domain_id,
                    'domain_name': domain.name
                }
            })

        except Exception as e:
            logger.error(f"[API] Error updating variable domain: {e}")
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    return JsonResponse({'success': False, 'error': 'Method not allowed'}, status=405)


def get_variable_info(request):
    """
    API endpoint to get variable information including domain
    """
    variable_id = request.GET.get('variable_id')
    if not variable_id:
        return JsonResponse({'success': False, 'error': 'Variable ID required'})

    try:
        from pybirdai.models.bird_meta_data_model import VARIABLE
        variable = VARIABLE.objects.get(variable_id=variable_id)

        return JsonResponse({
            'success': True,
            'variable': {
                'variable_id': variable.variable_id,
                'name': variable.name,
                'code': variable.code or '',
                'description': variable.description or '',
                'domain_id': variable.domain_id.domain_id if variable.domain_id else None,
                'domain_name': variable.domain_id.name if variable.domain_id else None
            }
        })
    except VARIABLE.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Variable not found'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def create_domain(request):
    """
    API endpoint to create a new enumerated reference domain
    """
    if request.method == 'POST':
        try:
            from pybirdai.models.bird_meta_data_model import DOMAIN, MAINTENANCE_AGENCY
            data = json.loads(request.body)

            domain_id = data.get('domain_id')

            if not domain_id:
                return JsonResponse({'success': False, 'error': 'Domain ID is required'})

            # Check if domain already exists - return it instead of error
            existing_domain = DOMAIN.objects.filter(domain_id=domain_id).first()
            if existing_domain:
                return JsonResponse({
                    'success': True,
                    'existing': True,
                    'domain': {
                        'domain_id': existing_domain.domain_id,
                        'name': existing_domain.name,
                        'code': existing_domain.code,
                        'is_enumerated': existing_domain.is_enumerated,
                        'is_reference': existing_domain.is_reference
                    }
                })

            # Get maintenance agency (use first one if available)
            maintenance_agency = MAINTENANCE_AGENCY.objects.first()

            # Create domain with ID = NAME = CODE
            domain = DOMAIN()
            domain.domain_id = domain_id
            domain.name = domain_id
            domain.code = domain_id
            domain.is_enumerated = True
            domain.is_reference = True
            domain.data_type = 'String'  # Default for enumerated domains
            domain.maintenance_agency_id = maintenance_agency
            domain.save()

            return JsonResponse({
                'success': True,
                'domain': {
                    'domain_id': domain.domain_id,
                    'name': domain.name,
                    'code': domain.code,
                    'is_enumerated': domain.is_enumerated,
                    'is_reference': domain.is_reference
                }
            })

        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    return JsonResponse({'success': False, 'error': 'Method not allowed'}, status=405)


def api_cube_structure(request, cube_id):
    """
    Generic API endpoint to get cube structure items for any cube.
    Returns JSON with cube details and hierarchical structure items.
    Reusable service for cube structure visualization.

    Now includes domain_id and existing hierarchy information for each variable
    to support hierarchy creation/editing from the cube structure viewer.
    """
    try:
        # Get the cube
        cube = get_object_or_404(CUBE, cube_id=cube_id)

        if not cube.cube_structure_id:
            return JsonResponse({
                'error': 'Cube has no associated structure'
            }, status=404)

        structure = cube.cube_structure_id

        # Get all structure items for this cube structure, ordered by order field
        structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=structure
        ).select_related(
            'variable_id',
            'variable_id__domain_id',
            'member_id',
            'subdomain_id'
        ).order_by('order')

        # Collect all domain IDs to batch-fetch hierarchies
        domain_ids = set()
        for item in structure_items:
            if item.variable_id and item.variable_id.domain_id:
                domain_ids.add(item.variable_id.domain_id.domain_id)

        # Build a mapping of domain_id -> list of hierarchies
        domain_hierarchies = {}
        if domain_ids:
            hierarchies = MEMBER_HIERARCHY.objects.filter(
                domain_id__domain_id__in=domain_ids
            ).select_related('domain_id')
            for h in hierarchies:
                if h.domain_id:
                    d_id = h.domain_id.domain_id
                    if d_id not in domain_hierarchies:
                        domain_hierarchies[d_id] = []
                    domain_hierarchies[d_id].append({
                        'hierarchy_id': h.member_hierarchy_id,
                        'name': h.name,
                        'code': h.code
                    })

        # Build the items array
        items = []
        for item in structure_items:
            # Get domain info if variable has a domain
            domain_id = None
            domain_name = None
            variable_hierarchies = []
            if item.variable_id and item.variable_id.domain_id:
                domain_id = item.variable_id.domain_id.domain_id
                domain_name = item.variable_id.domain_id.name
                variable_hierarchies = domain_hierarchies.get(domain_id, [])

            items.append({
                'order': item.order,
                'role': item.role,
                'role_display': dict(CUBE_STRUCTURE_ITEM.TYP_RL).get(item.role, item.role),
                'cube_variable_code': item.cube_variable_code,
                'variable_id': item.variable_id.variable_id if item.variable_id else None,
                'variable_name': item.variable_id.name if item.variable_id else 'Unknown',
                'variable_code': item.variable_id.code if item.variable_id else None,
                'domain_id': domain_id,
                'domain_name': domain_name,
                'hierarchies': variable_hierarchies,
                'member_id': item.member_id.member_id if item.member_id else None,
                'member_name': item.member_id.name if item.member_id else None,
                'subdomain_id': item.subdomain_id.subdomain_id if item.subdomain_id else None,
                'subdomain_name': item.subdomain_id.name if item.subdomain_id else None,
                'dimension_type': item.dimension_type,
                'is_mandatory': item.is_mandatory,
                'is_identifier': item.is_identifier,
            })

        response_data = {
            'cube_id': cube.cube_id,
            'cube_name': cube.name or cube.cube_id,
            'cube_code': cube.code,
            'structure_id': structure.cube_structure_id,
            'structure_name': structure.name or structure.cube_structure_id,
            'structure_code': structure.code,
            'items': items,
            'item_count': len(items)
        }

        return JsonResponse(response_data)

    except Exception as e:
        logger.error(f"Error fetching cube structure for {cube_id}: {e}")
        return JsonResponse({'error': str(e)}, status=500)


def cube_structure_viewer(request, cube_id):
    """
    Renders a standalone cube structure viewer page.
    Used for modal popup display of cube structures.
    """
    context = {
        'cube_id': cube_id,
    }
    return render(request, 'pybirdai/workflow/shared/viewers/cube_structure_viewer.html', context)


# =============================================================================
# Z-axis Variant Management APIs
# =============================================================================

@require_http_methods(["GET"])
def get_z_axis_siblings_api(request):
    """
    API endpoint to get all Z-axis sibling tables for a given table.

    Returns list of tables that share the same original base table ID,
    indicating they are Z-axis variants of the same table.
    """
    from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
        get_z_axis_sibling_tables,
        is_deduplicated_table,
        get_original_table_id
    )

    table_id = request.GET.get('table_id')
    if not table_id:
        return JsonResponse({'error': 'table_id required'}, status=400)

    try:
        table = TABLE.objects.get(table_id=table_id)

        is_dedup = is_deduplicated_table(table_id)
        if not is_dedup:
            return JsonResponse({
                'is_deduplicated': False,
                'siblings': [],
                'original_table_id': table_id
            })

        siblings = get_z_axis_sibling_tables(table_id)
        original_id = get_original_table_id(table_id)

        return JsonResponse({
            'is_deduplicated': True,
            'original_table_id': original_id,
            'current_table': {
                'table_id': table.table_id,
                'name': table.name,
                'code': table.code
            },
            'siblings': [
                {
                    'table_id': t.table_id,
                    'name': t.name,
                    'code': t.code
                }
                for t in siblings
            ]
        })

    except TABLE.DoesNotExist:
        return JsonResponse({'error': 'Table not found'}, status=404)


@require_http_methods(["POST"])
def save_selected_z_tables_api(request):
    """
    API endpoint to save selected Z-axis table IDs to session.
    Used in Step 5 to persist which tables are included in the multi-table mapping.

    POST body:
    {
        "selected_table_ids": ["C_07.00.a_EBA_qx50", "C_07.00.a_EBA_qx51"]
    }
    """
    import json

    try:
        data = json.loads(request.body)
        selected_table_ids = data.get('selected_table_ids', [])

        # Validate that all tables exist
        if selected_table_ids:
            existing_tables = TABLE.objects.filter(
                table_id__in=selected_table_ids
            ).values_list('table_id', flat=True)

            if len(existing_tables) != len(selected_table_ids):
                missing = set(selected_table_ids) - set(existing_tables)
                return JsonResponse({
                    'error': f'Tables not found: {", ".join(missing)}'
                }, status=404)

        # Save to session
        request.session['olmw_selected_z_tables'] = selected_table_ids
        request.session.modified = True

        return JsonResponse({
            'status': 'success',
            'message': f'Saved {len(selected_table_ids)} selected table(s)',
            'selected_tables': selected_table_ids
        })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        logger.error(f"Error saving selected Z-tables: {str(e)}", exc_info=True)
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
def regenerate_combinations_api(request):
    """
    API endpoint to regenerate combinations for an existing cube.
    Deletes existing combinations and creates new ones using the fixed cell lookup.
    """
    import json
    from django.http import JsonResponse

    try:
        data = json.loads(request.body)
        cube_id = data.get('cube_id')
        table_id = data.get('table_id')

        if not cube_id:
            return JsonResponse({'success': False, 'error': 'cube_id required'}, status=400)

        if not table_id:
            return JsonResponse({'success': False, 'error': 'table_id required'}, status=400)

        # Get the cube and table
        cube = CUBE.objects.get(cube_id=cube_id)
        table = TABLE.objects.get(table_id=table_id)

        logger.info(f"[REGENERATE] Regenerating combinations for cube {cube_id}, table {table_id}")

        # Delete existing combinations linked to this cube
        existing_links = CUBE_TO_COMBINATION.objects.filter(cube_id=cube)
        combination_ids = list(existing_links.values_list('combination_id', flat=True))

        deleted_count = len(combination_ids)
        existing_links.delete()
        COMBINATION.objects.filter(combination_id__in=combination_ids).delete()

        # Regenerate combinations using fixed cell lookup (CELL_POSITION traversal)
        table_axes = AXIS.objects.filter(table_id=table)
        table_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=table_axes)
        all_cell_positions = CELL_POSITION.objects.filter(axis_ordinate_id__in=table_ordinates)
        cell_ids = all_cell_positions.values_list('cell_id', flat=True).distinct()
        cells = TABLE_CELL.objects.filter(cell_id__in=cell_ids)

        # Create combinations
        generation_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        combination_creator = CombinationCreator(table.code, table.version or '1.0')
        created_combinations = []

        for cell in cells:
            combination = combination_creator.create_combination_for_cell(
                cell, cube, generation_timestamp
            )
            if combination:
                created_combinations.append(combination)
                cube_to_combo, _ = CUBE_TO_COMBINATION.objects.get_or_create(
                    cube_id=cube,
                    combination_id=combination
                )

        logger.info(f"[REGENERATE] Created {len(created_combinations)} new combinations")

        return JsonResponse({
            'success': True,
            'combinations_created': len(created_combinations),
            'deleted_count': deleted_count
        })

    except CUBE.DoesNotExist:
        return JsonResponse({'success': False, 'error': f'Cube {cube_id} not found'}, status=404)
    except TABLE.DoesNotExist:
        return JsonResponse({'success': False, 'error': f'Table {table_id} not found'}, status=404)
    except Exception as e:
        logger.error(f"Error regenerating combinations: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# ============================================================================
# BULK OPERATIONS FOR STEP 2
# ============================================================================

@transaction.atomic
def step2_apply_bulk(request):
    """
    Apply selected mappings - use them to skip directly to Step 7 generation.

    Supports two slider options:
    - apply_to_all_variants: Apply to all Z-axis table variants
    - use_all_mappings: Use all mappings (ignore selection)
    """
    if request.method != 'POST':
        messages.error(request, 'Invalid request method.')
        return redirect('pybirdai:output_layer_mapping_step2')

    # Get slider states
    apply_to_all_variants = request.POST.get('apply_to_all_variants', 'false') == 'true'
    use_all_mappings = request.POST.get('use_all_mappings', 'false') == 'true'

    # Get selected mapping IDs from POST data
    selected_mappings = request.POST.getlist('selected_mappings[]')

    if not selected_mappings:
        messages.error(request, 'No mappings selected. Please select at least one mapping.')
        return redirect('pybirdai:output_layer_mapping_step2')

    logger.info(f"[STEP 2 APPLY BULK] Applying {len(selected_mappings)} selected mapping(s)")
    logger.info(f"[STEP 2 APPLY BULK] Sliders: apply_to_all_variants={apply_to_all_variants}, use_all_mappings={use_all_mappings}")

    # Validate that all selected mappings exist
    mappings = MAPPING_DEFINITION.objects.filter(mapping_id__in=selected_mappings)
    if mappings.count() != len(selected_mappings):
        messages.error(request, 'Some selected mappings do not exist.')
        return redirect('pybirdai:output_layer_mapping_step2')

    # Store selected mappings in session for Step 7
    request.session['olmw_mapping_mode'] = 'regenerate_all' if apply_to_all_variants else 'use_existing'
    request.session['olmw_regenerate_mode'] = True  # CRITICAL: Tell Step 7 to use regenerate mode
    request.session['olmw_existing_mapping_ids'] = selected_mappings

    # Handle "Apply to All Variants" slider
    if apply_to_all_variants:
        # Get the table from session
        table_id = request.session.get('olmw_table_id')
        if not table_id:
            messages.error(request, 'No table selected. Please start from Step 1.')
            return redirect('pybirdai:output_layer_mapping_step1')

        try:
            table = TABLE.objects.get(table_id=table_id)
        except TABLE.DoesNotExist:
            messages.error(request, 'Selected table not found.')
            return redirect('pybirdai:output_layer_mapping_step1')

        # Find all Z-axis sibling tables
        from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
            is_deduplicated_table,
            get_z_axis_sibling_tables
        )

        table_variants = [table_id]  # Always include the selected table
        if is_deduplicated_table(table_id):
            siblings = get_z_axis_sibling_tables(table_id)
            table_variants.extend(siblings)
            logger.info(f"[STEP 2 APPLY BULK] Found {len(siblings)} Z-axis sibling tables")

        # Store replication configuration in session
        request.session['olmw_replicate_to_variants'] = True
        request.session['olmw_table_variants'] = table_variants

        messages.info(
            request,
            f"Applying to {len(table_variants)} table variant(s)"
        )

    # Verify Step 1 session keys exist (required by Step 7)
    required_keys = ['olmw_table_id', 'olmw_framework', 'olmw_version', 'olmw_table_code']
    missing_keys = [key for key in required_keys if key not in request.session]

    if missing_keys:
        logger.error(f"[STEP 2 APPLY BULK] Missing required session keys: {missing_keys}")
        messages.error(request, 'Session expired. Please start from Step 1.')
        return redirect('pybirdai:output_layer_mapping_step1')

    # Get mapping names for success message
    mapping_names = [m.name for m in mappings]

    messages.success(
        request,
        f"Applying {len(selected_mappings)} mapping(s): {', '.join(mapping_names[:3])}{'...' if len(mapping_names) > 3 else ''}"
    )

    # Debug: Verify session keys before redirect
    logger.info(f"[STEP 2 APPLY BULK] Session keys before redirect:")
    logger.info(f"  olmw_regenerate_mode = {request.session.get('olmw_regenerate_mode')}")
    logger.info(f"  olmw_existing_mapping_ids = {request.session.get('olmw_existing_mapping_ids')}")
    logger.info(f"  olmw_mapping_mode = {request.session.get('olmw_mapping_mode')}")
    logger.info(f"  olmw_table_id = {request.session.get('olmw_table_id')}")
    logger.info(f"  olmw_replicate_to_variants = {request.session.get('olmw_replicate_to_variants')}")

    # Explicitly save session to ensure persistence
    request.session.modified = True
    request.session.save()
    logger.info(f"[STEP 2 APPLY BULK] Session explicitly saved")

    # Skip directly to Step 7 (generation/confirmation)
    return redirect('pybirdai:output_layer_mapping_step7')


@transaction.atomic
def step2_reapply_all(request):
    """
    Reapply selected mappings to all concerned cubes/table variants.

    This regenerates output layer structures for ALL table variants that share
    the same base structure, using the selected mapping definitions.
    Skips directly to Step 7 generation.
    """
    if request.method != 'POST':
        messages.error(request, 'Invalid request method.')
        return redirect('pybirdai:output_layer_mapping_step2')

    # Get selected mapping IDs from POST data
    selected_mappings = request.POST.getlist('selected_mappings[]')

    if not selected_mappings:
        messages.error(request, 'No mappings selected. Please select at least one mapping.')
        return redirect('pybirdai:output_layer_mapping_step2')

    logger.info(f"[STEP 2 REAPPLY ALL] Reapplying {len(selected_mappings)} mapping(s) to all variants")

    # Validate that all selected mappings exist
    mappings = MAPPING_DEFINITION.objects.filter(mapping_id__in=selected_mappings)
    if mappings.count() != len(selected_mappings):
        messages.error(request, 'Some selected mappings do not exist.')
        return redirect('pybirdai:output_layer_mapping_step2')

    # Get the table from session
    table_id = request.session.get('olmw_table_id')
    if not table_id:
        messages.error(request, 'No table selected. Please start from Step 1.')
        return redirect('pybirdai:output_layer_mapping_step1')

    try:
        table = TABLE.objects.get(table_id=table_id)
    except TABLE.DoesNotExist:
        messages.error(request, 'Selected table not found.')
        return redirect('pybirdai:output_layer_mapping_step1')

    # Find all Z-axis sibling tables (tables sharing the same base structure)
    from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
        is_deduplicated_table,
        get_z_axis_sibling_tables
    )

    table_variants = [table_id]  # Always include the selected table
    if is_deduplicated_table(table_id):
        siblings = get_z_axis_sibling_tables(table_id)
        table_variants.extend(siblings)
        logger.info(f"[STEP 2 REAPPLY ALL] Found {len(siblings)} Z-axis sibling tables")

    # Store replication configuration in session
    request.session['olmw_mapping_mode'] = 'regenerate_all'
    request.session['olmw_regenerate_mode'] = True
    request.session['olmw_existing_mapping_ids'] = selected_mappings
    request.session['olmw_replicate_to_variants'] = True
    request.session['olmw_table_variants'] = table_variants

    messages.info(
        request,
        f"Ready to regenerate from {len(selected_mappings)} mapping(s) for {len(table_variants)} table variant(s). "
        f"Confirm in the next step to delete old structures and regenerate."
    )

    # Skip directly to Step 7 (generation)
    return redirect('pybirdai:output_layer_mapping_step7')


@transaction.atomic
def step2_edit_bulk(request):
    """
    Edit selected mappings - pre-populate the workflow with mapping data.

    Supports both single and batch editing:
    - If single mapping selected: Load it and proceed to Step 3 with pre-filled values
    - If multiple mappings selected: Enable batch edit mode with separate data per mapping
    """
    if request.method != 'POST':
        messages.error(request, 'Invalid request method.')
        return redirect('pybirdai:output_layer_mapping_step2')

    # Get selected mapping IDs from POST data (handle both formats)
    selected_mappings = request.POST.getlist('selected_mappings') or request.POST.getlist('selected_mappings[]')

    if not selected_mappings:
        messages.error(request, 'No mappings selected. Please select at least one mapping.')
        return redirect('pybirdai:output_layer_mapping_step2')

    # Single mapping edit mode
    if len(selected_mappings) == 1:
        mapping_id = selected_mappings[0]
        logger.info(f"[STEP 2 EDIT BULK] Loading single mapping {mapping_id} for editing")

        # Validate mapping exists
        try:
            mapping = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)
        except MAPPING_DEFINITION.DoesNotExist:
            messages.error(request, f'Mapping {mapping_id} not found.')
            return redirect('pybirdai:output_layer_mapping_step2')

        # Load existing mapping data into session
        request.session['olmw_mapping_mode'] = 'modify_existing'
        request.session['olmw_existing_mapping_id'] = mapping_id
        request.session['olmw_batch_edit_mode'] = False

        # Load mapping data using the existing helper function
        _load_existing_mapping_to_session(request, mapping_id)

        # Load ordinates from MAPPING_ORDINATE_LINK
        ordinate_links = MAPPING_ORDINATE_LINK.objects.filter(mapping_id=mapping)
        selected_ordinates = list(ordinate_links.values_list('axis_ordinate_id', flat=True))
        request.session['olmw_selected_ordinates'] = selected_ordinates
        logger.info(f"[STEP 2 EDIT BULK] Loaded {len(selected_ordinates)} ordinates from MAPPING_ORDINATE_LINK")

        # Pre-select Z-tables from MAPPING_TO_CUBE records
        import re
        from pybirdai.process_steps.output_layer_mapping_workflow.table_utils import get_base_table_id

        z_table_ids = set()
        mtc_records = MAPPING_TO_CUBE.objects.filter(mapping_id=mapping_id)

        # Get base table from session
        base_table_id = None
        current_table_id = request.session.get('olmw_table_id')
        if current_table_id:
            base_table_id = get_base_table_id(current_table_id)

        # Pattern to extract Z-member from cube_mapping_id
        z_variant_pattern = r'_(EBA_q[A-Z]+_EBA_q[a-z0-9]+)_'
        for mtc in mtc_records:
            if mtc.cube_mapping_id:
                match = re.search(z_variant_pattern, mtc.cube_mapping_id)
                if match:
                    z_member = match.group(1)
                    z_suffix = '_' + z_member

                    if base_table_id:
                        matching_tables = TABLE.objects.filter(
                            table_id__startswith=base_table_id,
                            table_id__endswith=z_suffix
                        )
                    else:
                        matching_tables = TABLE.objects.filter(table_id__endswith=z_suffix)

                    for tbl in matching_tables:
                        z_table_ids.add(tbl.table_id)

        # Always include the current session table (the table user is editing from)
        if current_table_id:
            z_table_ids.add(current_table_id)

        if z_table_ids:
            z_table_list = list(z_table_ids)
            request.session['olmw_selected_z_tables'] = z_table_list
            request.session.modified = True  # Ensure session is saved
            logger.info(f"[STEP 2 EDIT SINGLE] Pre-selected {len(z_table_ids)} Z-tables from existing mapping: {z_table_ids}")

        messages.info(
            request,
            f"Editing mapping: {mapping.name}. The workflow has been pre-populated with existing values."
        )

        # Proceed to Step 3 with pre-filled data
        return redirect('pybirdai:output_layer_mapping_step3')

    # Multiple mappings - batch edit mode with separate data per mapping
    else:
        logger.info(f"[STEP 2 EDIT BULK] Batch edit mode: {len(selected_mappings)} mappings selected")

        # Validate that all selected mappings exist
        mappings = MAPPING_DEFINITION.objects.filter(mapping_id__in=selected_mappings)
        if mappings.count() != len(selected_mappings):
            messages.error(request, 'Some selected mappings do not exist.')
            return redirect('pybirdai:output_layer_mapping_step2')

        # Enable batch edit mode in session
        request.session['olmw_mapping_mode'] = 'batch_edit'
        request.session['olmw_batch_edit_mode'] = True
        request.session['olmw_batch_edit_mapping_ids'] = selected_mappings

        # Load each mapping's data separately
        batch_edit_data = {}
        all_ordinates = set()

        for mapping in mappings:
            mapping_data = _load_mapping_data_for_batch_edit(mapping)

            # Load ordinates from MAPPING_ORDINATE_LINK
            ordinate_links = MAPPING_ORDINATE_LINK.objects.filter(mapping_id=mapping)
            mapping_ordinates = list(ordinate_links.values_list('axis_ordinate_id', flat=True))

            batch_edit_data[mapping.mapping_id] = {
                'name': mapping.name,
                'code': mapping.code,
                'mapping_type': mapping.mapping_type,
                'variable_groups': mapping_data.get('variable_groups', {}),
                'variable_groups_original': mapping_data.get('variable_groups', {}),  # For diff tracking
                'multi_mappings': mapping_data.get('multi_mappings', {}),
                'multi_mappings_original': mapping_data.get('multi_mappings', {}),
                'ordinates': mapping_ordinates,
                'ordinates_added': [],
                'ordinates_removed': []
            }
            all_ordinates.update(mapping_ordinates)

        # Store batch edit data in session
        request.session['olmw_batch_edit_data'] = json.dumps(batch_edit_data)

        # Store union of all ordinates as the initial selection
        request.session['olmw_selected_ordinates'] = list(all_ordinates)

        # Pre-select Z-tables from MAPPING_TO_CUBE records
        z_table_ids = set()
        import re
        from pybirdai.process_steps.output_layer_mapping_workflow.table_utils import get_base_table_id

        # Get the base table to filter by (from session or first mapping's ordinates)
        base_table_id = None
        current_table_id = request.session.get('olmw_table_id')
        if current_table_id:
            base_table_id = get_base_table_id(current_table_id)

        # Query MAPPING_TO_CUBE for all selected mappings
        mtc_records = MAPPING_TO_CUBE.objects.filter(mapping_id__in=selected_mappings)

        # Extract Z-variant pattern from cube_mapping_id
        z_variant_pattern = r'_(EBA_q[A-Z]+_EBA_q[a-z0-9]+)_'

        for mtc in mtc_records:
            if mtc.cube_mapping_id:
                match = re.search(z_variant_pattern, mtc.cube_mapping_id)
                if match:
                    z_member = match.group(1)
                    z_suffix = '_' + z_member
                    if base_table_id:
                        matching_tables = TABLE.objects.filter(
                            table_id__startswith=base_table_id,
                            table_id__endswith=z_suffix
                        )
                    else:
                        matching_tables = TABLE.objects.filter(table_id__endswith=z_suffix)

                    for tbl in matching_tables:
                        z_table_ids.add(tbl.table_id)

        # Always include the current session table (the table user is editing from)
        if current_table_id:
            z_table_ids.add(current_table_id)

        if z_table_ids:
            z_table_list = list(z_table_ids)
            request.session['olmw_selected_z_tables'] = z_table_list
            request.session.modified = True
            logger.info(f"[STEP 2 EDIT BULK] Pre-selected {len(z_table_ids)} Z-tables from existing mappings")

        logger.info(f"[STEP 2 EDIT BULK] Loaded data for {len(batch_edit_data)} mappings, {len(all_ordinates)} unique ordinates")

        mapping_names = [m.name for m in mappings]
        messages.info(
            request,
            f"Batch edit mode: Editing {len(selected_mappings)} mappings: {', '.join(mapping_names[:3])}{'...' if len(mapping_names) > 3 else ''}. "
            f"Each mapping's data is shown separately in tabs."
        )

        # Proceed to Step 3 with batch edit data
        return redirect('pybirdai:output_layer_mapping_step3')


def _load_mapping_data_for_batch_edit(mapping):
    """
    Helper function to load a single mapping's data for batch edit mode.

    Args:
        mapping: MAPPING_DEFINITION object

    Returns:
        dict: Contains variable_groups and multi_mappings data
    """
    result = {
        'variable_groups': {},
        'multi_mappings': {}
    }

    logger.info(f"[BATCH EDIT LOAD] Loading mapping {mapping.mapping_id}: variable_mapping_id={mapping.variable_mapping_id}, mapping_type='{mapping.mapping_type}'")

    # Load variable mappings
    if mapping.variable_mapping_id:
        var_items = VARIABLE_MAPPING_ITEM.objects.filter(
            variable_mapping_id=mapping.variable_mapping_id
        ).select_related('variable_id')

        # Group by source/target
        source_vars = []
        target_vars = []
        for item in var_items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if item.is_source == 'true':
                    source_vars.append(var_id)
                else:
                    target_vars.append(var_id)

        # Map database codes to template-expected names
        mapping_type_map = {'E': 'Dimension', 'O': 'Observation', 'A': 'Attribute'}
        group_type = mapping_type_map.get(mapping.mapping_type, 'Dimension')

        # Create a variable group
        group_id = f"group_{mapping.mapping_id}"
        result['variable_groups'][group_id] = {
            'name': mapping.name,
            'variable_ids': source_vars,
            'targets': target_vars,
            'mapping_type': 'direct',
            'group_type': group_type
        }
        logger.info(f"[BATCH EDIT LOAD] Created variable group: {len(source_vars)} source vars, {len(target_vars)} target vars, group_type='{group_type}'")
    else:
        logger.warning(f"[BATCH EDIT LOAD] Mapping {mapping.mapping_id} has no variable_mapping_id - no variable groups loaded")

    # Load member mappings
    if mapping.member_mapping_id:
        member_items = MEMBER_MAPPING_ITEM.objects.filter(
            member_mapping_id=mapping.member_mapping_id
        ).select_related('variable_id', 'member_id').order_by('member_mapping_row')

        # Group by row to reconstruct dimension combinations
        rows = {}
        for item in member_items:
            row_key = item.member_mapping_row or '1'
            if row_key not in rows:
                rows[row_key] = {}
            if item.variable_id and item.member_id:
                rows[row_key][item.variable_id.variable_id] = item.member_id.member_id

        # Map database codes to template-expected names
        mapping_type_map = {'E': 'Dimension', 'O': 'Observation', 'A': 'Attribute'}
        group_type = mapping_type_map.get(mapping.mapping_type, 'Dimension')

        # Create multi_mappings entry
        group_id = f"group_{mapping.mapping_id}"
        result['multi_mappings'][group_id] = {
            'mapping_name': mapping.name,
            'internal_id': mapping.code or mapping.mapping_id,
            'group_type': group_type,
            'dimensions': list(rows.values()),
            'observations': {},
            'attributes': {}
        }

    return result


@transaction.atomic
def step2_delete_bulk(request):
    """
    Delete selected mappings and all related artifacts.

    Deletes:
    - MAPPING_DEFINITION records
    - VARIABLE_MAPPING and VARIABLE_MAPPING_ITEM records
    - MEMBER_MAPPING and MEMBER_MAPPING_ITEM records
    - MAPPING_TO_CUBE links
    - All output layer artifacts (CUBE, CUBE_STRUCTURE, COMBINATION, etc.)
    """
    if request.method != 'POST':
        messages.error(request, 'Invalid request method.')
        return redirect('pybirdai:output_layer_mapping_step2')

    # Get selected mapping IDs from POST data
    selected_mappings = request.POST.getlist('selected_mappings[]')

    if not selected_mappings:
        messages.error(request, 'No mappings selected. Please select at least one mapping.')
        return redirect('pybirdai:output_layer_mapping_step2')

    logger.info(f"[STEP 2 DELETE BULK] Deleting {len(selected_mappings)} mapping(s) and related artifacts")

    # Validate that all selected mappings exist
    mappings = MAPPING_DEFINITION.objects.filter(mapping_id__in=selected_mappings)
    if mappings.count() != len(selected_mappings):
        messages.error(request, 'Some selected mappings do not exist.')
        return redirect('pybirdai:output_layer_mapping_step2')

    try:
        # Step 1: Delete all output layer artifacts (cubes, combinations, etc.)
        artifact_stats = delete_mapping_artifacts(selected_mappings)
        logger.info(f"[STEP 2 DELETE BULK] Deleted artifacts: {artifact_stats}")

        # Step 2: Delete MAPPING_TO_CUBE links
        mapping_to_cube_deleted = MAPPING_TO_CUBE.objects.filter(
            mapping_id__in=selected_mappings
        ).delete()
        logger.info(f"[STEP 2 DELETE BULK] Deleted {mapping_to_cube_deleted[0]} MAPPING_TO_CUBE links")

        # Step 3: Delete MEMBER_MAPPING_ITEM and MEMBER_MAPPING records
        member_mappings_deleted = 0
        member_items_deleted = 0
        for mapping in mappings:
            if mapping.member_mapping_id:
                # Delete items first (FK dependency)
                items_count = MEMBER_MAPPING_ITEM.objects.filter(
                    member_mapping_id=mapping.member_mapping_id
                ).delete()
                member_items_deleted += items_count[0] if items_count[0] else 0

                # Delete member mapping
                mm_count = MEMBER_MAPPING.objects.filter(
                    member_mapping_id=mapping.member_mapping_id.member_mapping_id
                ).delete()
                member_mappings_deleted += mm_count[0] if mm_count[0] else 0

        logger.info(f"[STEP 2 DELETE BULK] Deleted {member_items_deleted} MEMBER_MAPPING_ITEM records")
        logger.info(f"[STEP 2 DELETE BULK] Deleted {member_mappings_deleted} MEMBER_MAPPING records")

        # Step 4: Delete VARIABLE_MAPPING_ITEM and VARIABLE_MAPPING records
        variable_mappings_deleted = 0
        variable_items_deleted = 0
        for mapping in mappings:
            if mapping.variable_mapping_id:
                # Delete items first (FK dependency)
                items_count = VARIABLE_MAPPING_ITEM.objects.filter(
                    variable_mapping_id=mapping.variable_mapping_id
                ).delete()
                variable_items_deleted += items_count[0] if items_count[0] else 0

                # Delete variable mapping
                vm_count = VARIABLE_MAPPING.objects.filter(
                    variable_mapping_id=mapping.variable_mapping_id.variable_mapping_id
                ).delete()
                variable_mappings_deleted += vm_count[0] if vm_count[0] else 0

        logger.info(f"[STEP 2 DELETE BULK] Deleted {variable_items_deleted} VARIABLE_MAPPING_ITEM records")
        logger.info(f"[STEP 2 DELETE BULK] Deleted {variable_mappings_deleted} VARIABLE_MAPPING records")

        # Step 5: Delete MAPPING_DEFINITION records
        mapping_names = [m.name for m in mappings]
        mappings_deleted = mappings.delete()
        logger.info(f"[STEP 2 DELETE BULK] Deleted {mappings_deleted[0]} MAPPING_DEFINITION records")

        # Success message with statistics
        messages.success(
            request,
            f"Successfully deleted {len(selected_mappings)} mapping(s): "
            f"{', '.join(mapping_names[:3])}{'...' if len(mapping_names) > 3 else ''}. "
            f"Also deleted: {artifact_stats['cube']} cubes, {artifact_stats['combination']} combinations, "
            f"{variable_mappings_deleted} variable mappings, {member_mappings_deleted} member mappings."
        )

    except Exception as e:
        logger.error(f"[STEP 2 DELETE BULK] Error during deletion: {str(e)}")
        messages.error(request, f"Error deleting mappings: {str(e)}")

    # Redirect back to Step 2
    return redirect('pybirdai:output_layer_mapping_step2')


def step2_go_back(request):
    """
    Go back to Step 1 from Step 2, preserving framework/version/table selection in session.
    """
    # Session data is already preserved (olmw_framework, olmw_version, olmw_table_code, olmw_table_id)
    # Just redirect to Step 1
    messages.info(request, 'Returned to table selection. Your previous selection is preserved.')
    return redirect('pybirdai:output_layer_mapping_step1')


# =============================================================================
# Output Layer Viewer APIs for Task 1 Review
# =============================================================================

@require_http_methods(["GET"])
def api_output_layer_frameworks(request):
    """
    API endpoint to get list of frameworks that have tables (reference templates).
    Returns all frameworks that have associated tables.
    """
    try:
        from pybirdai.models.bird_meta_data_model_extension import FRAMEWORK_TABLE

        # Get distinct framework IDs that have tables
        framework_ids_with_tables = FRAMEWORK_TABLE.objects.values_list(
            'framework_id', flat=True
        ).distinct()

        # Get all frameworks that have tables associated
        frameworks = FRAMEWORK.objects.filter(
            framework_id__in=framework_ids_with_tables
        ).order_by('framework_id')

        framework_list = [
            {
                'framework_id': fw.framework_id,
                'name': fw.name or fw.framework_id,
                'code': fw.code,
            }
            for fw in frameworks
        ]

        return JsonResponse({
            'status': 'success',
            'frameworks': framework_list,
            'count': len(framework_list)
        })

    except Exception as e:
        logger.error(f"Error fetching frameworks with reference packages: {e}")
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)


@require_http_methods(["GET"])
def api_output_layer_tables(request, framework_id):
    """
    API endpoint to get list of tables for a given framework.
    Returns tables with their basic metadata.
    """
    try:
        from pybirdai.models.bird_meta_data_model_extension import FRAMEWORK_TABLE

        # Get table IDs associated with this framework
        framework_table_ids = FRAMEWORK_TABLE.objects.filter(
            framework_id=framework_id
        ).values_list('table_id', flat=True)

        # Get the tables
        tables = TABLE.objects.filter(
            table_id__in=framework_table_ids
        ).order_by('code', 'name')

        table_list = [
            {
                'table_id': tbl.table_id,
                'name': tbl.name or tbl.table_id,
                'code': tbl.code,
                'version': tbl.version,
            }
            for tbl in tables
        ]

        return JsonResponse({
            'status': 'success',
            'framework_id': framework_id,
            'tables': table_list,
            'count': len(table_list)
        })

    except Exception as e:
        logger.error(f"Error fetching tables for framework {framework_id}: {e}")
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)


@require_http_methods(["GET"])
def api_output_layer_detail(request, table_id):
    """
    API endpoint to get combined cube structure and reference template data for a table.
    Returns both the cube structure (if exists) and the reference template grid data.
    """
    try:
        # Get the table
        try:
            table = TABLE.objects.get(table_id=table_id)
        except TABLE.DoesNotExist:
            return JsonResponse({
                'status': 'error',
                'message': f'Table not found: {table_id}'
            }, status=404)

        # Try to find associated cube
        # Cubes are created with patterns like: {table_code}_{framework}_{version}_CUBE
        # or {table_code}_REF_CUBE_{timestamp}
        cube_data = None
        table_code = table.code or ''

        # Try multiple patterns to find associated cube
        cube = CUBE.objects.filter(
            cube_id__icontains=table_code,
            cube_id__endswith='_CUBE',
            cube_structure_id__isnull=False
        ).first()

        # Fallback: try matching by code field
        if not cube:
            cube = CUBE.objects.filter(
                code=table_code,
                cube_structure_id__isnull=False
            ).first()

        if cube and cube.cube_structure_id:
            structure = cube.cube_structure_id

            # Get structure items
            structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
                cube_structure_id=structure
            ).select_related(
                'variable_id',
                'variable_id__domain_id',
                'member_id',
                'subdomain_id'
            ).order_by('order')

            # Build items array
            items = []
            for item in structure_items:
                items.append({
                    'order': item.order,
                    'role': item.role,
                    'role_display': dict(CUBE_STRUCTURE_ITEM.TYP_RL).get(item.role, item.role),
                    'cube_variable_code': item.cube_variable_code,
                    'variable_id': item.variable_id.variable_id if item.variable_id else None,
                    'variable_name': item.variable_id.name if item.variable_id else 'Unknown',
                    'variable_code': item.variable_id.code if item.variable_id else None,
                    'domain_id': item.variable_id.domain_id.domain_id if item.variable_id and item.variable_id.domain_id else None,
                    'domain_name': item.variable_id.domain_id.name if item.variable_id and item.variable_id.domain_id else None,
                    'member_id': item.member_id.member_id if item.member_id else None,
                    'member_name': item.member_id.name if item.member_id else None,
                    'subdomain_id': item.subdomain_id.subdomain_id if item.subdomain_id else None,
                    'subdomain_name': item.subdomain_id.name if item.subdomain_id else None,
                    'is_mandatory': item.is_mandatory,
                    'is_identifier': item.is_identifier,
                })

            cube_data = {
                'cube_id': cube.cube_id,
                'cube_name': cube.name or cube.cube_id,
                'cube_code': cube.code,
                'structure_id': structure.cube_structure_id,
                'structure_name': structure.name or structure.cube_structure_id,
                'items': items,
                'item_count': len(items)
            }

        # Get reference template data (axes, ordinates, cells)
        table_axes = AXIS.objects.filter(table_id=table)
        table_ordinates = AXIS_ORDINATE.objects.filter(
            axis_id__in=table_axes
        ).select_related('axis_id')

        # Get ordinate items for annotations
        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=table_ordinates
        ).select_related('variable_id', 'member_id', 'axis_ordinate_id')

        # Build ordinate_id -> annotations mapping
        ordinate_annotations = {}
        for item in ordinate_items:
            ord_id = item.axis_ordinate_id.axis_ordinate_id if item.axis_ordinate_id else None
            if ord_id:
                if ord_id not in ordinate_annotations:
                    ordinate_annotations[ord_id] = []
                ordinate_annotations[ord_id].append({
                    'variable_id': item.variable_id.variable_id if item.variable_id else None,
                    'variable_code': item.variable_id.code if item.variable_id else None,
                    'variable_name': item.variable_id.name if item.variable_id else None,
                    'member_id': item.member_id.member_id if item.member_id else None,
                    'member_code': item.member_id.code if item.member_id else None,
                    'member_name': item.member_id.name if item.member_id else None,
                })

        # Get cell positions
        cell_positions = CELL_POSITION.objects.filter(
            axis_ordinate_id__in=table_ordinates
        ).select_related('axis_ordinate_id', 'axis_ordinate_id__axis_id', 'cell_id')

        cell_ids = cell_positions.values_list('cell_id', flat=True).distinct()
        table_cells = TABLE_CELL.objects.filter(cell_id__in=cell_ids)

        # Build cell_id -> positions mapping
        cell_to_positions = {}
        for pos in cell_positions:
            cell_id = pos.cell_id_id if hasattr(pos, 'cell_id_id') else (pos.cell_id.cell_id if pos.cell_id else None)
            if cell_id:
                if cell_id not in cell_to_positions:
                    cell_to_positions[cell_id] = []
                cell_to_positions[cell_id].append(pos)

        # Build ordinates data structure
        ordinates_data = {}
        for ordinate in table_ordinates:
            orientation = ordinate.axis_id.orientation if ordinate.axis_id else 'Unknown'
            ordinates_data[ordinate.axis_ordinate_id] = {
                'id': ordinate.axis_ordinate_id,
                'name': ordinate.name or ordinate.code or ordinate.axis_ordinate_id,
                'code': ordinate.code,
                'axis_name': ordinate.axis_id.name if ordinate.axis_id else 'Unknown',
                'axis_orientation': orientation,
                'level': ordinate.level or 0,
                'order': ordinate.order or 0,
                'is_abstract': ordinate.is_abstract_header,
                'annotations': ordinate_annotations.get(ordinate.axis_ordinate_id, []),
            }

        # Build cell matrix
        cell_matrix = {}
        row_ordinates_set = set()
        col_ordinates_set = set()
        z_ordinates_set = set()

        for cell in table_cells:
            positions = cell_to_positions.get(cell.cell_id, [])

            row_ord_id = None
            col_ord_id = None
            z_ord_id = None

            for pos in positions:
                if pos.axis_ordinate_id and pos.axis_ordinate_id.axis_id:
                    orientation = pos.axis_ordinate_id.axis_id.orientation
                    ord_id = pos.axis_ordinate_id.axis_ordinate_id

                    if orientation in ['Y', '2']:
                        row_ord_id = ord_id
                        row_ordinates_set.add(ord_id)
                    elif orientation in ['X', '1']:
                        col_ord_id = ord_id
                        col_ordinates_set.add(ord_id)
                    elif orientation in ['Z', '3']:
                        z_ord_id = ord_id
                        z_ordinates_set.add(ord_id)

            if row_ord_id and col_ord_id:
                cell_key = f"{row_ord_id}|{col_ord_id}"
                cell_matrix[cell_key] = {
                    'cell_id': cell.cell_id,
                    'is_shaded': cell.is_shaded,
                    'name': cell.name or '',
                    'system_data_code': cell.system_data_code,
                }

        # Sort ordinates by level and order
        row_ordinates = sorted(
            [ordinates_data[ord_id] for ord_id in row_ordinates_set if ord_id in ordinates_data],
            key=lambda x: (x.get('level', 0), x.get('order', 0))
        )
        col_ordinates = sorted(
            [ordinates_data[ord_id] for ord_id in col_ordinates_set if ord_id in ordinates_data],
            key=lambda x: (x.get('level', 0), x.get('order', 0))
        )
        z_ordinates = sorted(
            [ordinates_data[ord_id] for ord_id in z_ordinates_set if ord_id in ordinates_data],
            key=lambda x: (x.get('level', 0), x.get('order', 0))
        )

        return JsonResponse({
            'status': 'success',
            'table': {
                'table_id': table.table_id,
                'name': table.name,
                'code': table.code,
                'description': table.description,
                'version': table.version,
            },
            'cube': cube_data,
            'template': {
                'row_ordinates': row_ordinates,
                'col_ordinates': col_ordinates,
                'z_ordinates': z_ordinates,
                'cell_matrix': cell_matrix,
                'total_cells': len(cell_matrix),
            }
        })

    except Exception as e:
        logger.error(f"Error fetching output layer detail for table {table_id}: {e}")
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)
