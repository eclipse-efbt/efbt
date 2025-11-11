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

logger = logging.getLogger(__name__)

from pybirdai.models.bird_meta_data_model import (
    TABLE, TABLE_CELL, COMBINATION, COMBINATION_ITEM,
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION,
    MAPPING_DEFINITION, VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM,
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, MAPPING_TO_CUBE,
    DOMAIN, MEMBER, SUBDOMAIN, SUBDOMAIN_ENUMERATION,
    VARIABLE, FRAMEWORK, MAINTENANCE_AGENCY
)
from pybirdai.process_steps.output_layer_mapping_workflow.mapping_orchestrator import OutputLayerMappingOrchestrator
from pybirdai.process_steps.output_layer_mapping_workflow.combination_creator import CombinationCreator
from pybirdai.process_steps.output_layer_mapping_workflow.domain_manager import DomainManager
from pybirdai.process_steps.output_layer_mapping_workflow.cube_structure_generator import CubeStructureGenerator
from pybirdai.process_steps.output_layer_mapping_workflow.naming_utils import NamingUtils


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
        # Special case: EBA_ATY is a meta-variable representing all observation metrics
        # It has an enumerated domain but expands to actual observation variables at runtime
        if variable.variable_id == 'EBA_ATY':
            return True

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
            str(domain.domain_id) if domain.domain_id else ''
        ]
        return any(
            pattern in field
            for pattern in numeric_patterns
            for field in domain_fields
        )

    elif group_type == 'Attribute':
        # Attribute variables must have string domains
        string_patterns = ['String', 'EBA_String']
        domain_fields = [
            str(domain.code) if domain.code else '',
            str(domain.name) if domain.name else '',
            str(domain.domain_id) if domain.domain_id else ''
        ]
        return any(
            pattern in field
            for pattern in string_patterns
            for field in domain_fields
        )

    return False


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

    return render(request, 'pybirdai/output_layer_mapping_workflow/step1_select_table.html', context)


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
    # Use table.code and table.version to construct search pattern

    # Start with filtering by table code (required)
    mapping_query = MAPPING_TO_CUBE.objects.filter(
        cube_mapping_id__icontains=f"{table.code}_REF_{table.version}"
    )
    logging.info(f"Initial mapping query: {table.code}_REF_{table.version}")

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

        mapping_details.append(details)

    context = {
        'table': table,
        'existing_mappings': mapping_details,
        'has_existing': len(mapping_details) > 0,
        'step': 2,
        'total_steps': 7
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step2_existing_mappings.html', context)


def _load_existing_mapping_to_session(request, mapping_id):
    """
    Helper function to load existing mapping data into session for modification.
    """
    try:
        mapping = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)

        # Load variable mappings
        if mapping.variable_mapping_id:
            var_items = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id=mapping.variable_mapping_id
            )
            # Convert to session format
            # This is a placeholder - actual implementation would need to match the session structure
            breakdowns = {}
            for item in var_items:
                if item.variable_id:
                    breakdowns[str(item.variable_id.variable_id)] = {
                        'source': str(item.variable_id.variable_id),
                        'rule': 'direct'  # Default, would need to be extracted from algorithm
                    }
            request.session['olmw_breakdowns'] = json.dumps(breakdowns)

        # Load member mappings and other details
        # This would need to be expanded based on the actual mapping structure

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

    # If ordinates were selected, filter variables to only those from selected ordinates
    if selected_ordinates:
        from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM

        # Get variables directly from selected ordinates
        ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('variable_id', 'member_id')

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
                        'subdomain': None  # ORDINATE_ITEM doesn't have subdomain_id
                    }
                if item.member_id:
                    variables[var_id]['members'].add(item.member_id)
    else:
        # Fallback: Get all variables from table combinations (original behavior)
        # Get table cells with their combinations
        cells = TABLE_CELL.objects.filter(table_id=table).select_related()

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
                        'subdomain': item.subdomain_id
                    }
                if item.member_id:
                    variables[var_id]['members'].add(item.member_id)

    if request.method == 'POST':
        # ========== DEBUG: Log POST Data ==========
        print("\n" + "="*80)
        print("[STEP 2 DEBUG] POST Data Received:")
        for key in sorted(request.POST.keys()):
            if '_type' in key or '_name' in key:
                print(f"  {key} = {request.POST.get(key)}")
        print("="*80 + "\n")
        # ========== END DEBUG ==========

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

        # ========== DEBUG: Log Extracted Groups ==========
        print(f"\n[STEP 2] Extracted {len(groups)} Groups:")
        for group_id, group_data in groups.items():
            group_type_display = group_data.get('group_type') or 'MISSING/NULL'
            emoji = '✓' if group_data.get('group_type') else '✗'
            print(f"  {emoji} {group_id}: '{group_data['name']}' -> group_type='{group_type_display}'")
        print()
        # ========== END DEBUG ==========

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
            return render(request, 'pybirdai/output_layer_mapping_workflow/step4_variable_breakdown.html', context)

        # ========== DOMAIN VALIDATION: Check variables match their group type ==========
        domain_validation_errors = []
        for group_id, group_data in groups.items():
            group_type = group_data.get('group_type')
            if group_type:  # Only validate if group type is set
                group_name = group_data.get('name', group_id)
                for var_id in group_data['variable_ids']:
                    try:
                        variable = VARIABLE.objects.select_related('domain_id').get(variable_id=var_id)
                        if not is_valid_variable_for_group_type(variable, group_type):
                            domain = variable.domain_id
                            domain_info = f"{domain.domain_id}" if domain else "No domain"
                            if domain:
                                if group_type == 'Dimension':
                                    reason = f"domain is not enumerated (is_enumerated={domain.is_enumerated})"
                                elif group_type == 'Observation':
                                    reason = f"domain '{domain_info}' does not contain Integer or Float"
                                elif group_type == 'Attribute':
                                    reason = f"domain '{domain_info}' does not contain String"
                                else:
                                    reason = f"invalid domain for {group_type}"
                            else:
                                reason = "no domain assigned"

                            domain_validation_errors.append({
                                'var_id': var_id,
                                'group_name': group_name,
                                'group_type': group_type,
                                'reason': reason
                            })
                    except VARIABLE.DoesNotExist:
                        domain_validation_errors.append({
                            'var_id': var_id,
                            'group_name': group_name,
                            'group_type': group_type,
                            'reason': 'variable not found in database'
                        })

        if domain_validation_errors:
            # Build detailed error message
            error_details = []
            for error in domain_validation_errors:
                error_details.append(
                    f"Variable '{error['var_id']}' in group '{error['group_name']}' "
                    f"(type: {error['group_type']}): {error['reason']}"
                )
            error_message = (
                "❌ Domain validation failed. The following variables do not match their group type requirements:\n"
                + "\n".join(error_details)
            )
            messages.error(request, error_message)
            print(f"\n[STEP 4 DOMAIN VALIDATION ERROR]\n{error_message}\n")

            # Re-render form with error
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
            return render(request, 'pybirdai/output_layer_mapping_workflow/step4_variable_breakdown.html', context)
        # ========== END DOMAIN VALIDATION ==========

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
            print(f"\n[STEP 4 VALIDATION ERROR] {error_message}\n")

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
            return render(request, 'pybirdai/output_layer_mapping_workflow/step4_variable_breakdown.html', context)
        # ========== END STRICT VALIDATION ==========

        print(f"\n[STEP 2 SUCCESS] All {len(groups)} groups validated and saved to session.")
        for group_id, group_data in groups.items():
            print(f"  ✓ {group_data['name']}: {group_data['group_type']}")
        print()

        # ========== NORMALIZE group_type to lowercase for step5 compatibility ==========
        # Step4 validation uses title case ('Dimension', 'Observation', 'Attribute')
        # Step5 validation expects lowercase ('dimension', 'observation', 'attribute')
        # Normalize here before saving to session
        for group_id, group_data in groups.items():
            if group_data.get('group_type'):
                group_data['group_type'] = group_data['group_type'].lower()
        print(f"[DEBUG] Normalized group_type values to lowercase for session storage")
        # ========== END NORMALIZATION ==========

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

    # DEBUG: Log variable counts and sample IDs
    print(f"[DEBUG] Step 2 Variable Breakdown - Variables in context: {len(variables)}")
    print(f"[DEBUG] All variables count: {all_variables.count()}")
    print(f"[DEBUG] Target variables (filtered) count: {target_variables.count()}")
    if variables:
        sample_var_ids = list(variables.keys())[:5]
        print(f"[DEBUG] Sample variable IDs from context: {sample_var_ids}")
    else:
        print("[DEBUG] WARNING: No variables in context! Check if ordinates were selected.")
    if all_variables.exists():
        sample_all_vars = [v.variable_id for v in all_variables[:5]]
        print(f"[DEBUG] Sample all_variables IDs: {sample_all_vars}")
    else:
        print("[DEBUG] WARNING: No variables in database!")
    if target_variables.exists():
        sample_target_vars = [v.variable_id for v in target_variables[:5]]
        print(f"[DEBUG] Sample target_variables IDs: {sample_target_vars}")
    else:
        print("[DEBUG] WARNING: No target variables after filtering!")

    context = {
        'table': table,
        'variables': variables,
        'all_variables': all_variables,
        'target_variables': target_variables,
        'step': 4,
        'total_steps': 7
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step4_variable_breakdown.html', context)


def select_axis_ordinates(request):
    """
    Step 2.5: Allow user to select which axis ordinates should be mapped.
    These ordinates will be used to filter which cells are processed.
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

    # Get all cells for this table
    table_cells = TABLE_CELL.objects.filter(table_id=table)

    # Get all cell positions for these cells
    cell_positions = CELL_POSITION.objects.filter(
        cell_id__in=table_cells
    ).select_related('axis_ordinate_id', 'axis_ordinate_id__axis_id')

    # Build unique axis ordinates with their details
    ordinates_data = {}
    for cp in cell_positions:
        ordinate = cp.axis_ordinate_id
        if ordinate and ordinate.axis_ordinate_id not in ordinates_data:
            # Use the ordinate name directly instead of querying ORDINATE_ITEM
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

    return render(request, 'pybirdai/output_layer_mapping_workflow/step3_select_ordinates.html', context)


def generate_table_html(table, ordinates_data):
    """
    Generate HTML representation of TABLE with selectable ordinates.
    Creates an interactive table matching the actual table structure.
    """
    from pybirdai.models.bird_meta_data_model import (
        TABLE_CELL, CELL_POSITION, AXIS_ORDINATE, AXIS
    )

    # Get all cells for this table
    cells = TABLE_CELL.objects.filter(table_id=table).select_related()

    # Build structure: (row_ordinate_id, col_ordinate_id) -> cell
    cell_matrix = {}
    row_ordinates_set = set()
    col_ordinates_set = set()

    for cell in cells:
        positions = CELL_POSITION.objects.filter(cell_id=cell).select_related(
            'axis_ordinate_id', 'axis_ordinate_id__axis_id'
        )

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

    print(f"\n[STEP 5] Processing {len(variable_groups)} groups for table '{table.name}'")

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
        print(f"[STEP 5 ERROR] {error_msg}")
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
        print(f"[STEP 5] Querying ordinate members for {len(selected_ordinates)} selected ordinates")
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

        print(f"[STEP 5] Found ordinate members for {len(all_ordinate_members)} variables")

    # ========== 4. PROCESS EACH VARIABLE GROUP AS A SEPARATE MAPPING ==========
    from itertools import product

    mappings_data = []  # List of mapping objects, one per group

    for group_id, group_data in variable_groups.items():
        group_name = group_data.get('name', group_id)
        group_type = group_data['group_type']
        source_var_ids = group_data.get('variable_ids', [])
        target_var_ids = group_data.get('targets', [])

        print(f"[STEP 5] Processing group '{group_name}': {group_type} | {len(source_var_ids)} sources → {len(target_var_ids)} targets")

        # Skip groups with no targets
        if not target_var_ids:
            print(f"[STEP 5] Skipping '{group_name}' - no target variables defined")
            continue

        # Generate auto-name for this mapping
        auto_name = f"DPM_{table.code}_{table.version}_{','.join(source_var_ids)}"
        print(f"[STEP 5] Auto-generated name: {auto_name}")

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

        # Serialize ordinate members for JavaScript
        # Load ALL domain members for each variable, not just ordinate-filtered ones
        ordinate_members_json = {}
        for var_id in list(source_var_ids) + list(target_var_ids):
            try:
                variable = VARIABLE.objects.get(variable_id=var_id)
                if variable.domain_id and hasattr(variable.domain_id, 'domain_id'):
                    # Get ALL members from the domain
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
                    # Fallback to ordinate members if no domain
                    ordinate_members_json[var_id] = [
                        {
                            'member_id': m.member_id,
                            'name': m.name if hasattr(m, 'name') else '',
                            'code': m.code if hasattr(m, 'code') else ''
                        }
                        for m in group_ordinate_members.get(var_id, [])
                    ]
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
        if group_type == 'dimension' and dimension_source_vars:
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
                    print(f"[STEP 5] Warning: No ordinate members for dimension source variable {var_id}")
                    skipped_vars.append(var_id)

            # Generate Cartesian product only from variables with members
            if source_member_lists and vars_with_members:
                for combination in product(*source_member_lists):
                    combo_dict = {}
                    for i, member in enumerate(combination):
                        var_id = vars_with_members[i].variable_id
                        combo_dict[var_id] = member
                    dimension_combinations.append(combo_dict)

                print(f"[STEP 5] Generated {len(dimension_combinations)} dimension combinations for group '{group_name}'")

                if skipped_vars:
                    print(f"[STEP 5] Skipped {len(skipped_vars)} variables without ordinate members: {', '.join(skipped_vars)}")
            elif not source_member_lists:
                print(f"[STEP 5] No dimension variables with ordinate members found for group '{group_name}'")

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
        print("[STEP 5 WARNING] No mappings created - check target variables in Step 4")

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
        print(f"[STEP 5] Saved {len(all_mappings)} mappings to session")
        return redirect('pybirdai:output_layer_mapping_step6')

    # ========== 7. RENDER TEMPLATE ==========
    context = {
        'table': table,
        'mappings': mappings_data,
        'mappings_count': len(mappings_data),
        'all_variables': VARIABLE.objects.all().order_by('name'),
        'step': 5,
        'total_steps': 7
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step5_mapping_editor.html', context)


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

        # Generate member_id (using domain_id and code)
        member_id = f"{domain_id}_{code}"

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
        print(f"[ERROR] Error creating member: {str(e)}")
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
        summary = {
            'group_id': group_id,
            'group_name': variable_groups[group_id].get('name', group_id),
            'auto_name': mapping_data['auto_name'],
            'mapping_name': mapping_data.get('mapping_name', mapping_data['auto_name']),
            'group_type': mapping_data['group_type'],
            'dimension_count': len(mapping_data.get('dimensions', [])),
            'observation_count': len(mapping_data.get('observations', [])),
            'attribute_count': len(mapping_data.get('attributes', [])),
            'dimensions': mapping_data.get('dimensions', []),
            'observations': mapping_data.get('observations', []),
            'attributes': mapping_data.get('attributes', [])
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
        print(f"[STEP 6] Saved {len(all_mappings)} mappings with names")

        return redirect('pybirdai:output_layer_mapping_step7')

    # GET request - show review form
    context = {
        'table': table,
        'mapping_summaries': mapping_summaries,
        'mappings_count': len(mapping_summaries),
        'step': 6,
        'total_steps': 7
    }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step6_review_and_name.html', context)


@transaction.atomic
def generate_structures(request):
    """
    Step 7: Generate all the required structures for multiple mappings.
    Creates multiple MAPPING_DEFINITIONs, one shared CUBE, and all related structures.
    """
    # Check if we're in regenerate mode
    regenerate_mode = request.session.get('olmw_regenerate_mode', False)

    if regenerate_mode:
        # Regenerate mode: use existing mappings, skip creation of new MAPPING_DEFINITION
        existing_mapping_ids = request.session.get('olmw_existing_mapping_ids', [])
        if not existing_mapping_ids:
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

    if request.method == 'POST' and request.POST.get('confirm') == 'true':
        try:
            # In regenerate mode, delete old artifacts BEFORE creating new ones
            if regenerate_mode:
                existing_mapping_ids = request.session.get('olmw_existing_mapping_ids', [])
                if existing_mapping_ids:
                    print(f"[STEP 7 REGENERATE] Deleting old artifacts for {len(existing_mapping_ids)} mappings before regeneration")
                    deletion_stats = delete_mapping_artifacts(existing_mapping_ids)
                    logger.info(f"[STEP 7 REGENERATE] Deleted artifacts: {deletion_stats}")

            # Initialize orchestrator
            orchestrator = OutputLayerMappingOrchestrator()

            # Get or create maintenance agency
            maintenance_agency, created = MAINTENANCE_AGENCY.objects.get_or_create(
                maintenance_agency_id='USER',
                defaults={
                    'name': 'User Defined',
                    'code': 'USER'
                }
            )

            # Track created objects
            dimension_target_vars = []
            observation_target_vars = []
            attribute_target_vars = []

            # In regenerate mode, skip MAPPING_DEFINITION creation and use existing ones
            if regenerate_mode:
                # Use the existing mappings we retrieved earlier (set at line 1567-1570)
                print(f"[STEP 7 REGENERATE] Using {len(created_mapping_definitions)} existing mappings")

                # Extract target variables from existing mappings
                for mapping_info in created_mapping_definitions:
                    mapping_def = mapping_info['mapping_definition']
                    if mapping_def.variable_mapping_id:
                        var_items = VARIABLE_MAPPING_ITEM.objects.filter(
                            variable_mapping_id=mapping_def.variable_mapping_id,
                            is_source="false"  # Target variables
                        ).select_related('variable_id')

                        for item in var_items:
                            if item.variable_id:
                                var = item.variable_id
                                # Determine variable type based on mapping_type
                                if mapping_def.mapping_type == 'E':
                                    if var not in dimension_target_vars:
                                        dimension_target_vars.append(var)
                                elif mapping_def.mapping_type == 'O':
                                    if var not in observation_target_vars:
                                        observation_target_vars.append(var)
                                else:  # mapping_type == 'A'
                                    if var not in attribute_target_vars:
                                        attribute_target_vars.append(var)

                print(f"[STEP 7 REGENERATE] Extracted {len(dimension_target_vars)} dimensions, {len(observation_target_vars)} observations, {len(attribute_target_vars)} attributes")

            else:
                # Normal mode: Create new MAPPING_DEFINITIONs
                created_mapping_definitions = []

                # Extract target variables from variable_groups for CUBE_STRUCTURE_ITEMs
                variable_groups = json.loads(request.session['olmw_variable_groups'])
                all_mappings = json.loads(request.session['olmw_multi_mappings'])

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

                print(f"[STEP 7] Target variables: {len(dimension_target_vars)} dims, {len(observation_target_vars)} observations, {len(attribute_target_vars)} attributes")
                print(f"[STEP 7] Creating {len(all_mappings)} MAPPING_DEFINITIONs")

                # Generate sequential counter for mapping IDs (no timestamps)
                version_normalized = version.replace('.', '_')
                mapping_prefix = f"{table_code}_{version_normalized}_MAP"
                existing_count = MAPPING_DEFINITION.objects.filter(
                    code__startswith=mapping_prefix
                ).count()
                mapping_sequence_start = existing_count + 1

                # ========== LOOP THROUGH EACH MAPPING ==========
                mapping_counter = 0
                for group_id, mapping_data in all_mappings.items():
                    mapping_name = mapping_data['mapping_name']
                    internal_id = mapping_data['internal_id']
                    group_type = mapping_data.get('group_type', 'dimension').lower()
                    dimensions = mapping_data.get('dimensions', [])
                    observations = mapping_data.get('observations', [])
                    attributes = mapping_data.get('attributes', [])

                    # Calculate current mapping sequence number
                    current_sequence = mapping_sequence_start + mapping_counter
                    mapping_id_suffix = f"{current_sequence:03d}"
                    mapping_counter += 1

                    print(f"[STEP 7] Processing mapping '{mapping_name}' ({len(dimensions)} dims, {len(observations)} observations)")

                    # 1. Create VARIABLE_MAPPING for this mapping
                    variable_mapping = VARIABLE_MAPPING.objects.create(
                        variable_mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_VAR",
                        maintenance_agency_id=maintenance_agency,
                        name=mapping_name,
                        code=internal_id
                    )

                    # 2. Create VARIABLE_MAPPING_ITEMS from member mapping rows
                    # Get source and target variable IDs for this group from variable_groups
                    group_info = variable_groups.get(group_id, {})
                    source_var_ids = set(group_info.get('variable_ids', []))
                    target_var_ids = set(group_info.get('targets', []))

                    # Create VARIABLE_MAPPING_ITEMs for all variables in this mapping
                    created_var_ids = set()
                    all_var_ids = source_var_ids | target_var_ids

                    for var_id in all_var_ids:
                        if var_id not in created_var_ids:
                            variable = VARIABLE.objects.filter(variable_id=var_id).first()
                            if variable:
                                is_source = "true" if var_id in source_var_ids else "false"
                                VARIABLE_MAPPING_ITEM.objects.create(
                                    variable_mapping_id=variable_mapping,
                                    variable_id=variable,
                                    is_source=is_source
                                )
                                created_var_ids.add(var_id)

                    # 3. Create MEMBER_MAPPING if needed
                    member_mapping = None
                    if dimensions:
                        member_mapping = MEMBER_MAPPING.objects.create(
                            member_mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_MEM",
                            maintenance_agency_id=maintenance_agency,
                            name=f"{mapping_name} - Member Mappings",
                            code=f"{internal_id}_MEM"
                        )

                        # Create member mapping items from actual mapping rows
                        for row_idx, row in enumerate(dimensions):
                            for var_id, member_id in row.items():
                                if member_id:
                                    variable = VARIABLE.objects.filter(variable_id=var_id).first()
                                    member = MEMBER.objects.filter(member_id=member_id).first()
                                    if variable and member:
                                        MEMBER_MAPPING_ITEM.objects.create(
                                            member_mapping_id=member_mapping,
                                            member_mapping_row=str(row_idx + 1),
                                            variable_id=variable,
                                            is_source="true",  # Simplified
                                            member_id=member
                                        )

                    # 4. Create MAPPING_DEFINITION
                    algorithm = f"Mapping: {mapping_name}\n{len(dimensions)} dimension rows, {len(observations)} observation rows"

                    # Set mapping_type based on group_type
                    if group_type == 'dimension':
                        mapping_type_value = 'E'  # Enumeration
                    elif group_type == 'observation':
                        mapping_type_value = 'O'  # Observation
                    else:  # attribute
                        mapping_type_value = 'A'  # Attribute

                    mapping_definition = MAPPING_DEFINITION.objects.create(
                        mapping_id=f"{mapping_prefix}_{mapping_id_suffix}",
                        maintenance_agency_id=maintenance_agency,
                        name=mapping_name,
                        code=internal_id,
                        mapping_type=mapping_type_value,
                        algorithm=algorithm,
                        variable_mapping_id=variable_mapping,
                        member_mapping_id=member_mapping
                    )

                    created_mapping_definitions.append({
                        'name': mapping_name,
                        'mapping_definition': mapping_definition,
                        'internal_id': internal_id
                    })

                    print(f"[STEP 7] Created MAPPING_DEFINITION: {mapping_definition.mapping_id}")

            # End of mapping loop

            # ========== CREATE CUBE FOR ALL MAPPINGS ==========
            print(f"[STEP 7] Creating CUBE for {len(created_mapping_definitions)} mappings")

            # 5. Get or create CUBE_STRUCTURE with all target variables
            cube_structure_id = f"{table_code}_{version_normalized}_STRUCTURE"
            cube_structure, created = CUBE_STRUCTURE.objects.get_or_create(
                cube_structure_id=cube_structure_id,
                defaults={
                    'maintenance_agency_id': maintenance_agency,
                    'name': f"Reference structure for {table_code}",
                    'code': f"{table_code}_CS",
                    'description': f"Cube structure for {len(created_mapping_definitions)} mappings",
                    'version': version
                }
            )
            if created:
                print(f"[STEP 7] Created new CUBE_STRUCTURE: {cube_structure_id}")
            else:
                print(f"[STEP 7] Reusing existing CUBE_STRUCTURE: {cube_structure_id}")
                # Update description to reflect current mapping count
                cube_structure.description = f"Cube structure for {len(created_mapping_definitions)} mappings"
                cube_structure.save()

            # 6. Create CUBE_STRUCTURE_ITEMS for all target variables
            csi_generator = CubeStructureGenerator()
            order_counter = 1

            # Get unique target variables (de-duplicate in case of overlap)
            unique_dimension_vars = {v.variable_id: v for v in dimension_target_vars}.values()
            unique_observation_vars = {v.variable_id: v for v in observation_target_vars}.values()
            unique_attribute_vars = {v.variable_id: v for v in attribute_target_vars}.values()

            # Combine all unique target variables for later use
            unique_target_vars = list(unique_dimension_vars) + list(unique_observation_vars) + list(unique_attribute_vars)

            print(f"[STEP 7] Creating CUBE_STRUCTURE_ITEMs: {len(unique_dimension_vars)} dims, {len(unique_observation_vars)} observations, {len(unique_attribute_vars)} attributes")

            # Create dimension items (role="D")
            for variable in unique_dimension_vars:
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
                item, item_created = CUBE_STRUCTURE_ITEM.objects.get_or_create(
                    cube_structure_id=cube_structure,
                    cube_variable_code=cube_variable_code,
                    defaults={
                        'variable_id': variable,
                        'role': "D",
                        'order': order_counter,
                        'subdomain_id': subdomain,
                        'member_id': single_member,
                        'dimension_type': dimension_type,
                        'is_mandatory': True,
                        'is_implemented': True,
                        'description': f"Dimension: {variable.name}"
                    }
                )
                if not item_created:
                    # Update existing item
                    item.variable_id = variable
                    item.role = "D"
                    item.order = order_counter
                    item.subdomain_id = subdomain
                    item.member_id = single_member
                    item.dimension_type = dimension_type
                    item.is_mandatory = True
                    item.is_implemented = True
                    item.description = f"Dimension: {variable.name}"
                    item.save()
                order_counter += 1

            # Create observation items (role="O" for Observation)
            for variable in unique_observation_vars:
                cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"
                item, item_created = CUBE_STRUCTURE_ITEM.objects.get_or_create(
                    cube_structure_id=cube_structure,
                    cube_variable_code=cube_variable_code,
                    defaults={
                        'variable_id': variable,
                        'role': "O",
                        'order': order_counter,
                        'is_mandatory': True,
                        'is_implemented': True,
                        'is_flow': True,
                        'description': f"Observation: {variable.name}"
                    }
                )
                if not item_created:
                    # Update existing item
                    item.variable_id = variable
                    item.role = "O"
                    item.order = order_counter
                    item.is_mandatory = True
                    item.is_implemented = True
                    item.is_flow = True
                    item.description = f"Observation: {variable.name}"
                    item.save()
                order_counter += 1

            # Create attribute items (role="A")
            for variable in unique_attribute_vars:
                cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"
                item, item_created = CUBE_STRUCTURE_ITEM.objects.get_or_create(
                    cube_structure_id=cube_structure,
                    cube_variable_code=cube_variable_code,
                    defaults={
                        'variable_id': variable,
                        'role': "A",
                        'order': order_counter,
                        'is_mandatory': False,
                        'is_implemented': True,
                        'description': f"Attribute: {variable.name}"
                    }
                )
                if not item_created:
                    # Update existing item
                    item.variable_id = variable
                    item.role = "A"
                    item.order = order_counter
                    item.is_mandatory = False
                    item.is_implemented = True
                    item.description = f"Attribute: {variable.name}"
                    item.save()
                order_counter += 1

            # 7. Get or create CUBE
            framework_obj = FRAMEWORK.objects.filter(framework_id=framework).first()

            # Auto-create framework if it doesn't exist (e.g., COREP_REF, FINREP_REF)
            if not framework_obj:
                # Get or create EFBT maintenance agency
                efbt_agency, _ = MAINTENANCE_AGENCY.objects.get_or_create(
                    maintenance_agency_id='EFBT',
                    defaults={
                        'name': 'EFBT System',
                        'code': 'EFBT'
                    }
                )

                # Create the missing framework
                framework_obj, created = FRAMEWORK.objects.get_or_create(
                    framework_id=framework,
                    defaults={
                        'name': framework,
                        'code': framework,
                        'maintenance_agency_id': efbt_agency,
                        'description': f'Auto-generated framework for {framework}'
                    }
                )
                if created:
                    print(f"[STEP 7] Auto-created FRAMEWORK: {framework} with EFBT maintenance agency")

            cube_id = f"{table_code}_{framework}_{version_normalized}_CUBE"
            cube, cube_created = CUBE.objects.get_or_create(
                cube_id=cube_id,
                defaults={
                    'maintenance_agency_id': maintenance_agency,
                    'name': f"Reference cube for {table_code}",
                    'code': f"{table_code}_CUBE",
                    'framework_id': framework_obj,
                    'cube_structure_id': cube_structure,
                    'cube_type': "RC",  # Reference Cube
                    'is_allowed': True,
                    'published': False,
                    'version': version,
                    'description': f"Cube for {len(created_mapping_definitions)} mapping definitions"
                }
            )
            if cube_created:
                print(f"[STEP 7] Created new CUBE: {cube_id}")
            else:
                print(f"[STEP 7] Reusing existing CUBE: {cube_id}")
                # Update cube to point to current cube_structure
                cube.cube_structure_id = cube_structure
                cube.description = f"Cube for {len(created_mapping_definitions)} mapping definitions"
                cube.save()

            # 8. Link each MAPPING_DEFINITION to CUBE via MAPPING_TO_CUBE
            from pybirdai.models.bird_meta_data_model import MAPPING_TO_CUBE

            # First, delete old MAPPING_TO_CUBE records for this table
            # (they were kept during deletion so we could get here, now we delete them)
            cube_mapping_id_pattern = f"M_{table_code}_REF_{version}"
            old_mapping_to_cube = MAPPING_TO_CUBE.objects.filter(
                cube_mapping_id=cube_mapping_id_pattern
            )
            old_count = old_mapping_to_cube.count()
            if old_count > 0:
                old_mapping_to_cube.delete()
                print(f"[STEP 7] Deleted {old_count} old MAPPING_TO_CUBE record(s)")

            # Now create new MAPPING_TO_CUBE records
            # Set valid_from to today 00:00:00 and valid_to to 9999-12-31 00:00:00
            today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            far_future = datetime.datetime(9999, 12, 31, 0, 0, 0)

            for mapping_info in created_mapping_definitions:
                mapping_def = mapping_info['mapping_definition']

                # Generate cube_mapping_id: M_{table_code}_REF_{version}
                cube_mapping_id = f"M_{table_code}_REF_{version}"

                MAPPING_TO_CUBE.objects.create(
                    cube_mapping_id=cube_mapping_id,
                    mapping_id=mapping_def,
                    valid_from=today_start,
                    valid_to=far_future
                )
                print(f"[STEP 7] Linked {mapping_def.mapping_id} to {cube.cube_id}")

            # 9. Create non-reference combinations and link to cube
            table = TABLE.objects.get(table_id=table_id)
            cells = TABLE_CELL.objects.filter(table_id=table)

            # Filter cells based on selected ordinates if any
            selected_ordinates = request.session.get('olmw_selected_ordinates', [])
            if selected_ordinates:
                from pybirdai.models.bird_meta_data_model import CELL_POSITION

                # Get cells that have positions in selected ordinates
                cell_positions = CELL_POSITION.objects.filter(
                    axis_ordinate_id__in=selected_ordinates,
                    cell_id__in=cells
                ).values_list('cell_id', flat=True).distinct()

                # Filter to only cells with selected ordinates
                cells = cells.filter(cell_id__in=cell_positions)
                messages.info(request, f'Creating combinations for {cells.count()} cells matching selected ordinates.')

            # Generate single timestamp for entire generation run
            generation_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

            # Create combination creator with table code and version
            combination_creator = CombinationCreator(table_code, version_normalized)
            created_combinations = []

            for cell in cells:
                # Create combination for this cell
                combination = combination_creator.create_combination_for_cell(
                    cell, cube, generation_timestamp
                )
                if combination:
                    created_combinations.append(combination)

                    # Create CUBE_TO_COMBINATION link (get or create to avoid duplicates)
                    CUBE_TO_COMBINATION.objects.get_or_create(
                        cube_id=cube,
                        combination_id=combination
                    )

            # 10. Create/Update domains and members as needed
            domain_manager = DomainManager()
            for variable in unique_target_vars:
                domain_manager.ensure_domain_and_members(variable, maintenance_agency)

            # Clear session data
            for key in list(request.session.keys()):
                if key.startswith('olmw_'):
                    del request.session[key]

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
                    'combinations_created': len(created_combinations)
                },
                'step': 7,
                'total_steps': 7
            }

            messages.success(request, f'Successfully created {len(created_mapping_definitions)} output layer mappings with shared cube')

        except Exception as e:
            # Rollback is automatic with @transaction.atomic
            messages.error(request, f'Error creating structures: {str(e)}')
            context = {
                'success': False,
                'error': str(e),
                'step': 7,
                'total_steps': 7
            }

    else:
        # GET request - show confirmation page for all mappings
        mapping_summaries = []

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
                mapping_summaries.append({
                    'name': mapping_data['mapping_name'],
                    'internal_id': mapping_data.get('internal_id', ''),
                    'dimension_count': len(mapping_data.get('dimensions', [])),
                    'observation_count': len(mapping_data.get('observations', [])),
                    'attribute_count': len(mapping_data.get('attributes', []))
                })

        context = {
            'mappings': mapping_summaries,
            'mappings_count': len(mapping_summaries),
            'table_code': table_code,
            'framework': framework,
            'version': version,
            'step': 7,
            'total_steps': 7,
            'preview': True,
            'regenerate_mode': regenerate_mode
        }

    return render(request, 'pybirdai/output_layer_mapping_workflow/step7_confirmation.html', context)


@require_http_methods(["GET"])
def get_table_cells_api(request):
    """
    API endpoint to get table cells and their combinations for AJAX requests.
    """
    table_id = request.GET.get('table_id')
    if not table_id:
        return JsonResponse({'error': 'Table ID required'}, status=400)

    try:
        table = TABLE.objects.get(table_id=table_id)
        cells = TABLE_CELL.objects.filter(table_id=table)

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
                'code': table.code
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
            # Get all tables that match the framework (case-insensitive string matching)
            # Check if framework code appears in table_id or code
            tables = TABLE.objects.filter(
                models.Q(table_id__icontains=framework_code) |
                models.Q(code__icontains=framework_code)
            )

            # Get distinct versions from matching tables
            versions_list = tables.values_list('version', flat=True).distinct().order_by('version')
            versions_list = [v for v in versions_list if v]  # Filter out None/empty values

            return JsonResponse({
                'status': 'success',
                'versions': versions_list
            })

        # Case 3: Framework and version provided - return filtered tables
        if framework_code and version:
            # Filter tables by both framework (string matching) and version
            tables = TABLE.objects.filter(
                models.Q(table_id__icontains=framework_code) |
                models.Q(code__icontains=framework_code),
                version=version
            ).order_by('name')

            return JsonResponse({
                'status': 'success',
                'tables': [
                    {
                        'id': table.table_id,
                        'code': table.code,
                        'name': table.name,
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

            # Check if variable already exists
            if VARIABLE.objects.filter(variable_id=variable_id).exists():
                return JsonResponse({'success': False, 'error': 'Variable ID already exists'})

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

            # Check if domain already exists
            if DOMAIN.objects.filter(domain_id=domain_id).exists():
                return JsonResponse({'success': False, 'error': 'Domain ID already exists'})

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
            'member_id',
            'subdomain_id'
        ).order_by('order')

        # Build the items array
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
    return render(request, 'pybirdai/shared/cube_structure_viewer.html', context)
