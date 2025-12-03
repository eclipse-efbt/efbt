"""
Phase 3: Mappings

Creates variable mappings, member mappings, and mapping definitions.
MAPPING_TO_CUBE links are created later in Phase 3.5 (after CUBE is created in Phase 4).
This phase only runs in normal mode (not regenerate mode).
"""

import logging
import datetime
import json
from pybirdai.models.bird_meta_data_model import (
    VARIABLE, MEMBER, VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM,
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, MAPPING_DEFINITION, MAPPING_TO_CUBE,
    MAPPING_ORDINATE_LINK, AXIS_ORDINATE
)
from pybirdai.process_steps.output_layer_mapping_workflow.table_cell_utils import (
    extract_base_table_code
)

logger = logging.getLogger(__name__)


def execute_phase3_mappings(
    request,
    table_code,
    version,
    cube,
    maintenance_agency,
    dimension_target_vars,
    observation_target_vars,
    attribute_target_vars,
    debug_data
):
    """
    Execute Phase 3: Create mappings and mapping definitions.

    This phase only runs in normal mode. In regenerate mode, mappings are reused.
    MAPPING_TO_CUBE links are created in Phase 3.5 (after CUBE exists).

    Args:
        request: Django request object (for session data)
        table_code: Table code string
        version: Version string
        cube: CUBE object (not used in Phase 3, passed for signature compatibility)
        maintenance_agency: MAINTENANCE_AGENCY object
        dimension_target_vars: List of dimension VARIABLE objects
        observation_target_vars: List of observation VARIABLE objects
        attribute_target_vars: List of attribute VARIABLE objects
        debug_data: Dict to collect created objects

    Returns:
        list: created_mapping_definitions (list of dicts with mapping info)
    """
    logger.info("[PHASE 3] Creating mappings and mapping definitions...")
    
    created_mapping_definitions = []
    
    # Extract data from session
    variable_groups = json.loads(request.session['olmw_variable_groups'])
    all_mappings = json.loads(request.session['olmw_multi_mappings'])
    
    print(f"[PHASE 3] Target variables: {len(dimension_target_vars)} dims, "
          f"{len(observation_target_vars)} observations, {len(attribute_target_vars)} attributes")
    print(f"[PHASE 3] Creating {len(all_mappings)} MAPPING_DEFINITIONs")
    
    # ========== VALIDATE TARGET VARIABLES ==========
    # Validate that all requested target variables exist
    all_requested_var_ids = set()
    for group_id, group_data in variable_groups.items():
        target_var_ids = group_data.get('targets', [])
        all_requested_var_ids.update(target_var_ids)
    
    all_found_var_ids = set()
    for var in dimension_target_vars + observation_target_vars + attribute_target_vars:
        all_found_var_ids.add(var.variable_id)
    
    missing_var_ids = all_requested_var_ids - all_found_var_ids
    if missing_var_ids:
        missing_list = ', '.join(sorted(missing_var_ids))
        error_msg = (f"Cannot proceed: The following target variables do not exist in the database: "
                    f"{missing_list}. Please ensure these variables are created in Step 4 before generating structures.")
        logger.error(f"[PHASE 3 VALIDATION] {error_msg}")
        raise ValueError(error_msg)
    
    logger.info(f"[PHASE 3 VALIDATION] All {len(all_requested_var_ids)} target variables validated successfully")
    
    # Generate sequential counter for mapping IDs (no timestamps)
    version_normalized = version.replace('.', '_')

    # Get table_id from session to extract base table code
    # This strips any existing Z-axis suffix to create consistent mapping IDs
    # (mappings are shared across Z-axis variants, so we use the base table code)
    table_id = request.session.get('olmw_table_id', '')
    base_table_code = extract_base_table_code(table_id, table_code)

    # Normalize: replace spaces AND dots with underscores for consistent IDs
    table_code_normalized = base_table_code.replace(" ", "_").replace(".", "_")
    mapping_prefix = f"{table_code_normalized}_{version_normalized}_MAP"
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
        
        print(f"[PHASE 3] Processing mapping '{mapping_name}' ({len(dimensions)} dims, {len(observations)} observations)")
        
        # 1. Create VARIABLE_MAPPING for this mapping
        try:
            variable_mapping = VARIABLE_MAPPING.objects.create(
                variable_mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_VAR",
                maintenance_agency_id=maintenance_agency,
                name=mapping_name,
                code=internal_id
            )
            logger.info(f"[PHASE 3] Created VARIABLE_MAPPING: {variable_mapping.variable_mapping_id}")
            debug_data['VARIABLE_MAPPING'].append(variable_mapping)
        except Exception as e:
            logger.error(f"[PHASE 3 ERROR] Failed to create VARIABLE_MAPPING: {str(e)}")
            raise
        
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
                    vmi = VARIABLE_MAPPING_ITEM.objects.create(
                        variable_mapping_id=variable_mapping,
                        variable_id=variable,
                        is_source=is_source
                    )
                    debug_data['VARIABLE_MAPPING_ITEM'].append(vmi)
                    created_var_ids.add(var_id)
        
        # 3. Create MEMBER_MAPPING if needed
        logger.info(f"[PHASE 3 MEMBER_MAPPING DEBUG] Processing mapping '{mapping_name}'")
        logger.info(f"[PHASE 3 MEMBER_MAPPING DEBUG] dimensions type: {type(dimensions)}, length: {len(dimensions) if dimensions else 0}")
        if dimensions:
            logger.info(f"[PHASE 3 MEMBER_MAPPING DEBUG] Sample dimension row: {dimensions[0]}")
            logger.info(f"[PHASE 3 MEMBER_MAPPING DEBUG] Will create MEMBER_MAPPING with {len(dimensions)} rows")
        else:
            logger.warning(f"[PHASE 3 MEMBER_MAPPING DEBUG] No dimensions for mapping '{mapping_name}' - will NOT create MEMBER_MAPPING")
        
        member_mapping = None
        if dimensions:
            try:
                member_mapping = MEMBER_MAPPING.objects.create(
                    member_mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_MEM",
                    maintenance_agency_id=maintenance_agency,
                    name=f"{mapping_name} - Member Mappings",
                    code=f"{internal_id}_MEM"
                )
                logger.info(f"[PHASE 3] Created MEMBER_MAPPING: {member_mapping.member_mapping_id}")
                debug_data['MEMBER_MAPPING'].append(member_mapping)
            except Exception as e:
                logger.error(f"[PHASE 3 ERROR] Failed to create MEMBER_MAPPING: {str(e)}")
                raise
            
            # Create member mapping items from actual mapping rows
            for row_idx, row in enumerate(dimensions):
                for var_id, member_id in row.items():
                    if member_id:
                        variable = VARIABLE.objects.filter(variable_id=var_id).first()
                        member = MEMBER.objects.filter(member_id=member_id).first()
                        if variable and member:
                            mmi = MEMBER_MAPPING_ITEM.objects.create(
                                member_mapping_id=member_mapping,
                                member_mapping_row=str(row_idx + 1),
                                variable_id=variable,
                                is_source="true",  # Simplified
                                member_id=member
                            )
                            debug_data['MEMBER_MAPPING_ITEM'].append(mmi)
        
        # 4. Create MAPPING_DEFINITION
        algorithm = f"Mapping: {mapping_name}\n{len(dimensions)} dimension rows, {len(observations)} observation rows"
        
        # Set mapping_type based on group_type
        if group_type == 'dimension':
            mapping_type_value = 'E'  # Enumeration
        elif group_type == 'observation':
            mapping_type_value = 'O'  # Observation
        else:  # attribute
            mapping_type_value = 'A'  # Attribute
        
        # Validate variable_mapping exists before creating MAPPING_DEFINITION
        if not variable_mapping:
            error_msg = "[PHASE 3 ERROR] VARIABLE_MAPPING is None - cannot create MAPPING_DEFINITION"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Validate variable_mapping exists in database
        if not VARIABLE_MAPPING.objects.filter(variable_mapping_id=variable_mapping.variable_mapping_id).exists():
            error_msg = f"[PHASE 3 ERROR] VARIABLE_MAPPING {variable_mapping.variable_mapping_id} doesn't exist in database"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Validate member_mapping if it's set
        if member_mapping and not MEMBER_MAPPING.objects.filter(member_mapping_id=member_mapping.member_mapping_id).exists():
            error_msg = f"[PHASE 3 ERROR] MEMBER_MAPPING {member_mapping.member_mapping_id} doesn't exist in database"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"[PHASE 3 FK-VALIDATION] Creating MAPPING_DEFINITION with variable_mapping={variable_mapping.variable_mapping_id}, "
                   f"member_mapping={member_mapping.member_mapping_id if member_mapping else 'None'}")
        
        try:
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
            logger.info(f"[PHASE 3] Created MAPPING_DEFINITION: {mapping_definition.mapping_id}")
            debug_data['MAPPING_DEFINITION'].append(mapping_definition)
        except Exception as e:
            logger.error(f"[PHASE 3 ERROR] Failed to create MAPPING_DEFINITION: {str(e)}")
            logger.error(f"[PHASE 3 ERROR] variable_mapping type: {type(variable_mapping)}, value: {variable_mapping}")
            logger.error(f"[PHASE 3 ERROR] member_mapping type: {type(member_mapping)}, value: {member_mapping}")
            raise
        
        created_mapping_definitions.append({
            'name': mapping_name,
            'mapping_definition': mapping_definition,
            'internal_id': internal_id
        })

        print(f"[PHASE 3] Created MAPPING_DEFINITION: {mapping_definition.mapping_id}")

    # ========== CREATE MAPPING_ORDINATE_LINK RECORDS ==========
    # Link each mapping to the selected ordinates for edit reconstruction
    selected_ordinates = request.session.get('olmw_selected_ordinates', [])
    if selected_ordinates and created_mapping_definitions:
        logger.info(f"[PHASE 3] Creating MAPPING_ORDINATE_LINK records for {len(selected_ordinates)} ordinates")

        # Get all AXIS_ORDINATE objects for the selected ordinates
        ordinate_objects = AXIS_ORDINATE.objects.filter(axis_ordinate_id__in=selected_ordinates)
        ordinate_map = {o.axis_ordinate_id: o for o in ordinate_objects}

        links_created = 0
        for mapping_info in created_mapping_definitions:
            mapping_def = mapping_info['mapping_definition']
            for ordinate_id in selected_ordinates:
                ordinate = ordinate_map.get(ordinate_id)
                if ordinate:
                    MAPPING_ORDINATE_LINK.objects.get_or_create(
                        mapping_id=mapping_def,
                        axis_ordinate_id=ordinate
                    )
                    links_created += 1

        logger.info(f"[PHASE 3] Created {links_created} MAPPING_ORDINATE_LINK records")
        if 'MAPPING_ORDINATE_LINK' not in debug_data:
            debug_data['MAPPING_ORDINATE_LINK'] = []
        debug_data['MAPPING_ORDINATE_LINK'].append(f"{links_created} links created")

    # NOTE: MAPPING_TO_CUBE links are created in Phase 3.5 (in the view) after CUBE is created in Phase 4
    logger.info(f"[PHASE 3] Completed: Created {len(created_mapping_definitions)} mapping definitions")

    return created_mapping_definitions
