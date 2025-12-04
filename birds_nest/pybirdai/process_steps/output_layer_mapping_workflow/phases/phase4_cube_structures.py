"""
Phase 4: Cube Structures & Subdomains

Creates cube structure, cube structure items, subdomains, subdomain enumerations, and cube.
This is the critical phase most likely to have FK violations.
"""

import logging
from pybirdai.models.bird_meta_data_model import (
    CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE, MEMBER
)
from pybirdai.process_steps.output_layer_mapping_workflow.cube_structure_generator import CubeStructureGenerator

logger = logging.getLogger(__name__)


def execute_phase4_cube_structures(
    table_id,
    table_code,
    version,
    framework_obj,
    maintenance_agency,
    dimension_target_vars,
    observation_target_vars,
    attribute_target_vars,
    created_mapping_definitions,
    debug_data
):
    """
    Execute Phase 4: Create cube structures, items, subdomains, and cube.
    
    This is the most complex phase and the most likely source of FK violations.
    Subdomains and subdomain enumerations are created during cube structure item creation.
    
    Args:
        table_id: Full table ID with variant suffix (e.g., "F01_01_Z0")
        table_code: Base table code (e.g., "F01_01")
        version: Version string
        framework_obj: FRAMEWORK object created in Phase 1
        maintenance_agency: MAINTENANCE_AGENCY object
        dimension_target_vars: List of unique dimension VARIABLE objects
        observation_target_vars: List of unique observation VARIABLE objects
        attribute_target_vars: List of unique attribute VARIABLE objects
        created_mapping_definitions: List of mapping definition dicts
        debug_data: Dict to collect created objects
    
    Returns:
        dict: {
            'cube_structure': CUBE_STRUCTURE object,
            'cube': CUBE object,
            'order_counter': Final order counter value
        }
    """
    logger.info("[PHASE 4] Creating cube structure and subdomains...")
    
    # ========== CREATE CUBE_STRUCTURE ==========
    cube_structure_id = f"{table_id}_STRUCTURE"
    cube_structure, cs_created = CUBE_STRUCTURE.objects.get_or_create(
        cube_structure_id=cube_structure_id,
        defaults={
            'maintenance_agency_id': maintenance_agency,
            'name': f"Reference structure for {table_id}",
            'code': f"{table_code}_CS",
            'description': f"Cube structure for {len(created_mapping_definitions)} mappings",
            'version': version
        }
    )

    if cs_created:
        logger.info(f"[PHASE 4] Created NEW CUBE_STRUCTURE: {cube_structure_id}")
    else:
        logger.info(f"[PHASE 4] Reusing existing CUBE_STRUCTURE: {cube_structure_id}")
        # Update description to reflect current mapping count
        cube_structure.description = f"Cube structure for {len(created_mapping_definitions)} mappings"
        cube_structure.save()

    if debug_data is not None:
        debug_data['CUBE_STRUCTURE'].append(cube_structure)
    
    # ========== CREATE CUBE_STRUCTURE_ITEMS ==========
    csi_generator = CubeStructureGenerator()
    order_counter = 1
    
    # Get unique target variables (de-duplicate in case of overlap)
    unique_dimension_vars = {v.variable_id: v for v in dimension_target_vars}.values()
    unique_observation_vars = {v.variable_id: v for v in observation_target_vars}.values()
    unique_attribute_vars = {v.variable_id: v for v in attribute_target_vars}.values()
    
    print(f"[PHASE 4] Creating CUBE_STRUCTURE_ITEMs: {len(unique_dimension_vars)} dims, "
          f"{len(unique_observation_vars)} observations, {len(unique_attribute_vars)} attributes")
    
    # ========== CREATE DIMENSION ITEMS (with Subdomains/Subdomain Enumerations) ==========
    # Note: Subdomains and Subdomain Enumerations are created inside the loop
    # via CubeStructureGenerator.create_or_get_subdomain() for each dimension variable
    
    for variable in unique_dimension_vars:
        # Log domain and member count before subdomain creation
        if hasattr(variable, 'domain_id') and variable.domain_id:
            domain = variable.domain_id
            member_count = MEMBER.objects.filter(domain_id=domain).count()
            logger.info(f"[PHASE 4 PRE-SUBDOMAIN] Variable {variable.variable_id} has domain "
                       f"{domain.domain_id} with {member_count} members in database")
        else:
            logger.warning(f"[PHASE 4 PRE-SUBDOMAIN] Variable {variable.variable_id} has NO domain!")
        
        # Create or get subdomain (returns tuple: subdomain, single_member)
        # This also creates SUBDOMAIN_ENUMERATION records
        subdomain, single_member = csi_generator.create_or_get_subdomain(
            variable, cube_structure.cube_structure_id
        )
        
        # Track subdomain in debug_data
        if debug_data is not None:
            if subdomain and subdomain not in debug_data['SUBDOMAIN']:
                debug_data['SUBDOMAIN'].append(subdomain)
        
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
        
        # Track in debug_data
        if debug_data is not None:
            if item not in debug_data['CUBE_STRUCTURE_ITEM']:
                debug_data['CUBE_STRUCTURE_ITEM'].append(item)

    # ========== CREATE OBSERVATION ITEMS ==========
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

        # Track in debug_data
        if debug_data is not None:
            if item not in debug_data['CUBE_STRUCTURE_ITEM']:
                debug_data['CUBE_STRUCTURE_ITEM'].append(item)

    # ========== CREATE ATTRIBUTE ITEMS ==========
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

        # Track in debug_data
        if debug_data is not None:
            if item not in debug_data['CUBE_STRUCTURE_ITEM']:
                debug_data['CUBE_STRUCTURE_ITEM'].append(item)

    if debug_data is not None:
        logger.info(f"[PHASE 4] Created {len(debug_data['CUBE_STRUCTURE_ITEM'])} CUBE_STRUCTURE_ITEMs")
    else:
        logger.info("[PHASE 4] Created CUBE_STRUCTURE_ITEMs (debug tracking disabled)")
    
    # ========== CREATE CUBE ==========
    # Framework was created in Phase 1, just validate it exists
    if not framework_obj:
        error_msg = "[PHASE 4 ERROR] FRAMEWORK was not provided from Phase 1"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Validate cube_structure exists before creating CUBE
    if not cube_structure:
        error_msg = "[PHASE 4 ERROR] CUBE_STRUCTURE is None - cannot create CUBE"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Verify cube_structure actually exists in database
    if not CUBE_STRUCTURE.objects.filter(cube_structure_id=cube_structure.cube_structure_id).exists():
        error_msg = f"[PHASE 4 ERROR] CUBE_STRUCTURE {cube_structure.cube_structure_id} doesn't exist in database"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"[PHASE 4 FK-VALIDATION] Creating CUBE with framework={framework_obj.framework_id}, "
               f"cube_structure={cube_structure.cube_structure_id}")
    
    try:
        # Use full table_id to ensure uniqueness across variants
        framework = framework_obj.framework_id  # Get framework ID for cube_id
        cube_id = f"{table_id}_{framework}_CUBE"
        cube, cube_created = CUBE.objects.get_or_create(
            cube_id=cube_id,
            defaults={
                'maintenance_agency_id': maintenance_agency,
                'name': f"Reference cube for {table_id}",
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
        logger.info(f"[PHASE 4] {'Created new' if cube_created else 'Retrieved existing'} CUBE: {cube_id}")
        if debug_data is not None:
            debug_data['CUBE'].append(cube)
    except Exception as e:
        logger.error(f"[PHASE 4 ERROR] Failed to create CUBE {cube_id}: {str(e)}")
        logger.error(f"[PHASE 4 ERROR] framework_obj type: {type(framework_obj)}, value: {framework_obj}")
        logger.error(f"[PHASE 4 ERROR] cube_structure type: {type(cube_structure)}, value: {cube_structure}")
        raise
    
    if cube_created:
        print(f"[PHASE 4] Created new CUBE: {cube_id}")
    else:
        print(f"[PHASE 4] Reusing existing CUBE: {cube_id}")
        # Update cube to point to current cube_structure
        cube.cube_structure_id = cube_structure
        cube.description = f"Cube for {len(created_mapping_definitions)} mapping definitions"
        cube.save()
    
    if debug_data is not None:
        logger.info(f"[PHASE 4] Completed: CUBE_STRUCTURE, {len(debug_data['CUBE_STRUCTURE_ITEM'])} items, "
                   f"{len(debug_data['SUBDOMAIN'])} subdomains, and CUBE created")
    else:
        logger.info("[PHASE 4] Completed: CUBE_STRUCTURE, items, subdomains, and CUBE created")
    
    return {
        'cube_structure': cube_structure,
        'cube': cube,
        'order_counter': order_counter
    }
