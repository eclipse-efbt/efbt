"""
Phase 4: Cube Structures & Subdomains

Creates cube structure, cube structure items, subdomains, subdomain enumerations, and cube.
This is a thin orchestration layer that delegates to lib functions.
"""

import logging
from pybirdai.process_steps.output_layer_mapping_workflow.lib.cube_structure_generator import (
    CubeStructureGenerator
)

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

    # Create cube structure generator
    csi_generator = CubeStructureGenerator()
    mapping_count = len(created_mapping_definitions)

    # Create or get cube structure
    cube_structure = csi_generator.create_or_get_cube_structure(
        table_id=table_id,
        table_code=table_code,
        version=version,
        framework_obj=framework_obj,
        maintenance_agency=maintenance_agency,
        mapping_count=mapping_count,
        debug_data=debug_data
    )

    # De-duplicate target variables
    unique_dimension_vars = list({v.variable_id: v for v in dimension_target_vars}.values())
    unique_observation_vars = list({v.variable_id: v for v in observation_target_vars}.values())
    unique_attribute_vars = list({v.variable_id: v for v in attribute_target_vars}.values())

    logger.info(f"[PHASE 4] Creating CUBE_STRUCTURE_ITEMs: {len(unique_dimension_vars)} dims, "
                f"{len(unique_observation_vars)} observations, {len(unique_attribute_vars)} attributes")

    # Create dimension items (with subdomains)
    _, order_counter = csi_generator.create_dimension_items(
        cube_structure=cube_structure,
        dimension_vars=unique_dimension_vars,
        start_order=1,
        debug_data=debug_data
    )

    # Create observation items
    _, order_counter = csi_generator.create_observation_items(
        cube_structure=cube_structure,
        observation_vars=unique_observation_vars,
        start_order=order_counter,
        debug_data=debug_data
    )

    # Create attribute items
    _, order_counter = csi_generator.create_attribute_items(
        cube_structure=cube_structure,
        attribute_vars=unique_attribute_vars,
        start_order=order_counter,
        debug_data=debug_data
    )

    # Create or get cube
    cube = csi_generator.create_or_get_cube(
        table_id=table_id,
        table_code=table_code,
        version=version,
        framework_obj=framework_obj,
        cube_structure=cube_structure,
        maintenance_agency=maintenance_agency,
        mapping_count=mapping_count,
        debug_data=debug_data
    )

    logger.info(f"[PHASE 4] Completed: CUBE_STRUCTURE, items, subdomains, and CUBE created")

    return {
        'cube_structure': cube_structure,
        'cube': cube,
        'order_counter': order_counter
    }
