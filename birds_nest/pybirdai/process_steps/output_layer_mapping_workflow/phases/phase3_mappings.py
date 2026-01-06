"""
Phase 3: Mappings

Creates variable mappings, member mappings, and mapping definitions.
MAPPING_TO_CUBE links are created later in Phase 3.5 (after CUBE is created in Phase 4).
This phase only runs in normal mode (not regenerate mode).

This is a thin orchestration layer that delegates to lib functions.
"""

import logging
import json
from pybirdai.process_steps.output_layer_mapping_workflow.lib.naming_utils import NamingUtils
from pybirdai.process_steps.output_layer_mapping_workflow.lib.transaction_validator import (
    validate_target_variables
)
from pybirdai.process_steps.output_layer_mapping_workflow.lib.mapping_creators import (
    VariableMappingCreator,
    MemberMappingCreator,
    MappingDefinitionCreator,
    OrdinateLinkCreator
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

    # Extract data from session
    variable_groups = json.loads(request.session['olmw_variable_groups'])
    all_mappings = json.loads(request.session['olmw_multi_mappings'])
    table_id = request.session.get('olmw_table_id', '')

    # DIAGNOSTIC: Log what Phase 3 receives
    logger.info(f"[PHASE 3] Target variables: {len(dimension_target_vars)} dims, "
                f"{len(observation_target_vars)} observations, {len(attribute_target_vars)} attributes")
    logger.info(f"[PHASE 3] variable_groups has {len(variable_groups)} group(s)")
    logger.info(f"[PHASE 3] all_mappings has {len(all_mappings)} mapping(s) to create")

    # Warn if all_mappings is empty
    if not all_mappings:
        logger.warning("[PHASE 3] WARNING: all_mappings is EMPTY - no MAPPING_DEFINITIONs will be created!")
        logger.warning("[PHASE 3] This usually means Step 5/6 did not properly save mapping data to session.")
    else:
        for gid, gdata in all_mappings.items():
            has_internal_id = 'internal_id' in gdata
            logger.info(f"[PHASE 3]   - {gid}: mapping_name='{gdata.get('mapping_name', 'N/A')}', "
                       f"has_internal_id={has_internal_id}, dimensions={len(gdata.get('dimensions', []))}")

    # Validate target variables exist
    validate_target_variables(
        variable_groups,
        dimension_target_vars,
        observation_target_vars,
        attribute_target_vars
    )

    # Generate mapping prefix and calculate starting sequence
    mapping_prefix = NamingUtils.generate_mapping_prefix(table_code, version, table_id)
    mapping_sequence_start = NamingUtils.calculate_next_sequence(mapping_prefix)

    # Initialize creators
    var_mapping_creator = VariableMappingCreator(maintenance_agency)
    member_mapping_creator = MemberMappingCreator(maintenance_agency)
    mapping_def_creator = MappingDefinitionCreator(maintenance_agency)
    ordinate_link_creator = OrdinateLinkCreator()

    created_mapping_definitions = []
    mapping_counter = 0

    # Process each mapping
    for group_id, mapping_data in all_mappings.items():
        mapping_name = mapping_data['mapping_name']
        internal_id = mapping_data['internal_id']
        group_type = mapping_data.get('group_type', 'dimension')
        dimensions = mapping_data.get('dimensions', [])

        # Get source and target variable IDs
        group_info = variable_groups.get(group_id, {})
        source_var_ids = set(group_info.get('variable_ids', []))
        target_var_ids = set(group_info.get('targets', []))

        # Calculate mapping sequence
        current_sequence = mapping_sequence_start + mapping_counter
        mapping_id_suffix = NamingUtils.format_mapping_id_suffix(current_sequence)
        mapping_counter += 1

        # Log progress
        unique_dim_vars = set()
        for row in dimensions:
            if isinstance(row, dict):
                unique_dim_vars.update(row.keys())
        observations = mapping_data.get('observations', {})
        unique_obs_vars = set()
        if isinstance(observations, dict):
            unique_obs_vars.update(observations.get('source_vars', []))
            unique_obs_vars.update(observations.get('target_vars', []))
        print(f"[PHASE 3] Processing mapping '{mapping_name}' ({len(unique_dim_vars)} dims, {len(unique_obs_vars)} observations)")

        # 1. Create VARIABLE_MAPPING
        variable_mapping = var_mapping_creator.create_variable_mapping(
            mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_VAR",
            name=mapping_name,
            code=internal_id,
            debug_data=debug_data
        )

        # 2. Create VARIABLE_MAPPING_ITEMs
        var_mapping_creator.create_variable_mapping_items(
            variable_mapping=variable_mapping,
            source_var_ids=source_var_ids,
            target_var_ids=target_var_ids,
            debug_data=debug_data
        )

        # 3. Create MEMBER_MAPPING if dimensions exist
        member_mapping = None
        if dimensions:
            logger.info(f"[PHASE 3] Will create MEMBER_MAPPING with {len(dimensions)} rows")
            member_mapping = member_mapping_creator.create_member_mapping(
                mapping_id=f"{mapping_prefix}_{mapping_id_suffix}_MEM",
                name=f"{mapping_name} - Member Mappings",
                code=f"{internal_id}_MEM",
                debug_data=debug_data
            )

            # Create MEMBER_MAPPING_ITEMs
            member_mapping_creator.create_member_mapping_items(
                member_mapping=member_mapping,
                dimensions=dimensions,
                source_var_ids=source_var_ids,
                debug_data=debug_data
            )

        # 4. Create MAPPING_DEFINITION
        mapping_type = MappingDefinitionCreator.get_mapping_type(group_type)
        algorithm = f"Mapping: {mapping_name}\n{len(dimensions)} dimension rows, {len(observations)} observation rows"

        mapping_definition = mapping_def_creator.create_mapping_definition(
            mapping_id=f"{mapping_prefix}_{mapping_id_suffix}",
            name=mapping_name,
            code=internal_id,
            mapping_type=mapping_type,
            variable_mapping=variable_mapping,
            member_mapping=member_mapping,
            algorithm=algorithm,
            debug_data=debug_data
        )

        created_mapping_definitions.append({
            'name': mapping_name,
            'mapping_definition': mapping_definition,
            'internal_id': internal_id
        })

        print(f"[PHASE 3] Created MAPPING_DEFINITION: {mapping_definition.mapping_id}")

    # Create MAPPING_ORDINATE_LINK records
    selected_ordinates = request.session.get('olmw_selected_ordinates', [])
    ordinate_link_creator.create_ordinate_links(
        mapping_definitions=created_mapping_definitions,
        selected_ordinates=selected_ordinates,
        debug_data=debug_data
    )

    # NOTE: MAPPING_TO_CUBE links are created in Phase 3.5 (in the view) after CUBE is created in Phase 4
    logger.info(f"[PHASE 3] Completed: Created {len(created_mapping_definitions)} mapping definitions")

    return created_mapping_definitions
