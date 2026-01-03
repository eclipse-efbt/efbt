"""
Phase 2: Domains & Members

Extracts member codes from mappings and ensures all domains and members exist
in the database before subdomain creation in Phase 4.
This is a thin orchestration layer that delegates to lib functions.
"""

import logging
from pybirdai.process_steps.output_layer_mapping_workflow.lib.domain_manager import DomainManager

logger = logging.getLogger(__name__)


def execute_phase2_domains_members(
    all_mappings,
    created_mapping_definitions,
    unique_target_vars,
    maintenance_agency,
    regenerate_mode,
    debug_data
):
    """
    Execute Phase 2: Extract member codes and ensure domains/members exist.

    This phase must run BEFORE Phase 4 (cube structures) to prevent FK violations
    when creating subdomain enumerations.

    Args:
        all_mappings: Dict of mapping data from session (normal mode)
        created_mapping_definitions: List of existing mapping defs (regenerate mode)
        unique_target_vars: List of unique target VARIABLE objects
        maintenance_agency: MAINTENANCE_AGENCY object for member creation
        regenerate_mode: Boolean indicating regenerate mode
        debug_data: Dict to collect created objects

    Returns:
        dict: {
            'variable_to_members_map': Dict mapping variable_id -> [member_ids],
            'members_created_count': Number of members created,
            'members_validated_count': Number of existing members validated
        }
    """
    logger.info("[PHASE 2] Beginning member extraction and domain/member validation")
    logger.info(f"[PHASE 2] Regenerate mode: {regenerate_mode}")

    # Create domain manager
    domain_manager = DomainManager()

    # Extract member codes using domain manager
    variable_to_members_map = domain_manager.extract_member_codes(
        all_mappings=all_mappings,
        created_mapping_definitions=created_mapping_definitions,
        regenerate_mode=regenerate_mode
    )

    # Log extraction results
    total_members = sum(len(members) for members in variable_to_members_map.values())
    logger.info(f"[PHASE 2] Extracted member codes for {len(variable_to_members_map)} variables, "
                f"{total_members} total unique member references")

    # Diagnostic logging
    if variable_to_members_map:
        for var_id, member_ids in list(variable_to_members_map.items())[:5]:
            logger.debug(f"[PHASE 2] {var_id}: {len(member_ids)} members - {member_ids[:3]}")

    # Handle empty extraction warning
    if total_members == 0:
        logger.warning("[PHASE 2] No members found in mappings - subdomain enumeration may fail")

    # Ensure members exist for all target variables
    stats = domain_manager.ensure_members_for_variables(
        unique_target_vars=unique_target_vars,
        variable_to_members_map=variable_to_members_map,
        maintenance_agency=maintenance_agency,
        debug_data=debug_data
    )

    logger.info(f"[PHASE 2] Completed: {stats['members_created_count']} members created, "
                f"{stats['members_validated_count']} members validated")

    return {
        'variable_to_members_map': variable_to_members_map,
        'members_created_count': stats['members_created_count'],
        'members_validated_count': stats['members_validated_count']
    }
