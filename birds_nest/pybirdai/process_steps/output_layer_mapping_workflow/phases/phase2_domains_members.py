"""
Phase 2: Domains & Members

Extracts member codes from mappings and ensures all domains and members exist
in the database before subdomain creation in Phase 4.
"""

import logging
from pybirdai.models.bird_meta_data_model import MEMBER, MEMBER_MAPPING_ITEM
from pybirdai.process_steps.output_layer_mapping_workflow.domain_manager import DomainManager

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
    logger.info(f"[PHASE 2] all_mappings type: {type(all_mappings)}, is None: {all_mappings is None}")
    if all_mappings is not None:
        logger.info(f"[PHASE 2] all_mappings has {len(all_mappings)} entries")
    logger.info(f"[PHASE 2] created_mapping_definitions has {len(created_mapping_definitions)} entries")
    
    # ========== EXTRACT MEMBER CODES ==========
    # Build a mapping of variable_id -> [member_ids] to ensure members exist
    variable_to_members_map = {}
    
    if regenerate_mode:
        # Extract member codes from existing MEMBER_MAPPING_ITEM records
        logger.info("[PHASE 2] Extracting member codes from existing mappings (regenerate mode)")
        for mapping_info in created_mapping_definitions:
            mapping_def = mapping_info['mapping_definition']
            if mapping_def.member_mapping_id:
                member_items = MEMBER_MAPPING_ITEM.objects.filter(
                    member_mapping_id=mapping_def.member_mapping_id
                ).select_related('variable_id', 'member_id')
                
                for item in member_items:
                    if item.variable_id and item.member_id:
                        var_id = item.variable_id.variable_id
                        member_id = item.member_id.member_id
                        
                        if var_id not in variable_to_members_map:
                            variable_to_members_map[var_id] = []
                        if member_id not in variable_to_members_map[var_id]:
                            variable_to_members_map[var_id].append(member_id)
    else:
        # Extract member codes from all_mappings dimensions
        logger.info("[PHASE 2] Extracting member codes from mappings (normal mode)")
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
    logger.info(f"[PHASE 2] Extracted member codes for {len(variable_to_members_map)} variables, "
                f"{total_members_to_ensure} total unique member references")
    
    # ========== DIAGNOSTIC LOGGING ==========
    logger.info(f"[PHASE 2 DEBUG] variable_to_members_map has {len(variable_to_members_map)} variables:")
    for var_id, member_ids in list(variable_to_members_map.items())[:5]:  # Show first 5
        logger.info(f"[PHASE 2 DEBUG]   {var_id}: {len(member_ids)} members - {member_ids[:3]}")
    if len(variable_to_members_map) > 5:
        logger.info(f"[PHASE 2 DEBUG]   ... and {len(variable_to_members_map) - 5} more variables")
    
    # Handle empty extraction
    if total_members_to_ensure == 0:
        logger.warning("[PHASE 2 WARNING] No members found in mappings - subdomain enumeration may fail")
        logger.warning("[PHASE 2 WARNING] This may cause FK violations if subdomains require member enumerations")
        if regenerate_mode:
            logger.warning(f"[PHASE 2 WARNING] In regenerate mode - checked {len(created_mapping_definitions)} mappings")
        else:
            logger.warning(f"[PHASE 2 WARNING] In normal mode - checked {len(all_mappings)} mappings")
    
    # ========== DOMAIN AND MEMBER CREATION ==========
    # Ensure all domains and members exist BEFORE creating subdomains/cube structure items
    # This prevents FK violations when creating subdomain enumerations in Phase 4
    logger.info(f"[PHASE 2] Ensuring domains and members exist for {len(unique_target_vars)} target variables")
    domain_manager = DomainManager()
    members_created_count = 0
    members_validated_count = 0
    
    for variable in unique_target_vars:
        # Ensure domain exists for this variable
        domain = domain_manager.ensure_domain_and_members(variable, maintenance_agency)
        
        # Get member_ids that should exist for this variable
        member_ids_for_var = variable_to_members_map.get(variable.variable_id, [])
        
        # Ensure each member exists in the database
        for member_id in member_ids_for_var:
            # Check if member exists
            existing_member = MEMBER.objects.filter(member_id=member_id).first()
            
            if not existing_member:
                # Member doesn't exist - try to create it
                # Extract code from member_id (handle both "DOMAIN_CODE" and "CODE" formats)
                if domain and domain.domain_id in member_id:
                    # Format: DOMAIN_CODE (e.g., "EBA_COREP_MB_1")
                    code = member_id.replace(f"{domain.domain_id}_", "")
                else:
                    # Format: CODE (e.g., "MB_1")
                    code = member_id.split('_')[-1] if '_' in member_id else member_id
                
                try:
                    # Create the member
                    new_member = MEMBER.objects.create(
                        member_id=member_id,
                        maintenance_agency_id=maintenance_agency,
                        code=code,
                        name=code,  # Use code as name for simplicity
                        domain_id=domain,
                        description=f"Member {code} for domain {domain.name if domain else 'Unknown'}"
                    )
                    members_created_count += 1
                    logger.info(f"[PHASE 2] Created member {member_id} for variable {variable.variable_id}")
                    
                    # Track in debug_data
                    if new_member not in debug_data['MEMBER']:
                        debug_data['MEMBER'].append(new_member)
                except Exception as e:
                    logger.error(f"[PHASE 2] Failed to create member {member_id}: {str(e)}")
                    # If member_id format is different, member might already exist but wasn't found
                    pass
            else:
                members_validated_count += 1
                
                # Track existing member in debug_data
                if existing_member not in debug_data['MEMBER']:
                    debug_data['MEMBER'].append(existing_member)
    
    logger.info(f"[PHASE 2] Completed domain and member validation/creation: "
                f"{members_created_count} members created, {members_validated_count} members validated")
    
    return {
        'variable_to_members_map': variable_to_members_map,
        'members_created_count': members_created_count,
        'members_validated_count': members_validated_count
    }
