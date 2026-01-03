"""
Transaction validation utilities for output layer generation.

Provides FK validation, SQLite PRAGMA checks, and target variable validation
between transaction phases.

Includes dictionary-based caching for performance optimization.
"""

import logging
from typing import Dict, List, Set, Any, Optional
from django.db import connection
from pybirdai.models.bird_meta_data_model import (
    VARIABLE, MEMBER, DOMAIN, SUBDOMAIN, SUBDOMAIN_ENUMERATION,
    FRAMEWORK, MAINTENANCE_AGENCY, CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
    COMBINATION, COMBINATION_ITEM, VARIABLE_MAPPING, MEMBER_MAPPING,
    MAPPING_DEFINITION
)

logger = logging.getLogger(__name__)

# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

# Module-level caches for existence checks
_variable_cache: Dict[str, bool] = {}
_member_cache: Dict[str, bool] = {}
_domain_cache: Dict[str, bool] = {}
_subdomain_cache: Dict[str, bool] = {}
_framework_cache: Dict[str, bool] = {}
_agency_cache: Dict[str, bool] = {}


def clear_validation_cache():
    """Clear all validation caches. Call between workflow runs."""
    global _variable_cache, _member_cache, _domain_cache
    global _subdomain_cache, _framework_cache, _agency_cache
    _variable_cache.clear()
    _member_cache.clear()
    _domain_cache.clear()
    _subdomain_cache.clear()
    _framework_cache.clear()
    _agency_cache.clear()
    logger.debug("Validation caches cleared")


# ============================================================================
# CACHED EXISTENCE CHECK UTILITIES
# ============================================================================

def variable_exists(variable_id: str) -> bool:
    """
    Check if a variable exists in the database (cached).

    Args:
        variable_id: The variable ID to check

    Returns:
        True if exists, False otherwise
    """
    if variable_id in _variable_cache:
        return _variable_cache[variable_id]

    exists = VARIABLE.objects.filter(variable_id=variable_id).exists()
    _variable_cache[variable_id] = exists
    return exists


def get_variable(variable_id: str) -> Optional[VARIABLE]:
    """
    Get a variable by ID, returning None if not found.
    Updates the existence cache as a side effect.

    Args:
        variable_id: The variable ID to get

    Returns:
        VARIABLE object or None
    """
    var = VARIABLE.objects.filter(variable_id=variable_id).first()
    _variable_cache[variable_id] = var is not None
    return var


def member_exists(member_id: str) -> bool:
    """
    Check if a member exists in the database (cached).

    Args:
        member_id: The member ID to check

    Returns:
        True if exists, False otherwise
    """
    if member_id in _member_cache:
        return _member_cache[member_id]

    exists = MEMBER.objects.filter(member_id=member_id).exists()
    _member_cache[member_id] = exists
    return exists


def get_member(member_id: str) -> Optional[MEMBER]:
    """
    Get a member by ID, returning None if not found.
    Updates the existence cache as a side effect.

    Args:
        member_id: The member ID to get

    Returns:
        MEMBER object or None
    """
    mem = MEMBER.objects.filter(member_id=member_id).first()
    _member_cache[member_id] = mem is not None
    return mem


def domain_exists(domain_id: str) -> bool:
    """
    Check if a domain exists in the database (cached).

    Args:
        domain_id: The domain ID to check

    Returns:
        True if exists, False otherwise
    """
    if domain_id in _domain_cache:
        return _domain_cache[domain_id]

    exists = DOMAIN.objects.filter(domain_id=domain_id).exists()
    _domain_cache[domain_id] = exists
    return exists


def subdomain_exists(subdomain_id: str) -> bool:
    """
    Check if a subdomain exists in the database (cached).

    Args:
        subdomain_id: The subdomain ID to check

    Returns:
        True if exists, False otherwise
    """
    if subdomain_id in _subdomain_cache:
        return _subdomain_cache[subdomain_id]

    exists = SUBDOMAIN.objects.filter(subdomain_id=subdomain_id).exists()
    _subdomain_cache[subdomain_id] = exists
    return exists


def framework_exists(framework_id: str) -> bool:
    """
    Check if a framework exists in the database (cached).

    Args:
        framework_id: The framework ID to check

    Returns:
        True if exists, False otherwise
    """
    if framework_id in _framework_cache:
        return _framework_cache[framework_id]

    exists = FRAMEWORK.objects.filter(framework_id=framework_id).exists()
    _framework_cache[framework_id] = exists
    return exists


def agency_exists(agency_id: str) -> bool:
    """
    Check if a maintenance agency exists in the database (cached).

    Args:
        agency_id: The agency ID to check

    Returns:
        True if exists, False otherwise
    """
    if agency_id in _agency_cache:
        return _agency_cache[agency_id]

    exists = MAINTENANCE_AGENCY.objects.filter(maintenance_agency_id=agency_id).exists()
    _agency_cache[agency_id] = exists
    return exists


# ============================================================================
# BATCH VALIDATION UTILITIES
# ============================================================================

def validate_variables_exist(variable_ids: List[str]) -> List[str]:
    """
    Validate that multiple variables exist, returning list of missing IDs.

    Args:
        variable_ids: List of variable IDs to check

    Returns:
        List of variable IDs that don't exist (empty if all exist)
    """
    missing = []
    for var_id in variable_ids:
        if not variable_exists(var_id):
            missing.append(var_id)
    return missing


def validate_members_exist(member_ids: List[str]) -> List[str]:
    """
    Validate that multiple members exist, returning list of missing IDs.

    Args:
        member_ids: List of member IDs to check

    Returns:
        List of member IDs that don't exist (empty if all exist)
    """
    missing = []
    for mem_id in member_ids:
        if not member_exists(mem_id):
            missing.append(mem_id)
    return missing


# ============================================================================
# TARGET VARIABLE VALIDATION
# ============================================================================


def validate_target_variables(
    variable_groups: Dict[str, Any],
    dimension_target_vars: List,
    observation_target_vars: List,
    attribute_target_vars: List
) -> None:
    """
    Validate that all target variables from variable_groups exist in the database.

    Args:
        variable_groups: Dict of group_id -> group_data with 'targets' list
        dimension_target_vars: List of dimension VARIABLE objects
        observation_target_vars: List of observation VARIABLE objects
        attribute_target_vars: List of attribute VARIABLE objects

    Raises:
        ValueError: If any requested target variables don't exist
    """
    # Collect all requested variable IDs from groups
    all_requested_var_ids: Set[str] = set()
    for group_id, group_data in variable_groups.items():
        target_var_ids = group_data.get('targets', [])
        all_requested_var_ids.update(target_var_ids)

    # Collect all found variable IDs
    all_found_var_ids: Set[str] = set()
    all_target_vars = dimension_target_vars + observation_target_vars + attribute_target_vars
    for var in all_target_vars:
        all_found_var_ids.add(var.variable_id)

    # Check for missing variables
    missing_var_ids = all_requested_var_ids - all_found_var_ids
    if missing_var_ids:
        missing_list = ', '.join(sorted(missing_var_ids))
        error_msg = (
            f"Cannot proceed: The following target variables do not exist in the database: "
            f"{missing_list}. Please ensure these variables are created in Step 4 "
            f"before generating structures."
        )
        logger.error(f"[VALIDATION] {error_msg}")
        raise ValueError(error_msg)

    logger.info(f"[VALIDATION] All {len(all_requested_var_ids)} target variables validated successfully")


def run_pragma_foreign_key_check(cursor):
    """
    Execute SQLite PRAGMA foreign_key_check and return any violations.
    
    Returns:
        list: List of dicts with FK violation details, or empty list if no violations
    """
    try:
        cursor.execute("PRAGMA foreign_key_check")
        violations = cursor.fetchall()
        
        if violations:
            logger.error(f"[PRAGMA FK CHECK] Found {len(violations)} FK constraint violations")
            formatted_violations = []
            for v in violations:
                violation_info = {
                    'table': v[0] if len(v) > 0 else 'unknown',
                    'rowid': v[1] if len(v) > 1 else 'unknown',
                    'parent': v[2] if len(v) > 2 else 'unknown',
                    'fkid': v[3] if len(v) > 3 else 'unknown',
                }
                formatted_violations.append(violation_info)
                logger.error(f"  ❌ Table: {violation_info['table']}, "
                           f"RowID: {violation_info['rowid']}, "
                           f"Parent: {violation_info['parent']}, "
                           f"FK ID: {violation_info['fkid']}")
            return formatted_violations
        else:
            logger.info("[PRAGMA FK CHECK] No FK constraint violations found ✓")
            return []
            
    except Exception as e:
        logger.warning(f"[PRAGMA FK CHECK] Could not run PRAGMA check: {e}")
        return []


def validate_orm_foreign_keys(model_instances, fk_fields):
    """
    Validate FK references at ORM level for given model instances.
    
    Args:
        model_instances: QuerySet or list of model instances to validate
        fk_fields: List of tuples (field_name, related_model, related_field_name)
                   e.g., [('variable_id', VARIABLE, 'variable_id')]
    
    Returns:
        list: List of error messages, or empty list if all valid
    """
    errors = []
    
    for instance in model_instances:
        model_name = instance.__class__.__name__
        instance_id = getattr(instance, f"{model_name.lower()}_id", 'unknown')
        
        for field_name, related_model, related_field in fk_fields:
            fk_value = getattr(instance, field_name, None)
            
            if fk_value is None:
                # Check if field is required
                field_obj = instance._meta.get_field(field_name)
                if not field_obj.null and not field_obj.blank:
                    errors.append(
                        f"{model_name} {instance_id}: {field_name} is NULL (required)"
                    )
                continue
            
            # Get the actual FK value (might be an object or a primitive)
            if hasattr(fk_value, related_field):
                fk_id = getattr(fk_value, related_field)
            else:
                fk_id = fk_value
            
            # Check if referenced object exists
            filter_kwargs = {related_field: fk_id}
            if not related_model.objects.filter(**filter_kwargs).exists():
                errors.append(
                    f"{model_name} {instance_id}: {field_name} references "
                    f"non-existent {related_model.__name__} '{fk_id}'"
                )
    
    return errors


def validate_fks_for_phase(phase_name, debug_data, cursor=None):
    """
    Enhanced FK validation specific to each phase's models.

    Args:
        phase_name: Name of the phase (e.g., "Phase 1: Base Setup")
        debug_data: Dict containing created objects by model type
        cursor: Database cursor for PRAGMA checks (optional)

    Raises:
        ValueError: If FK validation fails
    """
    if debug_data is None:
        logger.info(f"[{phase_name} FK VALIDATION] Debug tracking disabled - skipping FK validation")
        return

    logger.info(f"[{phase_name} FK VALIDATION] Validating foreign key references...")
    fk_validation_errors = []
    
    # Phase 1: FRAMEWORK FKs
    if 'FRAMEWORK' in debug_data:
        for fw in debug_data['FRAMEWORK']:
            if fw.maintenance_agency_id:
                if not MAINTENANCE_AGENCY.objects.filter(
                    maintenance_agency_id=fw.maintenance_agency_id.maintenance_agency_id
                ).exists():
                    fk_validation_errors.append(
                        f"FRAMEWORK {fw.framework_id}: maintenance_agency "
                        f"'{fw.maintenance_agency_id.maintenance_agency_id}' does not exist"
                    )
    
    # Phase 2: MEMBER FKs
    if 'MEMBER' in debug_data:
        for member in debug_data['MEMBER']:
            if member.domain_id:
                if not DOMAIN.objects.filter(domain_id=member.domain_id.domain_id).exists():
                    fk_validation_errors.append(
                        f"MEMBER {member.member_id}: domain '{member.domain_id.domain_id}' does not exist"
                    )
    
    # Phase 3: Mapping FKs
    if 'VARIABLE_MAPPING' in debug_data:
        for vm in debug_data['VARIABLE_MAPPING']:
            # VARIABLE_MAPPING only has maintenance_agency_id FK (not variable FKs)
            if vm.maintenance_agency_id and not MAINTENANCE_AGENCY.objects.filter(
                maintenance_agency_id=vm.maintenance_agency_id.maintenance_agency_id
            ).exists():
                fk_validation_errors.append(
                    f"VARIABLE_MAPPING {vm.variable_mapping_id}: maintenance_agency "
                    f"'{vm.maintenance_agency_id.maintenance_agency_id}' does not exist"
                )

    if 'VARIABLE_MAPPING_ITEM' in debug_data:
        for vmi in debug_data['VARIABLE_MAPPING_ITEM']:
            if vmi.variable_id and not VARIABLE.objects.filter(
                variable_id=vmi.variable_id.variable_id
            ).exists():
                fk_validation_errors.append(
                    f"VARIABLE_MAPPING_ITEM: variable '{vmi.variable_id.variable_id}' does not exist"
                )
    
    if 'MEMBER_MAPPING' in debug_data:
        for mm in debug_data['MEMBER_MAPPING']:
            # MEMBER_MAPPING only has maintenance_agency_id FK (not variable_id)
            if mm.maintenance_agency_id and not MAINTENANCE_AGENCY.objects.filter(
                maintenance_agency_id=mm.maintenance_agency_id.maintenance_agency_id
            ).exists():
                fk_validation_errors.append(
                    f"MEMBER_MAPPING {mm.member_mapping_id}: maintenance_agency "
                    f"'{mm.maintenance_agency_id.maintenance_agency_id}' does not exist"
                )

    if 'MEMBER_MAPPING_ITEM' in debug_data:
        for mmi in debug_data['MEMBER_MAPPING_ITEM']:
            # Validate variable_id FK
            if mmi.variable_id and not VARIABLE.objects.filter(
                variable_id=mmi.variable_id.variable_id
            ).exists():
                fk_validation_errors.append(
                    f"MEMBER_MAPPING_ITEM: variable '{mmi.variable_id.variable_id}' does not exist"
                )
            # Validate member_id FK
            if mmi.member_id and not MEMBER.objects.filter(
                member_id=mmi.member_id.member_id
            ).exists():
                fk_validation_errors.append(
                    f"MEMBER_MAPPING_ITEM: member '{mmi.member_id.member_id}' does not exist"
                )
    
    # Phase 4: Cube Structure FKs
    if 'CUBE_STRUCTURE' in debug_data:
        for cs in debug_data['CUBE_STRUCTURE']:
            if cs.maintenance_agency_id and not MAINTENANCE_AGENCY.objects.filter(
                maintenance_agency_id=cs.maintenance_agency_id.maintenance_agency_id
            ).exists():
                fk_validation_errors.append(
                    f"CUBE_STRUCTURE {cs.cube_structure_id}: maintenance_agency "
                    f"'{cs.maintenance_agency_id.maintenance_agency_id}' does not exist"
                )
    
    if 'CUBE_STRUCTURE_ITEM' in debug_data:
        for csi in debug_data['CUBE_STRUCTURE_ITEM']:
            if csi.variable_id and not VARIABLE.objects.filter(
                variable_id=csi.variable_id.variable_id
            ).exists():
                fk_validation_errors.append(
                    f"CUBE_STRUCTURE_ITEM {csi.cube_structure_item_id}: variable "
                    f"'{csi.variable_id.variable_id}' does not exist"
                )
            if csi.subdomain_id and not SUBDOMAIN.objects.filter(
                subdomain_id=csi.subdomain_id.subdomain_id
            ).exists():
                fk_validation_errors.append(
                    f"CUBE_STRUCTURE_ITEM {csi.cube_structure_item_id}: subdomain "
                    f"'{csi.subdomain_id.subdomain_id}' does not exist"
                )
    
    if 'SUBDOMAIN_ENUMERATION' in debug_data:
        for sde in debug_data['SUBDOMAIN_ENUMERATION']:
            if sde.member_id and not MEMBER.objects.filter(
                member_id=sde.member_id.member_id
            ).exists():
                fk_validation_errors.append(
                    f"SUBDOMAIN_ENUMERATION: member '{sde.member_id.member_id}' does not exist"
                )
    
    if 'CUBE' in debug_data:
        for cube in debug_data['CUBE']:
            if cube.framework_id:
                if not FRAMEWORK.objects.filter(
                    framework_id=cube.framework_id.framework_id
                ).exists():
                    fk_validation_errors.append(
                        f"CUBE {cube.cube_id}: framework '{cube.framework_id.framework_id}' does not exist"
                    )
            else:
                fk_validation_errors.append(f"CUBE {cube.cube_id}: framework_id is NULL (required)")
            
            if cube.cube_structure_id:
                if not CUBE_STRUCTURE.objects.filter(
                    cube_structure_id=cube.cube_structure_id.cube_structure_id
                ).exists():
                    fk_validation_errors.append(
                        f"CUBE {cube.cube_id}: cube_structure "
                        f"'{cube.cube_structure_id.cube_structure_id}' does not exist"
                    )
            else:
                fk_validation_errors.append(f"CUBE {cube.cube_id}: cube_structure_id is NULL (required)")
    
    # Phase 5: Combination FKs
    if 'COMBINATION' in debug_data:
        for combo_info in debug_data['COMBINATION']:
            # Handle both dictionary format {'combination': combo} and raw COMBINATION objects
            if isinstance(combo_info, dict):
                combo = combo_info.get('combination')
            else:
                combo = combo_info
            
            if combo and hasattr(combo, 'metric') and combo.metric:
                if not VARIABLE.objects.filter(
                    variable_id=combo.metric.variable_id
                ).exists():
                    fk_validation_errors.append(
                        f"COMBINATION {combo.combination_id}: metric variable "
                        f"'{combo.metric.variable_id}' does not exist"
                    )
    
    if 'COMBINATION_ITEM' in debug_data:
        for item in debug_data['COMBINATION_ITEM']:
            if item.variable_id and not VARIABLE.objects.filter(
                variable_id=item.variable_id.variable_id
            ).exists():
                fk_validation_errors.append(
                    f"COMBINATION_ITEM: variable '{item.variable_id.variable_id}' does not exist"
                )
            if item.member_id and not MEMBER.objects.filter(
                member_id=item.member_id.member_id
            ).exists():
                fk_validation_errors.append(
                    f"COMBINATION_ITEM: member '{item.member_id.member_id}' does not exist"
                )
            if item.subdomain_id and not SUBDOMAIN.objects.filter(
                subdomain_id=item.subdomain_id.subdomain_id
            ).exists():
                fk_validation_errors.append(
                    f"COMBINATION_ITEM: subdomain '{item.subdomain_id.subdomain_id}' does not exist"
                )
    
    # Run PRAGMA check if cursor provided
    if cursor:
        pragma_violations = run_pragma_foreign_key_check(cursor)
        if pragma_violations:
            for v in pragma_violations:
                fk_validation_errors.append(
                    f"PRAGMA FK violation: Table={v['table']}, RowID={v['rowid']}, "
                    f"Parent={v['parent']}, FKID={v['fkid']}"
                )
    
    # Raise if any errors found
    if fk_validation_errors:
        logger.error(f"[{phase_name} FK VALIDATION] Found {len(fk_validation_errors)} FK validation errors:")
        for error in fk_validation_errors[:10]:  # Log first 10 errors
            logger.error(f"  ❌ {error}")
        if len(fk_validation_errors) > 10:
            logger.error(f"  ... and {len(fk_validation_errors) - 10} more errors")
        raise ValueError(
            f"{phase_name} FK validation failed with {len(fk_validation_errors)} error(s): "
            + "; ".join(fk_validation_errors[:3])
        )
    
    logger.info(f"[{phase_name} FK VALIDATION] All FK references validated successfully ✓")
