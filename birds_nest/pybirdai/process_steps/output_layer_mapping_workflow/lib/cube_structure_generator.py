"""
Module for cube structure management.
Handles cube structure, cube, and subdomain creation for output layer mappings.

Includes dictionary-based caching for performance optimization.
"""

import logging
from typing import Optional, Tuple, List, Dict, Any

from pybirdai.models.bird_meta_data_model import (
    VARIABLE, DOMAIN, SUBDOMAIN, SUBDOMAIN_ENUMERATION, MEMBER,
    MAINTENANCE_AGENCY, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE, FRAMEWORK
)
from .naming_utils import NamingUtils

logger = logging.getLogger(__name__)

# Module-level caches
_subdomain_cache: Dict[str, Optional[SUBDOMAIN]] = {}
_cube_structure_cache: Dict[str, CUBE_STRUCTURE] = {}
_cube_cache: Dict[str, CUBE] = {}


def clear_cube_structure_cache():
    """Clear all caches in this module. Call between workflow runs."""
    global _subdomain_cache, _cube_structure_cache, _cube_cache
    _subdomain_cache.clear()
    _cube_structure_cache.clear()
    _cube_cache.clear()
    logger.debug("Cube structure generator caches cleared")


class CubeStructureGenerator:
    """
    Handles subdomain creation for cube structures in output layer mappings.
    """

    def __init__(self):
        """Initialize the cube structure generator."""
        self.created_structures = []
        self.created_items = []

    def create_or_get_subdomain(
        self,
        variable: VARIABLE,
        cube_structure_id: str
    ) -> Tuple[Optional[SUBDOMAIN], Optional[MEMBER]]:
        """
        Create or get a subdomain for a variable in a cube structure.
        If the domain has exactly one member, returns that member instead of a subdomain.

        Args:
            variable: The VARIABLE object
            cube_structure_id: The cube structure ID

        Returns:
            Tuple of (SUBDOMAIN, MEMBER) where:
            - If domain has 1 member: (None, member)
            - If domain has multiple members: (subdomain, None)
            - If error: (None, None)
        """
        logger.info(f"[SUBDOMAIN DEBUG] Creating subdomain for variable {variable.variable_id}")

        if not hasattr(variable, 'domain_id') or not variable.domain_id:
            logger.warning(f"[SUBDOMAIN DEBUG] Variable {variable.variable_id} has no domain!")
            return (None, None)

        domain = variable.domain_id
        logger.info(f"[SUBDOMAIN DEBUG] Variable domain: {domain.domain_id}")

        # Check member count BEFORE checking is_enumerated
        member_count = MEMBER.objects.filter(domain_id=domain).count()
        logger.info(f"[SUBDOMAIN DEBUG] Domain {domain.domain_id} has {member_count} members in database")

        # Check if domain has exactly one member
        if domain.is_enumerated:
            members = MEMBER.objects.filter(domain_id=domain)
            member_count = members.count()
            logger.info(f"[SUBDOMAIN DEBUG] Domain is enumerated with {member_count} members")

            if member_count == 1:
                single_member = members.first()
                logger.info(
                    f"Domain {domain.domain_id} has single member {single_member.code}, "
                    f"using direct member reference instead of subdomain"
                )
                return (None, single_member)
            elif member_count == 0:
                logger.warning(f"[SUBDOMAIN DEBUG] Domain {domain.domain_id} has no members - cannot create subdomain!")
                logger.warning(f"[SUBDOMAIN DEBUG] This will likely cause FK violations if subdomain enumeration is required")
                return (None, None)

        # Generate subdomain ID
        subdomain_id = f"{variable.variable_id}_OUTPUT_SD_{cube_structure_id}"

        # Check if subdomain exists
        subdomain = SUBDOMAIN.objects.filter(subdomain_id=subdomain_id).first()

        if subdomain:
            logger.info(f"Using existing subdomain: {subdomain_id}")
            return (subdomain, None)

        # Verify domain exists before creating subdomain (foreign key validation)
        if not DOMAIN.objects.filter(domain_id=domain.domain_id).exists():
            logger.error(
                f"Cannot create subdomain - domain {domain.domain_id} doesn't exist in database. "
                f"This would cause a foreign key constraint violation."
            )
            return (None, None)

        # Create new subdomain using AgencyManager
        from pybirdai.process_steps.output_layer_mapping_workflow.lib.entity_managers import (
            AgencyManager
        )
        maintenance_agency = AgencyManager().get_efbt_agency()

        try:
            subdomain = SUBDOMAIN.objects.create(
                subdomain_id=subdomain_id,
                maintenance_agency_id=maintenance_agency,
                name=f"{variable.name} subset for {cube_structure_id}",
                code=f"{variable.code}_SD" if hasattr(variable, 'code') else subdomain_id,
                domain_id=domain,
                is_listed=True,
                is_natural=False,
                description=f"Output subdomain for {variable.name} in {cube_structure_id}"
            )
        except Exception as e:
            logger.error(f"Error creating subdomain {subdomain_id}: {str(e)}")
            return (None, None)

        # Copy enumeration from domain if it's enumerated
        if domain.is_enumerated:
            logger.info(f"[SUBDOMAIN DEBUG] Domain is enumerated, copying enumerations to subdomain")
            from .subdomain_manager import SubdomainManager
            SubdomainManager().copy_domain_enumeration(domain, subdomain)
        else:
            logger.info(f"[SUBDOMAIN DEBUG] Domain is NOT enumerated, skipping enumeration copy")

        logger.info(f"[SUBDOMAIN DEBUG] Created new subdomain: {subdomain_id}")
        return (subdomain, None)

    # ========== Role and Dimension Type Methods ==========

    @staticmethod
    def determine_role(variable: VARIABLE) -> str:
        """
        Determine role (D/O/A) from variable domain characteristics.

        Args:
            variable: The VARIABLE object

        Returns:
            str: 'D' (dimension), 'O' (observation), or 'A' (attribute)
        """
        if not hasattr(variable, 'domain_id') or not variable.domain_id:
            return "A"  # Default to attribute if no domain

        domain = variable.domain_id
        if domain.is_enumerated:
            return "D"
        elif domain.domain_id in ("Integer", "Float", "MNTRY"):
            return "O"
        else:
            return "A"

    @staticmethod
    def determine_dimension_type(variable: VARIABLE) -> str:
        """
        Determine dimension type (B/T/M/U) from variable name patterns.

        Args:
            variable: The VARIABLE object

        Returns:
            str: 'B' (business), 'T' (temporal), 'M' (methodological), 'U' (unit)
        """
        var_id_upper = variable.variable_id.upper()

        if "TIME" in var_id_upper or "DATE" in var_id_upper or "PERIOD" in var_id_upper:
            return "T"  # Temporal
        elif "METHOD" in var_id_upper or "APPROACH" in var_id_upper:
            return "M"  # Methodological
        elif "UNIT" in var_id_upper or "CURRENCY" in var_id_upper:
            return "U"  # Unit
        else:
            return "B"  # Business (default)

    # ========== Cube Structure Creation Methods ==========

    def create_or_get_cube_structure(
        self,
        table_id: str,
        table_code: str,
        version: str,
        framework_obj: FRAMEWORK,
        maintenance_agency: MAINTENANCE_AGENCY,
        mapping_count: int,
        debug_data: Optional[Dict] = None
    ) -> CUBE_STRUCTURE:
        """
        Create or get a CUBE_STRUCTURE for the given table.

        Args:
            table_id: Full table ID with variant suffix
            table_code: Base table code
            version: Version string
            framework_obj: FRAMEWORK object
            maintenance_agency: MAINTENANCE_AGENCY object
            mapping_count: Number of mapping definitions
            debug_data: Optional dict to track created objects

        Returns:
            CUBE_STRUCTURE object
        """
        # Strip Z-ordinate suffix for clean naming
        clean_table_id = NamingUtils.strip_z_ordinate_suffix(table_id)

        # Extract framework base name (remove EBA_ prefix and _REF suffix)
        framework = framework_obj.framework_id
        framework_short = framework.replace('EBA_', '') if framework.startswith('EBA_') else framework
        framework_base = framework_short.replace('_REF', '') if framework_short.endswith('_REF') else framework_short
        # Normalize version format
        version_normalized = version.replace('.', '_') if version else '3_0'

        # Remove EBA_ and framework prefixes from table_id for cleaner reference IDs
        # e.g., EBA_COREP_C_07_00_a_4_0 -> C_07_00_a_4_0
        for prefix in [f"EBA_{framework_base}_", f"EBA_{framework_short}_", framework, f"{framework}_", "EBA_"]:
            if clean_table_id.startswith(prefix):
                clean_table_id = clean_table_id[len(prefix):]
                break
        clean_table_id = clean_table_id.lstrip('_')

        # Generate cube structure ID: {table_code}_REF_{framework_base}_{version}_cube_structure
        cube_structure_id = f"{clean_table_id}_REF_{framework_base}_{version_normalized}_cube_structure"

        cube_structure, cs_created = CUBE_STRUCTURE.objects.get_or_create(
            cube_structure_id=cube_structure_id,
            defaults={
                'maintenance_agency_id': maintenance_agency,
                'name': f"Reference structure for {clean_table_id}",
                'code': f"{clean_table_id}_REF_{framework_base}_CS",
                'description': f"Cube structure for {mapping_count} mappings",
                'version': version
            }
        )

        if cs_created:
            logger.info(f"Created NEW CUBE_STRUCTURE: {cube_structure_id}")
        else:
            logger.info(f"Reusing existing CUBE_STRUCTURE: {cube_structure_id}")
            # Delete old items when reusing
            CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_id_id=cube_structure).delete()
            cube_structure.description = f"Cube structure for {mapping_count} mappings"
            cube_structure.save()

        if debug_data is not None and 'CUBE_STRUCTURE' in debug_data:
            debug_data['CUBE_STRUCTURE'].append(cube_structure)

        self.created_structures.append(cube_structure)
        return cube_structure

    def create_or_get_cube(
        self,
        table_id: str,
        table_code: str,
        version: str,
        framework_obj: FRAMEWORK,
        cube_structure: CUBE_STRUCTURE,
        maintenance_agency: MAINTENANCE_AGENCY,
        mapping_count: int,
        debug_data: Optional[Dict] = None
    ) -> CUBE:
        """
        Create or get a CUBE for the given table.

        Args:
            table_id: Full table ID with variant suffix
            table_code: Base table code
            version: Version string
            framework_obj: FRAMEWORK object
            cube_structure: CUBE_STRUCTURE object
            maintenance_agency: MAINTENANCE_AGENCY object
            mapping_count: Number of mapping definitions
            debug_data: Optional dict to track created objects

        Returns:
            CUBE object

        Raises:
            ValueError: If framework or cube_structure is invalid
        """
        # Validate prerequisites
        if not framework_obj:
            raise ValueError("FRAMEWORK was not provided")

        if not cube_structure:
            raise ValueError("CUBE_STRUCTURE is None - cannot create CUBE")

        if not CUBE_STRUCTURE.objects.filter(cube_structure_id=cube_structure.cube_structure_id).exists():
            raise ValueError(f"CUBE_STRUCTURE {cube_structure.cube_structure_id} doesn't exist in database")

        # Clean naming

        framework = framework_obj.framework_id
        clean_table_id = NamingUtils.strip_z_ordinate_suffix(table_id)
        framework_short = framework.replace('EBA_', '') if framework.startswith('EBA_') else framework
        # Remove _REF suffix for cube naming (COREP_REF -> COREP, FINREP_REF -> FINREP)
        framework_base = framework_short.replace('_REF', '') if framework_short.endswith('_REF') else framework_short
        # Normalize version format (replace . with _)
        version_normalized = version.replace('.', '_') if version else '3_0'

        # Remove EBA_ and framework prefixes from table_id for cleaner reference cube ID
        # e.g., EBA_COREP_C_07_00_a_4_0 -> C_07_00_a_4_0
        for prefix in [f"EBA_{framework_base}_", f"EBA_{framework_short}_", framework, f"{framework}_", "EBA_"]:
            if clean_table_id.startswith(prefix):
                clean_table_id = clean_table_id[len(prefix):]
                break
        clean_table_id = clean_table_id.lstrip('_')

        # Use pattern: {table_code}_REF_{framework_base}_{version}
        cube_id = f"{clean_table_id}_REF_{framework_base}_{version_normalized}"

        try:
            cube, cube_created = CUBE.objects.get_or_create(
                cube_id=cube_id,
                defaults={
                    'maintenance_agency_id': maintenance_agency,
                    'name': f"{clean_table_id}",
                    'code': f"{framework_short}_REF_{table_code}_CUBE",
                    'framework_id': framework_obj,
                    'cube_structure_id': cube_structure,
                    'cube_type': "RC",  # Reference Cube
                    'is_allowed': True,
                    'published': False,
                    'version': version,
                    'description': f"Cube for {mapping_count} mapping definitions"
                }
            )

            if cube_created:
                logger.info(f"Created new CUBE: {cube_id}")
            else:
                logger.info(f"Reusing existing CUBE: {cube_id}")
                # Update cube to point to current cube_structure
                cube.cube_structure_id = cube_structure
                cube.description = f"Cube for {mapping_count} mapping definitions"
                cube.save()

            if debug_data is not None and 'CUBE' in debug_data:
                debug_data['CUBE'].append(cube)

            return cube

        except Exception as e:
            logger.error(f"Failed to create CUBE {cube_id}: {str(e)}")
            raise

    # ========== Cube Structure Item Creation Methods ==========

    def create_dimension_items(
        self,
        cube_structure: CUBE_STRUCTURE,
        dimension_vars: List[VARIABLE],
        start_order: int = 1,
        debug_data: Optional[Dict] = None
    ) -> Tuple[List[CUBE_STRUCTURE_ITEM], int]:
        """
        Create CUBE_STRUCTURE_ITEMs for dimensions with subdomains.

        Args:
            cube_structure: The CUBE_STRUCTURE object
            dimension_vars: List of dimension VARIABLE objects
            start_order: Starting order number
            debug_data: Optional dict to track created objects

        Returns:
            Tuple of (created items list, final order counter)
        """
        items = []
        order_counter = start_order

        for variable in dimension_vars:
            # Create or get subdomain
            subdomain, single_member = self.create_or_get_subdomain(
                variable, cube_structure.cube_structure_id
            )

            # Track subdomain in debug_data
            if debug_data is not None and 'SUBDOMAIN' in debug_data:
                if subdomain and subdomain not in debug_data['SUBDOMAIN']:
                    debug_data['SUBDOMAIN'].append(subdomain)

            # Determine dimension type
            dimension_type = self.determine_dimension_type(variable)

            cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"

            item = CUBE_STRUCTURE_ITEM.objects.create(
                cube_structure_id=cube_structure,
                cube_variable_code=cube_variable_code,
                variable_id=variable,
                role=self.determine_role(variable),
                order=order_counter,
                subdomain_id=subdomain,
                member_id=single_member,
                dimension_type=dimension_type,
                is_mandatory=True,
                is_implemented=True,
                description=f"Dimension: {variable.name}"
            )
            order_counter += 1
            items.append(item)

            if debug_data is not None and 'CUBE_STRUCTURE_ITEM' in debug_data:
                if item not in debug_data['CUBE_STRUCTURE_ITEM']:
                    debug_data['CUBE_STRUCTURE_ITEM'].append(item)

        self.created_items.extend(items)
        return (items, order_counter)

    def create_observation_items(
        self,
        cube_structure: CUBE_STRUCTURE,
        observation_vars: List[VARIABLE],
        start_order: int,
        debug_data: Optional[Dict] = None
    ) -> Tuple[List[CUBE_STRUCTURE_ITEM], int]:
        """
        Create CUBE_STRUCTURE_ITEMs for observations.

        Args:
            cube_structure: The CUBE_STRUCTURE object
            observation_vars: List of observation VARIABLE objects
            start_order: Starting order number
            debug_data: Optional dict to track created objects

        Returns:
            Tuple of (created items list, final order counter)
        """
        items = []
        order_counter = start_order

        for variable in observation_vars:
            cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"

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
            items.append(item)

            if debug_data is not None and 'CUBE_STRUCTURE_ITEM' in debug_data:
                if item not in debug_data['CUBE_STRUCTURE_ITEM']:
                    debug_data['CUBE_STRUCTURE_ITEM'].append(item)

        self.created_items.extend(items)
        return (items, order_counter)

    def create_attribute_items(
        self,
        cube_structure: CUBE_STRUCTURE,
        attribute_vars: List[VARIABLE],
        start_order: int,
        debug_data: Optional[Dict] = None
    ) -> Tuple[List[CUBE_STRUCTURE_ITEM], int]:
        """
        Create CUBE_STRUCTURE_ITEMs for attributes.

        Args:
            cube_structure: The CUBE_STRUCTURE object
            attribute_vars: List of attribute VARIABLE objects
            start_order: Starting order number
            debug_data: Optional dict to track created objects

        Returns:
            Tuple of (created items list, final order counter)
        """
        items = []
        order_counter = start_order

        for variable in attribute_vars:
            cube_variable_code = f"{cube_structure.code}__{variable.variable_id}"

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
            items.append(item)

            if debug_data is not None and 'CUBE_STRUCTURE_ITEM' in debug_data:
                if item not in debug_data['CUBE_STRUCTURE_ITEM']:
                    debug_data['CUBE_STRUCTURE_ITEM'].append(item)

        self.created_items.extend(items)
        return (items, order_counter)
