"""
Module for generating cube structures and cube structure items.
Handles the creation of reference cube structures for output layer mappings.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from pybirdai.models.bird_meta_data_model import (
    CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, VARIABLE,
    DOMAIN, SUBDOMAIN, SUBDOMAIN_ENUMERATION, MEMBER,
    VARIABLE_SET, VARIABLE_SET_ENUMERATION,
    MAINTENANCE_AGENCY
)
from pybirdai.utils.secure_logging import sanitize_log_value

logger = logging.getLogger(__name__)


class CubeStructureGenerator:
    """
    Generates cube structures and cube structure items for output layer mappings.
    Creates reference cube structures with appropriate dimensions and measures.
    """

    def __init__(self):
        """Initialize the cube structure generator."""
        self.created_structures = []
        self.created_items = []

    def generate_cube_structure(
        self,
        structure_name: str,
        dimensions: List[Dict],
        measures: List[Dict],
        maintenance_agency: MAINTENANCE_AGENCY,
        version: str = "1.0"
    ) -> CUBE_STRUCTURE:
        """
        Generate a complete cube structure with items.

        Args:
            structure_name: Name for the cube structure
            dimensions: List of dimension definitions
            measures: List of measure definitions
            maintenance_agency: The maintenance agency
            version: Version string

        Returns:
            The created CUBE_STRUCTURE object
        """
        from .naming_utils import NamingUtils

        # Generate IDs
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        internal_id = NamingUtils.generate_internal_id(structure_name)
        structure_id = f"{internal_id}_STRUCTURE_{timestamp}"

        # Create cube structure
        cube_structure = CUBE_STRUCTURE.objects.create(
            cube_structure_id=structure_id,
            maintenance_agency_id=maintenance_agency,
            name=structure_name,
            code=internal_id,
            description=f"Reference cube structure for {structure_name}",
            version=version
        )

        self.created_structures.append(cube_structure)
        logger.info(f"Created cube structure: {structure_id}")

        # Create cube structure items
        order_counter = 1

        # Add dimensions
        for dim_def in dimensions:
            csi = self._create_dimension_item(
                cube_structure, dim_def, order_counter
            )
            if csi:
                self.created_items.append(csi)
                order_counter += 1

        # Add measures
        for meas_def in measures:
            csi = self._create_measure_item(
                cube_structure, meas_def, order_counter
            )
            if csi:
                self.created_items.append(csi)
                order_counter += 1

        logger.info(f"Created {order_counter - 1} cube structure items")
        return cube_structure

    def _create_dimension_item(
        self,
        cube_structure: CUBE_STRUCTURE,
        dimension_def: Dict,
        order: int
    ) -> Optional[CUBE_STRUCTURE_ITEM]:
        """
        Create a cube structure item for a dimension.

        Args:
            cube_structure: The parent CUBE_STRUCTURE
            dimension_def: Dictionary with dimension definition
            order: Display order

        Returns:
            The created CUBE_STRUCTURE_ITEM or None
        """
        variable_id = dimension_def.get('variable_id')
        if not variable_id:
            logger.warning("Dimension definition missing variable_id")
            return None

        variable = VARIABLE.objects.filter(variable_id=variable_id).first()
        if not variable:
            logger.warning(f"Variable {variable_id} not found")
            return None

        # Generate cube variable code
        cube_variable_code = f"{cube_structure.code}__{variable_id}"

        # Create or get subdomain (or single member if domain has only 1 member)
        subdomain = None
        single_member = None
        if dimension_def.get('create_subdomain', True):
            subdomain, single_member = self.create_or_get_subdomain(
                variable, cube_structure.cube_structure_id
            )

        # Determine dimension type
        dimension_type = dimension_def.get('dimension_type')
        if not dimension_type:
            dimension_type = self._determine_dimension_type(variable)

        # Check for fixed member (overrides single_member if specified)
        fixed_member = None
        if dimension_def.get('fixed_member_code'):
            if hasattr(variable, 'domain_id') and variable.domain_id:
                fixed_member = MEMBER.objects.filter(
                    domain_id=variable.domain_id,
                    code=dimension_def['fixed_member_code']
                ).first()

        # Use fixed_member if specified, otherwise use single_member from subdomain check
        final_member = fixed_member if fixed_member else single_member

        # Create cube structure item
        # Note: If domain has 1 member, subdomain will be None and final_member will be set
        csi = CUBE_STRUCTURE_ITEM.objects.create(
            cube_structure_id=cube_structure,
            cube_variable_code=cube_variable_code,
            variable_id=variable,
            role="D",  # Dimension
            order=order,
            subdomain_id=subdomain,
            member_id=final_member,
            dimension_type=dimension_type,
            is_mandatory=dimension_def.get('is_mandatory', True),
            is_implemented=True,
            is_identifier=dimension_def.get('is_identifier', False),
            description=dimension_def.get('description', f"Dimension: {variable.name}")
        )

        logger.info(f"Created dimension CSI: {cube_variable_code}")
        return csi

    def _create_measure_item(
        self,
        cube_structure: CUBE_STRUCTURE,
        measure_def: Dict,
        order: int
    ) -> Optional[CUBE_STRUCTURE_ITEM]:
        """
        Create a cube structure item for a measure/metric.

        Args:
            cube_structure: The parent CUBE_STRUCTURE
            measure_def: Dictionary with measure definition
            order: Display order

        Returns:
            The created CUBE_STRUCTURE_ITEM or None
        """
        variable_id = measure_def.get('variable_id')
        if not variable_id:
            logger.warning("Measure definition missing variable_id")
            return None

        variable = VARIABLE.objects.filter(variable_id=variable_id).first()
        if not variable:
            logger.warning(f"Variable {variable_id} not found")
            return None

        # Generate cube variable code
        cube_variable_code = f"{cube_structure.code}__{variable_id}"

        # Create cube structure item
        csi = CUBE_STRUCTURE_ITEM.objects.create(
            cube_structure_id=cube_structure,
            cube_variable_code=cube_variable_code,
            variable_id=variable,
            role="O",  # Observation/Metric
            order=order,
            is_mandatory=measure_def.get('is_mandatory', True),
            is_implemented=True,
            is_flow=measure_def.get('is_flow', True),
            description=measure_def.get('description', f"Measure: {variable.name}")
        )

        logger.info(f"Created measure CSI: {cube_variable_code}")
        return csi

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
                    "Domain %s has single member %s, using direct member reference instead of subdomain",
                    sanitize_log_value(domain.domain_id),
                    sanitize_log_value(single_member.code),
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

        # Create new subdomain
        maintenance_agency = MAINTENANCE_AGENCY.objects.first()
        if not maintenance_agency:
            maintenance_agency = MAINTENANCE_AGENCY.objects.create(
                maintenance_agency_id='EFBT',
                name='EFBT System',
                code='EFBT'
            )

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
            logger.error(
                "Error creating subdomain %s: %s",
                sanitize_log_value(subdomain_id),
                sanitize_log_value(e),
            )
            return (None, None)

        # Copy enumeration from domain if it's enumerated
        if domain.is_enumerated:
            logger.info(f"[SUBDOMAIN DEBUG] Domain is enumerated, copying enumerations to subdomain")
            self._copy_domain_enumeration_to_subdomain(domain, subdomain)
        else:
            logger.info(f"[SUBDOMAIN DEBUG] Domain is NOT enumerated, skipping enumeration copy")

        logger.info(f"[SUBDOMAIN DEBUG] Created new subdomain: {subdomain_id}")
        return (subdomain, None)

    def _copy_domain_enumeration_to_subdomain(
        self,
        domain: DOMAIN,
        subdomain: SUBDOMAIN
    ):
        """
        Copy all members from a domain to a subdomain enumeration.
        Validates each member exists before creating enumeration (prevents foreign key violations).

        Args:
            domain: The source DOMAIN
            subdomain: The target SUBDOMAIN
        """
        logger.info(f"[ENUM DEBUG] Copying enumerations from domain {domain.domain_id} to subdomain {subdomain.subdomain_id}")

        members = MEMBER.objects.filter(domain_id=domain).order_by('name')
        member_count = members.count()

        logger.info(f"[ENUM DEBUG] Found {member_count} members in domain {domain.domain_id}")

        if member_count == 0:
            logger.warning(f"[ENUM DEBUG] No members found in domain {domain.domain_id} - will create 0 enumerations!")
            logger.warning(f"[ENUM DEBUG] This will likely cause FK violations if subdomain enumerations are required")
            return  # Exit early if no members

        order = 1
        created_count = 0
        skipped_count = 0

        for member in members:
            # Defensive check: verify member still exists before creating SUBDOMAIN_ENUMERATION
            # This prevents foreign key constraint violations
            if not MEMBER.objects.filter(member_id=member.member_id).exists():
                logger.warning(
                    "Skipping member %s - doesn't exist in database. Would cause foreign key violation.",
                    sanitize_log_value(member.member_id),
                )
                skipped_count += 1
                continue

            try:
                SUBDOMAIN_ENUMERATION.objects.create(
                    member_id=member,
                    subdomain_id=subdomain,
                    order=order
                )
                created_count += 1
                order += 1
            except Exception as e:
                logger.error(
                    "Error creating SUBDOMAIN_ENUMERATION for member %s: %s",
                    sanitize_log_value(member.member_id),
                    sanitize_log_value(e),
                )
                skipped_count += 1

        logger.info(
            f"[ENUM DEBUG] Copied {created_count} members to subdomain {subdomain.subdomain_id}"
            + (f" ({skipped_count} skipped due to errors)" if skipped_count > 0 else "")
        )

        if created_count == 0:
            logger.error(f"[ENUM DEBUG] CRITICAL: 0 SUBDOMAIN_ENUMERATIONs created for subdomain {subdomain.subdomain_id}!")
            logger.error(f"[ENUM DEBUG] This will cause FK constraint violations!")

    def _determine_dimension_type(self, variable: VARIABLE) -> str:
        """
        Determine the appropriate dimension type for a variable.

        Args:
            variable: The VARIABLE object

        Returns:
            Dimension type code (B, M, T, or U)
        """
        var_id = variable.variable_id.upper()
        var_name = variable.name.upper() if variable.name else ""

        # Temporal dimensions
        temporal_terms = ['DATE', 'TIME', 'PERIOD', 'YEAR', 'MONTH', 'DAY', 'QUARTER']
        if any(term in var_id or term in var_name for term in temporal_terms):
            return "T"

        # Unit dimensions
        unit_terms = ['UNIT', 'CURRENCY', 'CCY', 'CURR']
        if any(term in var_id or term in var_name for term in unit_terms):
            return "U"

        # Methodological dimensions
        method_terms = ['METHOD', 'APPROACH', 'CALC', 'BASIS', 'TYPE']
        if any(term in var_id or term in var_name for term in method_terms):
            return "M"

        # Default to Business dimension
        return "B"

    def create_attribute_item(
        self,
        cube_structure: CUBE_STRUCTURE,
        attribute_def: Dict,
        order: int
    ) -> Optional[CUBE_STRUCTURE_ITEM]:
        """
        Create a cube structure item for an attribute.

        Args:
            cube_structure: The parent CUBE_STRUCTURE
            attribute_def: Dictionary with attribute definition
            order: Display order

        Returns:
            The created CUBE_STRUCTURE_ITEM or None
        """
        variable_id = attribute_def.get('variable_id')
        if not variable_id:
            logger.warning("Attribute definition missing variable_id")
            return None

        variable = VARIABLE.objects.filter(variable_id=variable_id).first()
        if not variable:
            logger.warning(f"Variable {variable_id} not found")
            return None

        # Get associated variable if specified
        associated_var = None
        if attribute_def.get('associated_variable_id'):
            associated_var = VARIABLE.objects.filter(
                variable_id=attribute_def['associated_variable_id']
            ).first()

        # Generate cube variable code
        cube_variable_code = f"{cube_structure.code}__ATTR_{variable_id}"

        # Create cube structure item
        csi = CUBE_STRUCTURE_ITEM.objects.create(
            cube_structure_id=cube_structure,
            cube_variable_code=cube_variable_code,
            variable_id=variable,
            role="A",  # Attribute
            order=order,
            attribute_associated_variable=associated_var,
            is_mandatory=attribute_def.get('is_mandatory', False),
            is_implemented=True,
            description=attribute_def.get('description', f"Attribute: {variable.name}")
        )

        logger.info(f"Created attribute CSI: {cube_variable_code}")
        return csi

    def update_cube_structure_from_mapping(
        self,
        cube_structure: CUBE_STRUCTURE,
        mapping_config: Dict
    ) -> List[CUBE_STRUCTURE_ITEM]:
        """
        Update a cube structure based on mapping configuration.

        Args:
            cube_structure: The CUBE_STRUCTURE to update
            mapping_config: Dictionary with mapping configuration

        Returns:
            List of created/updated CUBE_STRUCTURE_ITEM objects
        """
        items = []
        order_counter = 1

        # Process dimensions
        if 'dimensions' in mapping_config:
            for var_id, mapping_def in mapping_config['dimensions'].items():
                target_var_id = mapping_def.get('target', var_id)

                # Check if CSI already exists
                existing_csi = CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=cube_structure,
                    variable_id__variable_id=target_var_id
                ).first()

                if existing_csi:
                    # Update existing
                    existing_csi.order = order_counter
                    existing_csi.save()
                    items.append(existing_csi)
                    logger.info(f"Updated existing CSI for {target_var_id}")
                else:
                    # Create new dimension item
                    dim_def = {
                        'variable_id': target_var_id,
                        'dimension_type': mapping_def.get('dimension_type', 'B'),
                        'is_mandatory': True,
                        'create_subdomain': True
                    }
                    csi = self._create_dimension_item(
                        cube_structure, dim_def, order_counter
                    )
                    if csi:
                        items.append(csi)

                order_counter += 1

        # Process measures
        if 'measures' in mapping_config:
            for var_id, mapping_def in mapping_config['measures'].items():
                target_var_id = mapping_def.get('target', var_id)

                # Check if CSI already exists
                existing_csi = CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=cube_structure,
                    variable_id__variable_id=target_var_id
                ).first()

                if existing_csi:
                    # Update existing
                    existing_csi.order = order_counter
                    existing_csi.role = "O"
                    existing_csi.is_flow = True
                    existing_csi.save()
                    items.append(existing_csi)
                    logger.info(f"Updated existing CSI for {target_var_id}")
                else:
                    # Create new measure item
                    meas_def = {
                        'variable_id': target_var_id,
                        'is_flow': True,
                        'is_mandatory': True
                    }
                    csi = self._create_measure_item(
                        cube_structure, meas_def, order_counter
                    )
                    if csi:
                        items.append(csi)

                order_counter += 1

        logger.info(f"Updated cube structure with {len(items)} items")
        return items

    def validate_cube_structure(self, cube_structure: CUBE_STRUCTURE) -> Dict:
        """
        Validate a cube structure for completeness and consistency.

        Args:
            cube_structure: The CUBE_STRUCTURE object

        Returns:
            Dict with validation results
        """
        issues = []
        warnings = []
        info = []

        # Get cube structure items
        items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube_structure
        ).order_by('order')

        if not items:
            issues.append("Cube structure has no items")
            return {
                'valid': False,
                'issues': issues,
                'warnings': warnings,
                'info': info
            }

        # Check for at least one measure
        measures = items.filter(role="O")
        if not measures:
            issues.append("Cube structure has no measures (role='O')")
        else:
            info.append(f"Cube structure has {measures.count()} measures")

        # Check for dimensions
        dimensions = items.filter(role="D")
        if not dimensions:
            warnings.append("Cube structure has no dimensions (role='D')")
        else:
            info.append(f"Cube structure has {dimensions.count()} dimensions")

        # Check dimension types
        for dim in dimensions:
            if not dim.dimension_type:
                warnings.append(f"Dimension {dim.cube_variable_code} has no dimension_type")

        # Check for duplicate variables
        variable_ids = []
        for item in items:
            if item.variable_id:
                var_id = item.variable_id.variable_id
                if var_id in variable_ids:
                    issues.append(f"Duplicate variable {var_id} in cube structure")
                variable_ids.append(var_id)

        # Check order sequence
        orders = [item.order for item in items]
        if orders != sorted(orders):
            warnings.append("Item orders are not sequential")

        # Check subdomain consistency
        for item in items:
            if item.subdomain_id and item.variable_id:
                if hasattr(item.variable_id, 'domain_id'):
                    if item.subdomain_id.domain_id != item.variable_id.domain_id:
                        issues.append(
                            f"Subdomain {item.subdomain_id.subdomain_id} "
                            f"doesn't match variable domain for {item.cube_variable_code}"
                        )

        is_valid = len(issues) == 0

        return {
            'valid': is_valid,
            'issues': issues,
            'warnings': warnings,
            'info': info,
            'dimension_count': dimensions.count(),
            'measure_count': measures.count(),
            'attribute_count': items.filter(role="A").count(),
            'total_items': items.count()
        }

    def get_cube_structure_summary(self, cube_structure: CUBE_STRUCTURE) -> Dict:
        """
        Get a summary of a cube structure.

        Args:
            cube_structure: The CUBE_STRUCTURE object

        Returns:
            Dict with cube structure summary
        """
        items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube_structure
        ).select_related('variable_id', 'subdomain_id', 'member_id').order_by('order')

        dimensions = []
        measures = []
        attributes = []

        for item in items:
            item_info = {
                'cube_variable_code': item.cube_variable_code,
                'variable': item.variable_id.name if item.variable_id else None,
                'variable_id': item.variable_id.variable_id if item.variable_id else None,
                'order': item.order
            }

            if item.role == "D":
                item_info['dimension_type'] = item.dimension_type
                item_info['subdomain'] = item.subdomain_id.name if item.subdomain_id else None
                item_info['fixed_member'] = item.member_id.name if item.member_id else None
                dimensions.append(item_info)
            elif item.role == "O":
                item_info['is_flow'] = item.is_flow
                measures.append(item_info)
            elif item.role == "A":
                item_info['associated_variable'] = (
                    item.attribute_associated_variable.name
                    if item.attribute_associated_variable else None
                )
                attributes.append(item_info)

        return {
            'cube_structure_id': cube_structure.cube_structure_id,
            'name': cube_structure.name,
            'code': cube_structure.code,
            'version': cube_structure.version,
            'dimensions': dimensions,
            'measures': measures,
            'attributes': attributes,
            'total_items': len(dimensions) + len(measures) + len(attributes)
        }
