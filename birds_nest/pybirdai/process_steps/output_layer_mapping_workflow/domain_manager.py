"""
Module for managing domains, members, and subdomains.
Handles creation and updates of domain structures for reference output layers.
"""

import logging
from typing import List, Optional, Dict, Set
from datetime import datetime

from pybirdai.models.bird_meta_data_model import (
    DOMAIN, MEMBER, SUBDOMAIN, SUBDOMAIN_ENUMERATION,
    VARIABLE, MAINTENANCE_AGENCY, MEMBER_HIERARCHY,
    MEMBER_HIERARCHY_NODE
)
from pybirdai.utils.secure_logging import sanitize_log_value

logger = logging.getLogger(__name__)


class DomainManager:
    """
    Manages domains, members, and subdomains for output layer mappings.
    Ensures proper domain structures exist and are updated as needed.
    """

    def __init__(self):
        """Initialize the domain manager."""
        self.created_domains = []
        self.created_members = []
        self.created_subdomains = []
        self.updated_subdomains = []

    def ensure_domain_and_members(
        self,
        variable: VARIABLE,
        maintenance_agency: MAINTENANCE_AGENCY,
        member_codes: Optional[List[str]] = None
    ) -> DOMAIN:
        """
        Ensure a domain exists for a variable and has the necessary members.

        Args:
            variable: The VARIABLE object
            maintenance_agency: The maintenance agency
            member_codes: Optional list of member codes to ensure exist

        Returns:
            The DOMAIN object (existing or newly created)
        """
        # Check if variable has a domain
        if hasattr(variable, 'domain_id') and variable.domain_id:
            domain = variable.domain_id
            logger.info(f"Using existing domain {domain.domain_id} for variable {variable.variable_id}")
        else:
            # Create a new domain for the variable
            domain = self._create_domain_for_variable(variable, maintenance_agency)
            # Link variable to domain
            variable.domain_id = domain
            variable.save()
            logger.info(f"Created new domain {domain.domain_id} for variable {variable.variable_id}")

        # Ensure members exist if codes are provided
        if member_codes:
            self._ensure_members_exist(domain, member_codes, maintenance_agency)

        return domain

    def create_or_update_subdomain(
        self,
        domain: DOMAIN,
        subdomain_name: str,
        member_codes: List[str],
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> SUBDOMAIN:
        """
        Create or update a subdomain with specific members.

        Args:
            domain: The parent DOMAIN
            subdomain_name: Name for the subdomain
            member_codes: List of member codes to include
            maintenance_agency: The maintenance agency

        Returns:
            The SUBDOMAIN object
        """
        from .naming_utils import NamingUtils

        # Generate subdomain ID
        subdomain_id = NamingUtils.generate_internal_id(subdomain_name) + "_SD"

        # Check if subdomain exists
        subdomain = SUBDOMAIN.objects.filter(subdomain_id=subdomain_id).first()

        if subdomain:
            logger.info(f"Updating existing subdomain {subdomain_id}")
            self.updated_subdomains.append(subdomain)
        else:
            # Create new subdomain
            subdomain = SUBDOMAIN.objects.create(
                subdomain_id=subdomain_id,
                maintenance_agency_id=maintenance_agency,
                name=subdomain_name,
                code=NamingUtils.generate_internal_id(subdomain_name),
                domain_id=domain,
                is_listed=True,
                is_natural=False,
                description=f"Subdomain for {subdomain_name}"
            )
            self.created_subdomains.append(subdomain)
            logger.info(f"Created new subdomain {subdomain_id}")

        # Update subdomain enumeration
        self._update_subdomain_enumeration(subdomain, domain, member_codes, maintenance_agency)

        return subdomain

    def recompute_reference_subdomains(
        self,
        cube_structure_id: str,
        maintenance_agency: MAINTENANCE_AGENCY
    ):
        """
        Recompute subdomains for a reference cube structure.

        Args:
            cube_structure_id: The cube structure ID
            maintenance_agency: The maintenance agency
        """
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM

        # Get all cube structure items
        csi_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id__cube_structure_id=cube_structure_id
        ).select_related('variable_id', 'subdomain_id')

        for csi in csi_items:
            if csi.role == "D" and csi.variable_id:  # Dimension
                # Check if subdomain needs updating
                if hasattr(csi.variable_id, 'domain_id') and csi.variable_id.domain_id:
                    domain = csi.variable_id.domain_id

                    # Get all members currently used in combinations for this variable
                    used_members = self._get_used_members_for_variable(
                        csi.variable_id, cube_structure_id
                    )

                    if used_members:
                        # Create or update subdomain
                        subdomain_name = f"{csi.variable_id.name} subset for {cube_structure_id}"
                        subdomain = self.create_or_update_subdomain(
                            domain, subdomain_name, used_members, maintenance_agency
                        )

                        # Update CSI with new subdomain
                        if csi.subdomain_id != subdomain:
                            csi.subdomain_id = subdomain
                            csi.save()
                            logger.info(f"Updated CSI subdomain for variable {csi.variable_id.variable_id}")

    def create_member_hierarchy(
        self,
        domain: DOMAIN,
        hierarchy_name: str,
        hierarchy_structure: Dict,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> MEMBER_HIERARCHY:
        """
        Create a member hierarchy for aggregation.

        Args:
            domain: The DOMAIN for the hierarchy
            hierarchy_name: Name for the hierarchy
            hierarchy_structure: Dict defining parent-child relationships
            maintenance_agency: The maintenance agency

        Returns:
            The MEMBER_HIERARCHY object
        """
        from .naming_utils import NamingUtils

        hierarchy_id = NamingUtils.generate_internal_id(hierarchy_name) + "_HIER"

        # Create hierarchy
        hierarchy = MEMBER_HIERARCHY.objects.create(
            member_hierarchy_id=hierarchy_id,
            maintenance_agency_id=maintenance_agency,
            code=NamingUtils.generate_internal_id(hierarchy_name),
            name=hierarchy_name,
            domain_id=domain,
            is_main_hierarchy=False,
            description=f"Hierarchy for {domain.name}"
        )

        # Create hierarchy nodes
        self._create_hierarchy_nodes(hierarchy, hierarchy_structure, domain)

        logger.info(f"Created member hierarchy {hierarchy_id}")
        return hierarchy

    def _create_domain_for_variable(
        self,
        variable: VARIABLE,
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> DOMAIN:
        """
        Create a new domain for a variable.

        Args:
            variable: The VARIABLE object
            maintenance_agency: The maintenance agency

        Returns:
            The created DOMAIN object
        """
        from .naming_utils import NamingUtils

        # Generate domain ID from variable name
        domain_id = f"{variable.variable_id}_DOMAIN"
        domain_code = NamingUtils.generate_internal_id(variable.name)

        # Determine data type based on variable characteristics
        data_type = self._determine_data_type(variable)

        domain = DOMAIN.objects.create(
            domain_id=domain_id,
            maintenance_agency_id=maintenance_agency,
            name=f"Domain for {variable.name}",
            code=domain_code,
            is_enumerated=True,  # Default to enumerated
            data_type=data_type,
            is_reference=False,
            description=f"Generated domain for variable {variable.variable_id}"
        )

        self.created_domains.append(domain)
        return domain

    def _determine_data_type(self, variable: VARIABLE) -> str:
        """
        Determine the appropriate data type for a domain based on the variable.

        Args:
            variable: The VARIABLE object

        Returns:
            Data type string
        """
        var_id = variable.variable_id.upper()

        # Check for specific patterns
        if any(term in var_id for term in ['AMOUNT', 'VALUE', 'BALANCE']):
            return "Decimal"
        elif any(term in var_id for term in ['COUNT', 'NUMBER', 'QUANTITY']):
            return "BigInteger"
        elif any(term in var_id for term in ['DATE', 'TIME']):
            return "DateTime"
        elif any(term in var_id for term in ['FLAG', 'IS_', 'HAS_']):
            return "Boolean"
        else:
            return "String"  # Default to String

    def _ensure_members_exist(
        self,
        domain: DOMAIN,
        member_codes: List[str],
        maintenance_agency: MAINTENANCE_AGENCY
    ):
        """
        Ensure specific members exist in a domain.

        Args:
            domain: The DOMAIN object
            member_codes: List of member codes to ensure exist
            maintenance_agency: The maintenance agency
        """
        existing_members = MEMBER.objects.filter(
            domain_id=domain,
            code__in=member_codes
        ).values_list('code', flat=True)

        for code in member_codes:
            if code not in existing_members:
                # Create new member
                member = MEMBER.objects.create(
                    member_id=f"{domain.domain_id}_{code}",
                    maintenance_agency_id=maintenance_agency,
                    code=code,
                    name=self._humanize_code(code),
                    domain_id=domain,
                    description=f"Member {code} for domain {domain.name}"
                )
                self.created_members.append(member)
                logger.info(
                    "Created member %s in domain %s",
                    sanitize_log_value(code),
                    sanitize_log_value(domain.domain_id),
                )

    def _humanize_code(self, code: str) -> str:
        """
        Convert a code to a human-readable name.

        Args:
            code: The code string

        Returns:
            Human-readable name
        """
        # Replace underscores with spaces and capitalize words
        return code.replace('_', ' ').title()

    def _update_subdomain_enumeration(
        self,
        subdomain: SUBDOMAIN,
        domain: DOMAIN,
        member_codes: List[str],
        maintenance_agency: MAINTENANCE_AGENCY
    ):
        """
        Update the subdomain enumeration with specific members.

        Args:
            subdomain: The SUBDOMAIN object
            domain: The parent DOMAIN
            member_codes: List of member codes to include
            maintenance_agency: The maintenance agency
        """
        # Ensure members exist
        self._ensure_members_exist(domain, member_codes, maintenance_agency)

        # Get member objects
        members = MEMBER.objects.filter(
            domain_id=domain,
            code__in=member_codes
        )

        # Delete existing enumeration for this subdomain
        SUBDOMAIN_ENUMERATION.objects.filter(subdomain_id=subdomain).delete()

        # Create new enumeration
        order = 1
        for member in members:
            SUBDOMAIN_ENUMERATION.objects.create(
                member_id=member,
                subdomain_id=subdomain,
                order=order
            )
            order += 1

        logger.info(f"Updated subdomain enumeration with {len(members)} members")

    def _get_used_members_for_variable(
        self,
        variable: VARIABLE,
        cube_structure_id: str
    ) -> List[str]:
        """
        Get member codes used in combinations for a variable.

        Args:
            variable: The VARIABLE object
            cube_structure_id: The cube structure ID

        Returns:
            List of member codes
        """
        from pybirdai.models.bird_meta_data_model import (
            CUBE, CUBE_TO_COMBINATION, COMBINATION_ITEM
        )

        # Get cubes with this structure
        cubes = CUBE.objects.filter(
            cube_structure_id__cube_structure_id=cube_structure_id
        )

        member_codes = set()

        for cube in cubes:
            # Get combinations for this cube
            cube_combinations = CUBE_TO_COMBINATION.objects.filter(
                cube_id=cube
            ).values_list('combination_id', flat=True)

            # Get combination items for this variable
            items = COMBINATION_ITEM.objects.filter(
                combination_id__combination_id__in=cube_combinations,
                variable_id=variable
            ).select_related('member_id')

            for item in items:
                if item.member_id:
                    member_codes.add(item.member_id.code)

        return list(member_codes)

    def _create_hierarchy_nodes(
        self,
        hierarchy: MEMBER_HIERARCHY,
        hierarchy_structure: Dict,
        domain: DOMAIN
    ):
        """
        Create hierarchy nodes based on structure definition.

        Args:
            hierarchy: The MEMBER_HIERARCHY object
            hierarchy_structure: Dict with parent-child relationships
            domain: The DOMAIN object
        """
        # hierarchy_structure format:
        # {
        #     "TOTAL": {
        #         "children": ["SUBTOTAL_1", "SUBTOTAL_2"],
        #         "operator": "+",
        #         "level": 1
        #     },
        #     "SUBTOTAL_1": {
        #         "children": ["ITEM_1", "ITEM_2"],
        #         "operator": "+",
        #         "level": 2
        #     }
        # }

        for parent_code, node_info in hierarchy_structure.items():
            parent_member = MEMBER.objects.filter(
                domain_id=domain,
                code=parent_code
            ).first()

            if not parent_member:
                logger.warning(f"Parent member {parent_code} not found")
                continue

            # Create node for parent
            parent_node = MEMBER_HIERARCHY_NODE.objects.create(
                member_hierarchy_id=hierarchy,
                member_id=parent_member,
                level=node_info.get('level', 1),
                operator=node_info.get('operator', '+'),
                comparator=node_info.get('comparator', '=')
            )

            # Create nodes for children
            for child_code in node_info.get('children', []):
                child_member = MEMBER.objects.filter(
                    domain_id=domain,
                    code=child_code
                ).first()

                if child_member:
                    MEMBER_HIERARCHY_NODE.objects.create(
                        member_hierarchy_id=hierarchy,
                        member_id=child_member,
                        parent_member_id=parent_member,
                        level=node_info.get('level', 1) + 1,
                        operator='+',
                        comparator='='
                    )

    def validate_domain_structure(self, domain: DOMAIN) -> Dict:
        """
        Validate a domain structure for completeness.

        Args:
            domain: The DOMAIN object

        Returns:
            Dict with validation results
        """
        issues = []
        warnings = []
        info = []

        # Check if domain has members
        member_count = MEMBER.objects.filter(domain_id=domain).count()
        if member_count == 0:
            issues.append(f"Domain {domain.domain_id} has no members")
        else:
            info.append(f"Domain has {member_count} members")

        # Check if domain has subdomains
        subdomain_count = SUBDOMAIN.objects.filter(domain_id=domain).count()
        if subdomain_count == 0:
            warnings.append(f"Domain {domain.domain_id} has no subdomains")
        else:
            info.append(f"Domain has {subdomain_count} subdomains")

        # Check if domain has hierarchies
        hierarchy_count = MEMBER_HIERARCHY.objects.filter(domain_id=domain).count()
        if hierarchy_count == 0:
            warnings.append(f"Domain {domain.domain_id} has no hierarchies")
        else:
            info.append(f"Domain has {hierarchy_count} hierarchies")

        # Check enumeration consistency
        if domain.is_enumerated:
            # Check all subdomains have proper enumeration
            subdomains = SUBDOMAIN.objects.filter(domain_id=domain)
            for subdomain in subdomains:
                enum_count = SUBDOMAIN_ENUMERATION.objects.filter(
                    subdomain_id=subdomain
                ).count()
                if enum_count == 0:
                    warnings.append(f"Subdomain {subdomain.subdomain_id} has no enumeration")

        is_valid = len(issues) == 0

        return {
            'valid': is_valid,
            'issues': issues,
            'warnings': warnings,
            'info': info,
            'member_count': member_count,
            'subdomain_count': subdomain_count,
            'hierarchy_count': hierarchy_count
        }

    def get_domain_summary(self, domain: DOMAIN) -> Dict:
        """
        Get a summary of a domain's structure.

        Args:
            domain: The DOMAIN object

        Returns:
            Dict with domain summary
        """
        # Get members
        members = MEMBER.objects.filter(domain_id=domain).order_by('name')
        member_list = [
            {'id': m.member_id, 'code': m.code, 'name': m.name}
            for m in members[:20]  # Limit to first 20
        ]

        # Get subdomains
        subdomains = SUBDOMAIN.objects.filter(domain_id=domain).order_by('name')
        subdomain_list = [
            {'id': s.subdomain_id, 'name': s.name, 'code': s.code}
            for s in subdomains
        ]

        # Get hierarchies
        hierarchies = MEMBER_HIERARCHY.objects.filter(domain_id=domain)
        hierarchy_list = [
            {'id': h.member_hierarchy_id, 'name': h.name, 'is_main': h.is_main_hierarchy}
            for h in hierarchies
        ]

        return {
            'domain_id': domain.domain_id,
            'name': domain.name,
            'code': domain.code,
            'is_enumerated': domain.is_enumerated,
            'is_reference': domain.is_reference,
            'data_type': domain.data_type,
            'members': member_list,
            'member_count': len(members),
            'subdomains': subdomain_list,
            'hierarchies': hierarchy_list
        }
