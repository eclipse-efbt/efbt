"""
Module for managing domains, members, and subdomains.
Handles creation and updates of domain structures for reference output layers.
Also includes member extraction from mappings.
"""

import logging
from typing import List, Optional, Dict, Any

from pybirdai.models.bird_meta_data_model import (
    DOMAIN, MEMBER, VARIABLE, MAINTENANCE_AGENCY, MEMBER_MAPPING_ITEM
)

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
                logger.info(f"Created member {code} in domain {domain.domain_id}")

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

    # ========== Member Extraction Methods ==========

    def extract_member_codes(
        self,
        all_mappings: Optional[Dict] = None,
        created_mapping_definitions: Optional[List] = None,
        regenerate_mode: bool = False
    ) -> Dict[str, List[str]]:
        """
        Extract member codes from mappings.

        Delegates to appropriate extraction method based on mode.

        Args:
            all_mappings: Session mapping data (normal mode)
            created_mapping_definitions: Existing mapping definitions (regenerate mode)
            regenerate_mode: Whether in regenerate mode

        Returns:
            Dict mapping variable_id to list of member_ids
        """
        if regenerate_mode:
            return self.extract_from_mapping_definitions(created_mapping_definitions or [])
        else:
            return self.extract_from_session_mappings(all_mappings or {})

    def extract_from_mapping_definitions(
        self,
        created_mapping_definitions: List
    ) -> Dict[str, List[str]]:
        """
        Extract member codes from existing MEMBER_MAPPING_ITEM records.

        Used in regenerate mode when mappings already exist in database.

        Args:
            created_mapping_definitions: List of mapping definition dicts

        Returns:
            Dict mapping variable_id to list of member_ids
        """
        variable_to_members_map = {}

        logger.info("Extracting member codes from existing mappings (regenerate mode)")

        for mapping_info in created_mapping_definitions:
            mapping_def = mapping_info.get('mapping_definition')
            if mapping_def and mapping_def.member_mapping_id:
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

        return variable_to_members_map

    def extract_from_session_mappings(
        self,
        all_mappings: Dict
    ) -> Dict[str, List[str]]:
        """
        Extract member codes from session mapping data.

        Used in normal mode when mappings come from session.

        Args:
            all_mappings: Dict of mapping data from session

        Returns:
            Dict mapping variable_id to list of member_ids
        """
        variable_to_members_map = {}

        logger.info("Extracting member codes from mappings (normal mode)")

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

        return variable_to_members_map

    def ensure_members_for_variables(
        self,
        unique_target_vars: List[VARIABLE],
        variable_to_members_map: Dict[str, List[str]],
        maintenance_agency: MAINTENANCE_AGENCY,
        debug_data: Optional[Dict] = None
    ) -> Dict[str, int]:
        """
        Ensure all members exist for target variables.

        Args:
            unique_target_vars: List of unique target VARIABLE objects
            variable_to_members_map: Dict mapping variable_id to member_ids
            maintenance_agency: MAINTENANCE_AGENCY for new members
            debug_data: Optional dict to track created objects

        Returns:
            Dict with 'members_created_count' and 'members_validated_count'
        """
        members_created_count = 0
        members_validated_count = 0

        logger.info(f"Ensuring domains and members exist for {len(unique_target_vars)} target variables")

        for variable in unique_target_vars:
            # Ensure domain exists for this variable
            domain = self.ensure_domain_and_members(variable, maintenance_agency)

            # Get member_ids that should exist for this variable
            member_ids_for_var = variable_to_members_map.get(variable.variable_id, [])

            # Ensure each member exists in the database
            for member_id in member_ids_for_var:
                existing_member = MEMBER.objects.filter(member_id=member_id).first()

                if not existing_member:
                    # Create member
                    new_member = self._create_member_for_variable(
                        member_id, domain, maintenance_agency
                    )
                    if new_member:
                        members_created_count += 1
                        logger.info(f"Created member {member_id} for variable {variable.variable_id}")

                        if debug_data is not None and 'MEMBER' in debug_data:
                            if new_member not in debug_data['MEMBER']:
                                debug_data['MEMBER'].append(new_member)
                else:
                    members_validated_count += 1

                    if debug_data is not None and 'MEMBER' in debug_data:
                        if existing_member not in debug_data['MEMBER']:
                            debug_data['MEMBER'].append(existing_member)

        logger.info(f"Completed: {members_created_count} members created, "
                    f"{members_validated_count} members validated")

        return {
            'members_created_count': members_created_count,
            'members_validated_count': members_validated_count
        }

    def _create_member_for_variable(
        self,
        member_id: str,
        domain: Optional[DOMAIN],
        maintenance_agency: MAINTENANCE_AGENCY
    ) -> Optional[MEMBER]:
        """
        Create a member with proper code extraction.

        Args:
            member_id: The member ID to create
            domain: The domain for the member
            maintenance_agency: The maintenance agency

        Returns:
            Created MEMBER object or None if creation fails
        """
        try:
            # Extract code from member_id
            if domain and domain.domain_id in member_id:
                # Format: DOMAIN_CODE (e.g., "EBA_COREP_MB_1")
                code = member_id.replace(f"{domain.domain_id}_", "")
            else:
                # Format: CODE (e.g., "MB_1")
                code = member_id.split('_')[-1] if '_' in member_id else member_id

            new_member = MEMBER.objects.create(
                member_id=member_id,
                maintenance_agency_id=maintenance_agency,
                code=code,
                name=code,
                domain_id=domain,
                description=f"Member {code} for domain {domain.name if domain else 'Unknown'}"
            )
            self.created_members.append(new_member)
            return new_member

        except Exception as e:
            logger.error(f"Failed to create member {member_id}: {str(e)}")
            return None
