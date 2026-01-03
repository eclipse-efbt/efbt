"""
Subdomain management utilities for output layer mapping workflow.

Centralizes subdomain creation, enumeration copying, and single-member optimization.
Includes dictionary-based caching for performance.
"""

import logging
from typing import Dict, Optional, Tuple, List

from pybirdai.models.bird_meta_data_model import (
    VARIABLE, DOMAIN, SUBDOMAIN, SUBDOMAIN_ENUMERATION, MEMBER, MAINTENANCE_AGENCY
)

logger = logging.getLogger(__name__)

# Module-level caches
_subdomain_cache: Dict[str, SUBDOMAIN] = {}
_single_member_cache: Dict[str, Optional[MEMBER]] = {}
_domain_member_count_cache: Dict[str, int] = {}


def clear_subdomain_cache():
    """Clear all subdomain-related caches. Call between workflow runs."""
    global _subdomain_cache, _single_member_cache, _domain_member_count_cache
    _subdomain_cache.clear()
    _single_member_cache.clear()
    _domain_member_count_cache.clear()
    logger.debug("Subdomain caches cleared")


class SubdomainManager:
    """
    Manages subdomain creation and member optimization for cube structures.

    Centralizes logic previously duplicated across CubeStructureGenerator
    and CombinationCreator.
    """

    def __init__(self, maintenance_agency: Optional[MAINTENANCE_AGENCY] = None):
        """
        Initialize the subdomain manager.

        Args:
            maintenance_agency: Optional agency for creating new subdomains.
                               If not provided, uses AgencyManager.get_efbt_agency().
        """
        self._maintenance_agency = maintenance_agency

    @property
    def maintenance_agency(self) -> MAINTENANCE_AGENCY:
        """Get or create the maintenance agency (lazy initialization)."""
        if self._maintenance_agency is None:
            from pybirdai.process_steps.output_layer_mapping_workflow.lib.entity_managers import (
                AgencyManager
            )
            self._maintenance_agency = AgencyManager().get_efbt_agency()
        return self._maintenance_agency

    def get_domain_member_count(self, domain: DOMAIN) -> int:
        """
        Get the number of members in a domain (cached).

        Args:
            domain: The DOMAIN object

        Returns:
            Number of members in the domain
        """
        domain_id = domain.domain_id
        if domain_id in _domain_member_count_cache:
            return _domain_member_count_cache[domain_id]

        count = MEMBER.objects.filter(domain_id=domain).count()
        _domain_member_count_cache[domain_id] = count
        return count

    def get_single_member_if_exists(self, domain: DOMAIN) -> Optional[MEMBER]:
        """
        Check if domain has exactly one member and return it.

        This is an optimization: if a domain has only one member,
        we can use a direct member reference instead of a subdomain.

        Args:
            domain: The DOMAIN object to check

        Returns:
            The single MEMBER if domain has exactly 1 member, None otherwise
        """
        domain_id = domain.domain_id
        if domain_id in _single_member_cache:
            return _single_member_cache[domain_id]

        if not domain.is_enumerated:
            _single_member_cache[domain_id] = None
            return None

        members = MEMBER.objects.filter(domain_id=domain)
        count = members.count()

        if count == 1:
            member = members.first()
            _single_member_cache[domain_id] = member
            logger.debug(f"Domain {domain_id} has single member: {member.member_id}")
            return member

        _single_member_cache[domain_id] = None
        return None

    def get_single_member_from_subdomain(
        self,
        subdomain: Optional[SUBDOMAIN]
    ) -> Optional[MEMBER]:
        """
        Check if subdomain has exactly one member and return it.

        Args:
            subdomain: The SUBDOMAIN object to check

        Returns:
            The single MEMBER if subdomain has exactly 1 member, None otherwise
        """
        if not subdomain:
            return None

        subdomain_id = subdomain.subdomain_id
        cache_key = f"sd_{subdomain_id}"

        if cache_key in _single_member_cache:
            return _single_member_cache[cache_key]

        subdomain_members = SUBDOMAIN_ENUMERATION.objects.filter(
            subdomain_id=subdomain
        ).select_related('member_id')

        if subdomain_members.count() == 1:
            member = subdomain_members.first().member_id
            _single_member_cache[cache_key] = member
            return member

        _single_member_cache[cache_key] = None
        return None

    def create_or_get_subdomain(
        self,
        variable: VARIABLE,
        cube_structure_id: str,
        copy_enumeration: bool = True
    ) -> Tuple[Optional[SUBDOMAIN], Optional[MEMBER]]:
        """
        Create or get a subdomain for a variable in a cube structure.

        If the domain has exactly one member, returns that member instead
        of creating a subdomain (optimization).

        Args:
            variable: The VARIABLE object
            cube_structure_id: The cube structure ID
            copy_enumeration: Whether to copy domain enumeration to subdomain

        Returns:
            Tuple of (SUBDOMAIN, MEMBER) where:
            - If domain has 1 member: (None, member)
            - If domain has multiple members: (subdomain, None)
            - If error: (None, None)
        """
        logger.debug(f"Creating/getting subdomain for variable {variable.variable_id}")

        if not hasattr(variable, 'domain_id') or not variable.domain_id:
            logger.warning(f"Variable {variable.variable_id} has no domain")
            return (None, None)

        domain = variable.domain_id

        # Check for single-member optimization
        single_member = self.get_single_member_if_exists(domain)
        if single_member:
            logger.info(
                f"Domain {domain.domain_id} has single member {single_member.code}, "
                f"using direct member reference instead of subdomain"
            )
            return (None, single_member)

        # Check if domain has no members (error case)
        member_count = self.get_domain_member_count(domain)
        if domain.is_enumerated and member_count == 0:
            logger.warning(
                f"Domain {domain.domain_id} has no members - "
                f"cannot create subdomain (would cause FK violations)"
            )
            return (None, None)

        # Generate subdomain ID
        subdomain_id = f"{variable.variable_id}_OUTPUT_SD_{cube_structure_id}"

        # Check cache first
        if subdomain_id in _subdomain_cache:
            logger.debug(f"Using cached subdomain: {subdomain_id}")
            return (_subdomain_cache[subdomain_id], None)

        # Check database
        subdomain = SUBDOMAIN.objects.filter(subdomain_id=subdomain_id).first()
        if subdomain:
            logger.debug(f"Using existing subdomain: {subdomain_id}")
            _subdomain_cache[subdomain_id] = subdomain
            return (subdomain, None)

        # Validate domain exists (FK validation)
        from pybirdai.process_steps.output_layer_mapping_workflow.lib.transaction_validator import (
            domain_exists
        )
        if not domain_exists(domain.domain_id):
            logger.error(
                f"Cannot create subdomain - domain {domain.domain_id} doesn't exist. "
                f"This would cause a foreign key constraint violation."
            )
            return (None, None)

        # Create new subdomain
        try:
            subdomain = SUBDOMAIN.objects.create(
                subdomain_id=subdomain_id,
                maintenance_agency_id=self.maintenance_agency,
                name=f"{variable.name} subset for {cube_structure_id}",
                code=f"{variable.code}_SD" if hasattr(variable, 'code') else subdomain_id,
                domain_id=domain,
                is_listed=True,
                is_natural=False,
                description=f"Output subdomain for {variable.name} in {cube_structure_id}"
            )
            logger.info(f"Created new subdomain: {subdomain_id}")

            # Copy enumeration if requested and domain is enumerated
            if copy_enumeration and domain.is_enumerated:
                self.copy_domain_enumeration(domain, subdomain)

            _subdomain_cache[subdomain_id] = subdomain
            return (subdomain, None)

        except Exception as e:
            logger.error(f"Error creating subdomain {subdomain_id}: {str(e)}")
            return (None, None)

    def copy_domain_enumeration(
        self,
        domain: DOMAIN,
        subdomain: SUBDOMAIN
    ) -> int:
        """
        Copy all members from a domain to a subdomain enumeration.

        Validates each member exists before creating enumeration
        to prevent FK violations.

        Args:
            domain: The source DOMAIN
            subdomain: The target SUBDOMAIN

        Returns:
            Number of enumerations created
        """
        logger.debug(
            f"Copying enumerations from domain {domain.domain_id} "
            f"to subdomain {subdomain.subdomain_id}"
        )

        members = MEMBER.objects.filter(domain_id=domain).order_by('name')
        member_count = members.count()

        if member_count == 0:
            logger.warning(f"Domain {domain.domain_id} has no members to copy")
            return 0

        created_count = 0
        order = 1

        for member in members:
            # Check if already exists
            existing = SUBDOMAIN_ENUMERATION.objects.filter(
                subdomain_id=subdomain,
                member_id=member
            ).exists()

            if not existing:
                try:
                    SUBDOMAIN_ENUMERATION.objects.create(
                        subdomain_id=subdomain,
                        member_id=member,
                        order=order
                    )
                    created_count += 1
                except Exception as e:
                    logger.error(
                        f"Error creating subdomain enumeration for "
                        f"member {member.member_id}: {str(e)}"
                    )

            order += 1

        logger.info(
            f"Created {created_count} subdomain enumerations "
            f"(out of {member_count} domain members)"
        )
        return created_count

    def get_subdomain_members(
        self,
        subdomain: SUBDOMAIN
    ) -> List[MEMBER]:
        """
        Get all members in a subdomain.

        Args:
            subdomain: The SUBDOMAIN object

        Returns:
            List of MEMBER objects in the subdomain
        """
        enumerations = SUBDOMAIN_ENUMERATION.objects.filter(
            subdomain_id=subdomain
        ).select_related('member_id').order_by('order')

        return [enum.member_id for enum in enumerations]
