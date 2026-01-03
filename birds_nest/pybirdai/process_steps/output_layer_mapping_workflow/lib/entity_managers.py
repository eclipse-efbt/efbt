"""
Entity managers for maintenance agencies and frameworks.

Provides reusable business logic for creating and retrieving
MAINTENANCE_AGENCY and FRAMEWORK entities.
"""

import logging
from typing import Dict, Optional, Any

from pybirdai.models.bird_meta_data_model import MAINTENANCE_AGENCY, FRAMEWORK

logger = logging.getLogger(__name__)


class AgencyManager:
    """
    Manages MAINTENANCE_AGENCY entities.

    Provides methods for creating and retrieving maintenance agencies
    with proper defaults and tracking.
    """

    # Default agency definitions
    DEFAULT_AGENCIES = {
        'USER': {'name': 'User Defined', 'code': 'USER'},
        'EBA': {'name': 'European Banking Authority', 'code': 'EBA'},
        'EFBT': {'name': 'EFBT System', 'code': 'EFBT'},
    }

    def get_or_create_agency(
        self,
        agency_id: str,
        name: Optional[str] = None,
        code: Optional[str] = None,
        debug_data: Optional[Dict] = None
    ) -> MAINTENANCE_AGENCY:
        """
        Get or create a maintenance agency.

        Args:
            agency_id: The agency ID (e.g., 'USER', 'EBA', 'EFBT')
            name: Optional agency name (uses default if not provided)
            code: Optional agency code (uses default if not provided)
            debug_data: Optional dict to track created objects

        Returns:
            MAINTENANCE_AGENCY object
        """
        # Use defaults if available
        defaults = self.DEFAULT_AGENCIES.get(agency_id, {})
        agency_name = name or defaults.get('name', agency_id)
        agency_code = code or defaults.get('code', agency_id)

        agency, created = MAINTENANCE_AGENCY.objects.get_or_create(
            maintenance_agency_id=agency_id,
            defaults={
                'name': agency_name,
                'code': agency_code
            }
        )

        if created:
            logger.info(f"Created MAINTENANCE_AGENCY: {agency_id}")

        # Track in debug_data
        if debug_data is not None:
            if 'MAINTENANCE_AGENCY' in debug_data and agency not in debug_data['MAINTENANCE_AGENCY']:
                debug_data['MAINTENANCE_AGENCY'].append(agency)

        return agency

    def ensure_default_agencies(
        self,
        agency_ids: Optional[list] = None,
        debug_data: Optional[Dict] = None
    ) -> Dict[str, MAINTENANCE_AGENCY]:
        """
        Ensure default maintenance agencies exist.

        Args:
            agency_ids: List of agency IDs to ensure (defaults to ['USER', 'EBA'])
            debug_data: Optional dict to track created objects

        Returns:
            Dict mapping agency_id to MAINTENANCE_AGENCY object
        """
        if agency_ids is None:
            agency_ids = ['USER', 'EBA']

        agencies = {}
        for agency_id in agency_ids:
            agencies[agency_id] = self.get_or_create_agency(agency_id, debug_data=debug_data)

        logger.info(f"Ensured maintenance agencies: {', '.join(agency_ids)}")
        return agencies

    def get_efbt_agency(self, debug_data: Optional[Dict] = None) -> MAINTENANCE_AGENCY:
        """
        Get or create the EFBT system agency.

        This agency is used as the default for auto-generated entities.

        Args:
            debug_data: Optional dict to track created objects

        Returns:
            MAINTENANCE_AGENCY object for EFBT
        """
        return self.get_or_create_agency('EFBT', debug_data=debug_data)


class FrameworkManager:
    """
    Manages FRAMEWORK entities.

    Provides methods for creating and retrieving frameworks
    with proper agency assignment.
    """

    def __init__(self, agency_manager: Optional[AgencyManager] = None):
        """
        Initialize the framework manager.

        Args:
            agency_manager: Optional AgencyManager instance (creates one if not provided)
        """
        self.agency_manager = agency_manager or AgencyManager()

    def get_or_create_framework(
        self,
        framework_id: str,
        debug_data: Optional[Dict] = None
    ) -> FRAMEWORK:
        """
        Get or create a framework with EFBT maintenance agency.

        Args:
            framework_id: The framework ID (e.g., 'FINREP_REF', 'COREP_REF')
            debug_data: Optional dict to track created objects

        Returns:
            FRAMEWORK object

        Raises:
            ValueError: If framework cannot be created or retrieved
        """
        # Try to get existing framework
        framework_obj = FRAMEWORK.objects.filter(framework_id=framework_id).first()

        if not framework_obj:
            # Auto-create framework with EFBT agency
            try:
                efbt_agency = self.agency_manager.get_efbt_agency(debug_data=debug_data)

                framework_obj, created = FRAMEWORK.objects.get_or_create(
                    framework_id=framework_id,
                    defaults={
                        'name': framework_id,
                        'code': framework_id,
                        'maintenance_agency_id': efbt_agency,
                        'description': f'Auto-generated framework for {framework_id}'
                    }
                )

                if created:
                    logger.info(f"Auto-created FRAMEWORK: {framework_id} with EFBT maintenance agency")
                else:
                    logger.info(f"Reusing existing FRAMEWORK: {framework_id}")

            except Exception as e:
                logger.error(f"Failed to create FRAMEWORK {framework_id}: {str(e)}")
                raise

        # Track in debug_data
        if debug_data is not None:
            if 'FRAMEWORK' in debug_data and framework_obj not in debug_data['FRAMEWORK']:
                debug_data['FRAMEWORK'].append(framework_obj)

        # Validate framework exists
        if not framework_obj:
            error_msg = f"FRAMEWORK {framework_id} could not be created or retrieved"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"Framework validated: {framework_obj.framework_id}")
        return framework_obj

    def validate_framework(self, framework_obj: Optional[FRAMEWORK], framework_id: str) -> None:
        """
        Validate that a framework object exists.

        Args:
            framework_obj: The framework object to validate
            framework_id: The expected framework ID

        Raises:
            ValueError: If framework is None or invalid
        """
        if not framework_obj:
            error_msg = f"FRAMEWORK {framework_id} could not be created or retrieved"
            logger.error(error_msg)
            raise ValueError(error_msg)
