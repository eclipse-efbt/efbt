"""
Phase 1: Base Setup

Creates maintenance agencies and framework objects required for subsequent phases.
This is a thin orchestration layer that delegates to lib functions.
"""

import logging
from pybirdai.process_steps.output_layer_mapping_workflow.lib.entity_managers import (
    AgencyManager, FrameworkManager
)

logger = logging.getLogger(__name__)


def execute_phase1_base_setup(framework_id, debug_data):
    """
    Execute Phase 1: Create maintenance agencies and framework.

    Args:
        framework_id: Framework ID string (e.g., 'FINREP_REF', 'COREP_REF')
        debug_data: Dict to collect created objects

    Returns:
        dict: {
            'framework': FRAMEWORK object,
            'maintenance_agencies': dict of created agencies
        }

    Raises:
        ValueError: If framework cannot be created or retrieved
    """
    logger.info("[PHASE 1] Creating maintenance agencies and framework...")

    # Create/get maintenance agencies using AgencyManager
    agency_manager = AgencyManager()
    agencies = agency_manager.ensure_default_agencies(
        agency_ids=['USER', 'EBA'],
        debug_data=debug_data
    )

    # Create/get framework using FrameworkManager
    framework_manager = FrameworkManager(agency_manager)
    framework_obj = framework_manager.get_or_create_framework(
        framework_id=framework_id,
        debug_data=debug_data
    )

    logger.info(f"[PHASE 1] Completed: Framework {framework_obj.framework_id} ready")

    return {
        'framework': framework_obj,
        'maintenance_agencies': {
            'USER': agencies.get('USER'),
            'EBA': agencies.get('EBA'),
            'EFBT': agency_manager.get_efbt_agency() if framework_obj else None
        }
    }
