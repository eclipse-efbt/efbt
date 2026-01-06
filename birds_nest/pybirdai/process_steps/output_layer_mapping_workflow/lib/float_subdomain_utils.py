"""
Module to ensure Float subdomain exists and is associated with MTRC variable.
This script is idempotent and can be run multiple times safely.
"""

import logging
from typing import Dict, Any

from pybirdai.models.bird_meta_data_model import (
    DOMAIN, SUBDOMAIN, VARIABLE, MAINTENANCE_AGENCY
)

logger = logging.getLogger(__name__)


def ensure_float_subdomain_for_mtrc() -> Dict[str, Any]:
    """
    Ensure Float subdomain exists and MTRC variable is associated with it.

    Returns:
        Dict with status information:
        {
            'success': bool,
            'subdomain_created': bool,
            'subdomain_id': str,
            'mtrc_updated': bool,
            'message': str
        }
    """
    result = {
        'success': False,
        'subdomain_created': False,
        'subdomain_id': None,
        'mtrc_updated': False,
        'message': ''
    }

    try:
        # Step 1: Check if Float domain exists
        try:
            float_domain = DOMAIN.objects.get(domain_id='Float')
            logger.info(f"Float domain found: {float_domain.domain_id}")
        except DOMAIN.DoesNotExist:
            result['message'] = "Float domain does not exist. Please create Float domain first."
            logger.error(result['message'])
            return result

        # Step 2: Check if Float subdomain exists
        float_subdomain_id = "Float"
        subdomain = SUBDOMAIN.objects.filter(subdomain_id=float_subdomain_id).first()

        if not subdomain:
            # Create Float subdomain
            logger.info(f"Float subdomain not found. Creating subdomain: {float_subdomain_id}")

            # Get or create maintenance agency using AgencyManager
            from pybirdai.process_steps.output_layer_mapping_workflow.lib.entity_managers import (
                AgencyManager
            )
            maintenance_agency = AgencyManager().get_efbt_agency()

            # Create subdomain
            subdomain = SUBDOMAIN.objects.create(
                subdomain_id=float_subdomain_id,
                maintenance_agency_id=maintenance_agency,
                name="Float",
                code="Float",
                domain_id=float_domain,
                is_listed=True,
                is_natural=False,
                description="Float subdomain for numeric floating-point values"
            )

            result['subdomain_created'] = True
            logger.info(f"Created Float subdomain: {subdomain.subdomain_id}")
        else:
            logger.info(f"Float subdomain already exists: {subdomain.subdomain_id}")

        result['subdomain_id'] = subdomain.subdomain_id

        # Step 3: Check if MTRC variable exists
        try:
            mtrc_variable = VARIABLE.objects.get(variable_id='MTRC')
            logger.info(f"MTRC variable found: {mtrc_variable.variable_id}")

            # Step 4: Update MTRC to reference Float subdomain
            if mtrc_variable.subdomain_id != subdomain:
                old_subdomain = mtrc_variable.subdomain_id.subdomain_id if mtrc_variable.subdomain_id else None
                mtrc_variable.subdomain_id = subdomain
                mtrc_variable.save()

                result['mtrc_updated'] = True
                logger.info(
                    f"Updated MTRC variable subdomain from "
                    f"{old_subdomain} to {subdomain.subdomain_id}"
                )
            else:
                logger.info(f"MTRC already has Float subdomain: {subdomain.subdomain_id}")

            result['success'] = True
            result['message'] = (
                f"Float subdomain {'created' if result['subdomain_created'] else 'verified'}. "
                f"MTRC variable {'updated' if result['mtrc_updated'] else 'already has'} "
                f"Float subdomain association."
            )

        except VARIABLE.DoesNotExist:
            result['success'] = True  # Subdomain created successfully
            result['message'] = (
                f"Float subdomain {'created' if result['subdomain_created'] else 'verified'}, "
                f"but MTRC variable not found in database."
            )
            logger.warning("MTRC variable not found in database")

    except Exception as e:
        result['message'] = f"Error: {str(e)}"
        logger.error(f"Error ensuring Float subdomain for MTRC: {str(e)}", exc_info=True)
        return result

    logger.info(result['message'])
    return result
