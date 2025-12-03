"""
Phase 1: Base Setup

Creates maintenance agencies and framework objects required for subsequent phases.
"""

import logging
from pybirdai.models.bird_meta_data_model import MAINTENANCE_AGENCY, FRAMEWORK

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
    
    # ========== MAINTENANCE AGENCIES ==========
    # Get or create USER maintenance agency
    maintenance_agency, created = MAINTENANCE_AGENCY.objects.get_or_create(
        maintenance_agency_id='USER',
        defaults={
            'name': 'User Defined',
            'code': 'USER'
        }
    )
    if created:
        logger.info("[PHASE 1] Created MAINTENANCE_AGENCY: USER")
    
    # Ensure EBA maintenance agency exists (needed for DPM-sourced data)
    eba_agency, created = MAINTENANCE_AGENCY.objects.get_or_create(
        maintenance_agency_id='EBA',
        defaults={
            'name': 'European Banking Authority',
            'code': 'EBA'
        }
    )
    if created:
        logger.info("[PHASE 1] Created MAINTENANCE_AGENCY: EBA")
    
    logger.info("[PHASE 1] Ensured maintenance agencies: USER, EBA")
    
    # Track in debug_data
    if maintenance_agency not in debug_data['MAINTENANCE_AGENCY']:
        debug_data['MAINTENANCE_AGENCY'].append(maintenance_agency)
    if eba_agency not in debug_data['MAINTENANCE_AGENCY']:
        debug_data['MAINTENANCE_AGENCY'].append(eba_agency)
    
    # ========== FRAMEWORK CREATION ==========
    # Get or create FRAMEWORK (needed by CUBE creation in Phase 4)
    framework_obj = FRAMEWORK.objects.filter(framework_id=framework_id).first()
    
    # Auto-create framework if it doesn't exist (e.g., COREP_REF, FINREP_REF)
    if not framework_obj:
        try:
            # Get or create EFBT maintenance agency
            efbt_agency, created = MAINTENANCE_AGENCY.objects.get_or_create(
                maintenance_agency_id='EFBT',
                defaults={
                    'name': 'EFBT System',
                    'code': 'EFBT'
                }
            )
            logger.info(f"[PHASE 1] EFBT maintenance agency exists: {efbt_agency.maintenance_agency_id}")
            
            # Track EFBT agency in debug_data
            if efbt_agency not in debug_data['MAINTENANCE_AGENCY']:
                debug_data['MAINTENANCE_AGENCY'].append(efbt_agency)
            
            # Create the missing framework
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
                logger.info(f"[PHASE 1] Auto-created FRAMEWORK: {framework_id} with EFBT maintenance agency")
            else:
                logger.info(f"[PHASE 1] Reusing existing FRAMEWORK: {framework_id}")
        except Exception as e:
            logger.error(f"[PHASE 1 ERROR] Failed to create FRAMEWORK {framework_id}: {str(e)}")
            raise
    
    # Add framework to debug_data (whether created or reused)
    if framework_obj and framework_obj not in debug_data['FRAMEWORK']:
        debug_data['FRAMEWORK'].append(framework_obj)
    
    # Validate framework_obj exists before proceeding
    if not framework_obj:
        error_msg = f"[PHASE 1 ERROR] FRAMEWORK {framework_id} could not be created or retrieved"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"[PHASE 1] Framework validated: {framework_obj.framework_id}")
    
    return {
        'framework': framework_obj,
        'maintenance_agencies': {
            'USER': maintenance_agency,
            'EBA': eba_agency,
            'EFBT': MAINTENANCE_AGENCY.objects.get(maintenance_agency_id='EFBT') if MAINTENANCE_AGENCY.objects.filter(maintenance_agency_id='EFBT').exists() else None
        }
    }
