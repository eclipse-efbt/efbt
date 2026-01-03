"""
Phase executor for managing savepoint-based transaction phases.

Coordinates execution of individual phases with savepoint management and FK validation.
"""

import logging
from django.db import transaction, connection
from .lib.transaction_validator import validate_fks_for_phase

logger = logging.getLogger(__name__)


class PhaseExecutor:
    """
    Executes transaction phases with savepoint handling and FK validation.
    
    Each phase is wrapped in a savepoint and validated before proceeding to the next phase.
    """
    
    def __init__(self):
        self.cursor = None
        self.completed_phases = []
    
    def get_cursor(self):
        """Get database cursor for PRAGMA checks."""
        if self.cursor is None:
            self.cursor = connection.cursor()
        return self.cursor
    
    def execute_phase(self, phase_name, phase_callable, debug_data, validate=True):
        """
        Execute a phase with savepoint and FK validation.
        
        Args:
            phase_name: Human-readable name of the phase
            phase_callable: Callable that executes the phase logic
            debug_data: Dict to collect created objects for debug export
            validate: Whether to run FK validation after phase (default True)
        
        Returns:
            Result from phase_callable execution
        
        Raises:
            Exception: If phase execution or FK validation fails
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"[PHASE EXECUTOR] Starting {phase_name}")
        logger.info(f"{'='*80}")
        
        # Create savepoint
        sid = transaction.savepoint()
        
        try:
            # Execute the phase
            result = phase_callable()
            
            # Log phase completion
            self._log_phase_summary(phase_name, debug_data)
            
            # Validate FKs if requested
            if validate:
                cursor = self.get_cursor()
                validate_fks_for_phase(phase_name, debug_data, cursor)
            
            # Commit savepoint
            transaction.savepoint_commit(sid)
            
            logger.info(f"[PHASE EXECUTOR] {phase_name} completed successfully ✓")
            self.completed_phases.append(phase_name)
            
            return result
            
        except Exception as e:
            # Rollback savepoint on error
            logger.error(f"[PHASE EXECUTOR] {phase_name} failed: {e}")
            transaction.savepoint_rollback(sid)
            
            logger.error(f"\n{'='*80}")
            logger.error(f"[PHASE EXECUTOR] Transaction rolled back to before {phase_name}")
            logger.error(f"[PHASE EXECUTOR] Completed phases: {self.completed_phases}")
            logger.error(f"[PHASE EXECUTOR] Failed phase: {phase_name}")
            logger.error(f"{'='*80}\n")
            
            raise
    
    def _log_phase_summary(self, phase_name, debug_data):
        """Log summary of objects created in this phase."""
        if debug_data is None:
            logger.info(f"\n[{phase_name}] Debug tracking disabled - skipping summary")
            return

        logger.info(f"\n[{phase_name}] Summary of created objects:")

        # Count objects by type
        for model_type, objects in debug_data.items():
            if objects:
                count = len(objects)
                logger.info(f"  - {model_type}: {count}")
                
                # Log first few IDs for each type
                if count > 0 and count <= 5:
                    ids = []
                    for obj in objects:
                        if isinstance(obj, dict):
                            # Handle dict format (e.g., {'combination': obj})
                            for key, value in obj.items():
                                if hasattr(value, f'{model_type.lower()}_id'):
                                    ids.append(getattr(value, f'{model_type.lower()}_id'))
                                    break
                        elif hasattr(obj, f'{model_type.lower()}_id'):
                            ids.append(getattr(obj, f'{model_type.lower()}_id'))
                    
                    if ids:
                        logger.info(f"    IDs: {', '.join(str(id) for id in ids)}")
                elif count > 5:
                    # Just show first 3 and last 1 for large lists
                    ids = []
                    for i, obj in enumerate(objects):
                        if i < 3 or i == count - 1:
                            if isinstance(obj, dict):
                                for key, value in obj.items():
                                    if hasattr(value, f'{model_type.lower()}_id'):
                                        ids.append(getattr(value, f'{model_type.lower()}_id'))
                                        break
                            elif hasattr(obj, f'{model_type.lower()}_id'):
                                ids.append(getattr(obj, f'{model_type.lower()}_id'))
                    
                    if ids:
                        if len(ids) == 4:
                            logger.info(f"    Sample IDs: {ids[0]}, {ids[1]}, {ids[2]}, ..., {ids[3]}")
                        else:
                            logger.info(f"    Sample IDs: {', '.join(str(id) for id in ids[:3])}...")
        
        logger.info("")
    
    def get_completed_phases(self):
        """Return list of successfully completed phases."""
        return self.completed_phases.copy()
