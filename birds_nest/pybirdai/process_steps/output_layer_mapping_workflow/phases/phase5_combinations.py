"""
Phase 5: Combinations

Creates combinations, combination items, and cube-to-combination links for table cells.
"""

import logging
import datetime
from pybirdai.models.bird_meta_data_model import (
    TABLE, AXIS, AXIS_ORDINATE, CELL_POSITION, TABLE_CELL, CUBE_TO_COMBINATION
)
from pybirdai.process_steps.output_layer_mapping_workflow.combination_creator import CombinationCreator
from pybirdai.process_steps.output_layer_mapping_workflow.table_utils import get_base_table_id, is_z_variant_table

logger = logging.getLogger(__name__)


def execute_phase5_combinations(
    request,
    table_id,
    table_code,
    version,
    cube,
    debug_data
):
    """
    Execute Phase 5: Create combinations and link to cube.
    
    Args:
        request: Django request object (for session data - selected ordinates)
        table_id: Full table ID with variant suffix
        table_code: Base table code
        version: Version string (normalized, e.g., "3_2_0")
        cube: CUBE object to link combinations to
        debug_data: Dict to collect created objects
    
    Returns:
        dict: {
            'created_combinations': List of created COMBINATION objects/dicts,
            'cells_count': Number of cells processed
        }
    """
    logger.info("[PHASE 5] Creating combinations for table cells...")

    # ========== GET TABLE CELLS ==========
    table = TABLE.objects.get(table_id=table_id)

    # For Z-variant tables, use base table for querying AXIS/CELL_POSITION/TABLE_CELL
    # Z-variant tables share the same cells as the base table
    base_table_id = get_base_table_id(table_id)

    if base_table_id != table_id:
        # This is a Z-variant table - query using base table
        logger.info(f"[PHASE 5] Detected Z-variant table '{table_id}', using base table '{base_table_id}' for cell queries")
        base_table = TABLE.objects.filter(table_id=base_table_id).first()

        if base_table:
            query_table = base_table
            logger.info(f"[PHASE 5] Successfully found base table '{base_table_id}' in database")
        else:
            # Base table doesn't exist - try with current table (fallback)
            logger.warning(f"[PHASE 5] Base table '{base_table_id}' not found in database, falling back to '{table_id}'")
            query_table = table
    else:
        # Not a Z-variant table - use table directly
        logger.info(f"[PHASE 5] Using table '{table_id}' directly (not a Z-variant)")
        query_table = table

    # Get cells via CELL_POSITION traversal for deduplicated table support
    # Path: TABLE -> AXIS -> AXIS_ORDINATE -> CELL_POSITION -> TABLE_CELL
    table_axes = AXIS.objects.filter(table_id=query_table)
    table_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=table_axes)
    all_cell_positions = CELL_POSITION.objects.filter(axis_ordinate_id__in=table_ordinates)
    cell_ids = all_cell_positions.values_list('cell_id', flat=True).distinct()
    cells = TABLE_CELL.objects.filter(cell_id__in=cell_ids)

    logger.info(f"[PHASE 5] Found {cells.count()} cells via CELL_POSITION traversal (query_table={query_table.table_id}, original_table={table_id})")
    
    # ========== FILTER CELLS BY SELECTED ORDINATES ==========
    selected_ordinates = request.session.get('olmw_selected_ordinates', [])
    if selected_ordinates:
        # Get cells that have positions in selected ordinates
        filtered_cell_positions = CELL_POSITION.objects.filter(
            axis_ordinate_id__in=selected_ordinates,
            cell_id__in=cells
        ).values_list('cell_id', flat=True).distinct()
        
        # Filter to only cells with selected ordinates
        cells = cells.filter(cell_id__in=filtered_cell_positions)
        logger.info(f'[PHASE 5] Creating combinations for {cells.count()} cells matching selected ordinates')
    
    # ========== CREATE COMBINATIONS ==========
    # Generate single timestamp for entire generation run
    generation_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
    # Create combination creator with table code and version
    combination_creator = CombinationCreator(table_code, version)
    created_combinations = []
    
    for cell in cells:
        # Create combination for this cell
        # CombinationCreator.create_combination_for_cell() creates:
        # - COMBINATION object
        # - COMBINATION_ITEM objects (tracked internally or via signals)
        combination = combination_creator.create_combination_for_cell(
            cell, cube, generation_timestamp
        )
        if combination:
            created_combinations.append(combination)
            
            # Track in debug_data (might be dict format)
            if combination not in debug_data['COMBINATION']:
                debug_data['COMBINATION'].append(combination)
            
            # Create CUBE_TO_COMBINATION link (get or create to avoid duplicates)
            cube_to_combo, _ = CUBE_TO_COMBINATION.objects.get_or_create(
                cube_id=cube,
                combination_id=combination
            )
            debug_data['CUBE_TO_COMBINATION'].append(cube_to_combo)
    
    logger.info(f"[PHASE 5] Created {len(created_combinations)} combinations and linked to cube")
    
    return {
        'created_combinations': created_combinations,
        'cells_count': cells.count()
    }
