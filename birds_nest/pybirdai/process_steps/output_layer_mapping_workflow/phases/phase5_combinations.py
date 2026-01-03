"""
Phase 5: Combinations

Creates combinations, combination items, and cube-to-combination links for table cells.
This is a thin orchestration layer that delegates to lib functions.
"""
from pybirdai.models import CUBE_TO_COMBINATION

import logging
from pybirdai.models.bird_meta_data_model import TABLE
from pybirdai.process_steps.output_layer_mapping_workflow.lib.table_cell_utils import (
    get_cells_for_table, filter_cells_by_ordinates
)
from pybirdai.process_steps.output_layer_mapping_workflow.lib.combination_creator import (
    CombinationCreator
)

logger = logging.getLogger(__name__)


def execute_phase5_combinations(
    request,
    table_id,
    table_code,
    version,
    cube,
    debug_data,
    sdd_context=None,
    context=None
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
        sdd_context: Optional SDD context for cube-to-combination mapping
        context: Optional context for save settings

    Returns:
        dict: {
            'created_combinations': List of created COMBINATION objects/dicts,
            'cells_count': Number of cells processed
        }
    """
    logger.info("[PHASE 5] Creating combinations for table cells...")

    # Delete existing combinations for this cube before regenerating
    ctcs = CUBE_TO_COMBINATION.objects.filter(cube_id=cube)
    for ctc in ctcs:
        if ctc.combination_id:
            # Delete combination items first (reverse accessor is combination_item_set)
            ctc.combination_id.combination_item_set.all().delete()
            # Delete the combination itself
            ctc.combination_id.delete()
        # Delete the cube_to_combination record
        ctc.delete()

    # Get table object
    table = TABLE.objects.get(table_id=table_id)

    # Get table cells (handles Z-variants automatically)
    cells = get_cells_for_table(table, table_id)
    logger.info(f"[PHASE 5] Found {cells.count()} cells")

    # Filter cells by selected ordinates if provided
    selected_ordinates = request.session.get('olmw_selected_ordinates', [])
    if selected_ordinates:
        cells = filter_cells_by_ordinates(cells, selected_ordinates)
        logger.info(f"[PHASE 5] After ordinate filtering: {cells.count()} cells")

    # Create combinations using CombinationCreator
    combination_creator = CombinationCreator(table_code, version, sdd_context, context)
    result = combination_creator.create_combinations_for_cells(
        cells=cells,
        cube=cube,
        debug_data=debug_data
    )

    logger.info(f"[PHASE 5] Created {len(result['created_combinations'])} combinations")

    return result
