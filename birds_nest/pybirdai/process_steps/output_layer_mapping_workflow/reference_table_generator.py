# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

"""
Reference Table Generator Module

Generates reference table artifacts from selected ordinates.
Creates a new reference TABLE with only the selected ordinates,
along with AXIS, AXIS_ORDINATE, ORDINATE_ITEM, TABLE_CELL, and CELL_POSITION records.
"""

import logging
from pybirdai.process_steps.output_layer_mapping_workflow.naming_utils import NamingUtils

logger = logging.getLogger(__name__)


def generate_reference_table_artifacts(source_table_id, selected_ordinates, framework, version, maintenance_agency, mapping_definitions=None):
    """
    Generate reference table artifacts from selected ordinates.
    Creates a new reference TABLE with only the selected ordinates,
    along with AXIS, AXIS_ORDINATE, ORDINATE_ITEM, TABLE_CELL, and CELL_POSITION records.

    Uses mapping definitions to show MAPPED variables/members instead of source ones.

    Args:
        source_table_id: str - Original DPM table ID
        selected_ordinates: list - Selected ordinate IDs from Step 3
        framework: str - Framework code (e.g., 'EBA_COREP')
        version: str - Version string (e.g., '4.0')
        maintenance_agency: MAINTENANCE_AGENCY object
        mapping_definitions: list - List of mapping definition dicts from Phase 3
            Each dict contains: {'mapping_definition': MAPPING_DEFINITION, 'name': str, ...}

    Returns:
        dict with keys:
            - success: bool
            - reference_table_id: str - ID of the created reference table
            - reference_table: TABLE object
            - error: str (if success=False)
    """
    from pybirdai.models.bird_meta_data_model import (
        TABLE, AXIS, AXIS_ORDINATE, ORDINATE_ITEM, TABLE_CELL, CELL_POSITION,
        VARIABLE_MAPPING_ITEM, MEMBER_MAPPING_ITEM
    )

    try:
        # Get source table
        source_table = TABLE.objects.get(table_id=source_table_id)
        source_table_code = source_table.code or source_table_id

        logger.info(f"[REF_TABLE] Starting reference table generation for {source_table_id}")
        logger.info(f"[REF_TABLE] Selected ordinates count: {len(selected_ordinates)}")

        # ========== BUILD MAPPING LOOKUP TABLES ==========
        # These maps source variable/member to target variable/member
        variable_mapping_lookup = {}  # {source_variable_id: target_variable}
        member_mapping_lookup = {}    # {(source_variable_id, source_member_id): target_member}

        if mapping_definitions:
            logger.info(f"[REF_TABLE] Building mapping lookups from {len(mapping_definitions)} mapping definitions")

            for mapping_info in mapping_definitions:
                mapping_def = mapping_info.get('mapping_definition')
                if not mapping_def:
                    continue

                # Build variable mapping lookup from MEMBER_MAPPING_ITEM rows
                # This gives us proper source->target variable pairing (each row pairs source and target)
                if mapping_def.member_mapping_id:
                    mm_items = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mapping_def.member_mapping_id
                    ).select_related('variable_id', 'member_id').order_by('member_mapping_row')

                    # Group by row to get source-target pairs for variable mapping
                    from itertools import groupby
                    for row_num, items_iter in groupby(mm_items, key=lambda x: x.member_mapping_row):
                        row_items = list(items_iter)
                        source_items = [item for item in row_items if item.is_source == "true"]
                        target_items = [item for item in row_items if item.is_source == "false"]

                        # Map each source variable to its paired target variable by position
                        for idx, source_item in enumerate(source_items):
                            source_var_id = source_item.variable_id.variable_id if source_item.variable_id else None
                            if source_var_id and target_items:
                                # Skip source items that don't have a corresponding target at the same position
                                if idx >= len(target_items):
                                    logger.debug(f"[REF_TABLE] Skipping source variable {source_var_id} - no target at index {idx}")
                                    continue
                                target_item = target_items[idx]
                                if source_var_id not in variable_mapping_lookup:
                                    if target_item.variable_id:
                                        variable_mapping_lookup[source_var_id] = target_item.variable_id
                                        logger.debug(f"[REF_TABLE] Variable mapping (from member_mapping): {source_var_id} -> {target_item.variable_id.variable_id}")

                # Also check variable_mapping_item for observation variables (which may not have member mappings)
                if mapping_def.variable_mapping_id:
                    vm_items = VARIABLE_MAPPING_ITEM.objects.filter(
                        variable_mapping_id=mapping_def.variable_mapping_id
                    ).select_related('variable_id')

                    # Separate source and target variables
                    source_vars = [item for item in vm_items if item.is_source == "true"]
                    target_vars = [item for item in vm_items if item.is_source == "false"]

                    # Only add mappings for variables not already mapped via member_mapping
                    # Pair source and target variables by position/index
                    for idx, source_item in enumerate(source_vars):
                        source_var_id = source_item.variable_id.variable_id if source_item.variable_id else None
                        if source_var_id and target_vars and source_var_id not in variable_mapping_lookup:
                            # Skip source items that don't have a corresponding target at the same position
                            if idx >= len(target_vars):
                                logger.debug(f"[REF_TABLE] Skipping source variable {source_var_id} - no target at index {idx}")
                                continue
                            variable_mapping_lookup[source_var_id] = target_vars[idx].variable_id
                            logger.debug(f"[REF_TABLE] Variable mapping (from variable_mapping): {source_var_id} -> {target_vars[idx].variable_id.variable_id}")

                # Build member mapping lookup: (source_var, source_member) -> target_member
                if mapping_def.member_mapping_id:
                    mm_items = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mapping_def.member_mapping_id
                    ).select_related('variable_id', 'member_id').order_by('member_mapping_row')

                    # Group by row to get source-target pairs
                    from itertools import groupby
                    for row_num, items_iter in groupby(mm_items, key=lambda x: x.member_mapping_row):
                        row_items = list(items_iter)
                        source_items = [item for item in row_items if item.is_source == "true"]
                        target_items = [item for item in row_items if item.is_source == "false"]

                        # Map each source (var, member) to corresponding target member by position
                        for idx, source_item in enumerate(source_items):
                            source_var_id = source_item.variable_id.variable_id if source_item.variable_id else None
                            source_mem_id = source_item.member_id.member_id if source_item.member_id else None
                            if source_var_id and source_mem_id and target_items:
                                # Skip source items that don't have a corresponding target at the same position
                                if idx >= len(target_items):
                                    logger.debug(f"[REF_TABLE] Skipping source member ({source_var_id}, {source_mem_id}) - no target at index {idx}")
                                    continue
                                target_item = target_items[idx]

                                if target_item.member_id:
                                    member_mapping_lookup[(source_var_id, source_mem_id)] = target_item.member_id
                                    logger.debug(f"[REF_TABLE] Member mapping: ({source_var_id}, {source_mem_id}) -> {target_item.member_id.member_id}")

            logger.info(f"[REF_TABLE] Built {len(variable_mapping_lookup)} variable mappings, {len(member_mapping_lookup)} member mappings")

        # Extract framework short name (e.g., 'EBA_COREP' -> 'COREP')
        framework_short = framework.replace('EBA_', '') if framework.startswith('EBA_') else framework
        version_normalized = version.replace('.', '_')

        # Strip Z-ordinate suffix from source_table_id for cleaner reference table ID
        # e.g., "EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx2029" -> "EBA_COREP_C_07_00_a_4_0"
        clean_table_id = NamingUtils.strip_z_ordinate_suffix(source_table_id)

        # Generate reference table ID: {FRAMEWORK_SHORT}_REF_{clean_table_id}
        # The clean_table_id already contains the version (e.g., C_07_00_a_4_0)
        reference_table_id = f"{framework_short}_REF_{clean_table_id}"

        # Check if reference table already exists
        existing_ref_table = TABLE.objects.filter(table_id=reference_table_id).first()
        if existing_ref_table:
            logger.info(f"[REF_TABLE] Reference table already exists: {reference_table_id}, cleaning up...")
            # Delete existing reference table and related artifacts
            ref_axes = AXIS.objects.filter(table_id=existing_ref_table)
            ref_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=ref_axes)
            CELL_POSITION.objects.filter(axis_ordinate_id__in=ref_ordinates).delete()
            TABLE_CELL.objects.filter(table_id=existing_ref_table).delete()
            ORDINATE_ITEM.objects.filter(axis_ordinate_id__in=ref_ordinates).delete()
            ref_ordinates.delete()
            ref_axes.delete()
            existing_ref_table.delete()

        # Create reference TABLE
        # Also strip Z-ordinate suffix from source_table_code for cleaner naming
        clean_table_code = NamingUtils.strip_z_ordinate_suffix(source_table_code)
        reference_table = TABLE.objects.create(
            table_id=reference_table_id,
            name=f"Reference: {source_table.name or clean_table_code}",
            code=f"{framework_short}_REF_{clean_table_code}",
            description=f"Reference table generated from {clean_table_id} with {len(selected_ordinates)} selected ordinates",
            maintenance_agency_id=maintenance_agency,
            version=version
        )
        logger.info(f"[REF_TABLE] Created reference TABLE: {reference_table_id}")

        # Get selected ordinate objects with their axis info
        selected_ordinate_objs = AXIS_ORDINATE.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('axis_id')

        # Group ordinates by axis orientation
        ordinates_by_orientation = {}

        for ordinate in selected_ordinate_objs:
            if ordinate.axis_id:
                orientation = ordinate.axis_id.orientation or 'X'
                if orientation not in ordinates_by_orientation:
                    ordinates_by_orientation[orientation] = []
                ordinates_by_orientation[orientation].append(ordinate)

        logger.info(f"[REF_TABLE] Orientations found: {list(ordinates_by_orientation.keys())}")

        # Create reference AXES and map old axis_id to new axis_id
        old_to_new_axis = {}
        for orientation, ordinates in ordinates_by_orientation.items():
            source_axis = ordinates[0].axis_id if ordinates else None
            if not source_axis:
                continue

            ref_axis_id = f"{reference_table_id}_AXIS_{orientation}"
            ref_axis = AXIS.objects.create(
                axis_id=ref_axis_id,
                code=f"{reference_table.code}_AXIS_{orientation}",
                orientation=orientation,
                order=source_axis.order,
                name=f"Reference Axis {orientation}",
                description=f"Reference axis for orientation {orientation}",
                table_id=reference_table,
                is_open_axis=False
            )
            old_to_new_axis[source_axis.axis_id] = ref_axis
            logger.info(f"[REF_TABLE] Created reference AXIS: {ref_axis_id}")

        # Create reference AXIS_ORDINATEs and map old to new
        # Two-pass approach: First create all ordinates, then update parent and path
        old_to_new_ordinate = {}
        source_ordinates_map = {}  # Map old ordinate ID to source ordinate object

        # Pass 1: Create all reference ordinates without parent/path
        for ordinate in selected_ordinate_objs:
            source_axis = ordinate.axis_id
            if source_axis and source_axis.axis_id in old_to_new_axis:
                ref_axis = old_to_new_axis[source_axis.axis_id]
                # Extract just the ordinate suffix (e.g., X_0220) to avoid duplicating the table ID
                ordinate_suffix = NamingUtils.extract_ordinate_suffix(
                    ordinate.axis_ordinate_id,
                    source_table_id
                )
                ref_ordinate_id = f"{reference_table_id}_{ordinate_suffix}"
                ref_ordinate = AXIS_ORDINATE.objects.create(
                    axis_ordinate_id=ref_ordinate_id,
                    is_abstract_header=ordinate.is_abstract_header,
                    code=ordinate.code,
                    order=ordinate.order,
                    level=ordinate.level,
                    path=None,  # Set in Pass 2
                    axis_id=ref_axis,
                    parent_axis_ordinate_id=None,  # Set in Pass 2
                    name=ordinate.name,
                    description=ordinate.description
                )
                old_to_new_ordinate[ordinate.axis_ordinate_id] = ref_ordinate
                source_ordinates_map[ordinate.axis_ordinate_id] = ordinate

        # Pass 2: Update path and infer parent from path
        for old_ord_id, ref_ordinate in old_to_new_ordinate.items():
            source_ordinate = source_ordinates_map[old_ord_id]

            # Update path with new ordinate IDs
            if source_ordinate.path:
                new_path_parts = []
                for old_id in source_ordinate.path.split('.'):
                    if old_id in old_to_new_ordinate:
                        new_path_parts.append(old_to_new_ordinate[old_id].axis_ordinate_id)
                    elif old_id:  # Skip empty strings
                        new_path_parts.append(old_id)
                ref_ordinate.path = '.'.join(new_path_parts) + '.'  # Ensure trailing dot

                # Infer parent from path (second-to-last element is parent, last is self)
                if len(new_path_parts) >= 2:
                    parent_id = new_path_parts[-2]
                    # Find parent in created ordinates
                    for new_ord in old_to_new_ordinate.values():
                        if new_ord.axis_ordinate_id == parent_id:
                            ref_ordinate.parent_axis_ordinate_id = new_ord
                            break

                ref_ordinate.save()

        logger.info(f"[REF_TABLE] Created {len(old_to_new_ordinate)} reference AXIS_ORDINATEs")

        # Copy ORDINATE_ITEMs for selected ordinates, applying mappings
        source_ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('variable_id', 'member_id', 'variable_id__domain_id')

        # Debug: Log source ordinate items and their variables
        source_items_list = list(source_ordinate_items)
        source_variables = set()
        for item in source_items_list:
            if item.variable_id:
                source_variables.add(item.variable_id.variable_id)
        logger.info(f"[REF_TABLE] Source ordinate items count: {len(source_items_list)}")
        logger.info(f"[REF_TABLE] Source variables found: {source_variables}")
        logger.info(f"[REF_TABLE] Selected ordinates: {selected_ordinates}")
        logger.info(f"[REF_TABLE] old_to_new_ordinate keys: {list(old_to_new_ordinate.keys())}")

        # Track created ordinate items to ensure no exact duplicates (ordinate + variable + member)
        created_ordinate_items = set()
        ordinate_items_created = 0
        duplicates_skipped = 0
        mappings_applied = 0
        ordinate_mismatch_skipped = 0
        unmapped_var_skipped = 0
        unmapped_member_skipped = 0

        # Observation/metric domains (data type domains) - these are always included
        # as they represent the actual reported values, not EBA-specific dimensions
        OBSERVATION_DOMAINS = {
            'EBA_Float', 'EBA_Integer', 'EBA_String', 'EBA_Date',
            'EBA_Boolean', 'EBA_FRQNCY', 'EBA_Decimal', 'EBA_Double'
        }

        for item in source_items_list:
            if item.axis_ordinate_id and item.axis_ordinate_id.axis_ordinate_id in old_to_new_ordinate:
                ref_ordinate = old_to_new_ordinate[item.axis_ordinate_id.axis_ordinate_id]

                # Apply variable mapping if available
                source_var_id = item.variable_id.variable_id if item.variable_id else None
                source_mem_id = item.member_id.member_id if item.member_id else None

                # Look up mapped variable
                mapped_variable = variable_mapping_lookup.get(source_var_id, item.variable_id)
                var_mapping_found = source_var_id in variable_mapping_lookup

                # Look up mapped member (using source var and member as key)
                member_key = (source_var_id, source_mem_id)
                mapped_member = member_mapping_lookup.get(
                    member_key,
                    item.member_id  # Fallback to original if no mapping
                )
                mem_mapping_found = member_key in member_mapping_lookup

                # Check if this is an observation variable (has a data type domain)
                # Observation variables hold the actual reported values and are always included
                is_observation_variable = False
                if item.variable_id and item.variable_id.domain_id:
                    domain_id = item.variable_id.domain_id.domain_id
                    is_observation_variable = domain_id in OBSERVATION_DOMAINS

                # Filter logic for reference tables:
                # 1. Observation variables are always included (they hold the reported data)
                # 2. Mapped variables require both variable AND member mapping to be included
                # 3. Unmapped non-observation variables are excluded (EBA-specific dimensions)
                if not is_observation_variable:
                    if not var_mapping_found:
                        # Unmapped EBA dimension - skip
                        unmapped_var_skipped += 1
                        logger.debug(f"[REF_TABLE] Skipping unmapped variable: {source_var_id}")
                        continue

                    if var_mapping_found and not mem_mapping_found:
                        # Variable mapped but member not mapped - incomplete mapping, skip
                        unmapped_member_skipped += 1
                        logger.debug(f"[REF_TABLE] Skipping item with unmapped member: var={source_var_id}, mem={source_mem_id}")
                        continue

                # Debug: Log each item's mapping lookup
                logger.info(f"[REF_TABLE] Processing item: ord={ref_ordinate.axis_ordinate_id}, "
                           f"src_var={source_var_id} -> {mapped_variable.variable_id if mapped_variable else None} (found={var_mapping_found}), "
                           f"src_mem={source_mem_id} -> {mapped_member.member_id if mapped_member else None} (found={mem_mapping_found})")

                # Build uniqueness key (ordinate + variable + member) to skip only exact duplicates
                unique_key = (
                    ref_ordinate.axis_ordinate_id,
                    mapped_variable.variable_id if mapped_variable else None,
                    mapped_member.member_id if mapped_member else None
                )

                # Skip only exact duplicates
                if unique_key in created_ordinate_items:
                    duplicates_skipped += 1
                    logger.info(f"[REF_TABLE] DUPLICATE SKIPPED: {unique_key}")
                    continue
                created_ordinate_items.add(unique_key)

                # Track if mapping was applied
                if mapped_variable != item.variable_id or mapped_member != item.member_id:
                    mappings_applied += 1
                    logger.debug(f"[REF_TABLE] Applied mapping for ordinate item: "
                                f"var {source_var_id} -> {mapped_variable.variable_id if mapped_variable else None}, "
                                f"mem {source_mem_id} -> {mapped_member.member_id if mapped_member else None}")

                ORDINATE_ITEM.objects.create(
                    axis_ordinate_id=ref_ordinate,
                    variable_id=mapped_variable,
                    member_id=mapped_member,
                    member_hierarchy_id=item.member_hierarchy_id,
                    member_hierarchy_valid_from=item.member_hierarchy_valid_from,
                    starting_member_id=item.starting_member_id,
                    is_starting_member_included=item.is_starting_member_included
                )
                ordinate_items_created += 1
            else:
                # Item's ordinate not in old_to_new_ordinate mapping
                ordinate_mismatch_skipped += 1
                item_ord_id = item.axis_ordinate_id.axis_ordinate_id if item.axis_ordinate_id else 'None'
                item_var_id = item.variable_id.variable_id if item.variable_id else 'None'
                logger.warning(f"[REF_TABLE] Skipped ordinate item - ordinate not in mapping: ord={item_ord_id}, var={item_var_id}")

        logger.info(f"[REF_TABLE] Created {ordinate_items_created} reference ORDINATE_ITEMs "
                   f"({mappings_applied} with applied mappings, {duplicates_skipped} duplicates skipped, "
                   f"{ordinate_mismatch_skipped} ordinate mismatch skipped, "
                   f"{unmapped_var_skipped} unmapped variables skipped, "
                   f"{unmapped_member_skipped} unmapped members skipped)")

        # Find cells that belong to selected ordinates
        source_cell_positions = CELL_POSITION.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('cell_id', 'axis_ordinate_id')

        # Group positions by cell
        cell_positions_map = {}
        for pos in source_cell_positions:
            if pos.cell_id:
                cell_id = pos.cell_id.cell_id
                if cell_id not in cell_positions_map:
                    cell_positions_map[cell_id] = []
                cell_positions_map[cell_id].append(pos)

        # Get source cells
        source_cells = TABLE_CELL.objects.filter(table_id=source_table, cell_id__in=cell_positions_map.keys())

        cells_created = 0
        positions_created = 0
        for source_cell in source_cells:
            positions = cell_positions_map.get(source_cell.cell_id, [])

            # Check if all positions for this cell are in selected ordinates
            all_positions = CELL_POSITION.objects.filter(cell_id=source_cell).count()
            if len(positions) < all_positions:
                continue

            # Create reference cell with clean ID: REF_{cell_number}
            # Strip both EBA_ prefix and Z-axis suffix (e.g., _qEC_qx2029)
            clean_cell_id = NamingUtils.strip_z_ordinate_suffix(source_cell.cell_id).replace("EBA_", "")
            ref_cell_id = f"REF_{clean_cell_id}"
            ref_cell = TABLE_CELL.objects.create(
                cell_id=ref_cell_id,
                is_shaded=source_cell.is_shaded,
                table_cell_combination_id=ref_cell_id,  # Use same ID for combination
                table_id=reference_table,
                system_data_code=source_cell.system_data_code,
                name=source_cell.name
            )
            cells_created += 1

            # Create reference cell positions
            for pos in positions:
                if pos.axis_ordinate_id and pos.axis_ordinate_id.axis_ordinate_id in old_to_new_ordinate:
                    ref_ordinate = old_to_new_ordinate[pos.axis_ordinate_id.axis_ordinate_id]
                    CELL_POSITION.objects.create(
                        cell_id=ref_cell,
                        axis_ordinate_id=ref_ordinate
                    )
                    positions_created += 1

        logger.info(f"[REF_TABLE] Created {cells_created} TABLE_CELLs and {positions_created} CELL_POSITIONs")
        logger.info(f"[REF_TABLE] Successfully completed: {reference_table_id}")

        return {
            'success': True,
            'reference_table_id': reference_table_id,
            'reference_table': reference_table
        }

    except Exception as e:
        logger.error(f"[REF_TABLE] Error generating reference table: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }
