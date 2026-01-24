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

        # ========== BUILD M:N MAPPING STRUCTURE ==========
        # The mapping is m:n - multiple source variables/members in a row map to
        # multiple target variables/members in the same row.
        # Structure: list of rows, each row has source_items set and target_items list
        member_mapping_rows = []  # [{source_items: {(var_id, mem_id), ...}, target_items: [(var_obj, mem_obj), ...]}, ...]
        observation_variable_mappings = {}  # {source_var_id: target_var_obj} for observation variables

        if mapping_definitions:
            logger.info(f"[REF_TABLE] Building m:n mapping structure from {len(mapping_definitions)} mapping definitions")

            for mapping_info in mapping_definitions:
                mapping_def = mapping_info.get('mapping_definition')
                if not mapping_def:
                    continue

                # Build row-based mapping structure from MEMBER_MAPPING_ITEM
                # Each row represents a complete source->target mapping
                if mapping_def.member_mapping_id:
                    mm_items = MEMBER_MAPPING_ITEM.objects.filter(
                        member_mapping_id=mapping_def.member_mapping_id
                    ).select_related('variable_id', 'member_id').order_by('member_mapping_row')

                    # Group by row to get complete source->target mappings
                    from itertools import groupby
                    for row_num, items_iter in groupby(mm_items, key=lambda x: x.member_mapping_row):
                        row_items = list(items_iter)
                        source_items_list = [item for item in row_items if item.is_source == "true"]
                        target_items_list = [item for item in row_items if item.is_source == "false"]

                        # Build source set: {(var_id, member_id), ...}
                        source_set = set()
                        for item in source_items_list:
                            if item.variable_id and item.member_id:
                                source_set.add((
                                    item.variable_id.variable_id,
                                    item.member_id.member_id
                                ))

                        # Build target list: [(variable_obj, member_obj), ...]
                        target_list = []
                        for item in target_items_list:
                            if item.variable_id:
                                target_list.append((item.variable_id, item.member_id))

                        if source_set and target_list:
                            member_mapping_rows.append({
                                'source_items': source_set,
                                'target_items': target_list,
                                'row_num': row_num
                            })
                            logger.debug(f"[REF_TABLE] Row {row_num}: {len(source_set)} source items -> {len(target_list)} target items")

                # Handle observation variable mappings (these don't have member mappings)
                if mapping_def.variable_mapping_id:
                    vm_items = VARIABLE_MAPPING_ITEM.objects.filter(
                        variable_mapping_id=mapping_def.variable_mapping_id
                    ).select_related('variable_id', 'variable_id__domain_id')

                    source_obs_vars = [item for item in vm_items if item.is_source == "true"]
                    target_obs_vars = [item for item in vm_items if item.is_source == "false"]

                    # For observation variables, pair by position (they're typically 1:1)
                    for idx, source_item in enumerate(source_obs_vars):
                        if source_item.variable_id and idx < len(target_obs_vars):
                            source_var_id = source_item.variable_id.variable_id
                            target_var = target_obs_vars[idx].variable_id
                            if target_var:
                                observation_variable_mappings[source_var_id] = target_var
                                logger.debug(f"[REF_TABLE] Observation mapping: {source_var_id} -> {target_var.variable_id}")

            logger.info(f"[REF_TABLE] Built {len(member_mapping_rows)} member mapping rows, {len(observation_variable_mappings)} observation mappings")
            if member_mapping_rows:
                for row in member_mapping_rows[:3]:  # Log first 3 rows as sample
                    source_vars = {s[0] for s in row['source_items']}
                    target_vars = [t[0].variable_id for t in row['target_items']]
                    logger.info(f"[REF_TABLE] Sample row {row['row_num']}: sources={source_vars}, targets={target_vars}")

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

        # Copy ORDINATE_ITEMs for selected ordinates, applying m:n mappings
        source_ordinate_items = ORDINATE_ITEM.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('variable_id', 'member_id', 'variable_id__domain_id')

        # Group source ordinate items by ordinate
        from collections import defaultdict
        ordinate_to_items = defaultdict(list)
        for item in source_ordinate_items:
            if item.axis_ordinate_id:
                ordinate_to_items[item.axis_ordinate_id.axis_ordinate_id].append(item)

        logger.info(f"[REF_TABLE] Source ordinates with items: {len(ordinate_to_items)}")
        logger.info(f"[REF_TABLE] Selected ordinates: {selected_ordinates}")
        logger.info(f"[REF_TABLE] old_to_new_ordinate keys: {list(old_to_new_ordinate.keys())}")

        # Track created ordinate items to ensure no exact duplicates
        created_ordinate_items = set()
        ordinate_items_created = 0
        duplicates_skipped = 0
        observation_items_created = 0
        dimension_items_created = 0
        no_matching_row_skipped = 0

        # Observation/metric domains (data type domains) - these are always included
        OBSERVATION_DOMAINS = {
            'EBA_Float', 'EBA_Integer', 'EBA_String', 'EBA_Date',
            'EBA_Boolean', 'EBA_FRQNCY', 'EBA_Decimal', 'EBA_Double'
        }

        # Process each source ordinate
        for source_ordinate_id, items in ordinate_to_items.items():
            if source_ordinate_id not in old_to_new_ordinate:
                logger.warning(f"[REF_TABLE] Source ordinate {source_ordinate_id} not in mapping - skipping")
                continue

            ref_ordinate = old_to_new_ordinate[source_ordinate_id]

            # Build source set for this ordinate: {(var_id, member_id), ...}
            source_set = set()
            observation_items = []  # Observation variables to handle separately

            for item in items:
                if item.variable_id:
                    var_id = item.variable_id.variable_id
                    mem_id = item.member_id.member_id if item.member_id else None

                    # Check if this is an observation variable
                    is_observation = False
                    if item.variable_id.domain_id:
                        domain_id = item.variable_id.domain_id.domain_id
                        is_observation = domain_id in OBSERVATION_DOMAINS

                    if is_observation:
                        observation_items.append(item)
                    elif mem_id:
                        source_set.add((var_id, mem_id))

            logger.debug(f"[REF_TABLE] Ordinate {source_ordinate_id}: {len(source_set)} dimension items, {len(observation_items)} observation items")

            # 1. Handle observation variables - map them directly
            for obs_item in observation_items:
                source_var_id = obs_item.variable_id.variable_id
                target_var = observation_variable_mappings.get(source_var_id, obs_item.variable_id)

                unique_key = (ref_ordinate.axis_ordinate_id, target_var.variable_id, None)
                if unique_key in created_ordinate_items:
                    duplicates_skipped += 1
                    continue
                created_ordinate_items.add(unique_key)

                ORDINATE_ITEM.objects.create(
                    axis_ordinate_id=ref_ordinate,
                    variable_id=target_var,
                    member_id=None,
                    member_hierarchy_id=obs_item.member_hierarchy_id,
                    member_hierarchy_valid_from=obs_item.member_hierarchy_valid_from,
                    starting_member_id=obs_item.starting_member_id,
                    is_starting_member_included=obs_item.is_starting_member_included
                )
                ordinate_items_created += 1
                observation_items_created += 1
                logger.info(f"[REF_TABLE] Created observation item: ord={ref_ordinate.axis_ordinate_id}, var={target_var.variable_id}")

            # 2. Handle dimension variables using m:n mapping
            # Find member_mapping_rows where source_items match (subset of) the ordinate's source set
            matched_rows = []
            for row in member_mapping_rows:
                row_source_items = row['source_items']
                # Check if row's source items are a subset of the ordinate's items
                if row_source_items and row_source_items.issubset(source_set):
                    matched_rows.append(row)
                    logger.debug(f"[REF_TABLE] Matched row {row['row_num']} for ordinate {source_ordinate_id}")

            if not matched_rows and source_set:
                # No matching rows found - try partial match (any source item matches)
                for row in member_mapping_rows:
                    row_source_items = row['source_items']
                    if row_source_items and row_source_items & source_set:  # Intersection not empty
                        matched_rows.append(row)
                        logger.debug(f"[REF_TABLE] Partial match row {row['row_num']} for ordinate {source_ordinate_id}")

            if matched_rows:
                # Create ordinate items for target side of matched rows
                for row in matched_rows:
                    for target_var, target_member in row['target_items']:
                        target_var_id = target_var.variable_id if target_var else None
                        target_mem_id = target_member.member_id if target_member else None

                        unique_key = (ref_ordinate.axis_ordinate_id, target_var_id, target_mem_id)
                        if unique_key in created_ordinate_items:
                            duplicates_skipped += 1
                            continue
                        created_ordinate_items.add(unique_key)

                        ORDINATE_ITEM.objects.create(
                            axis_ordinate_id=ref_ordinate,
                            variable_id=target_var,
                            member_id=target_member,
                            member_hierarchy_id=None,
                            member_hierarchy_valid_from=None,
                            starting_member_id=None,
                            is_starting_member_included=False
                        )
                        ordinate_items_created += 1
                        dimension_items_created += 1
                        logger.info(f"[REF_TABLE] Created dimension item: ord={ref_ordinate.axis_ordinate_id}, "
                                   f"var={target_var_id}, mem={target_mem_id}")
            elif source_set:
                no_matching_row_skipped += 1
                logger.warning(f"[REF_TABLE] No matching mapping row found for ordinate {source_ordinate_id} with source set: {source_set}")

        logger.info(f"[REF_TABLE] Created {ordinate_items_created} reference ORDINATE_ITEMs "
                   f"({observation_items_created} observation, {dimension_items_created} dimension, "
                   f"{duplicates_skipped} duplicates skipped, {no_matching_row_skipped} no matching row)")

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
