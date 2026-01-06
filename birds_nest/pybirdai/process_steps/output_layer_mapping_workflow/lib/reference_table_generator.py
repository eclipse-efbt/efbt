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

Also generates a reference table variant if the source is a specific Z-variant.
"""

import logging
from .naming_utils import NamingUtils
from .table_utils import get_base_table_id, get_z_variant_member_id, is_z_variant_table
from .table_cell_utils import get_all_z_axis_variants

logger = logging.getLogger(__name__)


def _replicate_reference_for_variant(
    base_ref_table,
    z_suffix,
    base_axes,
    base_ordinates,
    base_ordinate_items,
    base_cells,
    base_positions,
    maintenance_agency
):
    """
    Replicate reference table artifacts for a Z-variant.

    Creates copies of all reference artifacts with the Z-variant suffix appended to IDs.

    Args:
        base_ref_table: The base reference TABLE object
        z_suffix: The Z-variant suffix (e.g., '_EBA_qEC_EBA_qx2029')
        base_axes: QuerySet of AXIS objects from base reference table
        base_ordinates: QuerySet of AXIS_ORDINATE objects from base reference table
        base_ordinate_items: QuerySet of ORDINATE_ITEM objects from base reference table
        base_cells: QuerySet of TABLE_CELL objects from base reference table
        base_positions: QuerySet of CELL_POSITION objects from base reference table
        maintenance_agency: MAINTENANCE_AGENCY object

    Returns:
        dict with:
            - variant_table: The created variant TABLE object
            - variant_table_id: The variant table ID
    """
    from pybirdai.models.bird_meta_data_model import (
        TABLE, AXIS, AXIS_ORDINATE, ORDINATE_ITEM, TABLE_CELL, CELL_POSITION
    )

    # Create variant table ID and code
    variant_table_id = f"{base_ref_table.table_id}{z_suffix}"
    variant_table_code = f"{base_ref_table.code}{z_suffix}"

    # Check if variant already exists and clean up
    existing_variant = TABLE.objects.filter(table_id=variant_table_id).first()
    if existing_variant:
        logger.info(f"[REF_TABLE_VARIANT] Cleaning up existing variant: {variant_table_id}")
        var_axes = AXIS.objects.filter(table_id=existing_variant)
        var_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=var_axes)
        CELL_POSITION.objects.filter(axis_ordinate_id__in=var_ordinates).delete()
        TABLE_CELL.objects.filter(table_id=existing_variant).delete()
        ORDINATE_ITEM.objects.filter(axis_ordinate_id__in=var_ordinates).delete()
        var_ordinates.delete()
        var_axes.delete()
        existing_variant.delete()

    # Create variant TABLE
    variant_table = TABLE.objects.create(
        table_id=variant_table_id,
        name=f"{base_ref_table.name}{z_suffix}",
        code=variant_table_code,
        description=f"Z-variant of {base_ref_table.table_id}",
        maintenance_agency_id=maintenance_agency,
        version=base_ref_table.version
    )
    logger.info(f"[REF_TABLE_VARIANT] Created variant TABLE: {variant_table_id}")

    # Create variant AXES - map base axis_id to variant axis
    base_to_variant_axis = {}
    for base_axis in base_axes:
        variant_axis_id = f"{base_axis.axis_id}{z_suffix}"
        variant_axis = AXIS.objects.create(
            axis_id=variant_axis_id,
            code=f"{base_axis.code}{z_suffix}" if base_axis.code else None,
            orientation=base_axis.orientation,
            order=base_axis.order,
            name=base_axis.name,
            description=base_axis.description,
            table_id=variant_table,
            is_open_axis=base_axis.is_open_axis
        )
        base_to_variant_axis[base_axis.axis_id] = variant_axis

    logger.info(f"[REF_TABLE_VARIANT] Created {len(base_to_variant_axis)} variant AXES")

    # Create variant AXIS_ORDINATEs - map base ordinate_id to variant ordinate
    base_to_variant_ordinate = {}
    for base_ord in base_ordinates:
        if base_ord.axis_id and base_ord.axis_id.axis_id in base_to_variant_axis:
            variant_axis = base_to_variant_axis[base_ord.axis_id.axis_id]
            variant_ord_id = f"{base_ord.axis_ordinate_id}{z_suffix}"
            variant_ordinate = AXIS_ORDINATE.objects.create(
                axis_ordinate_id=variant_ord_id,
                is_abstract_header=base_ord.is_abstract_header,
                code=base_ord.code,
                order=base_ord.order,
                level=base_ord.level,
                path=None,  # Will update after all ordinates created
                axis_id=variant_axis,
                parent_axis_ordinate_id=None,  # Will update after all ordinates created
                name=base_ord.name,
                description=base_ord.description
            )
            base_to_variant_ordinate[base_ord.axis_ordinate_id] = variant_ordinate

    # Update parent and path for variant ordinates
    for base_ord_id, variant_ord in base_to_variant_ordinate.items():
        # Find the base ordinate to get its path/parent
        base_ord = next((o for o in base_ordinates if o.axis_ordinate_id == base_ord_id), None)
        if base_ord and base_ord.path:
            # Update path with variant ordinate IDs
            new_path_parts = []
            for old_id in base_ord.path.split('.'):
                if old_id in base_to_variant_ordinate:
                    new_path_parts.append(base_to_variant_ordinate[old_id].axis_ordinate_id)
                elif old_id:
                    new_path_parts.append(f"{old_id}{z_suffix}")
            variant_ord.path = '.'.join(new_path_parts) + '.'

            # Set parent from path
            if len(new_path_parts) >= 2:
                parent_id = new_path_parts[-2]
                for var_ord in base_to_variant_ordinate.values():
                    if var_ord.axis_ordinate_id == parent_id:
                        variant_ord.parent_axis_ordinate_id = var_ord
                        break

            variant_ord.save()

    logger.info(f"[REF_TABLE_VARIANT] Created {len(base_to_variant_ordinate)} variant AXIS_ORDINATEs")

    # Create variant ORDINATE_ITEMs
    ordinate_items_created = 0
    for base_item in base_ordinate_items:
        if base_item.axis_ordinate_id and base_item.axis_ordinate_id.axis_ordinate_id in base_to_variant_ordinate:
            variant_ordinate = base_to_variant_ordinate[base_item.axis_ordinate_id.axis_ordinate_id]
            ORDINATE_ITEM.objects.create(
                axis_ordinate_id=variant_ordinate,
                variable_id=base_item.variable_id,
                member_id=base_item.member_id,
                member_hierarchy_id=base_item.member_hierarchy_id,
                member_hierarchy_valid_from=base_item.member_hierarchy_valid_from,
                starting_member_id=base_item.starting_member_id,
                is_starting_member_included=base_item.is_starting_member_included
            )
            ordinate_items_created += 1

    logger.info(f"[REF_TABLE_VARIANT] Created {ordinate_items_created} variant ORDINATE_ITEMs")

    # Create variant TABLE_CELLs - map base cell_id to variant cell
    # Note: If base_cell.cell_id already contains '__', it already has Z-member info,
    # so don't add z_suffix again to avoid duplication like 4152944_REF__EBA_x__EBA_x
    base_to_variant_cell = {}
    for base_cell in base_cells:
        if '__' in base_cell.cell_id:
            # Cell already has Z-member info, reuse existing cell
            variant_cell = base_cell
        else:
            # Cell doesn't have Z-member, create new variant cell
            variant_cell_id = f"{base_cell.cell_id}{z_suffix}"
            variant_cell = TABLE_CELL.objects.create(
                cell_id=variant_cell_id,
                is_shaded=base_cell.is_shaded,
                table_cell_combination_id=base_cell.table_cell_combination_id,  # Base combination ID, not variant
                table_id=variant_table,
                system_data_code=base_cell.system_data_code,
                name=base_cell.name
            )
        base_to_variant_cell[base_cell.cell_id] = variant_cell

    logger.info(f"[REF_TABLE_VARIANT] Created {len(base_to_variant_cell)} variant TABLE_CELLs")

    # Create variant CELL_POSITIONs
    positions_created = 0
    for base_pos in base_positions:
        if (base_pos.cell_id and base_pos.cell_id.cell_id in base_to_variant_cell and
                base_pos.axis_ordinate_id and base_pos.axis_ordinate_id.axis_ordinate_id in base_to_variant_ordinate):
            variant_cell = base_to_variant_cell[base_pos.cell_id.cell_id]
            variant_ordinate = base_to_variant_ordinate[base_pos.axis_ordinate_id.axis_ordinate_id]
            CELL_POSITION.objects.create(
                cell_id=variant_cell,
                axis_ordinate_id=variant_ordinate
            )
            positions_created += 1

    logger.info(f"[REF_TABLE_VARIANT] Created {positions_created} variant CELL_POSITIONs")
    logger.info(f"[REF_TABLE_VARIANT] Successfully created variant: {variant_table_id}")

    return {
        'variant_table': variant_table,
        'variant_table_id': variant_table_id
    }


def generate_reference_table_artifacts(source_table_id, selected_ordinates, framework, version, maintenance_agency, mapping_definitions=None, cube=None):
    """
    Generate reference table artifacts from selected ordinates.
    Creates a new reference TABLE with only the selected ordinates,
    along with AXIS, AXIS_ORDINATE, ORDINATE_ITEM, TABLE_CELL, and CELL_POSITION records.

    If source_table_id is a Z-variant, also generates a single reference variant for
    that specific variant only. If source_table_id is a base table (no Z-suffix),
    only the base reference table is created (no variants).

    Uses mapping definitions to show MAPPED variables/members instead of source ones.
    If cube is provided, creates COMBINATION records for reference cells using CombinationCreator.

    Args:
        source_table_id: str - Original DPM table ID
        selected_ordinates: list - Selected ordinate IDs from Step 3
        framework: str - Framework code (e.g., 'EBA_COREP')
        version: str - Version string (e.g., '4.0')
        maintenance_agency: MAINTENANCE_AGENCY object
        mapping_definitions: list - List of mapping definition dicts from Phase 3
            Each dict contains: {'mapping_definition': MAPPING_DEFINITION, 'name': str, ...}
        cube: CUBE object - Reference cube for creating combinations (optional)

    Returns:
        dict with keys:
            - success: bool
            - reference_table_id: str - ID of the base reference table
            - reference_table: TABLE object - The base reference table
            - variant_table_ids: list - IDs of created Z-variant reference tables
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
                                # Pair source with target at same position, or first target if no match
                                target_item = target_items[idx] if idx < len(target_items) else target_items[0]
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
                    for source_item in source_vars:
                        source_var_id = source_item.variable_id.variable_id if source_item.variable_id else None
                        if source_var_id and target_vars and source_var_id not in variable_mapping_lookup:
                            # Use first target variable for unmapped sources
                            variable_mapping_lookup[source_var_id] = target_vars[0].variable_id
                            logger.debug(f"[REF_TABLE] Variable mapping (from variable_mapping): {source_var_id} -> {target_vars[0].variable_id.variable_id}")

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

                        # Map each source (var, member) to corresponding target member
                        for source_item in source_items:
                            source_var_id = source_item.variable_id.variable_id if source_item.variable_id else None
                            source_mem_id = source_item.member_id.member_id if source_item.member_id else None
                            if source_var_id and source_mem_id and target_items:
                                # Find target with matching variable (or use first target)
                                target_item = target_items[0]  # Default to first target
                                for t in target_items:
                                    if t.variable_id and source_var_id in variable_mapping_lookup:
                                        target_var = variable_mapping_lookup[source_var_id]
                                        if t.variable_id.variable_id == target_var.variable_id:
                                            target_item = t
                                            break

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

        # Generate reference table ID: {FRAMEWORK_SHORT}_REF_{table_code_part}
        # Extract just the code part by stripping framework prefix from clean_table_id
        # e.g., "EBA_COREP_C_07_00_a_4_0" -> "C_07_00_a_4_0" -> "COREP_REF_C_07_00_a_4_0"
        table_code_part = clean_table_id
        framework_prefix = f"EBA_{framework_short}_"
        if table_code_part.startswith(framework_prefix):
            table_code_part = table_code_part[len(framework_prefix):]
        elif table_code_part.startswith(f"{framework_short}_"):
            table_code_part = table_code_part[len(f"{framework_short}_"):]
        elif table_code_part.startswith("EBA_"):
            table_code_part = table_code_part[4:]  # Remove just "EBA_"

        reference_table_id = f"{framework_short}_REF_{table_code_part}"

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
        ).select_related('variable_id', 'member_id')

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
                   f"{ordinate_mismatch_skipped} ordinate mismatch skipped)")

        # Find cells that have positions matching selected ordinates on ALL axes (AND logic)
        # A cell must have at least one position on each axis's selected ordinates
        from collections import defaultdict

        # Group selected_ordinates by axis
        ordinates_by_axis = defaultdict(set)
        for ord_id in selected_ordinates:
            try:
                ordinate = AXIS_ORDINATE.objects.get(axis_ordinate_id=ord_id)
                ordinates_by_axis[ordinate.axis_id_id].add(ord_id)
            except AXIS_ORDINATE.DoesNotExist:
                logger.warning(f"[REF_TABLE] Selected ordinate not found: {ord_id}")

        logger.info(f"[REF_TABLE] Selected ordinates grouped by {len(ordinates_by_axis)} axes")

        # Get cell positions for selected ordinates
        source_cell_positions = CELL_POSITION.objects.filter(
            axis_ordinate_id__in=selected_ordinates
        ).select_related('cell_id', 'axis_ordinate_id')

        # Group positions by cell and by axis
        cell_axis_positions = defaultdict(lambda: defaultdict(list))
        cell_positions_map = defaultdict(list)
        for pos in source_cell_positions:
            if pos.cell_id and pos.axis_ordinate_id:
                cell_id = pos.cell_id.cell_id
                axis_id = pos.axis_ordinate_id.axis_id_id
                cell_axis_positions[cell_id][axis_id].append(pos)
                cell_positions_map[cell_id].append(pos)

        # Filter to cells that have positions on ALL axes (AND logic)
        required_axes = set(ordinates_by_axis.keys())
        valid_cell_ids = []
        for cell_id, axis_positions in cell_axis_positions.items():
            if set(axis_positions.keys()) >= required_axes:
                valid_cell_ids.append(cell_id)

        logger.info(f"[REF_TABLE] Filtered to {len(valid_cell_ids)} cells matching ALL {len(required_axes)} axes")

        # Get source cells
        source_cells = TABLE_CELL.objects.filter(cell_id__in=valid_cell_ids)

        cells_created = 0
        positions_created = 0
        created_ref_cells = []  # Track created cells for CombinationCreator
        source_to_ref_cell = {}  # Map source_cell -> ref_cell for metric lookup
        for source_cell in source_cells:
            positions = cell_positions_map.get(source_cell.cell_id, [])

            # Note: We removed the check that required ALL positions to be in selected ordinates.
            # For Z-variant tables, cells are shared across multiple variant tables, so counting
            # all_positions would include positions from other variants, causing cells to be
            # incorrectly skipped. Now we create a reference cell for any cell that has at least
            # one position in the selected ordinates.
            if not positions:
                continue

            # Create reference cell with clean ID (Z-agnostic for base reference table)
            # Z-suffix will be added in variant replication if needed
            # source: EBA_4152944 or EBA_4152944__EBA_qEC_EBA_qx16 -> ref: 4152944_REF
            if '__' in source_cell.cell_id:
                # Z-variant cell: use only the base part for base reference table
                parts = source_cell.cell_id.split('__', 1)
                base_cell = parts[0].replace("EBA_", "", 1)
                ref_cell_id = f"{base_cell}_REF"  # No Z-suffix for base reference
            else:
                # Base cell: just add _REF suffix
                cell_number = source_cell.cell_id.replace("EBA_", "", 1)
                ref_cell_id = f"{cell_number}_REF"
            ref_cell = TABLE_CELL.objects.create(
                cell_id=ref_cell_id,
                is_shaded=source_cell.is_shaded,
                table_cell_combination_id=None,  # Will be set by CombinationCreator
                table_id=reference_table,
                system_data_code=source_cell.system_data_code,
                name=source_cell.name
            )
            created_ref_cells.append(ref_cell)
            source_to_ref_cell[ref_cell.cell_id] = source_cell  # Track for metric lookup
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

        # ========== CREATE COMBINATIONS FOR REFERENCE CELLS ==========
        # Use CombinationCreator to generate proper combinations from ordinate items
        if cube and created_ref_cells:
            from .combination_creator import CombinationCreator
            import datetime

            # Extract table_code from reference_table_id (e.g., "COREP_REF_C_07_00_a_4_0" -> "C_07_00_a")
            table_code = NamingUtils.extract_base_table_code(source_table_id, source_table_code)
            version_normalized = version.replace('.', '_')

            combination_creator = CombinationCreator(
                table_code=table_code,
                table_version=version_normalized
            )

            timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            combinations_created = 0
            for ref_cell in created_ref_cells:
                # Get source cell for metric lookup (EBA_FIELD variables are in source, not ref)
                source_cell_for_metric = source_to_ref_cell.get(ref_cell.cell_id)
                combination = combination_creator.create_combination_for_cell(
                    cell=ref_cell,
                    cube=cube,
                    timestamp=timestamp,
                    source_cell=source_cell_for_metric
                )
                if combination:
                    combinations_created += 1

            logger.info(f"[REF_TABLE] Created {combinations_created} combinations for reference cells")

        logger.info(f"[REF_TABLE] Successfully completed base reference table: {reference_table_id}")

        # ========== GENERATE REFERENCE TABLE VARIANTS ==========
        # Only create a reference variant if the source_table_id is itself a Z-variant
        # This ensures we only create the SELECTED variant, not all variants
        variant_table_ids = []

        # Check if source_table_id is a Z-variant (user selected a specific variant)
        if is_z_variant_table(source_table_id):
            # Extract the Z-suffix from the selected source table
            z_suffix = get_z_variant_member_id(source_table_id)
            if z_suffix:
                logger.info(f"[REF_TABLE] Source is a Z-variant, creating single reference variant for: {z_suffix}")

                # Get the base reference table's artifacts for replication
                ref_axes = list(AXIS.objects.filter(table_id=reference_table))
                ref_ordinates = list(AXIS_ORDINATE.objects.filter(axis_id__in=ref_axes))
                ref_ordinate_ids = [o.axis_ordinate_id for o in ref_ordinates]
                ref_ordinate_items = list(ORDINATE_ITEM.objects.filter(
                    axis_ordinate_id__in=ref_ordinate_ids
                ))
                ref_cells = list(TABLE_CELL.objects.filter(table_id=reference_table))
                ref_positions = list(CELL_POSITION.objects.filter(
                    cell_id__in=[c.cell_id for c in ref_cells],
                    axis_ordinate_id__in=ref_ordinate_ids  # Filter by variant's ordinates
                ).select_related('cell_id', 'axis_ordinate_id'))

                # Add double underscore prefix ('__') for clear separation between base and variant
                z_suffix_with_prefix = f"__{z_suffix}" if not z_suffix.startswith('__') else z_suffix

                try:
                    variant_result = _replicate_reference_for_variant(
                        base_ref_table=reference_table,
                        z_suffix=z_suffix_with_prefix,
                        base_axes=ref_axes,
                        base_ordinates=ref_ordinates,
                        base_ordinate_items=ref_ordinate_items,
                        base_cells=ref_cells,
                        base_positions=ref_positions,
                        maintenance_agency=maintenance_agency
                    )
                    variant_table_ids.append(variant_result['variant_table_id'])
                except Exception as var_error:
                    logger.error(f"[REF_TABLE] Error creating variant for {z_suffix}: {str(var_error)}")
        else:
            logger.info(f"[REF_TABLE] Source is not a Z-variant, no variants created (base reference only)")

        logger.info(f"[REF_TABLE] Created {len(variant_table_ids)} reference table variants")

        # ========== DELETE BASE TABLE FOR Z-VARIANTS ==========
        # If source was a Z-variant, we only want the variant table, not the base.
        # Delete the base reference table now that variant has been created.
        # The variant has its own cells/positions, and combinations are Z-agnostic (linked to cube).
        if is_z_variant_table(source_table_id) and variant_table_ids:
            logger.info(f"[REF_TABLE] Source is Z-variant - deleting base reference table: {reference_table_id}")
            # Delete artifacts in reverse order to avoid FK violations
            ref_axes = AXIS.objects.filter(table_id=reference_table)
            ref_ordinates = AXIS_ORDINATE.objects.filter(axis_id__in=ref_axes)
            CELL_POSITION.objects.filter(axis_ordinate_id__in=ref_ordinates).delete()
            TABLE_CELL.objects.filter(table_id=reference_table).delete()
            ORDINATE_ITEM.objects.filter(axis_ordinate_id__in=ref_ordinates).delete()
            ref_ordinates.delete()
            ref_axes.delete()
            reference_table.delete()
            logger.info(f"[REF_TABLE] Deleted base reference table: {reference_table_id}")
            # Return the variant as the reference table
            variant_table = TABLE.objects.filter(table_id=variant_table_ids[0]).first()
            logger.info(f"[REF_TABLE] Generation complete: variant_only={variant_table_ids[0]}")
            return {
                'success': True,
                'reference_table_id': variant_table_ids[0],
                'reference_table': variant_table,
                'variant_table_ids': []  # No additional variants since we returned the variant as main
            }

        logger.info(f"[REF_TABLE] Generation complete: base={reference_table_id}, variants={variant_table_ids}")

        return {
            'success': True,
            'reference_table_id': reference_table_id,
            'reference_table': reference_table,
            'variant_table_ids': variant_table_ids
        }

    except Exception as e:
        logger.error(f"[REF_TABLE] Error generating reference table: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }
