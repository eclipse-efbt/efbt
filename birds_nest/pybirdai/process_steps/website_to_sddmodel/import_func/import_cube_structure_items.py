# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - refactoring into modular structure

"""Import cube structure items from ANCRDT CSV files."""

import os
import csv
import logging
from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, SUBDOMAIN
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .utils import find_variable_with_id, find_member_with_id

logger = logging.getLogger(__name__)


def import_cube_structure_items(base_path, context):
    '''
    Import all cube structure items from CSV file using bulk create
    '''
    file_location = base_path + os.sep + "cube_structure_item.csv"
    header_skipped = False
    items_to_create = []
    csi_creation_failed = set()
    missing_cube_structures = set()
    missing_members = set()
    missing_subdomains = set()
    total_rows = 0

    csi_counter = dict()

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                total_rows += 1
                structure_id = row[ColumnIndexes().sdd_cube_structure_item_cube_structure_id]
                dimension_id = row[ColumnIndexes().sdd_cube_structure_item_variable_id]
                member_id = row[ColumnIndexes().sdd_cube_structure_item_member_id]
                role = row[ColumnIndexes().sdd_cube_structure_item_role]
                order = row[ColumnIndexes().sdd_cube_structure_item_order]
                subdomain_id = row[ColumnIndexes().sdd_cube_structure_item_subdomain_id]
                cube_variable_code = row[ColumnIndexes().sdd_cube_structure_item_cube_variable_code]

                item = CUBE_STRUCTURE_ITEM()

                # Try to find cube_structure
                try:
                    item.cube_structure_id = CUBE_STRUCTURE.objects.get(cube_structure_id=structure_id)
                except CUBE_STRUCTURE.DoesNotExist:
                    missing_cube_structures.add(structure_id)
                    logger.error(f"CUBE_STRUCTURE not found: {structure_id}")
                    continue

                # Find variable (may return None)
                item.variable_id = find_variable_with_id(context, dimension_id)
                if not item.variable_id:
                    csi_creation_failed.add(dimension_id)
                    logger.warning(f"Variable not found: {dimension_id}")
                    continue

                # Find member (may return None)
                item.member_id = find_member_with_id(member_id, context)
                if not item.member_id and member_id:
                    missing_members.add(member_id)
                    logger.warning(f"Member not found: {member_id}")
                    continue

                item.role = role
                item.order = order
                if (item.cube_structure_id, item.variable_id) not in csi_counter:
                    csi_counter[(item.cube_structure_id, item.variable_id)] = 0
                item.cube_variable_code = cube_variable_code or "__".join([
                    item.cube_structure_id.cube_structure_id,
                    item.variable_id.variable_id,
                    str(csi_counter[(item.cube_structure_id, item.variable_id)])
                ])
                csi_counter[(item.cube_structure_id, item.variable_id)] += 1

                # Try to find subdomain if specified
                if subdomain_id:
                    try:
                        item.subdomain_id = SUBDOMAIN.objects.get(subdomain_id=subdomain_id)
                    except SUBDOMAIN.DoesNotExist:
                        missing_subdomains.add(subdomain_id)
                        logger.warning(f"SUBDOMAIN not found: {subdomain_id}")
                        continue

                items_to_create.append(item)

    if context.save_sdd_to_db and items_to_create:
        CUBE_STRUCTURE_ITEM.objects.bulk_create(items_to_create, batch_size=1000, ignore_conflicts=True)
        logger.info(f"CUBE_STRUCTURE_ITEM import: Created {len(items_to_create)} of {total_rows} items from CSV")
    else:
        logger.warning(f"CUBE_STRUCTURE_ITEM import: No items created. Total rows: {total_rows}")

    # Report all failures
    if missing_cube_structures:
        logger.error(f"Missing CUBE_STRUCTURES ({len(missing_cube_structures)}): {missing_cube_structures}")
    if csi_creation_failed:
        logger.error(f"Missing VARIABLES ({len(csi_creation_failed)}): {csi_creation_failed}")
    if missing_members:
        logger.error(f"Missing MEMBERS ({len(missing_members)}): {missing_members}")
    if missing_subdomains:
        logger.error(f"Missing SUBDOMAINS ({len(missing_subdomains)}): {missing_subdomains}")
