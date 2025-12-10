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
#

"""Import ordinate items from CSV file."""

import os
import csv
import logging
from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM
from pybirdai.context.csv_column_index_context import ColumnIndexes
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_ORDINATE_ITEMS
from .utilities import replace_dots
from .lookups import (
    find_axis_ordinate_with_id,
    find_variable_with_id,
    find_member_with_id,
    find_member_hierarchy_with_id
)

logger = logging.getLogger(__name__)


def import_ordinate_items(context, config=None):
    """
    Import all ordinate items from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
        config: DatasetConfig object specifying file_directory subdirectory (optional, defaults to "technical_export")
    """
    subdir = config.file_directory if config else "technical_export"
    file_location = context.file_directory + os.sep + subdir + os.sep + "ordinate_item.csv"
    header_skipped = False
    ordinate_items_to_create = []

    # Statistics tracking
    total_rows = 0
    skipped_rows = 0
    null_member_count = 0
    null_hierarchy_count = 0
    null_starting_member_count = 0

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        id_increment = 0
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                total_rows += 1
                axis_ordinate_id = row[ColumnIndexes().ordinate_item_axis_ordinate_id + id_increment]
                variable_id = row[ColumnIndexes().ordinate_item_variable_id + id_increment]
                member_id = row[ColumnIndexes().ordinate_item_member_id + id_increment]
                member_hierarchy_id = row[ColumnIndexes().ordinate_item_member_hierarchy_id + id_increment]
                starting_member_id = row[ColumnIndexes().ordinate_item_starting_member_id + id_increment]
                is_starting_member_included = row[ColumnIndexes().ordinate_item_is_starting_member_included + id_increment]

                # Lookup mandatory FKs (axis_ordinate_id, variable_id)
                axis_ordinate_obj = find_axis_ordinate_with_id(
                    context, replace_dots(axis_ordinate_id)) if axis_ordinate_id else None
                variable_obj = find_variable_with_id(
                    context, replace_dots(variable_id)) if variable_id else None

                # Skip record if mandatory FKs are missing
                if not axis_ordinate_obj or not variable_obj:
                    skipped_rows += 1
                    logger.warning(
                        f"Skipping ordinate_item row {total_rows}: "
                        f"axis_ordinate='{axis_ordinate_id}' {'found' if axis_ordinate_obj else 'NOT FOUND'}, "
                        f"variable='{variable_id}' {'found' if variable_obj else 'NOT FOUND'}"
                    )
                    continue

                # Lookup optional FKs (member_id, member_hierarchy_id, starting_member_id)
                # These can be None - will be stored as NULL in database
                member_obj = find_member_with_id(
                    replace_dots(member_id), context) if member_id else None
                hierarchy_obj = find_member_hierarchy_with_id(
                    replace_dots(member_hierarchy_id), context) if member_hierarchy_id else None
                starting_member_obj = find_member_with_id(
                    replace_dots(starting_member_id), context) if starting_member_id else None

                # Track NULL optional FKs for statistics
                if member_obj is None:
                    null_member_count += 1
                if hierarchy_obj is None:
                    null_hierarchy_count += 1
                if starting_member_obj is None:
                    null_starting_member_count += 1

                ordinate_item = ORDINATE_ITEM()
                ordinate_item.axis_ordinate_id = axis_ordinate_obj
                ordinate_item.variable_id = variable_obj
                ordinate_item.member_id = member_obj  # Can be None
                ordinate_item.member_hierarchy_id = hierarchy_obj  # Can be None
                ordinate_item.starting_member_id = starting_member_obj  # Can be None
                ordinate_item.is_starting_member_included = is_starting_member_included

                ordinate_items_to_create.append(ordinate_item)

                try:
                    ordinate_items = context.axis_ordinate_to_ordinate_items_map[ordinate_item.axis_ordinate_id.axis_ordinate_id]
                    ordinate_items.append(ordinate_item)
                except KeyError:
                    context.axis_ordinate_to_ordinate_items_map[ordinate_item.axis_ordinate_id.axis_ordinate_id] = [ordinate_item]

    # Log import statistics
    logger.info(f"[ORDINATE_ITEM IMPORT] Total CSV rows: {total_rows}")
    logger.info(f"[ORDINATE_ITEM IMPORT] Records to create: {len(ordinate_items_to_create)}")
    logger.info(f"[ORDINATE_ITEM IMPORT] Skipped (mandatory FK missing): {skipped_rows}")
    logger.info(f"[ORDINATE_ITEM IMPORT] NULL member_id: {null_member_count}")
    logger.info(f"[ORDINATE_ITEM IMPORT] NULL member_hierarchy_id: {null_hierarchy_count}")
    logger.info(f"[ORDINATE_ITEM IMPORT] NULL starting_member_id: {null_starting_member_count}")

    if context.save_sdd_to_db:
        if ordinate_items_to_create:
            ORDINATE_ITEM.objects.bulk_create(ordinate_items_to_create, batch_size=BULK_CREATE_BATCH_SIZE_ORDINATE_ITEMS, ignore_conflicts=True)
            logger.info(f"[ORDINATE_ITEM IMPORT] Successfully bulk created {len(ordinate_items_to_create)} records")

        # ALWAYS run FK cleanup after import (even if no new items created this run)
        # Fix: Convert empty/whitespace-only string FKs and 'None' string to NULL
        # This prevents FK violations where invalid strings are interpreted as FK references
        from django.db import connection
        with connection.cursor() as cursor:
            # Clean empty strings, whitespace, and 'None' string to NULL
            cursor.execute("UPDATE pybirdai_ordinate_item SET member_id_id = NULL WHERE member_id_id = '' OR TRIM(member_id_id) = '' OR member_id_id = 'None'")
            member_fixes = cursor.rowcount
            cursor.execute("UPDATE pybirdai_ordinate_item SET member_hierarchy_id_id = NULL WHERE member_hierarchy_id_id = '' OR TRIM(member_hierarchy_id_id) = '' OR member_hierarchy_id_id = 'None'")
            hierarchy_fixes = cursor.rowcount
            cursor.execute("UPDATE pybirdai_ordinate_item SET starting_member_id_id = NULL WHERE starting_member_id_id = '' OR TRIM(starting_member_id_id) = '' OR starting_member_id_id = 'None'")
            starting_fixes = cursor.rowcount

            if member_fixes + hierarchy_fixes + starting_fixes > 0:
                logger.info(f"[ORDINATE_ITEM IMPORT] Fixed invalid string FKs: "
                           f"member_id={member_fixes}, member_hierarchy_id={hierarchy_fixes}, "
                           f"starting_member_id={starting_fixes}")
            else:
                logger.info(f"[ORDINATE_ITEM IMPORT] FK cleanup completed - no invalid strings found")
