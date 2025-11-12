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

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        id_increment = 0
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                axis_ordinate_id = row[ColumnIndexes().ordinate_item_axis_ordinate_id + id_increment]
                variable_id = row[ColumnIndexes().ordinate_item_variable_id + id_increment]
                member_id = row[ColumnIndexes().ordinate_item_member_id + id_increment]
                member_hierarchy_id = row[ColumnIndexes().ordinate_item_member_hierarchy_id + id_increment]
                starting_member_id = row[ColumnIndexes().ordinate_item_starting_member_id + id_increment]
                is_starting_member_included = row[ColumnIndexes().ordinate_item_is_starting_member_included + id_increment]

                print(f"member_id: {member_id}")
                print(f"variable_id: {variable_id}")
                print(f"axis_ordinate_id: {axis_ordinate_id}")
                print(f"member_hierarchy_id: {member_hierarchy_id}")
                print(f"starting_member_id: {starting_member_id}")
                print(f"is_starting_member_included: {is_starting_member_included}")

                ordinate_item = ORDINATE_ITEM()
                ordinate_item.axis_ordinate_id = find_axis_ordinate_with_id(
                    context, replace_dots(axis_ordinate_id))

                print(ordinate_item.axis_ordinate_id)
                ordinate_item.variable_id = find_variable_with_id(
                    context, replace_dots(variable_id))
                ordinate_item.member_id = find_member_with_id(
                    replace_dots(member_id), context)
                ordinate_item.member_hierarchy_id = find_member_hierarchy_with_id(
                    replace_dots(member_hierarchy_id), context)
                ordinate_item.starting_member_id = find_member_with_id(
                    replace_dots(starting_member_id), context)
                ordinate_item.is_starting_member_included = is_starting_member_included

                ordinate_items_to_create.append(ordinate_item)

                try:
                    ordinate_items = context.axis_ordinate_to_ordinate_items_map[ordinate_item.axis_ordinate_id.axis_ordinate_id]
                    ordinate_items.append(ordinate_item)
                except KeyError:
                    context.axis_ordinate_to_ordinate_items_map[ordinate_item.axis_ordinate_id.axis_ordinate_id] = [ordinate_item]

    if context.save_sdd_to_db and ordinate_items_to_create:
        ORDINATE_ITEM.objects.bulk_create(ordinate_items_to_create, batch_size=BULK_CREATE_BATCH_SIZE_ORDINATE_ITEMS, ignore_conflicts=True)
