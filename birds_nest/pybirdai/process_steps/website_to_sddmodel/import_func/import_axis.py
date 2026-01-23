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

"""Import axes from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import AXIS
from pybirdai.context.csv_column_index_context import ColumnIndexes
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT
from .utilities import replace_dots
from .lookups import find_table_with_id


def import_axis(context, config=None):
    """
    Import all axes from the rendering package CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        config: DatasetConfig object specifying file_directory subdirectory (optional, defaults to "technical_export")
    """
    subdir = config.file_directory if config else "technical_export"
    file_location = context.file_directory + os.sep + subdir + os.sep + "axis.csv"
    header_skipped = False
    axes_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                axis_id = row[ColumnIndexes().axis_id]
                axis_orientation = row[ColumnIndexes().axis_orientation]
                axis_order = row[ColumnIndexes().axis_order]
                axis_name = row[ColumnIndexes().axis_name]
                axis_description = row[ColumnIndexes().axis_description]
                axis_table_id = row[ColumnIndexes().axis_table_id]
                axis_is_open_axis = row[ColumnIndexes().axis_is_open_axis]

                axis = AXIS(
                    name=axis_name if axis_name else replace_dots(axis_id))
                axis.axis_id = replace_dots(axis_id)
                axis.orientation = axis_orientation
                axis.description = axis_description
                axis.table_id = find_table_with_id(context, replace_dots(axis_table_id))

                axes_to_create.append(axis)
                context.axis_dictionary[axis.axis_id] = axis

    if context.save_sdd_to_db and axes_to_create:
        AXIS.objects.bulk_create(axes_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
