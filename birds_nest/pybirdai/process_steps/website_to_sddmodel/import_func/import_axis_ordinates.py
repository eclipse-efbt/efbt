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

"""Import axis ordinates from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import AXIS_ORDINATE
from pybirdai.context.csv_column_index_context import ColumnIndexes
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT
from .utilities import replace_dots
from .lookups import find_axis_with_id
from .config import get_csv_file_path


def import_axis_ordinates(context, config=None):
    """
    Import all axis ordinates from the rendering package CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        config: DatasetConfig object specifying file_directory subdirectory
    """
    file_location = get_csv_file_path(context, "axis_ordinate.csv", config)
    header_skipped = False
    ordinates_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                axis_ordinate_id = row[ColumnIndexes().axis_ordinate_axis_ordinate_id]
                axis_ordinate_is_abstract_header = row[ColumnIndexes().axis_ordinate_is_abstract_header]
                axis_ordinate_code = row[ColumnIndexes().axis_ordinate_code]
                axis_ordinate_order = row[ColumnIndexes().axis_ordinate_order]
                axis_ordinate_path = row[ColumnIndexes().axis_ordinate_path]
                axis_ordinate_axis_id = row[ColumnIndexes().axis_ordinate_axis_id]
                axis_ordinate_parent_axis_ordinate_id = row[ColumnIndexes().axis_ordinate_parent_axis_ordinate_id]
                axis_ordinate_name = row[ColumnIndexes().axis_ordinate_name]
                axis_ordinate_description = row[ColumnIndexes().axis_ordinate_description]

                axis_ordinate = AXIS_ORDINATE(
                    name=replace_dots(axis_ordinate_id))
                axis_ordinate.axis_ordinate_id = replace_dots(axis_ordinate_id)
                axis_ordinate.code = axis_ordinate_code
                axis_ordinate.path = axis_ordinate_path
                axis_ordinate.axis_id = find_axis_with_id(context, replace_dots(axis_ordinate_axis_id))
                axis_ordinate.name = axis_ordinate_name
                axis_ordinate.description = axis_ordinate_description

                ordinates_to_create.append(axis_ordinate)
                context.axis_ordinate_dictionary[axis_ordinate.axis_ordinate_id] = axis_ordinate

    if context.save_sdd_to_db and ordinates_to_create:
        AXIS_ORDINATE.objects.bulk_create(ordinates_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
