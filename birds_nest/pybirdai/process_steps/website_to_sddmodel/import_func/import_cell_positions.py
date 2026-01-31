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

"""Import cell positions from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import CELL_POSITION
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_axis_ordinate_with_id, find_table_cell_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_cell_positions(context, dpm=False, config=None):
    """
    Import all cell positions from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
        dpm: Boolean indicating if importing DPM data
        config: DatasetConfig object specifying file_directory subdirectory (optional, defaults to "technical_export")
    """
    subdir = config.file_directory if config else "smcubes_artefacts"
    file_location = context.file_directory + os.sep + subdir + os.sep + "cell_position.csv"
    header_skipped = False
    cell_positions_to_create = []
    id_increment = 0
    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                cell_positions_cell_id = row[ColumnIndexes().cell_positions_cell_id + id_increment]
                cell_positions_axis_ordinate_id = row[ColumnIndexes().cell_positions_axis_ordinate_id + id_increment]

                if cell_positions_cell_id.endswith("_REF") or dpm:
                    cell_position = CELL_POSITION()
                    cell_position.axis_ordinate_id = find_axis_ordinate_with_id(
                        context, replace_dots(cell_positions_axis_ordinate_id))
                    cell_position.cell_id = find_table_cell_with_id(
                        context, replace_dots(cell_positions_cell_id))

                    cell_positions_to_create.append(cell_position)

                    cell_positions_list = context.cell_positions_dictionary.setdefault(cell_position.cell_id.cell_id, [])
                    cell_positions_list.append(cell_position)

    if context.save_sdd_to_db and cell_positions_to_create:
        CELL_POSITION.objects.bulk_create(cell_positions_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
