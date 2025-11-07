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

"""Import table cells from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import TABLE_CELL
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_table_with_id


def import_table_cells(context, dpm=False):
    """
    Import all table cells from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
        dpm: Boolean indicating if importing DPM data
    """
    file_location = context.file_directory + os.sep + "technical_export" + os.sep + "table_cell.csv"
    header_skipped = False
    table_cells_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        id_increment = 0
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                table_cell_cell_id = row[ColumnIndexes().table_cell_cell_id + id_increment]
                table_cell_combination_id = row[ColumnIndexes().table_cell_combination_id + id_increment]
                table_cell_table_id = row[ColumnIndexes().table_cell_table_id + id_increment]

                if table_cell_cell_id.endswith("_REF") or dpm:
                    table_cell = TABLE_CELL(
                        name=replace_dots(table_cell_cell_id))
                    table_cell.cell_id = replace_dots(table_cell_cell_id)
                    table_cell.table_id = find_table_with_id(
                        context, replace_dots(table_cell_table_id))
                    table_cell.table_cell_combination_id = table_cell_combination_id

                    table_cells_to_create.append(table_cell)
                    context.table_cell_dictionary[table_cell.cell_id] = table_cell

                    table_cell_list = context.table_to_table_cell_dictionary.setdefault(table_cell.table_id, [])
                    table_cell_list.append(table_cell)

    if context.save_sdd_to_db and table_cells_to_create:
        TABLE_CELL.objects.bulk_create(table_cells_to_create, batch_size=1000, ignore_conflicts=True)
