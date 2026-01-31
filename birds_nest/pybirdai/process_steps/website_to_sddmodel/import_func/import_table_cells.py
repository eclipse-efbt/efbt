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
import logging
from pybirdai.models.bird_meta_data_model import TABLE_CELL
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_table_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT

logger = logging.getLogger(__name__)


def import_table_cells(context, dpm=False, config=None):
    """
    Import all table cells from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
        dpm: Boolean indicating if importing DPM data
        config: DatasetConfig object specifying file_directory subdirectory (optional, defaults to "technical_export")
    """
    subdir = config.file_directory if config else "smcubes_artefacts"
    file_location = context.file_directory + os.sep + subdir + os.sep + "table_cell.csv"
    header_skipped = False
    table_cells_to_create = []

    # Statistics tracking
    total_rows = 0
    processed_rows = 0
    skipped_rows = 0
    null_table_count = 0

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
                table_cell_cell_id = row[ColumnIndexes().table_cell_cell_id + id_increment]
                table_cell_combination_id = row[ColumnIndexes().table_cell_combination_id + id_increment]
                table_cell_table_id = row[ColumnIndexes().table_cell_table_id + id_increment]

                if table_cell_cell_id.endswith("_REF") or dpm:
                    processed_rows += 1

                    # Lookup table FK (can be None - table_id is nullable)
                    table_obj = find_table_with_id(
                        context, replace_dots(table_cell_table_id)) if table_cell_table_id else None

                    # Track NULL table_id for statistics
                    if table_obj is None:
                        null_table_count += 1
                        # Note: We continue processing even if table_id is None
                        # The model allows NULL table_id (blank=True, null=True)
                        logger.debug(
                            f"Table cell '{table_cell_cell_id}' has NULL table_id (table '{table_cell_table_id}' not found)"
                        )

                    table_cell = TABLE_CELL(
                        name=replace_dots(table_cell_cell_id))
                    table_cell.cell_id = replace_dots(table_cell_cell_id)
                    table_cell.table_id = table_obj  # Can be None
                    table_cell.table_cell_combination_id = table_cell_combination_id

                    table_cells_to_create.append(table_cell)
                    context.table_cell_dictionary[table_cell.cell_id] = table_cell

                    # Only add to table mapping if table_id exists
                    if table_obj is not None:
                        table_cell_list = context.table_to_table_cell_dictionary.setdefault(table_obj, [])
                        table_cell_list.append(table_cell)

    # Log import statistics
    logger.info(f"[TABLE_CELL IMPORT] Total CSV rows: {total_rows}")
    logger.info(f"[TABLE_CELL IMPORT] Rows matching filter: {processed_rows}")
    logger.info(f"[TABLE_CELL IMPORT] Records to create: {len(table_cells_to_create)}")
    logger.info(f"[TABLE_CELL IMPORT] NULL table_id: {null_table_count}")

    if context.save_sdd_to_db and table_cells_to_create:
        TABLE_CELL.objects.bulk_create(table_cells_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
        logger.info(f"[TABLE_CELL IMPORT] Successfully bulk created {len(table_cells_to_create)} records")
