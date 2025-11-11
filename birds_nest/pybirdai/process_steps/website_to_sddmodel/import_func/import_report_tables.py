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

"""Import report tables from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import TABLE
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_maintenance_agency_with_id


def import_report_tables(context, config=None):
    """
    Import all tables from the rendering package CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        config: DatasetConfig object specifying file_directory subdirectory (optional, defaults to "technical_export")
    """
    subdir = config.file_directory if config else "technical_export"
    file_location = context.file_directory + os.sep + subdir + os.sep + "table.csv"
    header_skipped = False
    tables_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                table_id = row[ColumnIndexes().table_table_id]
                display_name = row[ColumnIndexes().table_table_name]
                code = row[ColumnIndexes().table_code]
                description = row[ColumnIndexes().table_description]
                maintenance_agency_id = row[ColumnIndexes().table_maintenance_agency_id]
                version = row[ColumnIndexes().table_version]
                valid_from = row[ColumnIndexes().table_valid_from]
                valid_to = row[ColumnIndexes().table_valid_to]

                table = TABLE(
                    name=replace_dots(table_id))
                table.table_id = replace_dots(table_id)
                table.name = display_name
                table.code = code
                table.description = description
                maintenance_agency = find_maintenance_agency_with_id(context, maintenance_agency_id)
                table.maintenance_agency_id = maintenance_agency
                table.version = version

                tables_to_create.append(table)
                context.report_tables_dictionary[table.table_id] = table

    if context.save_sdd_to_db and tables_to_create:
        TABLE.objects.bulk_create(tables_to_create, batch_size=1000, ignore_conflicts=True)
