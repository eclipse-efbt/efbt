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
import logging
from pybirdai.models.bird_meta_data_model import TABLE, FRAMEWORK, FRAMEWORK_TABLE
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_maintenance_agency_with_id
from .config import get_csv_file_path
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT

logger = logging.getLogger(__name__)


def import_report_tables(context, config=None):
    """
    Import all tables from the rendering package CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        config: DatasetConfig object specifying file_directory subdirectory
    """
    file_location = get_csv_file_path(context, "table.csv", config)
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
        TABLE.objects.bulk_create(tables_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

        # Create FRAMEWORK_TABLE junction records
        logger.info(f"Creating FRAMEWORK_TABLE junction records for {len(tables_to_create)} tables")
        framework_table_links = []
        framework_cache = {}  # Cache to avoid repeated database queries

        for table in tables_to_create:
            # Extract framework code from table_id
            # Format: EBA_{framework_code}_{table_code}_{version}
            # Example: EBA_COREP_C_01_00_2_8 -> COREP
            parts = table.table_id.split('_')
            if len(parts) >= 2 and parts[0] == 'EBA':
                framework_code = parts[1]
                framework_id_str = f"EBA_{framework_code}"

                # Get or cache FRAMEWORK object
                if framework_id_str not in framework_cache:
                    try:
                        framework_obj = FRAMEWORK.objects.get(framework_id=framework_id_str)
                        framework_cache[framework_id_str] = framework_obj
                    except FRAMEWORK.DoesNotExist:
                        logger.warning(f"Framework {framework_id_str} not found for table {table.table_id}")
                        continue
                else:
                    framework_obj = framework_cache[framework_id_str]

                # Create junction record
                framework_table_link = FRAMEWORK_TABLE(
                    framework_id=framework_obj,
                    table_id=table
                )
                framework_table_links.append(framework_table_link)

        # Bulk create junction records
        if framework_table_links:
            FRAMEWORK_TABLE.objects.bulk_create(framework_table_links, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
            logger.info(f"Created {len(framework_table_links)} FRAMEWORK_TABLE junction records")
        else:
            logger.warning("No FRAMEWORK_TABLE junction records created - check table_id format")
