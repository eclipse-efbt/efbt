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
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT

logger = logging.getLogger(__name__)


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
        TABLE.objects.bulk_create(tables_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

        # Fetch the actual TABLE records from the database to ensure we have valid FK references
        # This is necessary because bulk_create with ignore_conflicts=True may skip existing records
        table_ids = [t.table_id for t in tables_to_create]
        db_tables = {t.table_id: t for t in TABLE.objects.filter(table_id__in=table_ids)}
        logger.info(f"Fetched {len(db_tables)} TABLE records from database for FK references")

        # Create FRAMEWORK_TABLE junction records
        logger.info(f"Creating FRAMEWORK_TABLE junction records for {len(db_tables)} tables")
        framework_table_links = []
        framework_cache = {}  # Cache to avoid repeated database queries

        for table_id, db_table in db_tables.items():
            # Extract framework code from table_id
            # Format: EBA_{framework_code}_{table_code}_{version}
            # Example: EBA_COREP_C_01_00_2_8 -> COREP
            parts = table_id.split('_')
            if len(parts) >= 2 and parts[0] == 'EBA':
                framework_code = parts[1]
                framework_id_str = f"EBA_{framework_code}"

                # Get or create FRAMEWORK object - ensures it exists in database
                if framework_id_str not in framework_cache:
                    maintenance_agency = find_maintenance_agency_with_id(context, 'EBA')
                    framework_obj, created = FRAMEWORK.objects.get_or_create(
                        framework_id=framework_id_str,
                        defaults={
                            'name': framework_code,  # e.g., "COREP"
                            'code': framework_code,
                            'maintenance_agency_id': maintenance_agency
                        }
                    )
                    if created:
                        logger.info(f"Auto-created framework {framework_id_str} for table {table_id}")
                    framework_cache[framework_id_str] = framework_obj
                else:
                    framework_obj = framework_cache[framework_id_str]

                # Create junction record using database-fetched objects
                framework_table_link = FRAMEWORK_TABLE(
                    framework_id=framework_obj,
                    table_id=db_table
                )
                framework_table_links.append(framework_table_link)

        # Bulk create junction records
        if framework_table_links:
            FRAMEWORK_TABLE.objects.bulk_create(framework_table_links, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
            logger.info(f"Created {len(framework_table_links)} FRAMEWORK_TABLE junction records")
        else:
            logger.warning("No FRAMEWORK_TABLE junction records created - check table_id format")
