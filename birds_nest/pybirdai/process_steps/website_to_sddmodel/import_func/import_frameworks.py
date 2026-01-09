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

"""Import frameworks from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import FRAMEWORK, MAINTENANCE_AGENCY
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_frameworks(context, config=None):
    """
    Import frameworks from CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        config: DatasetConfig object specifying file_directory subdirectory (optional, defaults to "technical_export")
    """
    subdir = config.file_directory if config else "technical_export"
    file_location = context.file_directory + os.sep + subdir + os.sep + "framework.csv"
    header_skipped = False
    frameworks_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                maintenance_agency_id_str = row[ColumnIndexes().framework_maintenance_agency_id]
                code = row[ColumnIndexes().framework_code]
                description = row[ColumnIndexes().framework_description]
                id = row[ColumnIndexes().framework_id]
                name = row[ColumnIndexes().framework_name]

                # Look up the MAINTENANCE_AGENCY record
                maintenance_agency = None
                if maintenance_agency_id_str:
                    # First try from context dictionary
                    maintenance_agency = context.agency_dictionary.get(maintenance_agency_id_str)
                    if not maintenance_agency:
                        # Fallback: try to find in database
                        try:
                            maintenance_agency = MAINTENANCE_AGENCY.objects.get(
                                maintenance_agency_id=maintenance_agency_id_str
                            )
                        except MAINTENANCE_AGENCY.DoesNotExist:
                            pass

                # Default to EFBT if specified agency doesn't exist
                if not maintenance_agency:
                    maintenance_agency = context.agency_dictionary.get('EFBT')
                    if not maintenance_agency:
                        try:
                            maintenance_agency = MAINTENANCE_AGENCY.objects.get(
                                maintenance_agency_id='EFBT'
                            )
                        except MAINTENANCE_AGENCY.DoesNotExist:
                            pass

                framework = FRAMEWORK(
                    name=name,
                    maintenance_agency_id=maintenance_agency
                )
                framework.code = code
                framework.description = description
                framework.framework_id = replace_dots(id)

                frameworks_to_create.append(framework)
                context.framework_dictionary[replace_dots(id)] = framework

    if context.save_sdd_to_db and frameworks_to_create:
        FRAMEWORK.objects.bulk_create(frameworks_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
