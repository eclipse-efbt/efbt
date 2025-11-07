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

"""Import maintenance agencies from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import MAINTENANCE_AGENCY
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots


def import_maintenance_agencies(context):
    """
    Import maintenance agencies from CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = context.file_directory + os.sep + "technical_export" + os.sep + "maintenance_agency.csv"
    header_skipped = False
    agencies_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                code = row[ColumnIndexes().maintenance_agency_code]
                description = row[ColumnIndexes().maintenance_agency_description]
                id = row[ColumnIndexes().maintenance_agency_id]
                name = row[ColumnIndexes().maintenance_agency_name]

                maintenance_agency = MAINTENANCE_AGENCY(
                    name=replace_dots(id))
                maintenance_agency.code = code
                maintenance_agency.description = description
                maintenance_agency.maintenance_agency_id = replace_dots(id)

                agencies_to_create.append(maintenance_agency)
                context.agency_dictionary[id] = maintenance_agency

    if context.save_sdd_to_db and agencies_to_create:
        MAINTENANCE_AGENCY.objects.bulk_create(agencies_to_create, batch_size=1000, ignore_conflicts=True)
