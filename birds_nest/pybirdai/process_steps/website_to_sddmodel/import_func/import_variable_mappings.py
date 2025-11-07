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

"""Import variable mappings from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import VARIABLE_MAPPING
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .lookups import find_maintenance_agency_with_id


def import_variable_mappings(context):
    """
    Import all variable mappings from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = context.file_directory + os.sep + "technical_export" + os.sep + "variable_mapping.csv"

    # Pre-filter SHS_ entries and build batch
    variable_mappings_to_create = []

    # Read entire CSV at once instead of line by line
    with open(file_location, encoding='utf-8') as csvfile:
        rows = list(csv.reader(csvfile))[1:]  # Skip header

        # Process in a single pass
        for row in rows:
            variable_mapping_id = row[ColumnIndexes().variable_mapping_variable_mapping_id]

            if not variable_mapping_id.startswith("SHS_") and variable_mapping_id not in context.variable_mapping_dictionary:
                variable_mapping = VARIABLE_MAPPING(
                    variable_mapping_id=variable_mapping_id,
                    maintenance_agency_id=find_maintenance_agency_with_id(
                        context, row[ColumnIndexes().variable_mapping_maintenance_agency_id]),
                    code=row[ColumnIndexes().variable_mapping_code],
                    name=row[ColumnIndexes().variable_mapping_name]
                )

                variable_mappings_to_create.append(variable_mapping)
                context.variable_mapping_dictionary[variable_mapping_id] = variable_mapping

    # Single bulk create with larger batch size
    if context.save_sdd_to_db and variable_mappings_to_create:
        VARIABLE_MAPPING.objects.bulk_create(variable_mappings_to_create, batch_size=5000, ignore_conflicts=True)
