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

"""Import variable mapping items from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import VARIABLE_MAPPING_ITEM
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .lookups import find_variable_with_id, find_variable_mapping_with_id
from .warning_writers import save_missing_mapping_variables_to_csv
from .config import get_csv_file_path
from .utilities import optional_datetime
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_variable_mapping_items(context):
    """
    Import all variable mapping items from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = get_csv_file_path(context, "variable_mapping_item.csv")
    missing_variables = []
    variable_mapping_items_to_create = []
    id_increment = 0
    # Cache variable lookups
    variable_cache = {}

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header_skipped = False
        id_increment = 0
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                mapping_id = row[ColumnIndexes().varaible_mapping_item_variable_mapping_id + id_increment]
                if mapping_id.startswith("SHS_"):
                    continue

                variable_id = row[ColumnIndexes().variable_mapping_item_variable_id + id_increment]

                # Use cached variable lookup
                if variable_id not in variable_cache:
                    variable_cache[variable_id] = find_variable_with_id(
                        context, variable_id)

                variable = variable_cache[variable_id]

                if variable is None:
                    missing_variables.append((
                        variable_id,
                        mapping_id,
                        row[ColumnIndexes().variable_mapping_item_valid_to + id_increment]
                    ))
                    continue

                variable_mapping_item = VARIABLE_MAPPING_ITEM(
                    variable_id=variable,
                    variable_mapping_id=find_variable_mapping_with_id(
                        context, mapping_id),
                    is_source=row[ColumnIndexes().variable_mapping_item_is_source + id_increment],
                    valid_from=optional_datetime(row[ColumnIndexes().variable_mapping_item_valid_from + id_increment]),
                    valid_to=optional_datetime(row[ColumnIndexes().variable_mapping_item_valid_to + id_increment])
                )

                variable_mapping_items_to_create.append(variable_mapping_item)

                # Build dictionary in a single operation
                context.variable_mapping_item_dictionary.setdefault(mapping_id, []).append(variable_mapping_item)

    # Single bulk create with larger batch size
    if context.save_sdd_to_db and variable_mapping_items_to_create:
        VARIABLE_MAPPING_ITEM.objects.bulk_create(variable_mapping_items_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

    if missing_variables:
        save_missing_mapping_variables_to_csv(context, missing_variables)
