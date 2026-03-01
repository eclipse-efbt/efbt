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

"""Import mapping definitions from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import MAPPING_DEFINITION
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .lookups import find_member_mapping_with_id, find_variable_mapping_with_id
from .config import get_csv_file_path
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_mapping_definitions(context):
    """
    Import all mapping definitions from the rendering package CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = get_csv_file_path(context, "mapping_definition.csv")
    mapping_definitions_to_create = []

    # Cache lookups
    member_mapping_cache = {}
    variable_mapping_cache = {}

    with open(file_location, encoding='utf-8') as csvfile:
        rows = list(csv.reader(csvfile))[1:]  # Skip header

        for row in rows:
            mapping_id = row[ColumnIndexes().mapping_definition_mapping_id]
            if mapping_id.startswith("SHS_"):
                continue

            member_mapping_id = row[ColumnIndexes().mapping_definition_member_mapping_id]
            if member_mapping_id not in member_mapping_cache:
                member_mapping_cache[member_mapping_id] = find_member_mapping_with_id(
                    context, member_mapping_id)

            variable_mapping_id = row[ColumnIndexes().mapping_definition_variable_mapping_id]
            if variable_mapping_id not in variable_mapping_cache:
                variable_mapping_cache[variable_mapping_id] = find_variable_mapping_with_id(
                    context, variable_mapping_id)

            mapping_definition = MAPPING_DEFINITION(
                mapping_id=mapping_id,
                name=row[ColumnIndexes().mapping_definition_name],
                code=row[ColumnIndexes().mapping_definition_code],
                mapping_type=row[ColumnIndexes().mapping_definition_mapping_type],
                member_mapping_id=member_mapping_cache[member_mapping_id],
                variable_mapping_id=variable_mapping_cache[variable_mapping_id]
            )

            mapping_definitions_to_create.append(mapping_definition)
            context.mapping_definition_dictionary[mapping_id] = mapping_definition

    if context.save_sdd_to_db and mapping_definitions_to_create:
        MAPPING_DEFINITION.objects.bulk_create(mapping_definitions_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
