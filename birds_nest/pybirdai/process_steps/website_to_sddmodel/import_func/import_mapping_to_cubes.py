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

"""Import mapping to cubes from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import MAPPING_TO_CUBE
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_mapping_definition_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_mapping_to_cubes(context):
    """
    Import all mapping to cubes from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = context.file_directory + os.sep + "technical_export" + os.sep + "mapping_to_cube.csv"
    header_skipped = False
    mapping_to_cubes_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        id_increment = 0
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                mapping_to_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_mapping_id + id_increment]
                mapping_to_cube_cube_mapping_id = row[ColumnIndexes().mapping_to_cube_cube_mapping_id + id_increment]
                mapping_to_cube_valid_from = row[ColumnIndexes().mapping_to_cube_valid_from + id_increment]
                mapping_to_cube_valid_to = row[ColumnIndexes().mapping_to_cube_valid_to + id_increment]

                if not mapping_to_cube_mapping_id.startswith("M_SHS"):
                    mapping_to_cube = MAPPING_TO_CUBE(
                        mapping_id=find_mapping_definition_with_id(context, mapping_to_cube_mapping_id),
                        cube_mapping_id=replace_dots(mapping_to_cube_cube_mapping_id),
                        valid_from=mapping_to_cube_valid_from,
                        valid_to=mapping_to_cube_valid_to
                    )

                    mapping_to_cubes_to_create.append(mapping_to_cube)

                    mapping_to_cube_list = context.mapping_to_cube_dictionary.setdefault(
                        mapping_to_cube.cube_mapping_id, [])
                    mapping_to_cube_list.append(mapping_to_cube)

    if context.save_sdd_to_db and mapping_to_cubes_to_create:
        MAPPING_TO_CUBE.objects.bulk_create(mapping_to_cubes_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
