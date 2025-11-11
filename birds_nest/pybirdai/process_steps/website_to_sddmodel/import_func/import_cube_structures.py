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
#    Benjamin Arfa - refactoring into modular structure

"""Import cube structures from ANCRDT CSV files."""

import os
import csv
from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .utils import find_maintenance_agency_with_id, replace_dots


def import_cube_structures(base_path, context):
    '''
    Import all cube structures from CSV file using bulk create
    '''
    file_location = base_path + os.sep + "cube_structure.csv"
    header_skipped = False
    structures_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                structure_id = row[ColumnIndexes().cube_structure_id_index]
                maintenence_agency = row[ColumnIndexes().cube_structure_maintenance_agency]
                name = row[ColumnIndexes().cube_structure_name_index]
                description = row[ColumnIndexes().cube_structure_description_index]

                if not CUBE_STRUCTURE.objects.filter(cube_structure_id=replace_dots(structure_id)).exists():
                    structure = CUBE_STRUCTURE(name=replace_dots(structure_id))
                    maintenance_agency_id = find_maintenance_agency_with_id(context, maintenence_agency)
                    structure.cube_structure_id = replace_dots(structure_id)
                    structure.name = name
                    structure.description = description
                    structure.maintenance_agency_id = maintenance_agency_id

                    structures_to_create.append(structure)
                    context.cube_structure_dictionary[structure.cube_structure_id] = structure

    if context.save_sdd_to_db and structures_to_create:
        CUBE_STRUCTURE.objects.bulk_create(structures_to_create, batch_size=1000, ignore_conflicts=True)
