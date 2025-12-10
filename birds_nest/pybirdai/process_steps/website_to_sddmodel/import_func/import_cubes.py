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

"""Import cubes from ANCRDT CSV files."""

import os
import csv
import logging
from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE, FRAMEWORK
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .utils import find_maintenance_agency_with_id, replace_dots
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT

logger = logging.getLogger(__name__)


def import_cubes(base_path, context):
    '''
    Import all cubes from CSV file using bulk create
    '''
    file_location = base_path + os.sep + "cube.csv"
    header_skipped = False
    cubes_to_create = []
    missing_frameworks = set()
    total_cube_rows = 0

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                total_cube_rows += 1
                cube_id = row[ColumnIndexes().cube_object_id_index]
                maintenence_agency = row[ColumnIndexes().cube_maintenance_agency_id]
                cube_type = row[ColumnIndexes().cube_cube_type_index]
                code = row[ColumnIndexes().cube_class_code_index]
                name = row[ColumnIndexes().cube_class_name_index]
                description = row[ColumnIndexes().cube_cube_structure_id_index]
                cube_structure_id = row[ColumnIndexes().cube_cube_structure_id_index]
                framework_id_str = row[ColumnIndexes().cube_framework_index]

                if not CUBE.objects.filter(cube_id=replace_dots(cube_id)).exists():
                    cube = CUBE(name=replace_dots(cube_id))
                    maintenance_agency_id = find_maintenance_agency_with_id(context, maintenence_agency)
                    cube.code = code
                    cube.cube_id = replace_dots(cube_id)
                    cube.name = name
                    cube.description = description
                    cube.maintenance_agency_id = maintenance_agency_id
                    cube.cube_structure_id = CUBE_STRUCTURE.objects.get(cube_structure_id=cube_structure_id)

                    # Assign framework_id
                    if framework_id_str:
                        try:
                            cube.framework_id = FRAMEWORK.objects.get(framework_id=framework_id_str)
                        except FRAMEWORK.DoesNotExist:
                            missing_frameworks.add(framework_id_str)
                            logger.warning(f"FRAMEWORK not found for cube {cube_id}: {framework_id_str}")
                            # Continue without framework - cube will be created but won't appear in filtered views

                    cubes_to_create.append(cube)
                    context.cube_dictionary[cube.cube_id] = cube

    if context.save_sdd_to_db and cubes_to_create:
        CUBE.objects.bulk_create(cubes_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
        logger.info(f"CUBE import: Created {len(cubes_to_create)} of {total_cube_rows} cubes from CSV")

    # Report missing frameworks
    if missing_frameworks:
        logger.error(f"Missing FRAMEWORKS ({len(missing_frameworks)}): {missing_frameworks}")
