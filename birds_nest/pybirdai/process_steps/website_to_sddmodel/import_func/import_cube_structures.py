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
import logging
from django.db import connection
from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .lookups import find_maintenance_agency_with_id
from .utilities import replace_dots
from .csv_copy_importer import get_framework_filtered_delete_sql
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT

logger = logging.getLogger(__name__)


def _get_framework_ids_from_context(context):
    """Get framework IDs from context for isolation."""
    if hasattr(context, 'current_frameworks') and context.current_frameworks:
        return context.current_frameworks
    elif hasattr(context, 'current_framework') and context.current_framework:
        return [context.current_framework]
    return None


def _delete_cube_entities_for_frameworks(framework_ids):
    """
    Delete ALL cube-related entities for specified frameworks in correct order.

    This runs at the start of cube imports (before CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE).
    Uses existing CUBE records to determine which structures to delete.

    Order: CUBE_STRUCTURE_ITEM → CUBE_STRUCTURE → CUBE
    """
    if not framework_ids:
        return

    with connection.cursor() as cursor:
        if connection.vendor == 'sqlite':
            cursor.execute("PRAGMA foreign_keys = 0;")

        # Delete in correct order (uses CUBE for lookups, so delete items/structures before cube)
        for table in ['pybirdai_cube_structure_item', 'pybirdai_cube_structure', 'pybirdai_cube']:
            sql, params = get_framework_filtered_delete_sql(table, framework_ids)
            if sql:
                cursor.execute(sql, params)
                logger.info(f"Deleted {cursor.rowcount} rows from {table} for frameworks {framework_ids}")

        if connection.vendor == 'sqlite':
            cursor.execute("PRAGMA foreign_keys = 1;")


def import_cube_structures(base_path, context):
    '''
    Import all cube structures from CSV file using bulk create.

    Framework Isolation:
    - If context.current_frameworks or context.current_framework is set,
      deletes existing CUBE_STRUCTURE_ITEM, CUBE_STRUCTURE, and CUBE
      for those frameworks before importing any cube data.
    - This function runs FIRST in the cube import sequence.
    '''
    # Framework-filtered deletion before any cube imports
    framework_ids = _get_framework_ids_from_context(context)
    if framework_ids:
        logger.info(f"Framework isolation enabled for cubes: {framework_ids}")
        _delete_cube_entities_for_frameworks(framework_ids)

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
        CUBE_STRUCTURE.objects.bulk_create(structures_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
