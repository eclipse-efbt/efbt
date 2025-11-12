# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

"""
Persistence manager for NROLC (Non-Reference Output Layer Creator).
Handles bulk save operations for all object types.
"""

from pybirdai.models.bird_meta_data_model import (
    CUBE, CUBE_STRUCTURE, COMBINATION, COMBINATION_ITEM,
    CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, SUBDOMAIN,
    SUBDOMAIN_ENUMERATION, TABLE_CELL
)
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


class PersistenceManager:
    """Manages bulk save operations for output layer objects."""

    @staticmethod
    def bulk_save_objects(
        cube_structures,
        cubes,
        combinations,
        subdomains,
        subdomain_enumerations,
        combination_items,
        cube_structure_items,
        cube_to_combinations,
        cells_to_update
    ):
        """
        Bulk save all created objects to the database.

        Args:
            cube_structures: List of CUBE_STRUCTURE instances
            cubes: List of CUBE instances
            combinations: List of COMBINATION instances
            subdomains: List of SUBDOMAIN instances
            subdomain_enumerations: List of SUBDOMAIN_ENUMERATION instances
            combination_items: List of COMBINATION_ITEM instances
            cube_structure_items: List of CUBE_STRUCTURE_ITEM instances
            cube_to_combinations: List of CUBE_TO_COMBINATION instances
            cells_to_update: List of TABLE_CELL instances to update
        """
        if cube_structures:
            CUBE_STRUCTURE.objects.bulk_create(
                cube_structures, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True
            )

        if cubes:
            CUBE.objects.bulk_create(
                cubes, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True
            )

        if combinations:
            COMBINATION.objects.bulk_create(
                combinations, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT
            )

        # Save subdomains first as they are referenced by other objects
        if subdomains:
            SUBDOMAIN.objects.bulk_create(
                subdomains, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT
            )

        if subdomain_enumerations:
            SUBDOMAIN_ENUMERATION.objects.bulk_create(
                subdomain_enumerations, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT
            )

        if combination_items:
            COMBINATION_ITEM.objects.bulk_create(
                combination_items, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT
            )

        if cube_structure_items:
            CUBE_STRUCTURE_ITEM.objects.bulk_create(
                cube_structure_items, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT
            )

        if cube_to_combinations:
            CUBE_TO_COMBINATION.objects.bulk_create(
                cube_to_combinations, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT
            )

        # Update cells with new combination IDs
        if cells_to_update:
            TABLE_CELL.objects.bulk_update(
                cells_to_update, ['table_cell_combination_id'], batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT
            )
