# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
"""
Cache management utilities for SDDContext.

Provides centralized cache update operations for cube links and related entities.
"""
import logging
from pybirdai.context.sdd_context_django import SDDContext

logger = logging.getLogger(__name__)


def remove_cube_link_from_cache(cube_link_id):
    """
    Remove a cube link and its related entries from the in-memory cache.

    Args:
        cube_link_id: The cube link ID to remove
    """
    sdd_context = SDDContext()

    # Remove from cube_link_dictionary
    try:
        del sdd_context.cube_link_dictionary[cube_link_id]
    except KeyError:
        pass

    # Remove from cube_link_to_foreign_cube_map
    try:
        del sdd_context.cube_link_to_foreign_cube_map[cube_link_id]
    except KeyError:
        pass

    # Remove from cube_link_to_join_identifier_map
    try:
        del sdd_context.cube_link_to_join_identifier_map[cube_link_id]
    except KeyError:
        pass

    # Remove from cube_link_to_join_for_report_id_map
    try:
        del sdd_context.cube_link_to_join_for_report_id_map[cube_link_id]
    except KeyError:
        pass


def add_cube_link_to_cache(cube_link):
    """
    Add a cube link to the in-memory cache.

    Args:
        cube_link: The CUBE_LINK model instance to add
    """
    sdd_context = SDDContext()

    # Add to cube_link_dictionary
    sdd_context.cube_link_dictionary[cube_link.cube_link_id] = cube_link

    # Add to cube_link_to_foreign_cube_map
    sdd_context.cube_link_to_foreign_cube_map[cube_link.cube_link_id] = cube_link.foreign_cube_id

    # Add to cube_link_to_join_identifier_map
    if cube_link.join_identifier:
        sdd_context.cube_link_to_join_identifier_map[cube_link.cube_link_id] = cube_link.join_identifier

    # Add to cube_link_to_join_for_report_id_map
    if cube_link.join_identifier:
        sdd_context.cube_link_to_join_for_report_id_map[cube_link.cube_link_id] = cube_link.join_identifier


def remove_cube_structure_item_link_from_cache(cube_structure_item_link_id, cube_link_id=None):
    """
    Remove a cube structure item link from the in-memory cache.

    Args:
        cube_structure_item_link_id: The link ID to remove
        cube_link_id: Optional cube link ID for updating the map
    """
    sdd_context = SDDContext()

    # Remove from cube_structure_item_links_dictionary
    try:
        del sdd_context.cube_structure_item_links_dictionary[cube_structure_item_link_id]
        logger.debug(f"Removed link ID {cube_structure_item_link_id} from cube_structure_item_links_dictionary")
    except KeyError:
        logger.debug(f"Link ID {cube_structure_item_link_id} not found in dictionary")

    # Remove from cube_structure_item_link_to_cube_link_map
    if cube_link_id:
        if cube_link_id in sdd_context.cube_structure_item_link_to_cube_link_map:
            original_count = len(sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id])
            sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id] = [
                item for item in sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id]
                if item.cube_structure_item_link_id != cube_structure_item_link_id
            ]
            new_count = len(sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id])
            logger.debug(f"List size changed from {original_count} to {new_count}")

            # If the list becomes empty, remove the key from the map
            if not sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id]:
                del sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id]
                logger.debug(f"Removed empty list for Cube Link ID {cube_link_id}")


def add_cube_structure_item_link_to_cache(link, cube_link_id):
    """
    Add a cube structure item link to the in-memory cache.

    Args:
        link: The CUBE_STRUCTURE_ITEM_LINK model instance
        cube_link_id: The cube link ID
    """
    sdd_context = SDDContext()

    # Add to cube_structure_item_links_dictionary
    sdd_context.cube_structure_item_links_dictionary[link.cube_structure_item_link_id] = link

    # Add to cube_structure_item_link_to_cube_link_map
    try:
        sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id].append(link)
    except KeyError:
        sdd_context.cube_structure_item_link_to_cube_link_map[cube_link_id] = [link]


def update_mapping_to_cube_cache(mapping_to_cube, operation='add'):
    """
    Update the mapping_to_cube_dictionary cache.

    Args:
        mapping_to_cube: The MAPPING_TO_CUBE model instance
        operation: 'add' or 'remove'
    """
    sdd_context = SDDContext()

    if operation == 'add':
        try:
            mapping_to_cube_list = sdd_context.mapping_to_cube_dictionary[mapping_to_cube.cube_mapping_id]
            mapping_to_cube_list.append(mapping_to_cube)
        except KeyError:
            sdd_context.mapping_to_cube_dictionary[mapping_to_cube.cube_mapping_id] = [mapping_to_cube]

    elif operation == 'remove':
        try:
            cube_mapping_list = sdd_context.mapping_to_cube_dictionary[mapping_to_cube.cube_mapping_id]
            for item in cube_mapping_list:
                if item.mapping_id.mapping_id == mapping_to_cube.mapping_id.mapping_id:
                    cube_mapping_list.remove(item)
                    break
        except KeyError:
            logger.debug(f"KeyError removing from cache: {mapping_to_cube.cube_mapping_id}")


def update_member_mapping_items_cache(item, operation='add'):
    """
    Update the member_mapping_items_dictionary cache.

    Args:
        item: The MEMBER_MAPPING_ITEM model instance
        operation: 'add' or 'remove'
    """
    sdd_context = SDDContext()
    member_mapping_id = item.member_mapping_id.member_mapping_id

    if operation == 'add':
        try:
            sdd_context.member_mapping_items_dictionary[member_mapping_id].append(item)
        except KeyError:
            sdd_context.member_mapping_items_dictionary[member_mapping_id] = [item]

    elif operation == 'remove':
        try:
            items_list = sdd_context.member_mapping_items_dictionary[member_mapping_id]
            for existing_item in items_list:
                if existing_item.id == item.id:
                    items_list.remove(existing_item)
                    break
        except KeyError:
            pass


def update_variable_mapping_item_cache(item, operation='add'):
    """
    Update the variable_mapping_item_dictionary cache.

    Args:
        item: The VARIABLE_MAPPING_ITEM model instance
        operation: 'add' or 'remove'
    """
    sdd_context = SDDContext()
    variable_mapping_id = item.variable_mapping_id.variable_mapping_id

    if operation == 'add':
        try:
            sdd_context.variable_mapping_item_dictionary[variable_mapping_id].append(item)
        except KeyError:
            sdd_context.variable_mapping_item_dictionary[variable_mapping_id] = [item]


def update_mapping_definition_cache(mapping_def, operation='add'):
    """
    Update the mapping_definition_dictionary cache.

    Args:
        mapping_def: The MAPPING_DEFINITION model instance
        operation: 'add' or 'remove'
    """
    sdd_context = SDDContext()

    if operation == 'add':
        sdd_context.mapping_definition_dictionary[mapping_def.mapping_id] = mapping_def
    elif operation == 'remove':
        try:
            del sdd_context.mapping_definition_dictionary[mapping_def.mapping_id]
        except KeyError:
            pass


def update_member_mapping_cache(member_mapping, operation='add'):
    """
    Update the member_mapping_dictionary cache.

    Args:
        member_mapping: The MEMBER_MAPPING model instance
        operation: 'add' or 'remove'
    """
    sdd_context = SDDContext()

    if operation == 'add':
        sdd_context.member_mapping_dictionary[member_mapping.member_mapping_id] = member_mapping
    elif operation == 'remove':
        try:
            del sdd_context.member_mapping_dictionary[member_mapping.member_mapping_id]
        except KeyError:
            pass


def update_variable_mapping_cache(variable_mapping, operation='add'):
    """
    Update the variable_mapping_dictionary cache.

    Args:
        variable_mapping: The VARIABLE_MAPPING model instance
        operation: 'add' or 'remove'
    """
    sdd_context = SDDContext()

    if operation == 'add':
        sdd_context.variable_mapping_dictionary[variable_mapping.variable_mapping_id] = variable_mapping
    elif operation == 'remove':
        try:
            del sdd_context.variable_mapping_dictionary[variable_mapping.variable_mapping_id]
        except KeyError:
            pass
