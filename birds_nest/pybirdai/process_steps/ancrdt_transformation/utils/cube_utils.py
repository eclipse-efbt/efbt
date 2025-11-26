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
Cube and structure iteration utilities for ANCRDT transformations.

This module provides helper functions for iterating over cube structures,
filtering cube links, and extracting metadata from cube structure items.
These utilities eliminate repetitive code patterns across transformation modules.
"""

from typing import Iterator, Tuple, Dict, List, Any, Optional

from .constants import (
    get_python_type,
    SKIP_FIELDS,
    ANCRDT_FRAMEWORK_ID,
    ANCRDT_TABLE_PREFIX
)


def iterate_cube_structure_items(
    cube_structure_items,
    skip_nevs: bool = True
) -> Iterator[Tuple[Any, str, str]]:
    """
    Generator that yields (variable, domain, python_type) tuples for cube structure items.

    This is a common pattern used across transformation generation code to
    iterate over cube structure items while skipping certain fields and
    extracting type information.

    Args:
        cube_structure_items: QuerySet or iterable of CUBE_STRUCTURE_ITEM objects
        skip_nevs (bool): If True, skip items with variable_id == "NEVS".
            Defaults to True.

    Yields:
        Tuple[variable, domain, python_type]: For each structure item:
            - variable: The VARIABLE object
            - domain (str): Domain ID (e.g., 'String', 'Integer')
            - python_type (str): Python type annotation (e.g., 'str', 'int')

    Example:
        >>> from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM
        >>> items = CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_id=my_structure)
        >>> for variable, domain, py_type in iterate_cube_structure_items(items):
        ...     print(f"def {variable.variable_id}(self) -> {py_type}:")
    """
    for cube_structure_item in cube_structure_items:
        variable = cube_structure_item.variable_id

        # Skip NEVS field if requested
        if skip_nevs and variable.variable_id in SKIP_FIELDS:
            continue

        domain = variable.domain_id.domain_id
        python_type = get_python_type(domain)

        yield variable, domain, python_type


def filter_ancrdt_cube_links(
    cube_link_map: Dict[str, Any],
    framework_filter: str = ANCRDT_FRAMEWORK_ID
) -> Dict[str, Any]:
    """
    Filter cube link map to only ANCRDT entries.

    This pattern is used when processing cube links to isolate ANCRDT-specific
    transformations from other regulatory frameworks.

    Args:
        cube_link_map (dict): Dictionary mapping cube link IDs to cube link objects
        framework_filter (str): Framework identifier to filter by.
            Defaults to 'ANCRDT'.

    Returns:
        dict: Filtered dictionary containing only ANCRDT cube links

    Example:
        >>> cube_links = sdd_context.cube_link_to_foreign_cube_map
        >>> ancrdt_links = filter_ancrdt_cube_links(cube_links)
        >>> print(f"Found {len(ancrdt_links)} ANCRDT cube links")
    """
    return {
        rolc_id: cube_links
        for rolc_id, cube_links in cube_link_map.items()
        if framework_filter in rolc_id
    }


def filter_ancrdt_tables(
    table_names: List[str],
    prefix: str = ANCRDT_TABLE_PREFIX
) -> List[str]:
    """
    Filter list of table names to only ANCRDT tables.

    Args:
        table_names (list): List of table name strings
        prefix (str): Table name prefix to filter by. Defaults to 'ANCRDT_'.

    Returns:
        list: Filtered list containing only ANCRDT table names

    Example:
        >>> all_tables = ['FINREP_F01', 'ANCRDT_INSTRMNT_C_1', 'ANCRDT_PRTY_C_1']
        >>> ancrdt_tables = filter_ancrdt_tables(all_tables)
        >>> print(ancrdt_tables)
        ['ANCRDT_INSTRMNT_C_1', 'ANCRDT_PRTY_C_1']
    """
    return [name for name in table_names if name.startswith(prefix)]


def get_cube_structure_items_with_metadata(
    cube_structure_items,
    skip_nevs: bool = True
) -> List[Dict[str, Any]]:
    """
    Extract cube structure items with full metadata as dictionaries.

    This converts cube structure items into a list of dictionaries containing
    all relevant metadata, useful for JSON serialization or template rendering.

    Args:
        cube_structure_items: QuerySet or iterable of CUBE_STRUCTURE_ITEM objects
        skip_nevs (bool): If True, skip NEVS fields. Defaults to True.

    Returns:
        list: List of dictionaries, each containing:
            - variable_id (str): Variable identifier
            - variable_name (str): Variable name
            - domain (str): Domain type
            - python_type (str): Python type annotation
            - role (str): Structure item role
            - order (int): Display order

    Example:
        >>> items = get_cube_structure_items_with_metadata(cube.cube_structure_id.items)
        >>> for item in items:
        ...     print(f"{item['variable_id']}: {item['python_type']}")
    """
    result = []

    for variable, domain, python_type in iterate_cube_structure_items(
        cube_structure_items, skip_nevs
    ):
        result.append({
            'variable_id': variable.variable_id,
            'variable_name': variable.name or variable.variable_id,
            'domain': domain,
            'python_type': python_type,
        })

    return result


def group_cube_links_by_foreign_cube(
    cube_structure_item_links
) -> Dict[str, List[Any]]:
    """
    Group cube structure item links by their foreign cube.

    This pattern is used to organize links for code generation, grouping all
    links that point to the same foreign cube together.

    Args:
        cube_structure_item_links: QuerySet or iterable of CUBE_STRUCTURE_ITEM_LINK objects

    Returns:
        dict: Dictionary mapping foreign cube IDs to lists of links

    Example:
        >>> from collections import defaultdict
        >>> links = CUBE_STRUCTURE_ITEM_LINK.objects.all()
        >>> grouped = group_cube_links_by_foreign_cube(links)
        >>> for cube_id, links_list in grouped.items():
        ...     print(f"{cube_id}: {len(links_list)} links")
    """
    from collections import defaultdict
    result = defaultdict(list)

    for link in cube_structure_item_links:
        if hasattr(link, 'cube_link_id') and link.cube_link_id:
            foreign_cube_id = link.cube_link_id.foreign_cube_id.cube_id
            result[foreign_cube_id].append(link)

    return dict(result)


def extract_dimension_enums(
    cube_structure_items,
    variable_ids: Optional[List[str]] = None
) -> Dict[str, List[str]]:
    """
    Extract enum values for dimensions from cube structure items.

    For dimensions with subdomains (enumerations), this extracts the list
    of valid enum values.

    Args:
        cube_structure_items: QuerySet or iterable of CUBE_STRUCTURE_ITEM objects
        variable_ids (list, optional): If provided, only extract enums for these variables.
            If None, extract for all variables.

    Returns:
        dict: Dictionary mapping variable IDs to lists of valid enum values

    Example:
        >>> items = cube_structure.cube_structure_items.all()
        >>> enums = extract_dimension_enums(items, ['PRPS', 'TYP_INSTRMNT'])
        >>> print(f"PRPS values: {enums['PRPS']}")
    """
    result = {}

    for cube_structure_item in cube_structure_items:
        variable = cube_structure_item.variable_id

        # Skip if filtering by variable_ids and this isn't in the list
        if variable_ids and variable.variable_id not in variable_ids:
            continue

        # Check if variable has a subdomain (enumeration)
        if cube_structure_item.subdomain_id:
            subdomain = cube_structure_item.subdomain_id

            # Get all members of this subdomain
            if hasattr(subdomain, 'members'):
                member_values = [
                    member.member_id
                    for member in subdomain.members.all()
                ]
                result[variable.variable_id] = sorted(member_values)

    return result


def is_ancrdt_cube(cube) -> bool:
    """
    Check if a cube is an ANCRDT cube.

    Args:
        cube: CUBE object to check

    Returns:
        bool: True if cube is ANCRDT, False otherwise

    Example:
        >>> from pybirdai.models.bird_meta_data_model import CUBE
        >>> for cube in CUBE.objects.all():
        ...     if is_ancrdt_cube(cube):
        ...         print(f"ANCRDT: {cube.cube_id}")
    """
    if hasattr(cube, 'framework_id') and cube.framework_id:
        framework_str = str(cube.framework_id.framework_id)
        return ANCRDT_FRAMEWORK_ID in framework_str
    return False


def get_ancrdt_cubes(cube_queryset=None):
    """
    Get all ANCRDT cubes from the database.

    Args:
        cube_queryset (QuerySet, optional): Custom queryset to filter.
            If None, uses CUBE.objects.all()

    Returns:
        QuerySet: Filtered queryset containing only ANCRDT cubes

    Example:
        >>> ancrdt_cubes = get_ancrdt_cubes()
        >>> print(f"Found {ancrdt_cubes.count()} ANCRDT cubes")
    """
    from pybirdai.models.bird_meta_data_model import CUBE

    if cube_queryset is None:
        cube_queryset = CUBE.objects.all()

    return cube_queryset.filter(
        framework_id__framework_id__icontains=ANCRDT_FRAMEWORK_ID
    )
