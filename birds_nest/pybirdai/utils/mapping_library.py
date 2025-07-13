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

import json
import logging
from typing import Dict, List, Set, Tuple, Any, Optional
from ..bird_meta_data_model import (
    MEMBER,
    MEMBER_MAPPING_ITEM,
    MAPPING_DEFINITION,
    MAPPING_TO_CUBE,
    VARIABLE,
    VARIABLE_MAPPING_ITEM
)
logger = logging.getLogger(__name__)

def update_member_mapping_item(member_mapping: MEMBER_MAPPING_ITEM, member_mapping_row: str, variable: VARIABLE, member: MEMBER, is_source_str: str) -> MEMBER_MAPPING_ITEM:
    """Updates or creates a member mapping item.

    Args:
        member_mapping: The member mapping object
        member_mapping_row: Row identifier
        variable: Variable object
        member: Member object
        is_source: Boolean indicating if source mapping

    Returns:
        Created or updated Member Mapping Item
    """
    logger.debug(f"Updating member mapping item: {member_mapping_row}")
    mapping_item, created = MEMBER_MAPPING_ITEM.objects.update_or_create(
        member_mapping_id=member_mapping,
        member_mapping_row=member_mapping_row,
        variable_id=variable,
        defaults={
            'member_id': member,
            'is_source': is_source_str
        }
    )
    return mapping_item

def get_filtered_var_items(variable_mapping_id: str) -> Tuple[List[VARIABLE_MAPPING_ITEM], List[VARIABLE_MAPPING_ITEM], List[VARIABLE_MAPPING_ITEM]]:
    """Gets filtered variable items for a given mapping ID.

    Args:
        variable_mapping_id: ID of the variable mapping to filter

    Returns:
        Tuple containing lists of all items, source items and target items
    """
 #   logger.debug(f"Getting filtered variable items for mapping ID: {variable_mapping_id}")
    var_items = VARIABLE_MAPPING_ITEM.objects.filter(variable_mapping_id=variable_mapping_id)
    source_vars = [item for item in var_items if item.is_source.lower() == 'true']
    target_vars = [item for item in var_items if item.is_source.lower() != 'true']
    return var_items, source_vars, target_vars

def build_mapping_results(mapping_definitions: List[MAPPING_DEFINITION]) -> Dict[str, Dict[str, Any]]:
    """Builds mapping results dictionary from mapping definitions.

    Args:
        mapping_definitions: List of mapping definition objects

    Returns:
        Dictionary containing mapping results
    """
    logger.debug("Building mapping results")
    results = {}
    for map_def in mapping_definitions:


        if not map_def.member_mapping_id:
            continue

        if map_def.variable_mapping_id:
            var_items, source_vars, target_vars = get_filtered_var_items(map_def.variable_mapping_id)
            if len(target_vars) == 0 or len(var_items) == 1:
                continue

        if map_def.mapping_id not in results:
            results[map_def.mapping_id] = {
                "variable_mapping_id": map_def.variable_mapping_id.code if map_def.variable_mapping_id else None,
                "has_member_mapping": True,
                "member_mapping_id": {
                    "code": map_def.member_mapping_id.code,
                    "items": []
                }
            }
    return results

def get_source_target_vars(var_items: List[VARIABLE_MAPPING_ITEM]) -> Dict[str, List[str]]:
    """Gets source and target variables from variable items.

    Args:
        var_items: List of variable mapping items

    Returns:
        Dictionary with source and target variable lists
    """
    logger.debug("Getting source and target variables")
    source_vars = [f"{item.variable_id.name} ({item.variable_id.code})" for item in var_items if item.is_source.lower() == 'true']
    target_vars = [f"{item.variable_id.name} ({item.variable_id.code})" for item in var_items if item.is_source.lower() != 'true']
    return {"source":source_vars, "target":target_vars}

def initialize_unique_set(member_mapping_items: List[MEMBER_MAPPING_ITEM]) -> Dict[str, Set[str]]:
    """Initializes unique set of member mappings.

    Args:
        member_mapping_items: List of member mapping items

    Returns:
        Dictionary with unique variable sets
    """
    logger.debug("Initializing unique set")
    unique_set = {}
    for item in member_mapping_items:
        vars_ = f"{item.variable_id.name} ({item.variable_id.code})"
        if vars_ not in unique_set:
            unique_set[vars_] = {elt.member_id: f"{elt.name} ({elt.code})"
            for elt in MEMBER.objects.filter(domain_id=item.variable_id.domain_id)}
    return unique_set

def build_temp_items(member_mapping_items: List[MEMBER_MAPPING_ITEM], unique_set:dict) -> Dict[str, Dict[str, Any]]:
    """Builds temporary items dictionary from member mappings.

    Args:
        member_mapping_items: List of member mapping items
        unique_set: Dictionary of unique variable sets

    Returns:
        Dictionary of temporary mapping items
    """
    logger.debug("Building temporary items")
    temp_items = {}
    for item in member_mapping_items:
        if item.member_mapping_row not in temp_items:
            temp_items[item.member_mapping_row] = {'has_source': False, 'has_target': False, 'items': {k:"None (None)" for k in unique_set}}

        vars_ = f"{item.variable_id.name} ({item.variable_id.code})"
        member_ = f"{item.member_id.name} ({item.member_id.code})"

        temp_items[item.member_mapping_row]['has_source' if item.is_source.lower() == 'true' else 'has_target'] = True
        temp_items[item.member_mapping_row]['items'][vars_] = member_
    return temp_items

def process_member_mappings(member_mapping_items: List[MEMBER_MAPPING_ITEM], var_items: List[VARIABLE_MAPPING_ITEM]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Set[str]], Dict[str, List[str]]]:
    """Processes member mappings to create temporary items and unique sets.

    Args:
        member_mapping_items: List of member mapping items
        var_items: List of variable mapping items

    Returns:
        Tuple of temporary items, unique sets and source/target variables
    """
    logger.debug("Processing member mappings")
    source_target = get_source_target_vars(var_items)
    unique_set = initialize_unique_set(member_mapping_items)
    temp_items = build_temp_items(member_mapping_items,unique_set)

    return temp_items, unique_set, source_target

def create_table_data(serialized_items: Dict[str, Dict[str, str]], columns_of_table: List[str]) -> Dict[str, Any]:
    """Creates table data structure from serialized items.

    Args:
        serialized_items: Dictionary of serialized mapping items
        unique_set: Dictionary of unique variable sets

    Returns:
        Dictionary containing table data structure
    """
    logger.debug("Creating table data")
    table_data = {
        'headers': ["row_id"]+columns_of_table,
        'rows': []
    }
    for row_id, row_data in serialized_items.items():
        table_row = {"row_id":int(row_id)}
        table_row.update(row_data)
        table_data['rows'].append(table_row)

    # Sort the rows by row_id
    table_data["rows"] = sorted(table_data["rows"], key=lambda x: x["row_id"])
    return table_data

def cascade_member_mapping_changes(member_mapping_item: MEMBER_MAPPING_ITEM) -> None:
    """Cascades changes from a new member mapping item through related mapping objects.

    Creates new:
    - Member Mapping
    - Variable Mapping if needed
    - Mapping definition

    Args:
        member_mapping_item: The source member mapping item
    """
    # Create mapping definition
    mapping_def = MAPPING_DEFINITION.objects.create(
        member_mapping_id=member_mapping_item.member_mapping_id,
        name=f"Generated mapping for {member_mapping_item.member_mapping_row}",
        code=f"GEN_MAP_{member_mapping_item.member_mapping_row}"
    )

def add_variable_to_mapping(mapping_id: str, variable_code: str, is_source_str: str) -> VARIABLE:
    """Adds a variable to an existing mapping.

    Args:
        mapping_id: Mapping identifier
        variable_code: Variable code to add
        is_source: Boolean indicating if source variable

    Returns:
        Created Variable object
    """
    logger.debug(f"Adding variable to mapping: {variable_code}")
    mapping_def = MAPPING_DEFINITION.objects.get(code=mapping_id)
    variable = VARIABLE.objects.get(code=variable_code)

    VARIABLE_MAPPING_ITEM.objects.create(
        variable_mapping_id=mapping_def.variable_mapping_id,
        variable_id=variable,
        is_source=is_source_str
    )
    return variable

def process_related_mappings(member_mapping: MEMBER_MAPPING_ITEM, mapping_def: MAPPING_DEFINITION, member_mapping_row: str) -> None:
    """Processes mappings related to a member mapping.

    Args:
        member_mapping: Member mapping object
        mapping_def: Mapping definition object
        member_mapping_row: Row identifier
    """
    logger.debug(f"Processing related mappings for row: {member_mapping_row}")
    related_mappings = MAPPING_DEFINITION.objects.filter(member_mapping_id=member_mapping)
    for rel_mapping in related_mappings:
        if rel_mapping != mapping_def:
            existing_items = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=member_mapping,
                member_mapping_row=member_mapping_row
            )
            for item in existing_items:
                MEMBER_MAPPING_ITEM.objects.update_or_create(
                    member_mapping_id=rel_mapping.member_mapping_id,
                    member_mapping_row=member_mapping_row,
                    variable_id=item.variable_id,
                    defaults={
                        'member_id': item.member_id,
                        'is_source': item.is_source
                    }
                )

def create_or_update_member(member_id: str, variable: VARIABLE, domain: Any) -> MEMBER:
    """Creates or updates a member object.

    Args:
        member_id: Member identifier
        variable: Variable object
        domain: Domain object

    Returns:
        Created or updated Member object
    """
    logger.debug(f"Creating or updating member: {member_id}")
    try:
        member = MEMBER.objects.get(code=member_id, domain_id=domain)
    except MEMBER.DoesNotExist:
        logger.info(f"Creating new member: {member_id}")
        member = MEMBER.objects.create(
            code=member_id,
            name=member_id,
            domain_id=domain
        )
    return member

def process_mapping_chain(variable: VARIABLE, mapping_def: MAPPING_DEFINITION) -> None:
    """Process a chain of related mappings starting from a variable.

    Args:
        variable: The source variable
        mapping_def: The mapping definition
    """
    logger.debug(f"Processing mapping chain for variable: {variable.code}")
    var_items = get_filtered_var_items(mapping_def.variable_mapping_id.variable_mapping_id)
    source_target = get_source_target_vars(var_items[0])

    for target_var in source_target["target"]:
        cascade_member_mapping_changes(MEMBER_MAPPING_ITEM.objects.filter(
            variable_id__code=target_var.split("(")[1].strip(")"),
            is_source="true"
        ).first())

def get_source_variables():
    source_variables = {}
    for v in VARIABLE.objects.all():
        if v.maintenance_agency_id:
            if "EBA" == v.maintenance_agency_id.code:
                domain = v.domain_id
                domain_members = {}
                members = MEMBER.objects.filter(domain_id=domain)
                if len(members):
                    for m in members:
                        domain_members[m.member_id] = {
                            'code': m.code,
                            'name': m.name
                        }
                    source_variables[v.variable_id] = {
                        'domain': {
                            'id': domain.domain_id,
                            'code': domain.code,
                            'name': domain.name,
                            'members': domain_members
                        }
                    }
    return source_variables

# Get all available variables from reference framework - FINREF_REF
def get_reference_variables():
    reference_variables = {}
    for v in VARIABLE.objects.all():
        if v.maintenance_agency_id:
            if "REF" == v.maintenance_agency_id.code:
                domain = v.domain_id
                domain_members = {}
                members = MEMBER.objects.filter(domain_id=domain)
                if len(members):
                    for m in members:
                        domain_members[m.member_id] = {
                            'code': m.code,
                            'name': m.name
                        }
                    reference_variables[v.variable_id] = {
                        'domain': {
                            'id': domain.domain_id,
                            'code': domain.code,
                            'name': domain.name,
                            'members': domain_members
                        }
                    }
    return reference_variables
