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

"""Lookup functions for finding SDD model entities in context dictionaries."""

from pybirdai.models.bird_meta_data_model import MEMBER


def find_member_mapping_with_id(context, member_mapping_id):
    """
    Find an existing member mapping with this id.

    Args:
        context: SDDContext containing member_mapping_dictionary
        member_mapping_id: ID of member mapping to find

    Returns:
        MEMBER_MAPPING instance or None
    """
    try:
        return context.member_mapping_dictionary[member_mapping_id]
    except KeyError:
        return None


def find_member_with_id(element_id, context):
    """
    Find an existing member with this id.

    Args:
        element_id: ID of member to find
        context: SDDContext containing member dictionaries

    Returns:
        MEMBER instance or None
    """
    try:
        return context.member_dictionary[element_id]
    except:
        try:
            return context.member_dictionary[element_id]
        except KeyError:
            try:
                return context.members_that_are_nodes[element_id]
            except KeyError:
                return None


def find_member_hierarchy_with_id(element_id, context):
    """
    Find an existing member hierarchy with this id.

    Args:
        element_id: ID of member hierarchy to find
        context: SDDContext containing member_hierarchy_dictionary

    Returns:
        MEMBER_HIERARCHY instance or None
    """
    try:
        return context.member_hierarchy_dictionary[element_id]
    except KeyError:
        return None


def find_variable_with_id(context, element_id):
    """
    Find an existing variable with this id.

    Args:
        context: SDDContext containing variable_dictionary
        element_id: ID of variable to find

    Returns:
        VARIABLE instance or None
    """
    try:
        return context.variable_dictionary[element_id]
    except KeyError:
        try:
            return context.variable_dictionary[element_id]
        except KeyError:
            return None


def find_maintenance_agency_with_id(context, element_id):
    """
    Find an existing maintenance agency with this id.

    Args:
        context: SDDContext containing agency_dictionary
        element_id: ID of maintenance agency to find

    Returns:
        MAINTENANCE_AGENCY instance or None
    """
    try:
        return context.agency_dictionary[element_id]
    except KeyError:
        return None


def find_domain_with_id(context, element_id):
    """
    Find an existing domain with this id.

    Args:
        context: SDDContext containing domain_dictionary
        element_id: ID of domain to find

    Returns:
        DOMAIN instance or None
    """
    try:
        return context.domain_dictionary[element_id]
    except KeyError:
        try:
            return_item = context.domain_dictionary[element_id]
            return return_item
        except KeyError:
            return None


def find_table_with_id(context, table_id):
    """
    Get the report table with the given id.

    Args:
        context: SDDContext containing report_tables_dictionary
        table_id: ID of table to find

    Returns:
        TABLE instance or None
    """
    try:
        return context.report_tables_dictionary[table_id]
    except KeyError:
        return None


def find_axis_with_id(context, axis_id):
    """
    Get the axis with the given id.

    Args:
        context: SDDContext containing axis_dictionary
        axis_id: ID of axis to find

    Returns:
        AXIS instance or None
    """
    try:
        return context.axis_dictionary[axis_id]
    except KeyError:
        return None


def find_table_cell_with_id(context, table_cell_id):
    """
    Get the table cell with the given id.

    Args:
        context: SDDContext containing table_cell_dictionary
        table_cell_id: ID of table cell to find

    Returns:
        TABLE_CELL instance or None
    """
    try:
        return context.table_cell_dictionary[table_cell_id]
    except KeyError:
        return None


def find_axis_ordinate_with_id(context, axis_ordinate_id):
    """
    Get the existing ordinate with the given id.

    Args:
        context: SDDContext containing axis_ordinate_dictionary
        axis_ordinate_id: ID of axis ordinate to find

    Returns:
        AXIS_ORDINATE instance or None
    """
    try:
        return context.axis_ordinate_dictionary[axis_ordinate_id]
    except KeyError:
        return None


def find_variable_mapping_with_id(context, variable_mapping_id):
    """
    Get the variable mapping with the given id.

    Args:
        context: SDDContext containing variable_mapping_dictionary
        variable_mapping_id: ID of variable mapping to find

    Returns:
        VARIABLE_MAPPING instance or None
    """
    try:
        return context.variable_mapping_dictionary[variable_mapping_id]
    except KeyError:
        return None


def find_mapping_definition_with_id(context, mapping_definition_id):
    """
    Get the mapping definition with the given id.

    Args:
        context: SDDContext containing mapping_definition_dictionary
        mapping_definition_id: ID of mapping definition to find

    Returns:
        MAPPING_DEFINITION instance or None
    """
    try:
        return context.mapping_definition_dictionary[mapping_definition_id]
    except KeyError:
        return None


def find_member_with_id_for_hierarchy(member_id, hierarchy, context):
    """
    Find a member within a specific hierarchy's domain.

    Args:
        member_id: ID of member to find
        hierarchy: MEMBER_HIERARCHY instance
        context: SDDContext

    Returns:
        MEMBER instance or None
    """
    domain = hierarchy.domain_id
    member = MEMBER.objects.filter(domain_id=domain, member_id=member_id).first()
    return member
