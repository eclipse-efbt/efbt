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

"""
Utility functions for SDD model import operations.

This module consolidates commonly used utility functions from lookups.py
and utilities.py for convenient importing in the modular import functions.
"""


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


def replace_dots(text):
    """
    Replace dots with underscores in the given text.

    Args:
        text: String to process

    Returns:
        String with dots replaced by underscores
    """
    return text.replace('.', '_')
