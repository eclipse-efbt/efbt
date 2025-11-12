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

"""Functions for writing warning CSV files when missing references are encountered."""

import os
import csv


def save_missing_domains_to_csv(context, missing_domains):
    """
    Save missing domain IDs to CSV file.

    Args:
        context: SDDContext containing output directory
        missing_domains: List of missing domain IDs
    """
    filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_domains.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(missing_domains)


def save_missing_members_to_csv(context, missing_members):
    """
    Save missing member references to CSV file.

    Args:
        context: SDDContext containing output directory
        missing_members: List of tuples (hierarchy_id, member_id)
    """
    filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_members.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(missing_members)


def save_missing_variables_to_csv(context, missing_variables):
    """
    Save missing variable references to CSV file.

    Args:
        context: SDDContext containing output directory
        missing_variables: List of missing variable IDs
    """
    filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_variables.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(missing_variables)


def save_missing_children_to_csv(context, missing_children):
    """
    Save missing children member references to CSV file.

    Args:
        context: SDDContext containing output directory
        missing_children: List of tuples (parent_member_id, member_id)
    """
    filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_children.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(missing_children)


def save_missing_hierarchies_to_csv(context, missing_hierarchies):
    """
    Save missing hierarchy IDs to CSV file.

    Args:
        context: SDDContext containing output directory
        missing_hierarchies: List of missing hierarchy IDs
    """
    filename = context.output_directory + os.sep + "generated_hierarchy_warnings" + os.sep + "missing_hierarchies.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(missing_hierarchies)


def save_missing_mapping_variables_to_csv(context, missing_variables):
    """
    Save missing mapping variable references to CSV file.

    Args:
        context: SDDContext containing output directory
        missing_variables: List of tuples (variable_id, mapping_id, valid_to)
    """
    filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_missing_variables.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Varaible", "Mapping", "Valid_to"])
        for var in missing_variables:
            writer.writerow([var[0], var[1], var[2]])


def save_missing_mapping_members_to_csv(context, missing_members):
    """
    Save missing mapping member references to CSV file.

    Args:
        context: SDDContext containing output directory
        missing_members: List of tuples (member_id, mapping_id, row_number, variable_id)
    """
    filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_missing_members.csv"
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Member", "Mapping", "Row", "Variable"])
        for mem in missing_members:
            writer.writerow([mem[0], mem[1], mem[2], mem[3]])

    create_mappings_warnings_summary(context, missing_members)


def create_mappings_warnings_summary(context, missing_members):
    """
    Create summary CSV of all mapping warnings (both variables and members).

    Args:
        context: SDDContext containing output directory
        missing_members: List of tuples (member_id, mapping_id, row_number, variable_id)
    """
    filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_warnings_summary.csv"
    # create a list of unique missing variable ids
    # read mappings_missing_variables file into a dictionary
    missing_variables = []
    written_members = []
    variables_filename = context.output_directory + os.sep + "generated_mapping_warnings" + os.sep + "mappings_missing_variables.csv"
    with open(variables_filename, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row[0] not in missing_variables:
                missing_variables.append(row[0])

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Variable", "Member"])
        for var in missing_variables:
            writer.writerow([var, ''])

        for mem in missing_members:
            variable = mem[3]
            member = mem[0]
            if member not in written_members:
                if variable not in missing_variables:
                    writer.writerow([variable, member])
                written_members.append(member)
