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

"""Import parent members with children from CSV file."""

import csv
import os
from pybirdai.models.bird_meta_data_model import MEMBER
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_member_hierarchy_with_id, find_member_with_id, find_maintenance_agency_with_id
from .warning_writers import save_missing_children_to_csv
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_parent_members_with_children(context):
    """
    Import all parent members with children locally.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    print("Creating all parent members with children locally")
    parent_members = set()  # Using set for faster lookups
    parent_members_to_create = []
    parent_members_child_triples = []
    missing_children = []

    # Pre-fetch all hierarchies for faster lookup
    hierarchy_cache = {}

    with open(os.path.join(context.file_directory, "technical_export", "member_hierarchy_node.csv"), encoding='utf-8') as csvfile:
        header_skipped = False
        id_increment = 0
        for row in csv.reader(csvfile):
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                parent_member_id = row[ColumnIndexes().member_hierarchy_node_parent_member_id + id_increment]
                member_id = row[ColumnIndexes().member_hierarchy_node_member_id + id_increment]
                hierarchy_id = row[ColumnIndexes().member_hierarchy_node_hierarchy_id + id_increment]

                if not parent_member_id:
                    continue

                if hierarchy_id not in hierarchy_cache:
                    hierarchy_cache[hierarchy_id] = find_member_hierarchy_with_id(hierarchy_id, context)

                hierarchy = hierarchy_cache[hierarchy_id]
                if hierarchy:
                    domain = hierarchy.domain_id
                    parent_members_child_triples.append((parent_member_id, member_id, domain))
                    parent_members.add(parent_member_id)

    # Process parent-child relationships in batches
    for parent_member_id, member_id, domain in parent_members_child_triples:
        if member_id in parent_members:
            if not any(parent_member_id in d for d in (context.members_that_are_nodes,
                                                     context.member_dictionary,
                                                     context.member_dictionary)):
                parent_member = MEMBER(
                    name=replace_dots(parent_member_id),
                    member_id=replace_dots(parent_member_id),
                    maintenance_agency_id=find_maintenance_agency_with_id(context, "NODE"),
                    domain_id=domain
                )
                parent_members_to_create.append(parent_member)
                context.member_dictionary[parent_member.member_id] = parent_member
                if not (parent_member.domain_id is None) and not (parent_member.domain_id == ""):
                    context.member_id_to_domain_map[parent_member] = domain
                    context.member_id_to_member_code_map[parent_member.member_id] = parent_member.member_id

                context.members_that_are_nodes[parent_member_id] = parent_member
        else:
            member = find_member_with_id(member_id, context)
            if member is None:
                missing_children.append((parent_member_id, member_id))
            elif not any(parent_member_id in d for d in (context.members_that_are_nodes,
                                                      context.member_dictionary,
                                                      context.member_dictionary)):
                parent_member = MEMBER(
                    name=replace_dots(parent_member_id),
                    member_id=replace_dots(parent_member_id),
                    maintenance_agency_id=find_maintenance_agency_with_id(context, "NODE"),
                    domain_id=domain
                )
                parent_members_to_create.append(parent_member)
                context.members_that_are_nodes[parent_member_id] = parent_member

                context.member_dictionary[parent_member.member_id] = parent_member
                if not (parent_member.domain_id is None) and not (parent_member.domain_id == ""):
                    context.member_id_to_domain_map[parent_member] = domain
                    context.member_id_to_member_code_map[parent_member.member_id] = parent_member.member_id

    if context.save_sdd_to_db and parent_members_to_create:
        MEMBER.objects.bulk_create(parent_members_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

    save_missing_children_to_csv(context, missing_children)
