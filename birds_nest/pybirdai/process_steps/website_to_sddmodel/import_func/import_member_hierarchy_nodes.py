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

"""Import member hierarchy nodes from CSV file."""

import os
import csv
from collections import defaultdict
from pybirdai.models.bird_meta_data_model import MEMBER, MEMBER_HIERARCHY_NODE
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .lookups import find_member_hierarchy_with_id
from .warning_writers import save_missing_members_to_csv, save_missing_hierarchies_to_csv
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_member_hierarchy_nodes(context):
    """
    Import all member hierarchy nodes from CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = context.file_directory + os.sep + "technical_export" + os.sep + "member_hierarchy_node.csv"
    header_skipped = False
    missing_members = []
    missing_hierarchies = []
    nodes_to_create = []

    # Pre-fetch all members to avoid N+1 queries (performance optimization)
    # Build a cache: domain_id -> {member_id -> member_object}
    member_cache = defaultdict(dict)
    all_members = MEMBER.objects.select_related('domain_id').all()
    for m in all_members:
        if m.domain_id:
            member_cache[m.domain_id.domain_id][m.member_id] = m
        # Also add to general cache for parent member lookup
        context.member_dictionary[m.member_id] = m

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        id_increment = 0
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                hierarchy_id = row[ColumnIndexes().member_hierarchy_node_hierarchy_id + id_increment]
                member_id = row[ColumnIndexes().member_hierarchy_node_member_id + id_increment]
                parent_member_id = row[ColumnIndexes().member_hierarchy_node_parent_member_id + id_increment]
                node_level = row[ColumnIndexes().member_hierarchy_node_level + id_increment]
                comparator = row[ColumnIndexes().member_hierarchy_node_comparator + id_increment]
                operator = row[ColumnIndexes().member_hierarchy_node_operator + id_increment]
                valid_from = row[ColumnIndexes().member_hierarchy_node_valid_from + id_increment]
                valid_to = row[ColumnIndexes().member_hierarchy_node_valid_to + id_increment]

                hierarchy = find_member_hierarchy_with_id(hierarchy_id, context)
                if hierarchy is None:
                    print(f"Hierarchy {hierarchy_id} not found")
                    missing_hierarchies.append(hierarchy_id)
                else:
                    # Use cache instead of database query (performance optimization)
                    domain_id = hierarchy.domain_id.domain_id if hierarchy.domain_id else None
                    member = member_cache.get(domain_id, {}).get(member_id, None)
                    if member is None:
                        print(f"Member {member_id} not found in the database for hierarchy {hierarchy_id}")
                        missing_members.append((hierarchy_id, member_id))
                    else:
                        # Parent member lookup now uses pre-populated context.member_dictionary
                        parent_member = context.member_dictionary.get(parent_member_id, None)
                        if not (parent_member is None):
                            hierarchy_node = MEMBER_HIERARCHY_NODE()
                            hierarchy_node.member_hierarchy_id = hierarchy
                            hierarchy_node.comparator = comparator
                            hierarchy_node.operator = operator
                            hierarchy_node.member_id = member
                            hierarchy_node.level = int(node_level)
                            hierarchy_node.parent_member_id = parent_member
                            nodes_to_create.append(hierarchy_node)
                            context.member_hierarchy_node_dictionary[hierarchy_id + ":" + member_id] = hierarchy_node

    if context.save_sdd_to_db and nodes_to_create:
        MEMBER_HIERARCHY_NODE.objects.bulk_create(nodes_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

    save_missing_members_to_csv(context, missing_members)
    save_missing_hierarchies_to_csv(context, missing_hierarchies)
