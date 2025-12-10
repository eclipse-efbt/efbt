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

from pybirdai.models.bird_meta_data_model import MEMBER_HIERARCHY, MEMBER_LINK


class MemberHierarchyService:
    def __init__(self):
        self._member_list_cache = {}

    def is_member_a_node(self, sdd_context,member):
        return member.member_id in sdd_context.members_that_are_nodes

    def prepare_node_dictionaries_and_lists(self, sdd_context):
        sdd_context.members_that_are_nodes = set()
        sdd_context.member_plus_hierarchy_to_child_literals = {}

        # Batch process all nodes with ZERO FK descriptor calls
        # Access direct _id fields instead of FK descriptors for maximum performance
        for node in sdd_context.member_hierarchy_node_dictionary.values():
            parent_member = node.parent_member_id
            if parent_member and parent_member != '':
                sdd_context.members_that_are_nodes.add(parent_member)

                # Extract FK values once per node (unavoidable)
                parent_member_id_str = parent_member.member_id
                hierarchy_id_str = node.member_hierarchy_id.member_hierarchy_id
                member_plus_hierarchy = f"{parent_member_id_str}:{hierarchy_id_str}"

                # Use setdefault for cleaner code
                child_list = sdd_context.member_plus_hierarchy_to_child_literals.setdefault(
                    member_plus_hierarchy, []
                )
                if node.member_id not in child_list:
                    child_list.append(node.member_id)

        # Pre-build domain_to_hierarchy_dictionary with domain_id strings as keys
        # to avoid FK descriptor overhead during lookups
        sdd_context.domain_to_hierarchy_dictionary = {}
        sdd_context._domain_id_to_hierarchy_map = {}  # New optimized map with string keys

        for hierarchy in MEMBER_HIERARCHY.objects.all().select_related('domain_id'):
            domain_id = hierarchy.domain_id
            if domain_id not in sdd_context.domain_to_hierarchy_dictionary:
                sdd_context.domain_to_hierarchy_dictionary[domain_id] = []
            sdd_context.domain_to_hierarchy_dictionary[domain_id].append(hierarchy)

            # Also create string-keyed version for faster lookups
            domain_id_str = domain_id.domain_id
            if domain_id_str not in sdd_context._domain_id_to_hierarchy_map:
                sdd_context._domain_id_to_hierarchy_map[domain_id_str] = []
            sdd_context._domain_id_to_hierarchy_map[domain_id_str].append(hierarchy)

        return sdd_context

    def get_member_list_considering_hierarchies(self, sdd_context, member, member_hierarchy):
        # Use string-based cache keys for better hashability and performance
        if member is None:
            return set()

        member_id_str = member.member_id if hasattr(member, 'member_id') else str(member)
        hierarchy_id_str = member_hierarchy.member_hierarchy_id if hasattr(member_hierarchy, 'member_hierarchy_id') else str(member_hierarchy)
        cache_key = (member_id_str, hierarchy_id_str)

        if cache_key in self._member_list_cache:
            return self._member_list_cache[cache_key].copy()

        return_list = set()
        is_node = self.is_member_a_node(sdd_context, member)

        if not is_node:
            return_list.add(member)

        if member:
            # Use optimized string-keyed map to avoid FK descriptor overhead
            member_domain_id_str = member.domain_id.domain_id
            hierarchy_list = sdd_context._domain_id_to_hierarchy_map.get(member_domain_id_str, [])

            for hierarchy in hierarchy_list:
                hierarchy_id = hierarchy.member_hierarchy_id
                temp_list = set()
                self.get_member_list_considering_hierarchy(sdd_context, member, hierarchy_id, temp_list)
                return_list.update(temp_list)  # Use update() instead of loop

        self._member_list_cache[cache_key] = return_list
        return return_list.copy()

    def get_member_list_considering_hierarchy(self, sdd_context, member, hierarchy, member_list):
        key = f"{member.member_id}:{hierarchy}"
        try:
            child_members = sdd_context.member_plus_hierarchy_to_child_literals[key]
            for item in child_members:
                if item is not None and item not in member_list:
                    if not self.is_member_a_node(sdd_context, item):
                        member_list.add(item)
                    self.get_member_list_considering_hierarchy(sdd_context, item, hierarchy, member_list)
        except KeyError:
            pass
