class MemberHierarchyService:
    def __init__(self):
        self._member_list_cache = {}

    def prepare_node_dictionaries_and_lists(self, sdd_context):
        sdd_context.members_that_are_nodes = set()
        sdd_context.member_plus_hierarchy_to_child_literals = {}
        sdd_context.domain_to_hierarchy_dictionary = {}

        for node in sdd_context.member_hierarchy_node_dictionary.values():
            if node.parent_member_id and node.parent_member_id != '':
                sdd_context.members_that_are_nodes.add(node.parent_member_id)
                member_plus_hierarchy = f"{node.parent_member_id.member_id}:{node.member_hierarchy_id.member_hierarchy_id}"

                if member_plus_hierarchy not in sdd_context.member_plus_hierarchy_to_child_literals:
                    sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy] = [node.member_id]
                else:
                    if node.member_id not in sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy]:
                        sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy].append(node.member_id)

        for hierarchy in sdd_context.member_hierarchy_dictionary.values():
            domain_id = hierarchy.domain_id
            if domain_id not in sdd_context.domain_to_hierarchy_dictionary:
                sdd_context.domain_to_hierarchy_dictionary[domain_id] = []
            sdd_context.domain_to_hierarchy_dictionary[domain_id].append(hierarchy)

    def get_member_list_considering_hierarchies(self, sdd_context, member, member_hierarchy):
        cache_key = (member, member_hierarchy) if member else None
        if cache_key in self._member_list_cache:
            return self._member_list_cache[cache_key].copy()

        return_list = []
        is_node = self.is_member_a_node(sdd_context, member)

        if member is None:
            self._member_list_cache[cache_key] = []
            return []

        if not is_node:
            return_list.append(member)

        if member:
            for domain, hierarchy_list in sdd_context.domain_to_hierarchy_dictionary.items():
                if domain.domain_id == member.domain_id.domain_id:
                    for hierarchy in hierarchy_list:
                        hierarchy_id = hierarchy.member_hierarchy_id
                        temp_list = []
                        self.get_member_list_considering_hierarchy(sdd_context, member, hierarchy_id, temp_list)
                        for item in temp_list:
                            if item not in return_list:
                                return_list.append(item)

        self._member_list_cache[cache_key] = return_list
        return return_list.copy()

    def get_member_list_considering_hierarchy(self, sdd_context, member, hierarchy, member_list):
        key = f"{member.member_id}:{hierarchy}"
        try:
            child_members = sdd_context.member_plus_hierarchy_to_child_literals[key]
            for item in child_members:
                if item is not None and item not in member_list:
                    if not self.is_member_a_node(sdd_context, item):
                        member_list.append(item)
                    self.get_member_list_considering_hierarchy(sdd_context, item, hierarchy, member_list)
        except KeyError:
            pass
