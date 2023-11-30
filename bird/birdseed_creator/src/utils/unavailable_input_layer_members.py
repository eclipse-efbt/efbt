# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
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

import os

class MemberFinder(object):
    
    def get_unreachable_members(self, sdd_context, reporting_framework,reporting_framework_version ):
        '''
        Get the output layer cubes of a framework
        For each cube structure item in each cube:
        Get the variable of the item
        Get the list of members from the subdomain
        Check if the input layer has that member in any cube structure items with 
        that varaible. If not check if it has a child of that member in accordance
        with absolutely any hierarchy. 
        Ideally we can say hierarchies are a bit pointless if they have leaf members
        that are not in the input layer members.
        '''
        output_file = open(sdd_context.output_directory + os.sep + 'csv' +
                 os.sep + 'unfound_members.csv',
                 "a",  encoding='utf-8')

        output_file.write("member,problem,hierarchy,cube_structure\n")
        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:
            framework_version = cube_structure_item.cube_structure_id.version
            if framework_version in reporting_framework_version:              
                
                variable = cube_structure_item.variable_id 
                subdomain = cube_structure_item.subdomain_id
                if not(subdomain is None) and not(variable is None):
                    
                    for item in subdomain.items:
                        member = item.member_id

                        member_exists_in_input_layer = MemberFinder.member_exists_in_input_layer(self,sdd_context,member,variable)

                        if not (member_exists_in_input_layer):
                            member_exists_in_or_under_any_hierarchy= MemberFinder.member_exists_in_or_under_any_hierarchy(self,sdd_context,member,variable,cube_structure_item.cube_structure_id.cube_structure_id,output_file)
                            if not(member_exists_in_or_under_any_hierarchy):
                                print ( member.name + ",cannot find member in inputlayer or hierarchy,, " + cube_structure_item.cube_structure_id.cube_structure_id)    
                                output_file.write(member.name + ",cannot find member in inputlayer or hierarchy,, " + cube_structure_item.cube_structure_id.cube_structure_id + "\n")
                            # member_exists_in_or_under_a_hierarchy_node_useful_to_the_input_layer= MemberFinder.member_exists_in_or_under_a_hierarchy_node_useful_to_the_input_layer(self,sdd_context,member,variable)

                else:
                    pass


    def get_hierarchies_wich_unreachable_leaves(framework):
        pass

    def find_cube_with_cube_structure(self, sdd_context, cube_structure):
        for cube in sdd_context.cubes_structure_items.cubes:
            if cube.cube_structure_id == cube_structure:
                return cube
            
    def get_attribute_list_from_variable_set(self,variable_set):
        variable_list = []
        variable_set_items = variable_set.variable_set_items
        for item in variable_set_items:
            variable = item.variable_id
            variable_list.append(variable)
             
        return variable_list
    

    def member_exists_in_input_layer(self,sdd_context, member,variable):
        exists = False
        
        for cube_structure_item in sdd_context.cube_structure_items.cubeStructureItems:

            #framework_version = cube_structure_item.cube_structure_id.version
            if cube_structure_item.cube_structure_id.cube_structure_id.startswith('BIRD') and \
                cube_structure_item.cube_structure_id.cube_structure_id.endswith('_EIL_1'):
                    if cube_structure_item.variable_id.code == variable.code:
                        subdomain = cube_structure_item.subdomain_id
                        for item in subdomain.items:
                            input_layer_member = item.member_id
                            if input_layer_member.code == member.code:
                                exists = True

        return exists
    
    def member_exists_in_or_under_any_hierarchy(self,sdd_context,member,variable,cube_structure_name,output_file):
        exists = False
        domain_id = member.domain_id.domain_id
        for node in sdd_context.member_hierarchies.memberHierarchiesNodes:
            if node.member_hierarchy_id.domain_id.domain_id == domain_id:
                if node.member_id == member:
                    parent_member_name = "None"
                    if not(node.parent_member_id is None):
                        parent_member_name = node.parent_member_id.member_id
                    node_contains_only_bird_framework_children = MemberFinder.node_contains_only_bird_framework_children(self,sdd_context,node,variable,node.member_hierarchy_id)
                    if not(node_contains_only_bird_framework_children):
                        print(member.member_id + ",hierachy node does not contain only input layer leafs," + node.member_hierarchy_id.name + "," + cube_structure_name) 
                        output_file.write(member.member_id + ",hierachy node does not contain only input layer leafs," + node.member_hierarchy_id.name + "," + cube_structure_name + "\n") 
                    else:
                        pass

                    exists = True
        return exists
    
    def node_contains_only_bird_framework_children(self,sdd_context,the_node, variable, member_hierarchy):
        contains_only_bird_framework_children = True
        child_list = []
        # get all child nodes transitivley
        # checkif they their members exist in the input layer
        # 
        for node in sdd_context.member_hierarchies.memberHierarchiesNodes:
            if node.member_hierarchy_id == member_hierarchy:
                #if MemberFinder.node_is_child_of_member(self,context,sdd_context,node,the_node.member_id,hierarchy):
                if node.parent_member_id == the_node.member_id:
                    node_is_in_bird_framework = MemberFinder.member_exists_in_input_layer(self,sdd_context,node.member_id,variable)
                    if not(node_is_in_bird_framework):
                        # check that it is a leaf, if it is a leaf set contains_only_bird_framework_children = False
                        if not (MemberFinder.member_has_children_in_hierarchy(self,sdd_context,node.member_id,member_hierarchy)):
                            contains_only_bird_framework_children = False
                        if node.member_id is None:
                            pass #print("node.member_id is None")
                        else:
                            pass # print(node.member_id.member_id + "is not in BIRD framework")

                    child_list.append(node)
                    again = True
                    try:
                        again = MemberFinder.node_contains_only_bird_framework_children(self,sdd_context,node,variable,member_hierarchy)
                    except RecursionError:
                        text = "None"
                        if not( node.member_id is None):
                            text = node.member_id.member_id
                        print("RecursionError for " + text + " : " + member_hierarchy.member_hierarchy_id)

                    if not(again):
                        contains_only_bird_framework_children = False
        return contains_only_bird_framework_children


    def node_is_child_of_member(self,context,sdd_context,node,member_id,hierarchy):
        parent_member_id = node.parent_member_id
        if parent_member_id is None:
            return False
        elif parent_member_id.member_id == member_id:
            return True
        else:
            parent_node = CombinationsToReportFilters.find_member_node(self,sdd_context,parent_member_id,hierarchy)
            return CombinationsToReportFilters.node_is_child_of_member(self,context,sdd_context,parent_node,member_id,hierarchy)

    def member_has_children_in_hierarchy(self,sdd_context,member_id,member_hierarchy):
        has_children = False
        for node in sdd_context.member_hierarchies.memberHierarchiesNodes:
            if node.member_hierarchy_id == member_hierarchy:                
                if node.parent_member_id == member_id:
                    has_children = True
        return has_children
        
    

                             

                

                
            
                

