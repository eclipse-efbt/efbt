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

from pybirdai.utils.utils import Utils

import os


class CreateExecutableFilters:
    def create_executable_filters(self,context, sdd_context):
        CreateExecutableFilters.prepare_node_dictionaries_and_lists(self,sdd_context)
        file = open(sdd_context.output_directory + os.sep + 'generated_python' + os.sep +  'report_cells.py', "a",  encoding='utf-8') 
        report_html_file = open(sdd_context.output_directory + os.sep + 'generated_html' + os.sep +  'report_templates.html', "a",  encoding='utf-8') 
        report_html_file.write("{% extends 'base.html' %}\n")
        report_html_file.write("{% block content %}\n")
        report_html_file.write("<!DOCTYPE html>\n")
        report_html_file.write("<html>\n")
        report_html_file.write("<head>\n")
        report_html_file.write("<title>Report Templates</title>\n")
        report_html_file.write("</head>\n")
        report_html_file.write("<body>\n")
        report_html_file.write("<h1>Report Templates</h1>\n")
        report_html_file.write("<table border=\"1\">\n") 

       
   
        file.write("from pybirdai.ldm_models import *\n")
        file.write("from .output_tables import *\n")
        file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")

        
        for cube_id, combination_list in sdd_context.combination_to_rol_cube_map.items():
            report_html_file.write("<tr><td><a href=\"{% url 'pybirdai:show_report' '" + cube_id +'.html' + "'%}\">" +cube_id + "</a></td></tr>\n")   
            filter_html_file = open(sdd_context.output_directory + os.sep + 'generated_html' + os.sep +  cube_id +'.html', "a",  encoding='utf-8') 
            
            filter_html_file.write("{% extends 'base.html' %}\n")
            filter_html_file.write("{% block content %}\n")
            filter_html_file.write("<!DOCTYPE html>\n")
            filter_html_file.write("<html>\n")
            filter_html_file.write("<head>\n")
            filter_html_file.write("<title>Execute Datapoints</title>\n")
            filter_html_file.write("</head>\n")
            filter_html_file.write("<body>\n")
            filter_html_file.write("<h1>" + cube_id + "</h1>\n")
            filter_html_file.write("<table border=\"1\">\n")
            

            for combination in combination_list:
                if combination.combination_id.metric:
                    filter_html_file.write("<tr><td><a href=\"{% url 'pybirdai:execute_data_point' '" + cube_id + "_" + combination.combination_id.combination_id + "'%}\">" + cube_id + "_" + combination.combination_id.combination_id + "</a></td></tr>\n")
                    
                    
                    file.write("class Cell_" +  cube_id + "_" + combination.combination_id.combination_id + ":\n")
                    file.write("\t" + cube_id + "_Table = None\n")
                    file.write("\t" + cube_id + "s = []\n")
                    file.write("\tdef metric_value(self):\n"  )
                    file.write("\t\ttotal = 0\n")
                    file.write("\t\tfor item in self." + cube_id + "s:\n")
                    file.write("\t\t\ttotal += item." + combination.combination_id.metric.name + "()\n")
                    file.write("\t\treturn total\n")
                    file.write("\tdef calc_referenced_items(self):\n")
                    file.write("\t\titems = self." + cube_id + "_Table." + cube_id + "s\n")
                    file.write("\t\tfor item in items:\n")
                    file.write("\t\t\tfilter_passsed = False\n")
                    combination_item_list = []
                    try:
                        combination_item_list =  sdd_context.combination_item_dictionary[combination.combination_id.combination_id]
                    except:
                        pass
                    for combination_item in combination_item_list:
                        leaf_node_members = CreateExecutableFilters.get_leaf_node_codes(self,
                                                                                      sdd_context,
                                                                                      combination_item.member_id,
                                                                                      combination_item.member_hierarchy)
                        
                        file.write("\t\t\tif ")
                        for leaf_node_member in leaf_node_members:
                            file.write("\t\t\t\t(item." + combination_item.variable_id.name + "() == '" + str(leaf_node_member.code) + "')  or\\\\ :\n")
                        file.write("\t\t\t\tFalse\n")
                        file.write("\t\t\t\tfilter_passed = True\n")
                        file.write("\t\t\tif filter_passed:\n")
                        file.write("\t\t\t\tself." + cube_id + "s.append(item)\n")
                    file.write("\tdef init(self):\n")
                    file.write("\t\tOrchestration().init(self)\n")
                    file.write("\t\tself.calc_referenced_items()\n")
                    file.write("\t\treturn None\n")
            
            
            filter_html_file.write("</table>\n")
            filter_html_file.write("</body>\n")
            filter_html_file.write("</html>\n")
            filter_html_file.write("</table>\n")
            filter_html_file.write("</body>\n")
            filter_html_file.write("</html>\n")
            filter_html_file.write("{% endblock %}\n")
        report_html_file.write("</table>\n")
        report_html_file.write("</body>\n")
        report_html_file.write("</html>\n")
        report_html_file.write("</table>\n")
        report_html_file.write("</body>\n")
        report_html_file.write("</html>\n")
        report_html_file.write("{% endblock %}\n")


    def get_leaf_node_codes(self,sdd_context, member,member_hierarchy  ):
        return_list = []
        if member is not None:
            return_list = CreateExecutableFilters.get_member_list_considering_hierarchies(self,sdd_context,member,member_hierarchy)
        return return_list

    

    def get_literal_list_considering_hierarchies(self,context,sdd_context,literal,member,domain_id, warning_list, template_code,combination_id, variable_id,framework,cube_type,input_cube_type):
        return_list = []
        is_node = CreateExecutableFilters.is_member_a_node(self,sdd_context,member)
        if literal is None:
            
            if not (is_node):
                warning_list.append( ("error", "member does not exist in input layer and is not a node", template_code,combination_id, variable_id, member.member_id, None,domain_id))
            pass
        else:
            if not (is_node):
                return_list = [literal]
            
        for domain,hierarchy_list in sdd_context.domain_to_hierarchy_dictionary.items():
            if domain is None:
                print("domain is None in hierarchy_list")
            if domain.domain_id == domain_id:
                for hierarchy in hierarchy_list:
                    hierarchy_id = hierarchy.member_hierarchy_id                
                    literal_list = []
                    CreateExecutableFilters.get_literal_list_considering_hierarchy(self,context,sdd_context,member,hierarchy_id,literal_list,framework,cube_type,input_cube_type)
                    return_list.extend(literal_list)
                    
        if len(return_list) == 0:
            warning_list.append( ("error","could not find any input layer members or sub members for member", template_code,combination_id, variable_id, member.member_id, None,domain_id))
        return return_list     

    
                

    def get_literal_list_considering_hierarchy(self,context,sdd_context,member,hierarchy,literal_list,framework,cube_type,input_cube_type):
        
        key = member.member_id + ":" + hierarchy
        child_members = []
        try:
            child_members = sdd_context.member_plus_hierarchy_to_child_literals[key]
            for item in child_members:
                if item.domain_id is None:
                    print("domain_id is None for " + item.member_id)
                if item is None:
                    print("item is None for " + item.member_id)
                literal = item
                if not(literal is None):
                    if not(literal in literal_list):
                        is_node = CreateExecutableFilters.is_member_a_node(self,sdd_context,literal)
                        if not (is_node):
                            literal_list.append(literal)
                    
            for item in child_members:
                CreateExecutableFilters.get_literal_list_considering_hierarchy(self,context,sdd_context,item,hierarchy, literal_list,framework,cube_type,input_cube_type)
        except KeyError:
            pass
        
            
    def is_member_a_node(self,sdd_context,member):
        if member in sdd_context.members_that_are_nodes:
            return True
        else:
            return False

        
    def find_member_node(self,sdd_context,member_id,hierarchy):
        try:
            return sdd_context.member_hierarchy_node_dictionary[hierarchy + ":" + member_id.member_id]
        except:
            pass

    def prepare_node_dictionaries_and_lists (self,sdd_context):
        
        sdd_context.members_that_are_nodes = []
        sdd_context.member_plus_hierarchy_to_child_literals = {}
        sdd_context.domain_to_hierarchy_dictionary = {}
        for node in sdd_context.member_hierarchy_node_dictionary.values():
            if not (node.parent_member_id is None) and not (node.parent_member_id == ''):
                sdd_context.members_that_are_nodes.append(node.parent_member_id)
                member_plus_hierarchy = node.parent_member_id.member_id + ":" +  node.member_hierarchy_id.member_hierarchy_id
                # ad the parent node to the dictionary of nodes that have children
                if not(member_plus_hierarchy in sdd_context.member_plus_hierarchy_to_child_literals.keys() ):
                    initial_member_list = []
                    initial_member_list.append(node.member_id)
                    sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy] = initial_member_list
                else:
                    member_list =  sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy]
                    if not(node.member_id in member_list):
                            member_list.append(node.member_id)
                            
        for hierarchy in sdd_context.member_hierarchy_dictionary.values():
            try:
                hierarchy_list = sdd_context.domain_to_hierarchy_dictionary[hierarchy.domain_id]
                hierarchy_list.append(hierarchy)
            except KeyError:
                hierarchy_list = []
                hierarchy_list.append(hierarchy)
                sdd_context.domain_to_hierarchy_dictionary[hierarchy.domain_id] = hierarchy_list

    def get_member_list_considering_hierarchies(self,sdd_context,member,member_hierarchy):
        return_list = []
        is_node = CreateExecutableFilters.is_member_a_node(self,sdd_context,member)
        if member is None:
            pass
        else:
            if not (is_node):
                return_list = [member]
        if member:   
            print ("member" + str(member))  
            for domain,hierarchy_list in sdd_context.domain_to_hierarchy_dictionary.items():
                if domain is None:
                    print ("the domain is none")
                elif domain.domain_id == member.domain_id.domain_id:
                    for hierarchy in hierarchy_list:
                        hierarchy_id = hierarchy.member_hierarchy_id                
                        member_list = []
                        CreateExecutableFilters.get_member_list_considering_hierarchy(self,sdd_context,member,hierarchy_id,member_list)
                        return_list.extend(member_list)

        return return_list     

    def get_member_list_considering_hierarchy(self,sdd_context,member,hierarchy,member_list):
        
        key = member.member_id + ":" + hierarchy
        child_members = []
        try:
            child_members = sdd_context.member_plus_hierarchy_to_child_literals[key]
            for item in child_members:
                member = item
                if not(member is None):
                    if not(member in member_list):
                        is_node = CreateExecutableFilters.is_member_a_node(self,sdd_context,member)
                        if not (is_node):
                            member_list.append(member)
                    
            for item in child_members:
                CreateExecutableFilters.get_member_list_considering_hierarchy(self,sdd_context,item,hierarchy, member_list)
        except KeyError:
            pass

