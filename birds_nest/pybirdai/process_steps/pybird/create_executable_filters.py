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
from django.conf import settings
from pybirdai.process_steps.pybird.orchestration import Orchestration
from pybirdai.models import Trail, MetaDataTrail, DerivedTable, FunctionText, TableCreationFunction
from datetime import datetime
from pybirdai.process_steps.pybird.typ_instrmnt_mapping import TypInstrmntMapper

import os


class CreateExecutableFilters:
    def __init__(self):
        # Keep caching for performance
        self._node_cache = {}
        self._member_list_cache = {}
        self._literal_list_cache = {}
        # Initialize TYP_INSTRMNT mapper
        self.typ_mapper = TypInstrmntMapper()

    def is_member_a_node(self, sdd_context, member):
        # Keep the member node caching
        if member not in self._node_cache:
            self._node_cache[member] = member in sdd_context.members_that_are_nodes
        return self._node_cache[member]
    
    def get_typ_instrmnt_values_for_combination(self, sdd_context, combination):
        """Extract TYP_INSTRMNT values from combination items"""
        typ_instrmnt_values = []
        combination_item_list = []
        
        try:
            combination_item_list = sdd_context.combination_item_dictionary[combination.combination_id.combination_id]
        except:
            pass
        
        for combination_item in combination_item_list:
            # Check if this is a TYP_INSTRMNT variable
            if combination_item.variable_id.name == 'TYP_INSTRMNT':
                # Get the original member value before leaf node expansion
                original_member = combination_item.member_id
                if original_member:
                    typ_instrmnt_values.append(original_member.member_id)
        
        return typ_instrmnt_values
    
    def get_product_classes_for_combination(self, sdd_context, combination, cube_id):
        """Get product-specific class names based on TYP_INSTRMNT values"""
        typ_instrmnt_values = self.get_typ_instrmnt_values_for_combination(sdd_context, combination)
        product_classes = []
        
        for typ_value in typ_instrmnt_values:
            # Get slice names from mapping
            slice_names = self.typ_mapper.get_slices_for_typ_instrmnt(typ_value)
            
            for slice_name in slice_names:
                # Format as class name
                class_name = self.typ_mapper.format_slice_name_for_class(slice_name, cube_id)
                if class_name not in product_classes:
                    product_classes.append(class_name)
        
        if not product_classes:
            print(f"WARNING: No TYP_INSTRMNT found for combination {combination.combination_id.combination_id}")
        
        return product_classes

    def create_executable_filters(self, context, sdd_context):
        CreateExecutableFilters.delete_generated_python_filter_files(self, context)
        CreateExecutableFilters.delete_generated_html_filter_files(self, context)
        CreateExecutableFilters.prepare_node_dictionaries_and_lists(self, sdd_context)

        # Initialize AORTA tracking
        orchestration = Orchestration()
        if hasattr(context, 'enable_lineage_tracking') and context.enable_lineage_tracking:
            orchestration.init_with_lineage(self, f"Filter_Generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            print("AORTA lineage tracking enabled for filter generation")
        
        file = open(sdd_context.output_directory + os.sep + 'generated_python_filters' + os.sep + 'report_cells.py', "a", encoding='utf-8')
        report_html_file = open(sdd_context.output_directory + os.sep + 'generated_html' + os.sep + 'report_templates.html', "a", encoding='utf-8')
        
        # Write HTML headers
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
        report_html_file.write("<a href=\"{% url 'pybirdai:step_by_step_mode'%}\">Back to the PyBIRD AI Home Page</a>\n")

        # Write Python imports
        file.write("from pybirdai.models.bird_data_model import *\n")
        file.write("# Note: output_tables.py no longer needed - using direct product-specific classes\n")
        file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")
        file.write("from pybirdai.annotations.decorators import lineage\n")
        
        # Import all logic files that contain product-specific classes
        logic_files = set()
        for cube_id in sdd_context.combination_to_rol_cube_map.keys():
            logic_files.add(f"{cube_id}_logic")
        
        file.write("\n# Import product-specific classes\n")
        for logic_file in sorted(logic_files):
            file.write(f"from .{logic_file} import *\n")
        
        file.write("\n")

        # Create a copy of combination_to_rol_cube_map which is ordered by cube_id
        cube_ids = sorted(sdd_context.combination_to_rol_cube_map.keys())
        for cube_id in cube_ids:
            combination_list = sdd_context.combination_to_rol_cube_map[cube_id]

            report_html_file.write("<tr><td><a href=\"{% url 'pybirdai:show_report' '" + cube_id + '.html' + "'%}\">" + cube_id + "</a></td></tr>\n")
            filter_html_file = open(sdd_context.output_directory + os.sep + 'generated_html' + os.sep + cube_id + '.html', "a", encoding='utf-8')

            # Write filter HTML header
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
            filter_html_file.write("<a href=\"{% url 'pybirdai:report_templates'%}\">Back to the PyBIRD Reports Templates Page</a>\n")

            for combination in combination_list:
                if combination.combination_id.metric:
                    filter_html_file.write("<tr><td><a href=\"{% url 'pybirdai:execute_data_point' '" + 
                                         combination.combination_id.combination_id + "'%}\">" + 
                                         cube_id + "_" + combination.combination_id.combination_id + 
                                         "</a></td></tr>\n")
                    
                    # Get product-specific classes for this combination
                    product_classes = self.get_product_classes_for_combination(sdd_context, combination, cube_id)
                    
                    # Generate the class
                    file.write("class Cell_" + combination.combination_id.combination_id + ":\n")
                    
                    # Create attributes for each product-specific class
                    for product_class in product_classes:
                        file.write("\t" + product_class + " = None\n")
                    
                    file.write("\t" + cube_id + "s = []\n")
                    
                    # Write metric_value method
                    file.write("\t@lineage(dependencies={\"" + cube_id + "." + combination.combination_id.metric.name + "\"})\n")
                    file.write("\tdef metric_value(self):\n")
                    file.write("\t\ttotal = 0\n")
                    file.write("\t\tfor item in self." + cube_id + "s:\n")
                    file.write("\t\t\ttotal += item." + combination.combination_id.metric.name + "()\n")
                    file.write("\t\treturn total\n")
                    
                    # Build calc_referenced_items method
                    calc_string = ''
                    calc_lineage_string = '\t@lineage(dependencies={'
                    
                    # Get combination items for filtering
                    combination_item_list = []
                    try:
                        combination_item_list = sdd_context.combination_item_dictionary[combination.combination_id.combination_id]
                    except:
                        pass
                    
                    # Build lineage dependencies
                    item_counter = 0
                    for combination_item in combination_item_list:
                        # Include TYP_INSTRMNT in lineage dependencies for completeness
                            
                        leaf_node_members = CreateExecutableFilters.get_leaf_node_codes(self,
                                                                                      sdd_context,
                                                                                      combination_item.member_id,
                                                                                      combination_item.member_hierarchy)
                        
                        if len(leaf_node_members) > 0:
                            if not ((len(leaf_node_members) == 1) and (str(leaf_node_members[0].code) == '0')):
                                if item_counter > 0:
                                    calc_lineage_string += ','
                                    calc_lineage_string += '\n\t\t\t'
                                calc_lineage_string += '"'
                                calc_lineage_string += cube_id 
                                calc_lineage_string += '.'
                                calc_lineage_string += combination_item.variable_id.name 
                                calc_lineage_string += '"'
                                item_counter += 1
                    
                    calc_lineage_string += '})\n'
                    
                    # Build the calc method
                    calc_string += "\tdef calc_referenced_items(self):\n"
                    
                    if not product_classes:
                        # No product classes found - this shouldn't happen
                        calc_string += "\t\t# ERROR: No TYP_INSTRMNT found for this combination\n"
                        calc_string += "\t\tpass\n"
                    else:
                        # Direct filtering on product-specific classes
                        calc_string += "\t\t# Filter directly on product-specific classes\n"
                        
                        for idx, product_class in enumerate(product_classes):
                            # Extract the product name from class name
                            product_name = product_class.replace(cube_id + "_", "").replace("_Table", "") + "s"
                            
                            calc_string += f"\t\t# Process {product_class}\n"
                            calc_string += f"\t\tif self.{product_class} is not None:\n"
                            calc_string += f"\t\t\titems = self.{product_class}.{product_name}\n"
                            calc_string += f"\t\t\tfor item in items:\n"
                            calc_string += f"\t\t\t\tfilter_passed = True\n"
                            
                            # Apply filters (including TYP_INSTRMNT for completeness)
                            calc_string += self._generate_filter_logic(combination_item_list, sdd_context, "\t\t\t\t")
                            
                            calc_string += f"\t\t\t\tif filter_passed:\n"
                            calc_string += f"\t\t\t\t\tself.{cube_id}s.append(item)\n"
                    
                    # Write the complete method
                    file.write(calc_lineage_string)
                    file.write(calc_string + '\n')
                    
                    # Write init method
                    file.write("\tdef init(self):\n")
                    file.write("\t\tOrchestration().init(self)\n")
                    file.write("\t\tself." + cube_id + "s = []\n")
                    file.write("\t\tself.calc_referenced_items()\n")
                    file.write("\t\treturn None\n\n")

            # Close HTML files
            filter_html_file.write("</table>\n")
            filter_html_file.write("</body>\n")
            filter_html_file.write("</html>\n")
            filter_html_file.write("</table>\n")
            filter_html_file.write("</body>\n")
            filter_html_file.write("</html>\n")
            filter_html_file.write("{% endblock %}\n")
            
        # Close report HTML file
        report_html_file.write("</table>\n")
        report_html_file.write("</body>\n")
        report_html_file.write("</html>\n")
        report_html_file.write("</table>\n")
        report_html_file.write("</body>\n")
        report_html_file.write("</html>\n")
        report_html_file.write("{% endblock %}\n")

    def _generate_filter_logic(self, combination_item_list, sdd_context, indent):
        """Generate filter logic for combination items (including TYP_INSTRMNT for completeness)"""
        filter_string = ""
        
        for combination_item in combination_item_list:
            # Include TYP_INSTRMNT filter even though it's redundant with product class selection
                
            leaf_node_members = CreateExecutableFilters.get_leaf_node_codes(self,
                                                                          sdd_context,
                                                                          combination_item.member_id,
                                                                          combination_item.member_hierarchy)
            
            if len(leaf_node_members) > 0:
                if (len(leaf_node_members) == 1) and (str(leaf_node_members[0].code) == '0'):
                    pass
                else:
                    filter_string += indent + "if "
                    for leaf_node_member in leaf_node_members:
                        filter_string += indent + "\t(item." + combination_item.variable_id.name + "() == '" + str(leaf_node_member.code) + "')  or \\\n"
                    filter_string += indent + "\tFalse:\n"
                    filter_string += indent + "\tpass\n"
                    filter_string += indent + "else:\n"
                    filter_string += indent + "\tfilter_passed = False\n"
            else:
                print("No leaf node members for " + combination_item.variable_id.name + ":" + combination_item.member_id.member_id)
        
        return filter_string

    def get_leaf_node_codes(self, sdd_context, member, member_hierarchy):
        return_list = []
        if member is not None:
            members = self.get_member_list_considering_hierarchies(sdd_context, member, member_hierarchy)
            return_list = members  # Keep original order
        return return_list

    def get_literal_list_considering_hierarchies(self, context, sdd_context, literal, member, domain_id, warning_list, template_code, combination_id, variable_id, framework, cube_type, input_cube_type):
        cache_key = (literal, member, domain_id) if literal and member else None
        if cache_key in self._literal_list_cache:
            return self._literal_list_cache[cache_key].copy()

        return_list = []
        is_node = self.is_member_a_node(sdd_context, member)

        if literal is None:
            if not is_node:
                warning_list.append(("error", "member does not exist in input layer and is not a node", template_code, combination_id, variable_id, member.member_id, None, domain_id))
        else:
            if not is_node:
                return_list = [literal]

        for domain, hierarchy_list in sdd_context.domain_to_hierarchy_dictionary.items():
            if domain.domain_id == domain_id:
                for hierarchy in hierarchy_list:
                    hierarchy_id = hierarchy.member_hierarchy_id
                    literal_list = []
                    self.get_literal_list_considering_hierarchy(context, sdd_context, member, hierarchy_id, literal_list, framework, cube_type, input_cube_type)
                    return_list.extend(literal_list)

        if len(return_list) == 0:
            warning_list.append(("error", "could not find any input layer members or sub members for member", template_code, combination_id, variable_id, member.member_id, None, domain_id))

        self._literal_list_cache[cache_key] = return_list
        return return_list.copy()

    def get_literal_list_considering_hierarchy(self, context, sdd_context, member, hierarchy, literal_list, framework, cube_type, input_cube_type):
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


    def find_member_node(self,sdd_context,member_id,hierarchy):
        try:
            return sdd_context.member_hierarchy_node_dictionary[hierarchy + ":" + member_id.member_id]
        except:
            pass

    def prepare_node_dictionaries_and_lists(self, sdd_context):
        # Use sets for faster membership testing
        sdd_context.members_that_are_nodes = set()
        sdd_context.member_plus_hierarchy_to_child_literals = {}
        sdd_context.domain_to_hierarchy_dictionary = {}

        # Pre-process hierarchy nodes
        for node in sdd_context.member_hierarchy_node_dictionary.values():
            if node.parent_member_id and node.parent_member_id != '':
                sdd_context.members_that_are_nodes.add(node.parent_member_id)
                member_plus_hierarchy = f"{node.parent_member_id.member_id}:{node.member_hierarchy_id.member_hierarchy_id}"

                if member_plus_hierarchy not in sdd_context.member_plus_hierarchy_to_child_literals:
                    sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy] = [node.member_id]
                else:
                    if node.member_id not in sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy]:
                        sdd_context.member_plus_hierarchy_to_child_literals[member_plus_hierarchy].append(node.member_id)

        # Build domain hierarchy mapping
        for hierarchy in sdd_context.member_hierarchy_dictionary.values():
            domain_id = hierarchy.domain_id
            if domain_id not in sdd_context.domain_to_hierarchy_dictionary:
                sdd_context.domain_to_hierarchy_dictionary[domain_id] = []
            sdd_context.domain_to_hierarchy_dictionary[domain_id].append(hierarchy)

    def get_member_list_considering_hierarchies(self, sdd_context, member, member_hierarchy):
        # Cache key based on member and hierarchy
        cache_key = (member, member_hierarchy) if member else None
        if cache_key in self._member_list_cache:
            return self._member_list_cache[cache_key].copy()  # Return a copy to prevent modifications

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
                            if item not in return_list:  # Keep original duplicate checking
                                return_list.append(item)

        self._member_list_cache[cache_key] = return_list
        return return_list.copy()  # Return a copy to prevent modifications

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

    def delete_generated_python_filter_files(self, context):
        base_dir = settings.BASE_DIR
        python_dir = os.path.join(base_dir, 'results', 'generated_python_filters')
        for file in os.listdir(python_dir):
            os.remove(os.path.join(python_dir, file))

    def delete_generated_html_filter_files(self, context):
        base_dir = settings.BASE_DIR
        html_dir = os.path.join(base_dir, 'results', 'generated_html')
        for file in os.listdir(html_dir):
            os.remove(os.path.join(html_dir, file))