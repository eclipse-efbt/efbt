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

from pybirdai.process_steps.utils import Utils
from django.conf import settings
from pybirdai.process_steps.pybird.orchestration import Orchestration
from pybirdai.models import Trail, MetaDataTrail, DerivedTable, FunctionText, TableCreationFunction
from datetime import datetime
from pybirdai.process_steps.pybird.typ_instrmnt_mapping import TypInstrmntMapper

import os
import shutil


class CreateExecutableFilters:
    def __init__(self):
        # Keep caching for performance
        self._node_cache = {}
        self._member_list_cache = {}
        self._literal_list_cache = {}
        # Initialize condition-to-slice mapper
        self.typ_mapper = TypInstrmntMapper()

    def is_member_a_node(self, sdd_context, member):
        # Keep the member node caching
        if member not in self._node_cache:
            self._node_cache[member] = member in sdd_context.members_that_are_nodes
        return self._node_cache[member]
    
    def get_product_classes_for_combination(self, sdd_context, combination, cube_id):
        """Get product-specific class names based on conditions from combination items"""
        product_classes = []
        # Get combination items
        combination_item_list = []
        try:
            combination_item_list = sdd_context.combination_item_dictionary[combination.combination_id.combination_id]
        except:
            pass

        # Build conditions from combination items and look up slices
        for combination_item in combination_item_list:
            variable_name = combination_item.variable_id.name
            member = combination_item.member_id
            if member:
                # Build condition string in format: VARIABLE_NAME=MEMBER_ID
                condition = f"{variable_name}={member.member_id}"

                # Get slice names from mapping using the condition
                slice_names = self.typ_mapper.get_slices_for_condition(condition)

                for slice_name in slice_names:
                    # Format as class name
                    class_name = self.typ_mapper.format_slice_name_for_class(slice_name, cube_id)
                    if class_name not in product_classes:
                        product_classes.append(class_name)

        if not product_classes:
            print(f"WARNING: No matching condition found for combination {combination.combination_id.combination_id}")

        return product_classes

    def create_executable_filters(self, context, sdd_context, framework="FINREP"):
        """
        Generate executable filter Python code for the specified framework.

        Args:
            context: The application context
            sdd_context: The SDD context containing cube and combination data
            framework (str): The reporting framework (e.g., 'FINREP', 'COREP', 'AE').
                           Defaults to 'FINREP' for backward compatibility.
        """
        CreateExecutableFilters.delete_generated_python_filter_files(self, context, framework)
        CreateExecutableFilters.delete_generated_html_filter_files(self, context)
        CreateExecutableFilters.prepare_node_dictionaries_and_lists(self, sdd_context)

        # Initialize AORTA tracking
        orchestration = Orchestration()
        if hasattr(context, 'enable_lineage_tracking') and context.enable_lineage_tracking:
            orchestration.init_with_lineage(self, f"Filter_Generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            print("AORTA lineage tracking enabled for filter generation")

        # Generate framework-specific report_cells.py file
        framework_lower = framework.lower().replace('_ref', '')
        framework_upper = framework.upper().replace('_REF', '')
        output_filename = f'{framework_lower}_report_cells.py'

        # Use unified folder structure: results/generated_python/templates/{FRAMEWORK}/filter/
        filters_dir = os.path.join(sdd_context.output_directory, 'generated_python', 'templates', framework_upper, 'filter')
        os.makedirs(filters_dir, exist_ok=True)
        file = open(os.path.join(filters_dir, output_filename), "a", encoding='utf-8')
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

        file.write("\n# Import product-specific classes from filter_code directory\n")
        for logic_file in sorted(logic_files):
            # New structure: filter_code/templates/{FRAMEWORK}/joins/{logic_file}
            file.write(f"from pybirdai.process_steps.filter_code.templates.{framework_upper}.joins.{logic_file} import *\n")
        
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
                    
                    # Write metric_value method with correct lineage dependencies
                    metric_lineage_deps = []
                    for product_class in product_classes:
                        # Extract product name from class name (e.g., Other_loans from F_01_01_REF_FINREP_3_0_Other_loans_Table)
                        product_name = product_class.replace(cube_id + "_", "").replace("_Table", "")
                        metric_lineage_deps.append(f"{product_name}.{combination.combination_id.metric.name}")
                    
                    lineage_string = ", ".join([f'"{dep}"' for dep in metric_lineage_deps])
                    file.write(f"\t@lineage(dependencies={{{lineage_string}}})\n")
                    file.write("\tdef metric_value(self):\n")
                    file.write("\t\ttotal = 0\n")
                    file.write("\t\t# Sum from filtered items collected in calc_referenced_items\n")
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
                    
                    # Build lineage dependencies for product-specific classes
                    calc_lineage_deps = []
                    for combination_item in combination_item_list:
                        leaf_node_members = CreateExecutableFilters.get_leaf_node_codes(self,
                                                                                      sdd_context,
                                                                                      combination_item.member_id,
                                                                                      combination_item.member_hierarchy)
                        
                        if len(leaf_node_members) > 0:
                            if not ((len(leaf_node_members) == 1) and (str(leaf_node_members[0].code) == '0')):
                                # Add dependency for each product class
                                for product_class in product_classes:
                                    product_name = product_class.replace(cube_id + "_", "").replace("_Table", "")
                                    dep = f"{product_name}.{combination_item.variable_id.name}"
                                    if dep not in calc_lineage_deps:
                                        calc_lineage_deps.append(dep)
                    
                    if calc_lineage_deps:
                        calc_lineage_string += '"' + '", "'.join(calc_lineage_deps) + '"'
                    calc_lineage_string += '})\n'
                    
                    # Build the calc method
                    calc_string += "\tdef calc_referenced_items(self):\n"
                    
                    if not product_classes:
                        # No product classes found - this shouldn't happen
                        calc_string += "\t\t# ERROR: No INSTRMNT_TYP_PRDCT found for this combination\n"
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
                            
                            # Apply filters (including INSTRMNT_TYP_PRDCT for completeness)
                            calc_string += self._generate_filter_logic(combination_item_list, sdd_context, "\t\t\t\t")
                            
                            calc_string += f"\t\t\t\tif filter_passed:\n"
                            calc_string += f"\t\t\t\t\tself.{cube_id}s.append(item)\n"
                    
                    # Write the complete method
                    file.write(calc_lineage_string)
                    file.write(calc_string + '\n')
                    
                    # Write init method - let orchestration handle initialization
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

        # Close files
        file.close()
        report_html_file.close()

        # Copy generated files to filter_code directory for runtime use
        self._copy_to_filter_code(sdd_context.output_directory, framework)

    def _copy_to_filter_code(self, output_directory, framework):
        """
        Copy generated filter files to their respective directories for runtime use.

        New structure:
        - Report cells → filter_code/reports/report_cells/{framework}.py
        - Logic files → filter_code/logic/templates/

        Args:
            output_directory: The results directory containing generated files
            framework: The framework name (e.g., 'FINREP', 'COREP')
        """
        framework_lower = framework.lower().replace('_ref', '')
        framework_upper = framework.upper().replace('_REF', '')

        # Source directory: results/generated_python/templates/{FRAMEWORK}/filter/
        source_dir = os.path.join(output_directory, 'generated_python', 'templates', framework_upper, 'filter')

        # Destination directory: filter_code/templates/{FRAMEWORK}/filter/
        filter_code_base = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
        dest_dir = os.path.join(filter_code_base, 'templates', framework_upper, 'filter')

        # Ensure destination directory exists
        os.makedirs(dest_dir, exist_ok=True)

        if not os.path.exists(source_dir):
            print(f"Source directory not found: {source_dir}")
            return

        # Copy all files from source to destination
        for filename in os.listdir(source_dir):
            src = os.path.join(source_dir, filename)
            dst = os.path.join(dest_dir, filename)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
                print(f"Copied {filename} to filter_code/templates/{framework_upper}/filter/")

    def _update_report_cells_imports(self, src_file, dst_file, framework_lower, framework_upper):
        """
        Update import statements in report_cells file to use new logic paths.
        """
        with open(src_file, 'r') as f:
            content = f.read()

        # Update imports from old relative imports to new absolute paths
        # Old: from .{name}_logic import *
        # New: from pybirdai.process_steps.filter_code.templates.{FRAMEWORK}.joins.{name}_logic import *
        import re
        content = re.sub(
            r'from \.(\w+_logic) import \*',
            rf'from pybirdai.process_steps.filter_code.templates.{framework_upper}.joins.\1 import *',
            content
        )

        with open(dst_file, 'w') as f:
            f.write(content)

    def _generate_filter_logic(self, combination_item_list, sdd_context, indent):
        """Generate filter logic using all([...]) pattern with 'in' checks"""

        # Collect all filter conditions
        filter_conditions = []

        for combination_item in combination_item_list:
            # Include INSTRMNT_TYP_PRDCT filter even though it's redundant with product class selection

            leaf_node_members = CreateExecutableFilters.get_leaf_node_codes(self,
                                                                          sdd_context,
                                                                          combination_item.member_id,
                                                                          combination_item.member_hierarchy)

            if len(leaf_node_members) > 0:
                # Skip if it's just the default '0' code
                if (len(leaf_node_members) == 1) and (str(leaf_node_members[0].code) == '0'):
                    continue

                # Collect all valid codes
                valid_codes = [str(leaf_node_member.code) for leaf_node_member in leaf_node_members]

                # Build the condition: item.VARIABLE() in ['val1', 'val2', ...]
                condition = f"item.{combination_item.variable_id.name}() in ["
                condition += ", ".join([f"'{code}'" for code in valid_codes])
                condition += "]"

                filter_conditions.append(condition)
            else:
                print("No leaf node members for " + combination_item.variable_id.name + ":" + combination_item.member_id.member_id)

        # Generate the filter_passed = all([...]) statement
        if filter_conditions:
            filter_string = indent + "filter_passed = all([\n"
            for condition in filter_conditions:
                filter_string += indent + "\t" + condition + ",\n"
            filter_string += indent + "])\n"
            return filter_string
        else:
            return ""

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

    def delete_generated_python_filter_files(self, context, framework="FINREP"):
        base_dir = settings.BASE_DIR
        framework_upper = framework.upper().replace('_REF', '')

        # Use unified folder structure: results/generated_python/templates/{FRAMEWORK}/filter/
        python_dir = os.path.join(base_dir, 'results', 'generated_python', 'templates', framework_upper, 'filter')
        if os.path.exists(python_dir):
            for file in os.listdir(python_dir):
                file_path = os.path.join(python_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)

    def delete_generated_html_filter_files(self, context):
        base_dir = settings.BASE_DIR
        html_dir = os.path.join(base_dir, 'results', 'generated_html')
        for file in os.listdir(html_dir):
            os.remove(os.path.join(html_dir, file))