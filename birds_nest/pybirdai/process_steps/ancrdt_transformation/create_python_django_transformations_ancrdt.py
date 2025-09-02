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

from pybirdai.utils.utils import Utils
from pybirdai.models.bird_meta_data_model import *
import os
from django.conf import settings
import ast
from pybirdai.process_steps.ancrdt_transformation.filter_buildr import TransformationBuildr

class CreatePythonTransformations:

    @staticmethod
    def create_python_joins(context, sdd_context,logger):
        '''
        Read in the transformation meta data and create python classes
        '''

        # CreatePythonTransformations.delete_generated_python_join_files(context)
        CreatePythonTransformations.create_output_classes( sdd_context,logger)
        CreatePythonTransformations.create_slice_classes(sdd_context,logger)
        # get all the cube_links for a report

    def create_output_classes(sdd_context,logger):

        #get all the cubes_structure_items for that cube and make a related Python class.
        file = open(sdd_context.output_directory + os.sep + 'generated_python_joins' + os.sep +  'ancrdt_output_tables.py', "a",  encoding='utf-8')
        file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")
        file.write("from datetime import datetime\n")
        file.write("from pybirdai.annotations.decorators import lineage\n")
        cube_link_to_foreign_cube_map__ = {
            rolc_id:cube_links
            for rolc_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items()
            if "ANCRDT" in rolc_id
        }
        for rolc_id, cube_links in cube_link_to_foreign_cube_map__.items():
            # logger.info(f"rolc_id: {rolc_id}")
            file.write("from ." + rolc_id  + "_logic import *\n")
            file.write("\nclass " + rolc_id + ":\n")
            file.write("\tunionOfLayers = None #  " + rolc_id + "_UnionItem  unionOfLayers\n")
            cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[rolc_id]
            for cube_structure_item in cube_structure_items:
                # logger.info(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id
                if cube_structure_item.variable_id.variable_id == "NEVS":
                    continue
                domain = variable.domain_id.domain_id
                file.write('\t@lineage(dependencies={"unionOfLayers.'+ variable.variable_id +'"})\n')
                if domain == 'String':
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '(self) -> int:\n')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '(self) -> datetime:\n')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '(self) -> float:\n')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '(self) -> bool:\n')
                else:
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                    file.write('\t\t\'\'\' return string from ' + domain + ' enumeration \'\'\'\n')

                file.write('\t\treturn self.unionOfLayers.' + variable.variable_id + '()\n')
                file.write('\n')
            file.write('\n')
            file.write('\n')
            file.write("class " +rolc_id + "_Table :\n" )
            #file.write("\tunionOfLayersTable = None # " + rolc_id + "_UnionTable\n" )
            file.write("\t" + rolc_id + "_UnionTable = None # unionOfLayersTable\n" )
            file.write("\t" + rolc_id + "s = [] #" + rolc_id + "[]\n" )
            file.write("\tdef  calc_" + rolc_id + "s(self) -> list[" + rolc_id + "] :\n" )
            file.write("\t\titems = [] # " + rolc_id + "[]\n" )
            file.write("\t\tfor item in self." +rolc_id + "_UnionTable." + rolc_id + "_UnionItems:\n" )
            file.write("\t\t\tnewItem = " + rolc_id + "()\n" )
            file.write("\t\t\tnewItem.unionOfLayers = item\n" )
            file.write("\t\t\titems.append(newItem)\n" )
            file.write("\t\treturn items\n" )
            file.write("\tdef init(self):\n" )
            file.write("\t\tOrchestration().init(self)\n" )
            file.write("\t\tself." + rolc_id + "s = []\n" )
            file.write("\t\tself." + rolc_id + "s.extend(self.calc_" + rolc_id + "s())\n" )
            file.write("\t\tCSVConverter.persist_object_as_csv(self,True)\n")
            file.write("\t\treturn None\n" )

    def create_slice_classes( sdd_context,logger):
        cube_link_to_foreign_cube_map__ = {
            rolc_id:cube_links
            for rolc_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items()
            if "ANCRDT" in rolc_id
        }

        for rolc_id, cube_links in cube_link_to_foreign_cube_map__.items():
            file = open(sdd_context.output_directory + os.sep + 'generated_python_joins' + os.sep +  rolc_id + '_logic.py', "a",  encoding='utf-8')
            file.write("from pybirdai.bird_data_model import *\n")
            file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")
            file.write("from pybirdai.process_steps.pybird.csv_converter import CSVConverter\n")
            file.write("from datetime import datetime\n")
            file.write("from pybirdai.annotations.decorators import lineage\n")

            file.write("\nclass " + rolc_id + "_UnionItem:\n")
            file.write("\tbase = None #" + rolc_id + "_Base\n")
            cube_structure_items = []
            cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[rolc_id]
            for cube_structure_item in cube_structure_items:
                # logger.info(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id
                if cube_structure_item.variable_id.variable_id == "NEVS":
                    continue

                domain = variable.domain_id.domain_id
                file.write('\t@lineage(dependencies={"base.'+ variable.variable_id +'"})\n')
                if domain == 'String':
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '(self) -> int:\n')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '(self) -> datetime:\n')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '(self) -> float:\n')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '(self) -> bool:\n')
                else:
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                    file.write('\t\t\'\'\' return string from ' + domain + ' enumeration \'\'\'\n')

                file.write('\t\treturn self.base.' + variable.variable_id + '()')
                file.write('\n')


            file.write("\nclass " + rolc_id + "_Base:\n")
            cube_structure_items = []
            cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[rolc_id]

            if len(cube_structure_items) == 0:
                file.write("\tpass\n")

            for cube_structure_item in cube_structure_items:
                # logger.info(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id
                if cube_structure_item.variable_id.variable_id == "NEVS":
                    continue

                domain = variable.domain_id.domain_id
                if domain == 'String':
                    file.write('\tdef ' + variable.variable_id + '() -> str:\n')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '() -> int:\n')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '() -> datetime:\n')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '() -> float:\n')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '() -> bool:\n')
                else:
                    file.write('\tdef ' + variable.variable_id + '() -> str:\n')
                    file.write('\t\t\'\'\' return string from ' + domain + ' enumeration \'\'\'\n')

                file.write('\t\tpass')
                file.write('\n')


            file.write("\nclass " + rolc_id + "_UnionTable :\n")
            file.write("\t" + rolc_id + "_UnionItems = [] # " +  rolc_id + "_UnionItem []\n" )
            join_ids_added = []
            cube_link_to_join_for_report_id_map__ = {
                join_for_rolc_id:cube_links
                for join_for_rolc_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items()
                if "ANCRDT_INSTRMNT_C_1" in join_for_rolc_id
            }
            logger.info(str(cube_link_to_join_for_report_id_map__))
            for join_for_rolc_id, cube_links in cube_link_to_join_for_report_id_map__.items():
                for cube_link in cube_links:
                    the_rolc_id = cube_link.foreign_cube_id.cube_id
                    if the_rolc_id == rolc_id:
                        if cube_link.join_identifier not in join_ids_added:
                            file.write("\t" + rolc_id + "_" + cube_link.join_identifier.replace(' ','_') + "_Table = None # " +  cube_link.join_identifier.replace(' ','_') + "\n")
                            join_ids_added.append(cube_link.join_identifier)
            file.write("\tdef calc_" + rolc_id + "_UnionItems(self) -> list[" + rolc_id + "_UnionItem] :\n")
            file.write("\t\titems = [] # " + rolc_id + "_UnionItem []\n")

            join_ids_added = []
            for join_for_rolc_id, cube_links in cube_link_to_join_for_report_id_map__.items():
                for cube_link in cube_links:
                    the_rolc_id = cube_link.foreign_cube_id.cube_id
                    if the_rolc_id == rolc_id:
                        if cube_link.join_identifier not in join_ids_added:
                            file.write("\t\tfor item in self." + rolc_id + "_" + cube_link.join_identifier.replace(' ','_') + "_Table." + cube_link.join_identifier.replace(' ','_') + "s:\n")
                            file.write("\t\t\tnewItem = " + rolc_id + "_UnionItem()\n")
                            file.write("\t\t\tnewItem.base = item\n")
                            file.write("\t\t\titems.append(newItem)\n")
                            join_ids_added.append(cube_link.join_identifier)
            file.write("\t\treturn items\n")
            file.write("\n")

            file.write("\tdef init(self):\n")
            file.write("\t\tOrchestration().init(self)\n")
            file.write("\t\tself." + rolc_id + "_UnionItems = []\n")
            file.write("\t\tself." + rolc_id + "_UnionItems.extend(self.calc_" + rolc_id + "_UnionItems())\n")
            file.write("\t\tCSVConverter.persist_object_as_csv(self,True)\n")
            file.write("\t\treturn None\n")
            file.write("\n")

            for join_for_rolc_id, cube_links in cube_link_to_join_for_report_id_map__.items():
                class_header_is_written = False
                for cube_link in cube_links:
                    the_rolc_id = cube_link.foreign_cube_id.cube_id
                    if the_rolc_id == rolc_id:
                        # only write the class header once
                        if not class_header_is_written:
                            file.write("\nclass " + cube_link.join_identifier.replace(' ','_') + "(" + rolc_id + "_Base):\n")
                            class_header_is_written = True

                        cube_structure_item_links = []
                        try:
                            cube_structure_item_links = sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]
                        except KeyError:
                            logger.info(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")
                        primary_cubes_added = []
                        if len(cube_structure_item_links) == 0:
                            file.write("\tpass\n")
                        for cube_structure_item_link in cube_structure_item_links:
                            if cube_structure_item_link.cube_link_id.primary_cube_id.cube_id not in primary_cubes_added:
                                file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + " = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "\n")
                                primary_cubes_added.append(cube_structure_item_link.cube_link_id.primary_cube_id.cube_id)
                        for cube_structure_item_link in cube_structure_item_links:
                            file.write('\t@lineage(dependencies={"'+ cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + '.' + cube_structure_item_link.primary_cube_variable_code.variable_id.variable_id +'"})\n')
                            file.write("\tdef " + cube_structure_item_link.foreign_cube_variable_code.variable_id.variable_id + "(self):\n")
                            file.write("\t\treturn self." +  cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "." + cube_structure_item_link.primary_cube_variable_code.variable_id.variable_id + "\n")


            for join_for_rolc_id, cube_links in cube_link_to_join_for_report_id_map__.items():
                report_and_join =   join_for_rolc_id.split(':')
                join_id = report_and_join[1]
                if report_and_join[0] == rolc_id:
                    file.write("\nclass " + rolc_id + "_" + join_id.replace(' ','_') + "_Table:\n" )
                    for cube_link in cube_links:
                        cube_structure_item_links = []
                        try:
                            cube_structure_item_links = sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]
                        except KeyError:
                            logger.info(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")

                        primary_cubes_added = []
                        for cube_structure_item_link in cube_structure_item_links:
                            if cube_structure_item_link.cube_link_id.primary_cube_id.cube_id not in primary_cubes_added:
                                file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + "_Table = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "\n")
                                primary_cubes_added.append(cube_structure_item_link.cube_link_id.primary_cube_id.cube_id)

                if report_and_join[0] == rolc_id:
                    join_id = report_and_join[1]
                    file.write("\t" + join_id.replace(' ','_') + "s = []# " + join_id.replace(' ','_') + "[]\n")
                    file.write("\tdef calc_" + join_id.replace(' ','_') + "s(self) :\n")
                    file.write("\t\titems = [] # " + join_id.replace(' ','_') + "[\n")
                    file.write("\t\t# Join up any refered tables that you need to join\n")
                    file.write("\t\t# loop through the main table\n")
                    file.write("\t\t# set any references you want to on the new Item so that it can refer to themin operations\n")
                    file.write("\t\treturn items\n")
                    file.write("\tdef init(self):\n")
                    file.write("\t\tOrchestration().init(self)\n")
                    file.write("\t\tself." + join_id.replace(' ','_') + "s = []\n")
                    file.write("\t\tself." + join_id.replace(' ','_') + "s.extend(self.calc_" + join_id.replace(' ','_') + "s())\n")
                    file.write("\t\tCSVConverter.persist_object_as_csv(self,True)\n")

                    file.write("\t\treturn None\n")
                    file.write("\n")

            for join_for_rolc_id, cube_links in cube_link_to_join_for_report_id_map__.items():
                report_and_join =   join_for_rolc_id.split(':')
                join_id = report_and_join[1]
                if report_and_join[0] == rolc_id:
                    class_inheriting_from_base = f"\nclass {rolc_id}_{join_id.replace(' ','_')}_filtered_and_aggregated({rolc_id}_Base):\n"
                    base_calc_function_name = f"calc_{rolc_id}_{join_id.replace(' ','_')}_filtered_and_aggregated"
                    table_class = f"\nclass {rolc_id}_{join_id.replace(' ','_')}_filtered_and_aggregated_Table:\n"
                    file.write(table_class)
                    cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[rolc_id]
                    for cube_structure_item in cube_structure_items:
                        # logger.info(f"cube_structure_item: {cube_structure_item}")
                        variable = cube_structure_item.variable_id
                        if cube_structure_item.variable_id.variable_id == "NEVS":
                            continue

                        domain = variable.domain_id.domain_id
                        file.write('\t@lineage(dependencies={"base.'+ variable.variable_id +'"})\n')
                        if domain == 'String':
                            file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                        elif domain == 'Integer':
                            file.write('\tdef ' + variable.variable_id + '(self) -> int:\n')
                        elif domain == 'Date':
                            file.write('\tdef ' + variable.variable_id + '(self) -> datetime:\n')
                        elif domain == 'Float':
                            file.write('\tdef ' + variable.variable_id + '(self) -> float:\n')
                        elif domain == 'Boolean':
                            file.write('\tdef ' + variable.variable_id + '(self) -> bool:\n')
                        else:
                            file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                            file.write('\t\t\'\'\' return string from ' + domain + ' enumeration \'\'\'\n')

                        file.write('\t\treturn self.base.' + variable.variable_id + '()')
                        file.write('\n')


                    # Generate filter for calc_ ... agregated_ function
                    generated_filter = []
                    for cube_link in cube_links:
                        cube_structure_item_link_ids = CUBE_STRUCTURE_ITEM_LINK.objects.all().filter(
                            cube_link_id = cube_link
                        )
                        for cube_structure_item_link in cube_structure_item_link_ids:
                            generated_filter += [f"{TransformationBuildr.define_filter_from_structure_link(
                                cube_structure_item_link.cube_structure_item_link_id
                            )}"]
                    if not generated_filter:
                        continue

                    generated_filter = " and ".join(generated_filter)

                    file.write(f'\tdef {base_calc_function_name}(self) -> str:\n')
                    file.write('\t\titems = [] # ' + rolc_id + '_Loans_and_advances_filtered_and_aggregated []\n')
                    file.write('\t\tfor item in self.' + rolc_id + '_Loans_and_advances_Table.' + rolc_id + '_Loans_and_advances:\n')
                    file.write(f'\t\t\tif {generated_filter}:\n')
                    file.write('\t\t\t\tnewItem = ' + rolc_id + '_Loans_and_advances_filtered_and_aggregated()\n')
                    file.write('\t\t\t\tnewItem.source = item\n')
                    file.write('\t\t\t\titems.append(newItem)\n')
                    file.write('\t\treturn items\n')


                    file.write(class_inheriting_from_base)

                    assignment_dicts = dict()
                    for cube_link in cube_links:
                        cube_structure_item_link_ids = CUBE_STRUCTURE_ITEM_LINK.objects.all().filter(
                            cube_link_id = cube_link
                        )
                        for cube_structure_item_link in cube_structure_item_link_ids:
                            assignment_dicts.update(TransformationBuildr.reverse_apply_member_links(
                                cube_structure_item_link.cube_structure_item_link_id
                            ))

                    cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[rolc_id]
                    for cube_structure_item in cube_structure_items:
                        # logger.info(f"cube_structure_item: {cube_structure_item}")
                        variable = cube_structure_item.variable_id
                        if cube_structure_item.variable_id.variable_id == "NEVS":
                            continue

                        domain = variable.domain_id.domain_id
                        for var, source_target_dict in assignment_dicts.items():
                            file.write('\tdef ' + var + '(self) -> str:\n')
                            file.write(f'\t\tsource = self.{rolc_id}_{join_id.replace(' ','_')}.{var}()\n')
                            for row in source_target_dict:
                                file.write(f"""\t\tif source == '{row.get("source")}' : return '{row.get("target")}'\n""")
                            file.write('\n')




    def delete_generated_python_join_files(context):
        base_dir = settings.BASE_DIR
        python_dir = os.path.join(base_dir, 'results',  'generated_python_joins')
        for file in os.listdir(python_dir):
            os.remove(os.path.join(python_dir, file))
