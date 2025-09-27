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
from pybirdai.models.bird_meta_data_model import *
import os
from django.conf import settings
from pybirdai.process_steps.pybird.orchestration import Orchestration
from pybirdai.models import Trail, MetaDataTrail, DerivedTable, FunctionText, TableCreationFunction
from datetime import datetime


class CreatePythonTransformations:

    @staticmethod
    def create_python_joins(context, sdd_context):
        '''
        Read in the transformation meta data and create python classes
        '''

        # Initialize AORTA tracking
        orchestration = Orchestration()
        if hasattr(context, 'enable_lineage_tracking') and context.enable_lineage_tracking:
            orchestration.init_with_lineage(None, f"Transformation_Generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            print("AORTA lineage tracking enabled for transformation generation")

        CreatePythonTransformations.delete_generated_python_join_files(context)
        # Skip output_tables.py generation - no longer needed with direct product filtering
        # CreatePythonTransformations.create_output_classes( sdd_context)
        CreatePythonTransformations.create_slice_classes(sdd_context)
        # get all the cube_links for a report

    def create_output_classes(  sdd_context):

         #get all the cubes_structure_items for that cube and make a related Python class.
        file = open(sdd_context.output_directory + os.sep + 'generated_python_joins' + os.sep +  'output_tables.py', "a",  encoding='utf-8')
        file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")
        file.write("from datetime import datetime\n")
        file.write("from pybirdai.annotations.decorators import lineage, track_table_init\n")
        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            print(f"report_id: {report_id}")
            file.write("from ." + report_id  + "_logic import *\n")
            file.write("\nclass " + report_id + ":\n")
            file.write("\tunionOfLayers = None #  " + report_id + "_UnionItem  unionOfLayers\n")
            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[report_id+ '_cube_structure']
            except KeyError:
                print(f"No cube structure items for report_id: {report_id}")
            for cube_structure_item in cube_structure_items:
                print(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id

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
            file.write("class " +report_id + "_Table :\n" )
            #file.write("\tunionOfLayersTable = None # " + report_id + "_UnionTable\n" )
            file.write("\t" + report_id + "_UnionTable = None # unionOfLayersTable\n" )
            file.write("\t" + report_id + "s = [] #" + report_id + "[]\n" )
            file.write("\tdef  calc_" + report_id + "s(self) -> list[" + report_id + "] :\n" )
            file.write("\t\titems = [] # " + report_id + "[]\n" )
            file.write("\t\tfor item in self." +report_id + "_UnionTable." + report_id + "_UnionItems:\n" )
            file.write("\t\t\tnewItem = " + report_id + "()\n" )
            file.write("\t\t\tnewItem.unionOfLayers = item\n" )
            file.write("\t\t\titems.append(newItem)\n" )
            file.write("\t\treturn items\n" )
            file.write("\t@track_table_init\n" )
            file.write("\tdef init(self):\n" )
            file.write("\t\tOrchestration().init(self)\n" )
            file.write("\t\tself." + report_id + "s = []\n" )
            file.write("\t\tself." + report_id + "s.extend(self.calc_" + report_id + "s())\n" )
            file.write("\t\tfrom pybirdai.process_steps.pybird.csv_converter import CSVConverter\n")
            file.write("\t\tCSVConverter.persist_object_as_csv(self,True)\n")
            file.write("\t\treturn None\n" )
            file.write('\n')

    def create_slice_classes( sdd_context):
        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            file = open(sdd_context.output_directory + os.sep + 'generated_python_joins' + os.sep +  report_id + '_logic.py', "a",  encoding='utf-8')
            file.write("from pybirdai.models.bird_data_model import *\n")
            file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")
            file.write("from pybirdai.process_steps.pybird.csv_converter import CSVConverter\n")
            file.write("from datetime import datetime\n")
            file.write("from pybirdai.annotations.decorators import lineage\n")

            # Skip UnionItem generation - no longer needed with direct product filtering
            # file.write("\nclass " + report_id + "_UnionItem:\n")
            # file.write("\tbase = None #" + report_id + "_Base\n")
            # Skip UnionItem method generation
            # cube_structure_items = []
            # ... (UnionItem methods skipped)


            file.write("\nclass " + report_id + "_Base:\n")
            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[report_id+ '_cube_structure']
            except KeyError:
                print(f"No cube structure items for report_id: {report_id}")

            if len(cube_structure_items) == 0:
                file.write("\tpass\n")

            for cube_structure_item in cube_structure_items:
                print(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id

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


            # Skip UnionTable generation - no longer needed with direct product filtering
            # file.write("\nclass " + report_id + "_UnionTable :\n")
            # ... (UnionTable methods skipped)

            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():
                class_header_is_written = False
                for cube_link in cube_links:
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        # only write the class header once
                        if not class_header_is_written:
                            file.write("\nclass " + cube_link.join_identifier.replace(' ','_') + "(" + report_id + "_Base):\n")
                            class_header_is_written = True

                        cube_structure_item_links = []
                        try:
                            cube_structure_item_links = sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]
                        except KeyError:
                            print(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")
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


            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():

                report_and_join =   join_for_report_id.split(':')
                join_id = report_and_join[1]
                if report_and_join[0] == report_id:
                    file.write("\nclass " + report_id + "_" + join_id.replace(' ','_') + "_Table:\n" )
                    for cube_link in cube_links:
                        cube_structure_item_links = []
                        try:
                            cube_structure_item_links = sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]
                        except KeyError:
                            print(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")

                        primary_cubes_added = []
                        for cube_structure_item_link in cube_structure_item_links:
                            if cube_structure_item_link.cube_link_id.primary_cube_id.cube_id not in primary_cubes_added:
                                file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + "_Table = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "\n")
                                primary_cubes_added.append(cube_structure_item_link.cube_link_id.primary_cube_id.cube_id)


                if report_and_join[0] == report_id:
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

    def delete_generated_python_join_files(context):
        base_dir = settings.BASE_DIR
        python_dir = os.path.join(base_dir, 'results',  'generated_python_joins')
        for file in os.listdir(python_dir):
            os.remove(os.path.join(python_dir, file))
