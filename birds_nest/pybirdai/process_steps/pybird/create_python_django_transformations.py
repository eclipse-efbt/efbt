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
from pybirdai.models.bird_meta_data_model import *
import os
import shutil
import logging
from django.conf import settings

logger = logging.getLogger(__name__)
from pybirdai.process_steps.pybird.orchestration import Orchestration
from pybirdai.models import Trail, MetaDataTrail, DerivedTable, FunctionText, TableCreationFunction
from datetime import datetime

# Mapping of frameworks to their type (datasets vs templates)
FRAMEWORK_TYPE_MAP = {
    'ANCRDT': 'datasets',
    # All other frameworks are templates
}

def get_framework_type(framework: str) -> str:
    """Get the type (datasets or templates) for a framework."""
    return FRAMEWORK_TYPE_MAP.get(framework.upper().replace('_REF', ''), 'templates')


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
            logger.debug("AORTA lineage tracking enabled for transformation generation")

        # Get framework for targeted deletion (preserves other frameworks' files)
        framework_id = getattr(sdd_context, 'current_framework', None)
        CreatePythonTransformations.delete_generated_python_join_files(context, framework_id)
        # Skip output_tables.py generation - no longer needed with direct product filtering
        # CreatePythonTransformations.create_output_classes( sdd_context)
        CreatePythonTransformations.create_slice_classes(sdd_context)

        # Automatic copy to filter_code disabled - use manual sync from UI instead
        # CreatePythonTransformations._copy_to_filter_code(sdd_context.output_directory, framework_id)

    @staticmethod
    def _copy_to_filter_code(output_directory, framework_id):
        """
        Copy generated join files to their respective directories for runtime use.

        New structure:
        - Logic files → filter_code/{type}/{FRAMEWORK}/joins/

        Args:
            output_directory: The results directory containing generated files
            framework_id: The framework name (e.g., 'FINREP', 'COREP', 'ANCRDT')
        """
        if not framework_id:
            return

        framework_upper = framework_id.upper().replace('_REF', '')
        fw_type = get_framework_type(framework_id)

        # Source directory: results/generated_python/{type}/{FRAMEWORK}/joins/
        source_dir = os.path.join(output_directory, 'generated_python', fw_type, framework_upper, 'joins')

        # Destination directory: filter_code/{type}/{FRAMEWORK}/joins/
        filter_code_base = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
        dest_dir = os.path.join(filter_code_base, fw_type, framework_upper, 'joins')

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
                print(f"Copied {filename} to filter_code/{fw_type}/{framework_upper}/joins/")

    def create_output_classes(  sdd_context):

         #get all the cubes_structure_items for that cube and make a related Python class.
        # Use unified folder structure: results/generated_python/{type}/{FRAMEWORK}/joins/
        framework = getattr(sdd_context, 'current_framework', 'LEGACY') or 'LEGACY'
        framework_upper = framework.upper().replace('_REF', '')
        fw_type = get_framework_type(framework)
        joins_dir = os.path.join(sdd_context.output_directory, 'generated_python', fw_type, framework_upper, 'joins')
        os.makedirs(joins_dir, exist_ok=True)
        file = open(os.path.join(joins_dir, 'output_tables.py'), "a", encoding='utf-8')
        file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")
        file.write("from datetime import datetime\n")
        file.write("from pybirdai.annotations.decorators import lineage, track_table_init\n")
        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            logger.debug(f"report_id: {report_id}")
            file.write("from ." + report_id  + "_logic import *\n")
            file.write("\nclass " + report_id + ":\n")
            file.write("\tunionOfLayers = None #  " + report_id + "_UnionItem  unionOfLayers\n")
            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[report_id+ '_cube_structure']
            except KeyError:
                logger.debug(f"No cube structure items for report_id: {report_id}")
            for cube_structure_item in cube_structure_items:
                logger.debug(f"cube_structure_item: {cube_structure_item}")
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
        # Use unified folder structure: results/generated_python/{type}/{FRAMEWORK}/joins/
        framework = getattr(sdd_context, 'current_framework', 'LEGACY') or 'LEGACY'
        framework_upper = framework.upper().replace('_REF', '')
        fw_type = get_framework_type(framework)
        joins_dir = os.path.join(sdd_context.output_directory, 'generated_python', fw_type, framework_upper, 'joins')
        os.makedirs(joins_dir, exist_ok=True)

        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            # Use framework-based directory structure instead of filename prefix
            filename = report_id + '_logic.py'
            file = open(os.path.join(joins_dir, filename), "a", encoding='utf-8')
            file.write("from pybirdai.models.bird_data_model import *\n")
            file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")
            file.write("from pybirdai.process_steps.pybird.csv_converter import CSVConverter\n")
            file.write("from datetime import datetime\n")
            file.write("from pybirdai.annotations.decorators import lineage, track_table_init\n")

            # Generate UnionItem class
            file.write("\nclass " + report_id + "_UnionItem:\n")
            file.write("\tbase = None #" + report_id + "_Base\n")

            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[report_id + '_cube_structure']
            except KeyError:
                logger.debug(f"No cube structure items for report_id: {report_id}")

            for cube_structure_item in cube_structure_items:
                variable = cube_structure_item.variable_id
                file.write('\t@lineage(dependencies={"base.' + variable.variable_id + '"})\n')

                domain = variable.domain_id.domain_id
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

                file.write('\t\treturn self.base.' + variable.variable_id + '()\n')

            file.write("\nclass " + report_id + "_Base:\n")
            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.bird_cube_structure_item_dictionary[report_id+ '_cube_structure']
            except KeyError:
                logger.debug(f"No cube structure items for report_id: {report_id}")

            if len(cube_structure_items) == 0:
                file.write("\tpass\n")

            for cube_structure_item in cube_structure_items:
                logger.debug(f"cube_structure_item: {cube_structure_item}")
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


            # Generate UnionTable class
            file.write("\nclass " + report_id + "_UnionTable :\n")
            file.write("\t" + report_id + "_UnionItems = [] # " + report_id + "_UnionItem []\n")

            # Add table attributes for each join
            join_ids = []
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():
                report_and_join = join_for_report_id.split(':')
                if report_and_join[0] == report_id:
                    join_id = report_and_join[1]
                    join_ids.append(join_id)
                    file.write("\t" + report_id + "_" + join_id.replace(' ', '_') + "_Table = None # " + join_id.replace(' ', '_') + "\n")

            # Generate calc_*_UnionItems method
            file.write("\tdef calc_" + report_id + "_UnionItems(self) -> list[" + report_id + "_UnionItem] :\n")
            file.write("\t\titems = [] # " + report_id + "_UnionItem []\n")

            # Iterate through all product tables
            for join_id in join_ids:
                file.write("\t\tfor item in self." + report_id + "_" + join_id.replace(' ', '_') + "_Table." + join_id.replace(' ', '_') + "s:\n")
                file.write("\t\t\tnewItem = " + report_id + "_UnionItem()\n")
                file.write("\t\t\tnewItem.base = item\n")
                file.write("\t\t\titems.append(newItem)\n")

            file.write("\t\treturn items\n\n")

            # Generate init method
            file.write("\tdef init(self):\n")
            file.write("\t\tOrchestration().init(self)\n")
            file.write("\t\tself." + report_id + "_UnionItems = []\n")
            file.write("\t\tself." + report_id + "_UnionItems.extend(self.calc_" + report_id + "_UnionItems())\n")
            file.write("\t\tCSVConverter.persist_object_as_csv(self,True)\n")
            file.write("\t\treturn None\n\n")

            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():
                class_header_is_written = False
                for cube_link in cube_links:
                    # Skip cube_links with null foreign_cube_id
                    if cube_link.foreign_cube_id is None:
                        print(f"Warning: Skipping cube_link {cube_link.cube_link_id} with null foreign_cube_id")
                        continue
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
                            logger.debug(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")
                        primary_cubes_added = []
                        if len(cube_structure_item_links) == 0:
                            file.write("\tpass\n")
                        for cube_structure_item_link in cube_structure_item_links:
                            # Skip if primary_cube_id is null
                            if cube_structure_item_link.cube_link_id.primary_cube_id is None:
                                print(f"Warning: Skipping cube_structure_item_link with null primary_cube_id for cube_link: {cube_structure_item_link.cube_link_id}")
                                continue
                            if cube_structure_item_link.cube_link_id.primary_cube_id.cube_id not in primary_cubes_added:
                                file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + " = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "\n")
                                primary_cubes_added.append(cube_structure_item_link.cube_link_id.primary_cube_id.cube_id)
                        for cube_structure_item_link in cube_structure_item_links:
                            # Skip if primary_cube_id is null
                            if cube_structure_item_link.cube_link_id.primary_cube_id is None:
                                continue
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
                            logger.debug(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")

                        primary_cubes_added = []
                        for cube_structure_item_link in cube_structure_item_links:
                            # Skip if primary_cube_id is null
                            if cube_structure_item_link.cube_link_id.primary_cube_id is None:
                                print(f"Warning: Skipping cube_structure_item_link with null primary_cube_id for cube_link: {cube_structure_item_link.cube_link_id}")
                                continue
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

    def delete_generated_python_join_files(context, framework_id=None):
        """Delete generated Python join files.

        Args:
            context: The context object
            framework_id: If provided, only delete files for this framework.
                         If None, delete all generated files (legacy behavior).
        """
        base_dir = settings.BASE_DIR

        if framework_id:
            # Use unified folder structure: results/generated_python/{type}/{FRAMEWORK}/joins/
            framework_upper = framework_id.upper().replace('_REF', '')
            fw_type = get_framework_type(framework_id)
            python_dir = os.path.join(base_dir, 'results', 'generated_python', fw_type, framework_upper, 'joins')

            if not os.path.exists(python_dir):
                return

            for file in os.listdir(python_dir):
                file_path = os.path.join(python_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        else:
            # Legacy behavior - delete all frameworks under both templates/ and datasets/
            for type_dir in ['templates', 'datasets']:
                type_path = os.path.join(base_dir, 'results', 'generated_python', type_dir)
                if not os.path.exists(type_path):
                    continue

                for framework_dir in os.listdir(type_path):
                    joins_dir = os.path.join(type_path, framework_dir, 'joins')
                    if os.path.isdir(joins_dir):
                        for file in os.listdir(joins_dir):
                            file_path = os.path.join(joins_dir, file)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
