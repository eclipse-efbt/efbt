# coding=UTF-8#
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

from pybirdai.bird_meta_data_model import *

import os
import csv

class CreateOutputLayers:
    def create_filters(self, context, sdd_context, framework, version):
        """
        Create output layers for each cube mapping based on variable mappings
        and expanded variable set mappings.

        Args:
            context: The context object containing file directory information.
            sdd_context: The SDD context object containing mapping information.
            framework: The reporting framework (e.g., 'FINREP_REF', 'AE_REF').
            version: The version of the framework.
        """
        file_location = os.path.join(
            context.file_directory, f"in_scope_reports_{framework}.csv"
        )
        in_scope_reports = self._get_in_scope_reports(
            file_location, framework, version
        )
        
        for destination_cube in sdd_context.mapping_to_cube_dictionary.keys():
            if destination_cube.replace('.', '_') in in_scope_reports:
                self.create_output_layer_for_cube_mapping(
                    context, sdd_context, destination_cube, framework
                )

    def _get_in_scope_reports(self, file_location, framework, version):
        """
        Retrieve the list of in-scope reports from a CSV file.

        Args:
            file_location (str): The path to the CSV file.
            framework (str): The reporting framework.
            version (str): The version of the framework.

        Returns:
            list: A list of in-scope report names.
        """
        in_scope_reports = []
        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(filereader)  # Skip header
            for row in filereader:
                report_template = row[0]
                report_name = self._generate_report_name(
                    report_template, framework, version
                )
                in_scope_reports.append(report_name)
        return in_scope_reports

    def _generate_report_name(self, report_template, framework, version):
        """
        Generate a report name based on the template, framework, and version.

        Args:
            report_template (str): The report template name.
            framework (str): The reporting framework.
            version (str): The version of the framework.

        Returns:
            str: The generated report name.
        """
        version_str = version.replace('.', '_')
        if framework == 'FINREP_REF':
            return f'M_{report_template}_REF_FINREP {version_str}'
        elif framework == 'AE_REF':
            return f'M_{report_template}_REF_AE{framework} {version_str}'

    def create_output_layer_for_cube_mapping(self, context, sdd_context, destination_cube, framework):
        """
        Create an output layer for each cube mapping.

        Args:
            context: The context object.
            sdd_context: The SDD context object.
            destination_cube (str): The destination cube name.
            framework (str): The reporting framework.
        """
        output_layer_cube, output_layer_cube_structure = self._create_cube_and_structure(destination_cube)
        
        sdd_context.rol_cube_structure_dictionary[output_layer_cube_structure.name] = output_layer_cube_structure
        sdd_context.rol_cube_dictionary[output_layer_cube.name] = output_layer_cube
        
        if context.save_derived_sdd_items:
            output_layer_cube_structure.save()
            output_layer_cube.save()
        
        if framework == 'FINREP_REF':
            sdd_context.finrep_output_cubes[output_layer_cube.name] = output_layer_cube
        elif framework == 'AE_REF':
            sdd_context.ae_output_cubes[output_layer_cube.name] = output_layer_cube

    def _create_cube_and_structure(self, destination_cube):
        """
        Create a cube and its corresponding structure.

        Args:
            destination_cube (str): The destination cube name.

        Returns:
            tuple: A tuple containing the created CUBE and CUBE_STRUCTURE objects.
        """
        cube_name = self._generate_cube_name(destination_cube)
        
        output_layer_cube = CUBE()
        output_layer_cube.cube_id = cube_name
        output_layer_cube.name = cube_name

        output_layer_cube_structure = CUBE_STRUCTURE()
        output_layer_cube_structure.cube_structure_id = f"{cube_name}_cube_structure"
        output_layer_cube_structure.name = f"{cube_name}_cube_structure"
        
        output_layer_cube.cube_structure_id = output_layer_cube_structure
        
        return output_layer_cube, output_layer_cube_structure

    def _generate_cube_name(self, destination_cube):
        """
        Generate a cube name from the destination cube string.

        Args:
            destination_cube (str): The destination cube name.

        Returns:
            str: The generated cube name.
        """
        return destination_cube.replace('.', '_').replace(' ', '_')[2:]
