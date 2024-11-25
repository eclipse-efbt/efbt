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
        """
        file_location = os.path.join(
            context.file_directory, "joins_configuration", f"in_scope_reports_{framework}.csv"
        )
        in_scope_reports = self._get_in_scope_reports(
            file_location, framework, version
        )
        
        # Lists to collect objects for bulk creation
        cubes_to_create = []
        structures_to_create = []
        
        for destination_cube in sdd_context.mapping_to_cube_dictionary.keys():
            if destination_cube.replace('.', '_') in in_scope_reports:
                cube, structure = self.create_output_layer_for_cube_mapping(
                    context, sdd_context, destination_cube, framework
                )
                if cube and structure:  # Only add if objects were created
                    cubes_to_create.append(cube)
                    structures_to_create.append(structure)
        
        # Bulk create if saving is enabled
        if context.save_derived_sdd_items and cubes_to_create:
            CUBE_STRUCTURE.objects.bulk_create(structures_to_create)
            CUBE.objects.bulk_create(cubes_to_create)

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
        with open(file_location, encoding='utf-8') as csvfile:
            return [
                self._generate_report_name(row[0], framework, version)
                for row in csv.reader(csvfile, delimiter=',', quotechar='"')
            ][1:]

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
        templates = {
            'FINREP_REF': f'M_{report_template}_REF_FINREP {version_str}',
            'AE_REF': f'M_{report_template}_REF_AE{framework} {version_str}'
        }
        return templates[framework]

    def create_output_layer_for_cube_mapping(self, context, sdd_context, destination_cube, framework):
        """
        Create an output layer for each cube mapping.
        Returns the created cube and structure instead of saving them.
        """
        output_layer_cube, output_layer_cube_structure = self._create_cube_and_structure(destination_cube)
        
        structures_and_cubes = {
            'structure': (sdd_context.rol_cube_structure_dictionary, output_layer_cube_structure),
            'cube': (sdd_context.rol_cube_dictionary, output_layer_cube),
            'FINREP_REF': (sdd_context.finrep_output_cubes, output_layer_cube),
            'AE_REF': (sdd_context.ae_output_cubes, output_layer_cube)
        }
        
        structures_and_cubes['structure'][0][output_layer_cube_structure.name] = output_layer_cube_structure
        structures_and_cubes['cube'][0][output_layer_cube.name] = output_layer_cube
        
        if framework in structures_and_cubes:
            structures_and_cubes[framework][0][output_layer_cube.name] = output_layer_cube
        
        return output_layer_cube, output_layer_cube_structure

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
