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
#

from pybirdai.models.bird_meta_data_model import *
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT
from pybirdai.services.framework_selection import get_or_create_maintenance_agency_for_framework

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
            # Use update_or_create pattern for idempotent re-runs
            # First, delete existing cubes and structures that we're about to recreate
            cube_ids = [c.cube_id for c in cubes_to_create]
            structure_ids = [s.cube_structure_id for s in structures_to_create]

            # Delete existing cubes first (they reference structures via foreign key)
            CUBE.objects.filter(cube_id__in=cube_ids).delete()
            CUBE_STRUCTURE.objects.filter(cube_structure_id__in=structure_ids).delete()

            # Now bulk create the new ones
            CUBE_STRUCTURE.objects.bulk_create(structures_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT)
            CUBE.objects.bulk_create(cubes_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT)

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

        Supports any framework by extracting the base framework name and generating
        a consistent naming pattern.

        Args:
            report_template (str): The report template name.
            framework (str): The reporting framework (e.g., 'FINREP_REF', 'COREP_REF', 'AE_REF').
            version (str): The version of the framework (e.g., '3.0', '4.0').

        Returns:
            str: The generated report name.
        """
        version_str = version.replace('.', '_')

        # Extract base framework name (e.g., FINREP from FINREP_REF, COREP from COREP_REF)
        base_framework = framework.replace('_REF', '')

        # Handle special cases for framework naming patterns
        if framework == 'AE_REF':
            # AE uses a different pattern: M_{template}_REF_AE{framework} {version}
            return f'M_{report_template}_REF_AE{framework} {version_str}'

        # Default pattern for most frameworks: M_{template}_REF_{FRAMEWORK} {version}
        # Works for FINREP_REF, COREP_REF, and other standard frameworks
        return f'M_{report_template}_REF_{base_framework} {version_str}'

    def create_output_layer_for_cube_mapping(self, context, sdd_context, destination_cube, framework):
        """
        Create an output layer for each cube mapping.
        Returns the created cube and structure instead of saving them.
        """
        output_layer_cube, output_layer_cube_structure = self._create_cube_and_structure(destination_cube, framework)

        # Always add to the common dictionaries
        sdd_context.bird_cube_structure_dictionary[output_layer_cube_structure.name] = output_layer_cube_structure
        sdd_context.bird_cube_dictionary[output_layer_cube.name] = output_layer_cube

        # Add to framework-specific dictionary if it exists
        # Extract base framework name (e.g., FINREP from FINREP_REF)
        base_framework = framework.replace('_REF', '').lower()

        # Try to get framework-specific output cubes dictionary
        framework_cubes_attr = f'{base_framework}_output_cubes'
        if hasattr(sdd_context, framework_cubes_attr):
            getattr(sdd_context, framework_cubes_attr)[output_layer_cube.name] = output_layer_cube
        else:
            # Fallback: create a generic output_cubes dictionary if it doesn't exist
            if not hasattr(sdd_context, 'framework_output_cubes'):
                sdd_context.framework_output_cubes = {}
            if framework not in sdd_context.framework_output_cubes:
                sdd_context.framework_output_cubes[framework] = {}
            sdd_context.framework_output_cubes[framework][output_layer_cube.name] = output_layer_cube

        return output_layer_cube, output_layer_cube_structure

    def _create_cube_and_structure(self, destination_cube, framework):
        """
        Create a cube and its corresponding structure.

        Args:
            destination_cube (str): The destination cube name.
            framework (str): The framework ID (e.g., 'FINREP_REF', 'AE_REF').

        Returns:
            tuple: A tuple containing the created CUBE and CUBE_STRUCTURE objects.
        """
        cube_name = self._generate_cube_name(destination_cube)

        # Get or create the framework object
        framework_obj, _ = FRAMEWORK.objects.get_or_create(
            framework_id=framework,
            defaults={'name': framework, 'code': framework}
        )

        # Get maintenance agency based on framework
        maintenance_agency = get_or_create_maintenance_agency_for_framework(framework)

        output_layer_cube = CUBE()
        output_layer_cube.cube_id = cube_name
        output_layer_cube.name = cube_name
        output_layer_cube.cube_type = 'RC'
        output_layer_cube.framework_id = framework_obj
        output_layer_cube.maintenance_agency_id = maintenance_agency

        output_layer_cube_structure = CUBE_STRUCTURE()
        output_layer_cube_structure.cube_structure_id = f"{cube_name}_cube_structure"
        output_layer_cube_structure.name = f"{cube_name}_cube_structure"
        output_layer_cube_structure.maintenance_agency_id = maintenance_agency

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
