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

import os
from pathlib import Path

import django
from django.apps import AppConfig
from django.conf import settings


class RunExportMappingTemplate(AppConfig):
    """
    Django AppConfig for exporting an empty mapping template CSV with example data.

    This class provides functionality to generate a CSV template that users can
    fill out to create new mappings in a business-friendly format.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_export_mapping_template():
        """
        Export an empty mapping template with 2-3 example rows.

        Returns:
            str: CSV content as string
        """
        from pybirdai.process_steps.template_mapping_definition.export_mapping_template import ExportMappingTemplate
        return ExportMappingTemplate.handle()


class RunExportMappingData(AppConfig):
    """
    Django AppConfig for exporting an existing mapping to CSV format.

    This class converts a MAPPING_DEFINITION from the database into a
    business-friendly CSV format for offline editing.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_export_mapping_data(mapping_id):
        """
        Export an existing mapping definition to CSV format.

        Args:
            mapping_id: The ID of the MAPPING_DEFINITION to export

        Returns:
            str: CSV content as string
        """
        from pybirdai.process_steps.template_mapping_definition.export_mapping_data import ExportMappingData
        return ExportMappingData.handle(mapping_id)


class RunImportMappingData(AppConfig):
    """
    Django AppConfig for importing mapping data from CSV files.

    This class handles the complete import workflow: parsing CSV,
    validating data, and creating database records.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_parse_mapping_csv(csv_file):
        """
        Parse a mapping CSV file into structured data.

        Args:
            csv_file: File object or file path

        Returns:
            dict: Parsed mapping data structure
        """
        from pybirdai.process_steps.template_mapping_definition.parse_mapping_csv import ParseMappingCSV
        return ParseMappingCSV.parse(csv_file)

    @staticmethod
    def run_validate_mapping_csv(parsed_data):
        """
        Validate parsed mapping data against database constraints.

        Args:
            parsed_data: Parsed mapping data structure

        Returns:
            dict: Validation report with errors and warnings
        """
        from pybirdai.process_steps.template_mapping_definition.validate_mapping_csv import ValidateMappingCSV
        return ValidateMappingCSV.validate(parsed_data)

    @staticmethod
    def run_import_mapping_data(parsed_data, mapping_name, mapping_code, mapping_type, algorithm, cube_ids, maintenance_agency_id, overwrite=False):
        """
        Import mapping data into the database.

        Args:
            parsed_data: Validated mapping data structure
            mapping_name: Name for the mapping definition
            mapping_code: Unique code for the mapping
            mapping_type: Type of mapping (e.g., 'ONE_TO_ONE', 'ONE_TO_MANY')
            algorithm: Algorithm description (optional)
            cube_ids: List of cube IDs to associate with this mapping
            maintenance_agency_id: ID of the maintenance agency
            overwrite: Whether to overwrite existing mapping with same name

        Returns:
            int: The ID of the created/updated MAPPING_DEFINITION
        """
        from pybirdai.process_steps.template_mapping_definition.import_mapping_data import ImportMappingData
        return ImportMappingData.handle(
            parsed_data=parsed_data,
            mapping_name=mapping_name,
            mapping_code=mapping_code,
            mapping_type=mapping_type,
            algorithm=algorithm,
            cube_ids=cube_ids,
            maintenance_agency_id=maintenance_agency_id,
            overwrite=overwrite
        )
