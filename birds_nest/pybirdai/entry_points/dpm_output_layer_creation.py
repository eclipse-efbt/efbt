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
# This script creates output layers (CUBE, CUBE_STRUCTURE, COMBINATION, etc.)
# from DPM table data for any framework
#
import django
import os
from django.apps import AppConfig
from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings
import logging

class RunDPMOutputLayerCreation(AppConfig):
    """
    Django AppConfig for running the DPM output layer creation process.

    This entry point creates output layers (CUBE, CUBE_STRUCTURE, COMBINATION, etc.)
    from DPM table data, supporting any framework (FINREP, COREP, AE, etc.).
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_creation(
        framework: str = "",
        version: str = "",
        table_code: str = ""):
        """
        Run the output layer creation process.

        This is the entry point that delegates to the business logic layer.
        Supports four different processing modes:
        1. Framework + Version: Process all tables for a specific framework version
        2. Framework only: Process all tables for a framework (all versions)
        3. Table code + Version: Process a specific table with version
        4. Table code only: Process all tables with the given code

        Args:
            framework: Optional framework name (e.g., 'FINREP', 'COREP', 'AE')
            version: Optional version string (e.g., '3.0.0')
            table_code: Optional specific table code to process (e.g., 'F01.01')

        Returns:
            dict: Results from the business layer processing
        """
        from pybirdai.process_steps.report_filters.create_non_reference_output_layers import CreateNROutputLayers
        from pybirdai.context.context import Context

        # Set up context
        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'results')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        context = Context()
        context.file_directory = sdd_context.output_directory
        context.output_directory = sdd_context.output_directory

        # Create output layer creator instance
        creator = CreateNROutputLayers()

        # Determine which processing mode to use and delegate to business layer
        try:
            if framework and version:
                # Mode 1: Framework + Version
                logging.info(f"Processing framework '{framework}' version '{version}'")
                return creator.process_by_framework_version(framework, version, save_to_db=True)
                
            elif framework and not version:
                # Mode 2: Framework only (all versions)
                logging.info(f"Processing all versions of framework '{framework}'")
                return creator.process_by_framework(framework, save_to_db=True)
                
            elif not framework and table_code and version:
                # Mode 3: Table code + Version
                logging.info(f"Processing table '{table_code}' version '{version}'")
                return creator.process_by_table_code_version(table_code, version, save_to_db=True)
                
            elif not framework and table_code and not version:
                # Mode 4: Table code only (all versions)
                logging.info(f"Processing all versions of table '{table_code}'")
                return creator.process_by_table_code(table_code, save_to_db=True)
                
            else:
                # Invalid parameter combination
                return {
                    'status': 'error',
                    'message': 'Invalid parameters. Please provide either: '
                               '1) framework and optionally version, or '
                               '2) table_code and optionally version'
                }
                
        except Exception as e:
            logging.error(f"Error during output layer creation: {str(e)}")
            return {
                'status': 'error',
                'message': f'Unexpected error: {str(e)}'
            }

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass