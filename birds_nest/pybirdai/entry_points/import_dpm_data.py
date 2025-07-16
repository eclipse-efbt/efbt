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
# This script imports DPM data from the EBA website and converts it to SDD format

import django
import os
from django.apps import AppConfig
from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings
import logging

class RunImportDPMData(AppConfig):
    """
    Django AppConfig for running the DPM data import process.

    This class sets up the necessary context and runs the import process
    to download and convert DPM data from the EBA website into SDD format.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_import():
        # Import and run the DPM integration service
        from pybirdai.process_steps.dpm_integration.dpm_integration_service import DPMImporterService
        from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
        from pybirdai.context.context import Context
        from django.conf import settings

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Run DPM import service
        logging.info("Mapping the DPM Metadata")
        dpm_service = DPMImporterService(output_directory=context.file_directory)
        dpm_service.run_application()

        # After DPM import, run the report templates import
        logging.info("Running Import on the DPM Metadata")
        RunImportReportTemplatesFromWebsite.run_import()

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass
