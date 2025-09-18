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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RunImportDPMData(AppConfig):
    """
    Django AppConfig for running the DPM data import process.

    This class sets up the necessary context and runs the import process
    to download and convert DPM data from the EBA website into SDD format.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')
    logger = logging.getLogger(__name__)

    @staticmethod
    def run_import(import_:bool):
        # Get logger instance
        logger = logging.getLogger(__name__)

        logger.info(f"Starting DPM import process with import_={import_}")

        # Import and run the DPM integration service
        from pybirdai.process_steps.dpm_integration.dpm_integration_service import DPMImporterService
        from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import (
            ImportWebsiteToSDDModel
        )
        from pybirdai.context.context import Context
        from django.conf import settings

        base_dir = settings.BASE_DIR
        logger.debug(f"Base directory: {base_dir}")

        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'results')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        logger.info(f"SDD context output directory: {sdd_context.output_directory}")

        context = Context()
        context.file_directory = sdd_context.output_directory
        context.output_directory = sdd_context.output_directory
        logger.debug(f"Context file directory: {context.file_directory}")

        try:
            # Run DPM import service
            if not import_:
                logger.info("Mapping the DPM Metadata")
                logger.info(f"Initializing DPMImporterService with directory: {context.file_directory}")
                dpm_service = DPMImporterService(output_directory=context.file_directory)

                logger.info("Starting DPM service application run")
                dpm_service.run_application()
                logger.info("DPM metadata mapping completed successfully")

            # After DPM import, run the report templates import
            if import_:
                logger.info("Running Import on the DPM Metadata")
                logger.info("Starting report templates import from SDD")
                ImportWebsiteToSDDModel().import_report_templates_from_sdd(sdd_context,dpm=True)
                logger.info("Report templates import completed successfully")

        except Exception as e:
            logger.error(f"Error during DPM import process: {e}", exc_info=True)
            raise

        logger.info("DPM import process completed")

    def ready(self):
        # This method is still needed for Django's AppConfig
        self.logger.debug("RunImportDPMData AppConfig is ready")
        pass
