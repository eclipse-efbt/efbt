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
import django
import os
import sys
from django.apps import AppConfig
from django.conf import settings
import logging
from importlib import metadata
import ast

# Create a logger
logger = logging.getLogger(__name__)

class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise

if __name__ == "__main__":
    DjangoSetup.configure_django()
    from pybirdai.entry_points.import_dpm_data import RunImportDPMData
    from pybirdai.entry_points.dpm_output_layer_creation import RunDPMOutputLayerCreation
    from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

    app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
    app_config.run_delete_bird_metadata_database()

    app_config = RunImportDPMData('pybirdai', 'birds_nest')
    app_config.run_import(import_=False)

    app_config = RunImportDPMData('pybirdai', 'birds_nest')
    app_config.run_import(import_=True)

    app_config = RunDPMOutputLayerCreation('pybirdai', 'birds_nest')
    results = app_config.run_creation(framework="EBA_COREP", table_code="", version="4.0")
