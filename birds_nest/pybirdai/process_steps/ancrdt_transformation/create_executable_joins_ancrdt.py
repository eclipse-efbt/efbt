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
"""Entry point for creating generation rules."""

import os
from pathlib import Path
import sys
import django
from django.apps import AppConfig
from django.conf import settings

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log.log"),
        logging.StreamHandler()
    ]
)

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
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
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

class RunCreateExecutableJoins(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    DjangoSetup.configure_django()
    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def create_python_joins_from_db(logger=logger):
        """Execute the process of creating generation rules from the database when the app is ready."""

        from pybirdai.process_steps.input_model.import_database_to_sdd_model import (
            ImportDatabaseToSDDModel
        )
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context
        from pybirdai.process_steps.ancrdt_transformation.create_python_django_transformations_ancrdt import (
            CreatePythonTransformations
        )

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Only import the necessary tables for joins
        importer = ImportDatabaseToSDDModel()

        importer.import_sdd_for_joins(sdd_context, [
            'MAINTENANCE_AGENCY',
            'DOMAIN',
            'VARIABLE',
            'CUBE',
            'CUBE_STRUCTURE',
            'CUBE_STRUCTURE_ITEM',
            'CUBE_LINK',
            'CUBE_STRUCTURE_ITEM_LINK'
        ])
        CreatePythonTransformations().create_python_joins(context, sdd_context,logger)

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass

def main():
    DjangoSetup.configure_django()
    RunCreateExecutableJoins.create_python_joins_from_db()

if __name__ == "__main__":
    main()
