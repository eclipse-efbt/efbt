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

# Create a logger
logger = logging.getLogger(__name__)


def _check_ancrdt_files_exist(base_dir):
    """Check if ANCRDT technical_export files exist."""
    technical_export_dir = os.path.join(base_dir, 'resources', 'technical_export')

    # Check for key ANCRDT files
    required_files = [
        'cube.csv',
        'cube_structure.csv',
        'cube_structure_item.csv',
        'variable.csv',
    ]

    for filename in required_files:
        filepath = os.path.join(technical_export_dir, filename)
        if not os.path.exists(filepath):
            return False

    return True


def _fetch_ancrdt_from_github():
    """Fetch ANCRDT data from configured GitHub repository."""
    try:
        from pybirdai.api.workflow_api import AutomodeConfigurationService
        from pybirdai.models.workflow_model import AutomodeConfiguration

        logger.info("Fetching ANCRDT data from GitHub repository...")

        config = AutomodeConfiguration.get_active_configuration()
        # Use the configured ANCRDT pipeline URL
        github_url = getattr(config, 'pipeline_url_ancrdt', None) if config else None

        if not github_url:
            github_url = 'https://github.com/regcommunity/FreeBIRD_ANCRDT'

        logger.info(f"Using GitHub URL: {github_url}")

        service = AutomodeConfigurationService()
        # Use GITHUB_TOKEN from environment if available
        github_token = os.getenv('GITHUB_TOKEN')
        result = service.fetch_files_for_framework(
            'ANCRDT',
            github_token=github_token,
            branch='main'
        )

        errors = result.get('errors', [])
        files_count = result.get('technical_export', 0)

        if errors:
            error_msg = '; '.join(errors)
            logger.warning(f"Some errors during ANCRDT fetch: {error_msg}")

        logger.info(f"ANCRDT data fetched: {files_count} files")
        return files_count > 0

    except Exception as e:
        logger.error(f"Failed to fetch ANCRDT data from GitHub: {e}")
        raise

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

class RunANCRDTImport(AppConfig):
    """
    Django AppConfig for running the website to SDD model conversion process.

    This class sets up the necessary context and runs the import process
    to convert website data into an SDD  model.
    """

    # path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_import():
        DjangoSetup.configure_django()
        from pybirdai.process_steps.ancrdt_transformation.context_ancrdt import Context
        from pybirdai.process_steps.ancrdt_transformation.sdd_context_django_ancrdt import SDDContext
        from pybirdai.process_steps.input_model.import_input_model import ImportInputModel

        # Use unified import from website_to_sddmodel
        from pybirdai.process_steps.website_to_sddmodel.import_func.import_report_templates_from_sdd import (
            import_report_templates_from_sdd
        )

        base_dir = settings.BASE_DIR

        # Check if ANCRDT files exist, fetch from GitHub if missing
        if not _check_ancrdt_files_exist(base_dir):
            logger.info("ANCRDT technical_export files not found, fetching from GitHub...")
            _fetch_ancrdt_from_github()
        else:
            logger.info("ANCRDT technical_export files found, proceeding with import")

        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        sdd_context.save_sdd_to_db = True

        # Set frameworks for AnaCredit import - BIRD + ANCRDT for isolation
        sdd_context.current_frameworks = ['BIRD', 'ANCRDT']
        logger.info(f"Framework isolation enabled: {sdd_context.current_frameworks}")

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Set frameworks for AnaCredit import - BIRD + ANCRDT for isolation
        context.current_frameworks = ['BIRD', 'ANCRDT']

        # Step 1a: Import input model first (like FINREP does)
        # This creates maintenance agencies, primitive domains, and Django model cubes
        logger.info("Importing input model (maintenance agencies, primitive domains, model cubes)...")
        ImportInputModel.import_input_model(sdd_context, context, framework_id='BIRD_EIL')
        logger.info("Input model import completed")

        if not sdd_context.exclude_reference_info_from_website:
            # Step 1b: Import ANCRDT-specific data from technical_export
            # This imports ANCRDT cubes, subdomains, members, variables, etc.
            logger.info("Importing ANCRDT-specific data from technical_export...")
            import_report_templates_from_sdd(
                sdd_context,
                dataset_type="ancrdt",
                file_dir="technical_export"
            )
            logger.info("ANCRDT data import completed")

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass

def main():
    DjangoSetup.configure_django()
    RunANCRDTImport.run_import()

if __name__ == "__main__":
    main()
