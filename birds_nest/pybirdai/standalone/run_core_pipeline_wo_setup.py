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
"""
Unified pipeline script that runs all core BIRD processing steps in a single Python process.
Maintains a shared SDDContext across steps for efficient data processing.

This is the "without setup" version - it assumes setup has already been done.
By default, it skips the framework fetch (use --fetch to enable it).

Usage:
    # Run without framework fetch (default)
    uv run pybirdai/standalone/run_core_pipeline_wo_setup.py

    # Run with framework fetch
    uv run pybirdai/standalone/run_core_pipeline_wo_setup.py --fetch
"""
import django
import os
import sys
import logging
import cProfile
import argparse

# Parse arguments
parser = argparse.ArgumentParser(description='Run FINREP/BIRD Core Pipeline (without setup)')
parser.add_argument('--fetch', action='store_true',
                    help='Fetch framework files from GitHub (skipped by default)')
parser.add_argument('--token', default=None,
                    help='GitHub token for private repositories (or set GITHUB_TOKEN env var)')
args = parser.parse_args()

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


def fetch_framework_files(github_token=None):
    """
    Step 0: Fetch FINREP framework files from the configured pipeline URL.

    Uses pipeline_url_main from AutomodeConfiguration (default: FreeBIRD_IL_66).
    """
    logger.info("="*80)
    logger.info("STEP 0: FETCHING FINREP FRAMEWORK FILES")
    logger.info("="*80)

    from pybirdai.api.workflow_api import AutomodeConfigurationService

    service = AutomodeConfigurationService()
    result = service.fetch_files_for_framework('FINREP', github_token=github_token)

    if result.get('errors'):
        for error in result['errors']:
            logger.error(error)
        raise RuntimeError("Failed to fetch framework files")

    logger.info(f"Fetched {result.get('technical_export', 0)} files from {result.get('github_url')}")
    logger.info("Step 0 completed successfully")
    return result


def run_step_1():
    """
    Step 1: Import data into Django database
    - Delete existing database
    - Import input model from SQL Developer
    - Import report templates
    - Import hierarchies
    - Import semantic integrations
    """
    logger.info("="*80)
    logger.info("STEP 1: IMPORTING DATA INTO DATABASE")
    logger.info("="*80)

    from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
    from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
    from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
    from pybirdai.entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
    from pybirdai.entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite

    with cProfile.Profile() as pr:
        logger.info("Deleting existing BIRD metadata database...")
        app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
        app_config.run_delete_bird_metadata_database()

        logger.info("Importing input model from SQL Developer...")
        app_config = RunImportInputModelFromSQLDev("pybirdai", "birds_nest")
        app_config.ready()

        logger.info("Importing report templates...")
        RunImportReportTemplatesFromWebsite.run_import()

        logger.info("Importing hierarchy analysis...")
        RunImportHierarchiesFromWebsite.import_hierarchies()

        logger.info("Importing semantic integrations...")
        RunImportSemanticIntegrationsFromWebsite.import_mappings_from_website()

        pr.dump_stats("CoreStep1.prof")

    logger.info("Step 1 completed successfully")


def run_step_2(sdd_context, context):
    """
    Step 2: Create filters and joins metadata
    - Create output layers
    - Create report filters
    - Create joins metadata

    Args:
        sdd_context: Shared SDDContext with loaded data
        context: General context object
    """
    logger.info("="*80)
    logger.info("STEP 2: CREATING FILTERS AND JOINS METADATA")
    logger.info("="*80)

    from pybirdai.process_steps.report_filters.create_output_layers import CreateOutputLayers
    from pybirdai.process_steps.report_filters.create_report_filters import CreateReportFilters
    from pybirdai.process_steps.joins_meta_data.create_joins_meta_data import JoinsMetaDataCreator
    from pybirdai.process_steps.joins_meta_data.main_category_finder import MainCategoryFinder

    with cProfile.Profile() as pr:
        logger.info("Creating output layers...")
        CreateOutputLayers().create_filters(
            context, sdd_context, "FINREP_REF", "3.0"
        )

        logger.info("Creating report filters...")
        CreateReportFilters().create_report_filters(
            context, sdd_context, "FINREP_REF", "3.0"
        )

        logger.info("Creating main category maps...")
        MainCategoryFinder().create_report_to_main_category_maps(
            context,
            sdd_context,
            "FINREP_REF",
            ["3", "3.0-Ind", "FINREP 3.0-Ind"]
        )

        logger.info("Generating joins metadata...")
        JoinsMetaDataCreator().generate_joins_meta_data(
            context,
            sdd_context,
            "FINREP_REF"
        )

        pr.dump_stats("CoreStep2.prof")

    logger.info("Step 2 completed successfully")


def run_step_3(sdd_context, context):
    """
    Step 3: Generate executable Python code
    - Generate executable filters
    - Generate executable joins

    Args:
        sdd_context: Shared SDDContext with loaded data
        context: General context object
    """
    logger.info("="*80)
    logger.info("STEP 3: GENERATING EXECUTABLE PYTHON CODE")
    logger.info("="*80)

    from pybirdai.process_steps.pybird.create_executable_filters import CreateExecutableFilters
    from pybirdai.process_steps.pybird.create_python_django_transformations import CreatePythonTransformations

    with cProfile.Profile() as pr:
        logger.info("Generating executable filter code...")
        CreateExecutableFilters().create_executable_filters(context, sdd_context)

        logger.info("Generating executable join code...")
        CreatePythonTransformations().create_python_joins(context, sdd_context)

        pr.dump_stats('CoreStep3.prof')

    logger.info("Step 3 completed successfully")


def run_step_4():
    """
    Step 4: Run regulatory template tests
    - Fetch test suite from GitHub
    - Run tests from bird-default-test-suite
    """
    logger.info("="*80)
    logger.info("STEP 4: RUNNING TESTS")
    logger.info("="*80)

    from pybirdai.api.workflow.automode_service import AutomodeConfigurationService
    from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

    # Get test suite URL from environment or use default
    test_suite_url = os.environ.get(
        'TEST_SUITE_URL_MAIN',
        'https://github.com/BIRD-Software-Solutions/bird-default-test-suite'
    )
    github_token = os.environ.get('GITHUB_TOKEN')

    # Fetch test suite from GitHub
    logger.info(f"Fetching test suite from: {test_suite_url}")
    try:
        workflow_service = AutomodeConfigurationService()
        workflow_service._fetch_test_suite_from_github(test_suite_url, token=github_token, use_mirror=True)
    except Exception as e:
        logger.error(f"Failed to fetch test suite: {str(e)}")
        raise

    # Use the bird-default-test-suite folder
    suite_name = 'bird-default-test-suite'
    suite_path = os.path.join('tests', suite_name)
    config_file_path = os.path.join(suite_path, 'configuration_file_tests.json')

    if not os.path.exists(config_file_path):
        logger.warning(f"No test configuration found at {config_file_path} - skipping tests")
        return

    logger.info(f"Running test suite: {suite_name}")

    # Create test runner instance
    test_runner = RegulatoryTemplateTestRunner(False)

    # Configure test runner
    test_runner.args.uv = "True"
    test_runner.args.config_file = config_file_path
    test_runner.args.dp_value = None
    test_runner.args.reg_tid = None
    test_runner.args.dp_suffix = None
    test_runner.args.scenario = None
    test_runner.args.suite_name = suite_name

    # Execute tests
    try:
        test_runner.main()
        logger.info(f"Completed test suite: {suite_name}")
    except Exception as e:
        logger.error(f"Error running test suite {suite_name}: {str(e)}")
        raise

    logger.info("Step 4 completed successfully")


if __name__ == "__main__":
    # Configure Django once at the start
    DjangoSetup.configure_django()

    from django.conf import settings
    from pybirdai.context.sdd_context_django import SDDContext
    from pybirdai.context.context import Context
    from pybirdai.process_steps.input_model.import_database_to_sdd_model import ImportDatabaseToSDDModel

    logger.info("="*80)
    logger.info("UNIFIED CORE PIPELINE - START")
    logger.info("="*80)

    try:
        # STEP 0: Fetch FINREP framework files (optional, enabled with --fetch)
        if args.fetch:
            github_token = args.token or os.environ.get('GITHUB_TOKEN')
            fetch_framework_files(github_token=github_token)
        else:
            logger.info("Skipping framework file fetch (use --fetch to enable)")

        # STEP 1: Import data into Django database
        run_step_1()

        # Initialize shared context objects
        logger.info("="*80)
        logger.info("LOADING DATA FROM DATABASE INTO CONTEXT")
        logger.info("="*80)

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Load all data from Django ORM into SDDContext dictionaries
        logger.info("Importing SDD data from database...")
        importer = ImportDatabaseToSDDModel()
        importer.import_sdd(sdd_context)
        logger.info("Context loaded successfully")

        # STEP 2: Create filters and joins metadata (using shared context)
        run_step_2(sdd_context, context)

        # STEP 3: Generate executable code (using shared context)
        run_step_3(sdd_context, context)

        # STEP 4: Run tests (no context needed)
        run_step_4()

        logger.info("="*80)
        logger.info("UNIFIED CORE PIPELINE - COMPLETED SUCCESSFULLY")
        logger.info("="*80)

    except Exception as e:
        logger.error("="*80)
        logger.error("PIPELINE FAILED")
        logger.error("="*80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)
