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
ANCRDT (AnaCredit) Pipeline Script.

This script runs the complete ANCRDT pipeline with framework-specific imports.
It fetches data from the configured pipeline_url_ancrdt (default: FreeBIRD_ANCRDT)
before running the workflow steps.

Usage:
    # Using environment variable for GitHub token (private repos)
    export GITHUB_TOKEN=ghp_xxx
    uv run pybirdai/standalone/run_ancrdt_pipeline.py

    # Or using command line argument
    uv run pybirdai/standalone/run_ancrdt_pipeline.py --token ghp_xxx

    # Skip framework fetch (use existing files)
    uv run pybirdai/standalone/run_ancrdt_pipeline.py --skip-fetch

    # Skip setup (use existing database)
    uv run pybirdai/standalone/run_ancrdt_pipeline.py --skip-setup

    # Run tests after generating executable code
    uv run pybirdai/standalone/run_ancrdt_pipeline.py --run-tests

    # Run full pipeline with tests
    uv run pybirdai/standalone/run_ancrdt_pipeline.py --run-tests --skip-fetch
"""
import os
import argparse

# Parse arguments before running setup
parser = argparse.ArgumentParser(description='Run ANCRDT/AnaCredit Pipeline')
parser.add_argument('--token', default=None,
                    help='GitHub token for private repositories (or set GITHUB_TOKEN env var, or use .pybird_github_token file)')
parser.add_argument('--skip-fetch', action='store_true',
                    help='Skip fetching framework files (use existing)')
parser.add_argument('--skip-setup', action='store_true',
                    help='Skip database setup (use existing database)')
parser.add_argument('--run-tests', action='store_true',
                    help='Run ANCRDT tests after generating executable code')
parser.add_argument('--test-config', default=None,
                    help='Path to test configuration file (auto-discovers ANCRDT suite if not specified)')
args = parser.parse_args()

# Resolve GitHub token from multiple sources: CLI arg → .pybird_github_token file → env var
def _get_standalone_github_token():
    """Get GitHub token for standalone scripts.

    Priority: CLI --token → .pybird_github_token file → GITHUB_TOKEN env var
    """
    # 1. CLI argument takes highest priority
    if args.token:
        return args.token

    # 2. Try to load from .pybird_github_token file (same as web interface)
    try:
        from pybirdai.views.workflow.github import _get_github_token
        token = _get_github_token(request=None)
        if token:
            return token
    except ImportError:
        pass  # Django not set up yet, will try after setup

    # 3. Fall back to environment variable
    return os.getenv('GITHUB_TOKEN')

# Store resolved token for later use (after Django setup)
_cli_token = args.token

# Only run setup if not skipping
if not args.skip_setup:
    os.system("uv run pybirdai/standalone/run_core_pipeline_eil_setup.py")

import django
import sys
import logging
import cProfile

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

def run_step_1_core():
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

def fetch_input_layer_files(github_token=None):
    """
    Fetch Input Layer (IL) files from the Main BIRD repository.

    Uses pipeline_url_main from AutomodeConfiguration (default: FreeBIRD_IL_66).
    This provides the base DM files and Input Layer data (INSTRMNT, PRTY, etc.)
    """
    logger.info("="*80)
    logger.info("FETCHING INPUT LAYER FILES FROM MAIN BIRD REPO")
    logger.info("="*80)

    from pybirdai.api.workflow_api import AutomodeConfigurationService

    service = AutomodeConfigurationService()
    result = service.fetch_files_for_framework('EIL', github_token=github_token)

    if result.get('errors'):
        for error in result['errors']:
            logger.error(error)
        raise RuntimeError("Failed to fetch Input Layer files")

    logger.info(f"Fetched {result.get('technical_export', 0)} files from {result.get('github_url')}")
    logger.info("Input Layer file fetch completed successfully")
    return result


def import_input_layer():
    """
    Import Input Layer data into Django database.

    This imports the base BIRD data model and Input Layer cubes
    (INSTRMNT, INSTRMNT_RL, PRTY, CLLTRL, etc.) needed for ANCRDT joins.
    """
    logger.info("="*80)
    logger.info("IMPORTING INPUT LAYER DATA")
    logger.info("="*80)

    from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
    from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
    from pybirdai.process_steps.website_to_sddmodel.import_func.import_report_templates_from_sdd import import_report_templates_from_sdd
    from pybirdai.context.sdd_context_django import SDDContext
    from django.conf import settings

    with cProfile.Profile() as pr:
        logger.info("Deleting existing BIRD metadata database...")
        app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
        app_config.run_delete_bird_metadata_database()

        logger.info("Importing input model from SQL Developer (DM files)...")
        app_config = RunImportInputModelFromSQLDev("pybirdai", "birds_nest")
        app_config.ready()

        logger.info("Importing Input Layer technical export...")
        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        sdd_context.current_frameworks = ['BIRD']

        # Import IL data (cubes, variables, subdomains, etc.) - NOT FINREP report templates
        import_report_templates_from_sdd(
            sdd_context,
            dataset_type="eil",
            file_dir="technical_export"
        )

        pr.dump_stats("InputLayerImport.prof")

    logger.info("Input Layer import completed successfully")


def fetch_framework_files(github_token=None):
    """
    Fetch ANCRDT framework files from the configured pipeline URL.

    Uses pipeline_url_ancrdt from AutomodeConfiguration.
    """
    logger.info("="*80)
    logger.info("FETCHING ANCRDT FRAMEWORK FILES FROM GITHUB")
    logger.info("="*80)

    from pybirdai.api.workflow_api import AutomodeConfigurationService

    service = AutomodeConfigurationService()
    result = service.fetch_files_for_framework('ANCRDT', github_token=github_token)

    if result.get('errors'):
        for error in result['errors']:
            logger.error(error)
        raise RuntimeError("Failed to fetch framework files")

    logger.info(f"Fetched {result.get('technical_export', 0)} files from {result.get('github_url')}")
    logger.info("Framework file fetch completed successfully")
    return result


def run_step_1():
    """
    Step 1: Import ANCRDT data into Django database

    This step reads the downloaded CSV files and imports them
    into the Django database models.
    """
    logger.info("="*80)
    logger.info("ANCRDT STEP 1: IMPORT DATA INTO DATABASE")
    logger.info("="*80)

    from pybirdai.process_steps.ancrdt_transformation.ancrdt_importer import RunANCRDTImport

    try:
        with cProfile.Profile() as pr:
            RunANCRDTImport.run_import()
            pr.dump_stats("ANCRDTStep1.prof")
        logger.info("Step 1 completed successfully")
    except Exception as e:
        logger.error(f"Step 1 failed: {str(e)}", exc_info=True)
        raise


def run_step_2(sdd_context, context):
    """
    Step 2: Create joins metadata

    Generates join metadata for ANCRDT transformations. This step
    prefetches data from the database but is run in the same process
    for efficiency.

    Args:
        sdd_context: Shared SDDContext with loaded data (for future use)
        context: General context object (for future use)
    """
    logger.info("="*80)
    logger.info("ANCRDT STEP 2: CREATE JOINS METADATA")
    logger.info("="*80)

    from pybirdai.process_steps.ancrdt_transformation.create_joins_meta_data_ancrdt import (
        JoinsMetaDataCreatorANCRDT
    )

    try:
        with cProfile.Profile() as pr:
            creator = JoinsMetaDataCreatorANCRDT()
            # JoinsMetaDataCreatorANCRDT prefetches its own data in __init__
            # Future optimization: use sdd_context to avoid re-fetching
            creator.generate_joins_meta_data()
            pr.dump_stats("ANCRDTStep2.prof")
        logger.info("Step 2 completed successfully")
    except Exception as e:
        logger.error(f"Step 2 failed: {str(e)}", exc_info=True)
        raise


def run_step_3(sdd_context, context):
    """
    Step 3: Create executable joins (generate Python code)

    Uses the shared SDDContext to generate executable Python code
    without reloading data from the database.

    Args:
        sdd_context: Shared SDDContext with loaded data
        context: General context object
    """
    logger.info("="*80)
    logger.info("ANCRDT STEP 3: CREATE EXECUTABLE JOINS")
    logger.info("="*80)

    from pybirdai.process_steps.ancrdt_transformation.create_python_django_transformations_ancrdt import (
        CreatePythonTransformations
    )

    try:
        with cProfile.Profile() as pr:
            CreatePythonTransformations().create_python_joins(context, sdd_context, logger)
            pr.dump_stats("ANCRDTStep3.prof")
        logger.info("Step 3 completed successfully")
    except Exception as e:
        logger.error(f"Step 3 failed: {str(e)}", exc_info=True)
        raise


def run_step_4_tests(config_file=None):
    """
    Step 4: Run ANCRDT tests

    Executes the ANCRDT test suite using the test configuration file.
    This validates that the generated transformations produce correct outputs.

    Args:
        config_file: Path to the test configuration JSON file. If None, auto-discovers.
    """
    logger.info("="*80)
    logger.info("ANCRDT STEP 4: RUN TEST SUITE")
    logger.info("="*80)

    from pybirdai.entry_points.run_ancrdt_tests import RunANCRDTTests
    from pybirdai.utils.test_discovery import get_ancrdt_test_suite

    # Auto-discover suite if not provided
    discovered_path, suite_name = get_ancrdt_test_suite()
    if not config_file:
        config_file = discovered_path

    if not config_file or not suite_name:
        raise FileNotFoundError(
            "No ANCRDT test suite found. Please ensure a test suite with "
            "test_type='ancrdt' exists in the tests/ directory."
        )

    logger.info(f"Using test suite: {suite_name}")
    logger.info(f"Configuration file: {config_file}")

    try:
        with cProfile.Profile() as pr:
            exit_code = RunANCRDTTests.run_tests(
                config_file_path=config_file,
                suite_name=suite_name,
                use_uv=True
            )
            pr.dump_stats("ANCRDTStep4.prof")

        if exit_code == 0:
            logger.info("Step 4 (Tests) completed successfully")
        else:
            logger.warning("Step 4 (Tests) completed with failures")

        return exit_code

    except Exception as e:
        logger.error(f"Step 4 (Tests) failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    # Configure Django once at the start
    DjangoSetup.configure_django()

    from django.conf import settings
    from pybirdai.context.sdd_context_django import SDDContext
    from pybirdai.context.context import Context
    from pybirdai.process_steps.input_model.import_database_to_sdd_model import ImportDatabaseToSDDModel

    logger.info("="*80)
    logger.info("UNIFIED ANCRDT PIPELINE - START")
    logger.info("="*80)

    try:
        # Resolve GitHub token (CLI → .pybird_github_token file → env var)
        github_token = _get_standalone_github_token()
        if github_token:
            logger.info("GitHub token loaded successfully")
        else:
            logger.info("No GitHub token found (public repos only)")

        if not args.skip_fetch:
            # Step 1: Fetch Input Layer files from Main BIRD repo
            fetch_input_layer_files(github_token=github_token)
        else:
            logger.info("Skipping Input Layer file fetch (--skip-fetch)")

        # Step 2: Import Input Layer data (DM files + IL cubes like INSTRMNT, PRTY, etc.)
        import_input_layer()

        if not args.skip_fetch:
            # Step 3: Fetch ANCRDT framework files from export repo
            fetch_framework_files(github_token=github_token)
        else:
            logger.info("Skipping ANCRDT framework file fetch (--skip-fetch)")

        # Step 4: Import ANCRDT data (overlays on top of IL data)
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
        # Only load tables needed for ANCRDT joins
        logger.info("Importing SDD data from database for ANCRDT...")
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
        logger.info("Context loaded successfully")

        # STEP 2: Create joins metadata (using shared context)
        run_step_2(sdd_context, context)

        # Explicitly commit Step 2 database changes before Step 3
        from django.db import transaction
        transaction.commit()
        logger.info("Committed Step 2 database changes")

        # Reload CUBE_LINK data that was created by Step 2
        logger.info("Reloading CUBE_LINK data created by Step 2...")
        importer.create_cube_links(sdd_context)
        importer.create_cube_structure_item_links(sdd_context)
        logger.info("CUBE_LINK data reloaded successfully")

        # STEP 3: Generate executable code (using shared context)
        run_step_3(sdd_context, context)

        # STEP 4: Run tests (optional)
        if args.run_tests:
            test_exit_code = run_step_4_tests(args.test_config)
            if test_exit_code != 0:
                logger.warning("Some tests failed - check test output for details")
        else:
            logger.info("Skipping test execution (use --run-tests to enable)")

        logger.info("="*80)
        logger.info("UNIFIED ANCRDT PIPELINE - COMPLETED SUCCESSFULLY")
        logger.info("="*80)

    except Exception as e:
        logger.error("="*80)
        logger.error("ANCRDT PIPELINE FAILED")
        logger.error("="*80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)
