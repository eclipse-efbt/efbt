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
import subprocess

subprocess.run(["uv", "run", "pybirdai/standalone/run_core_pipeline_setup.py"], check=True)

"""
Unified ANCRDT pipeline script that runs all steps in a single Python process.
Maintains a shared SDDContext across steps for efficient data processing.
"""
import django
import os
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

def run_step_0():
    """
    Step 0: Fetch ANCRDT CSV data from ECB website

    This step downloads ANCRDT framework data from the ECB BIRD website
    and saves it to the results/ancrdt_csv directory.
    """
    logger.info("="*80)
    logger.info("ANCRDT STEP 0: FETCH CSV DATA FROM ECB WEBSITE")
    logger.info("="*80)

    from pybirdai.utils.bird_ecb_website_fetcher import BirdEcbWebsiteClient

    try:
        client = BirdEcbWebsiteClient()
        output_dir = client.request_and_save(
            tree_root_ids="ANCRDT",
            tree_root_type="FRAMEWORK",
            output_dir="results/ancrdt_csv",
            format_type="csv",
            include_mapping_content=False,
            include_rendering_content=False,
            include_transformation_content=False,
            only_currently_valid_metadata=False
        )
        logger.info(f"Step 0 completed successfully. Data saved to: {output_dir}")
    except Exception as e:
        logger.error(f"Step 0 failed: {str(e)}", exc_info=True)
        raise


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
        # STEP 1 Core: Setup with the first step of the BIRD Main Process
        run_step_1_core()

        # STEP 0: Fetch ANCRDT CSV data from ECB website
        run_step_0()

        # STEP 1: Import ANCRDT data into Django database
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

        logger.info("="*80)
        logger.info("UNIFIED ANCRDT PIPELINE - COMPLETED SUCCESSFULLY")
        logger.info("="*80)

    except Exception as e:
        logger.error("="*80)
        logger.error("ANCRDT PIPELINE FAILED")
        logger.error("="*80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)
