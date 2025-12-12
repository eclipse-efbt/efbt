# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
"""
Setup views for full BIRD setup and test reports.
"""
import os
import logging
from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render

from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from pybirdai.entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from pybirdai.entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
from pybirdai.entry_points.create_filters import RunCreateFilters
from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata
from pybirdai.entry_points.import_export_mapping_join_metadata import RunExporterJoins, RunMappingJoinsEIL_LDM

from .loading_helpers import create_response_with_loading
from .csv_views import load_variables_from_csv_file
from pybirdai.views.core.utils_views import ensure_results_directory, process_test_results_files

logger = logging.getLogger(__name__)


def execute_full_setup_core():
    """
    Core business logic for running the full BIRD metadata database setup.
    This can be called from both view and service contexts.
    """
    logger.info("Starting full setup...")

    delete_cmd = RunDeleteBirdMetadataDatabase('pybirdai', 'birds_nest')
    delete_cmd.run_delete_bird_metadata_database()
    logger.info("Deleted existing bird metadata.")

    # Populate bird metadata database with BIRD datamodel metadata
    import_model_cmd = RunImportInputModelFromSQLDev('pybirdai', 'birds_nest')
    import_model_cmd.ready()
    logger.info("Imported input model from sqldev.")

    # Populate bird metadata database with BIRD report templates
    import_reports_cmd = RunImportReportTemplatesFromWebsite('pybirdai', 'birds_nest')
    import_reports_cmd.run_import()
    logger.info("Imported report templates from website.")

    # Load extra variables from CSV file
    base_dir = settings.BASE_DIR
    extra_variables_path = os.path.join(base_dir, 'resources', 'extra_variables', 'extra_variables.csv')
    variables_loaded = load_variables_from_csv_file(extra_variables_path)
    if variables_loaded > 0:
        logger.info(f"Loaded {variables_loaded} extra variables from CSV file.")
    else:
        logger.info("No extra variables loaded (file not found or empty).")

    # Import hierarchies from BIRD Website
    import_hierarchies_cmd = RunImportHierarchiesFromWebsite('pybirdai', 'birds_nest')
    import_hierarchies_cmd.import_hierarchies()
    logger.info("Imported hierarchies from website.")

    # Import semantic integration from bird website
    import_semantic_cmd = RunImportSemanticIntegrationsFromWebsite('pybirdai', 'birds_nest')
    import_semantic_cmd.import_mappings_from_website()
    logger.info("Imported semantic integrations from website.")

    app_config = RunCreateFilters('pybirdai', 'birds_nest')
    app_config.run_create_filters()
    logger.info("Created filters and executable filters.")

    app_config = RunCreateJoinsMetadata('pybirdai', 'birds_nest')
    app_config.run_create_joins_meta_data()
    logger.info("Created joins metadata.")

    app_config = RunExporterJoins('pybirdai', 'birds_nest')
    app_config.run_export_joins_meta_data()
    logger.info("Exported joins metadata successfully.")

    app_config = RunMappingJoinsEIL_LDM('pybirdai', 'birds_nest')
    app_config.run_mapping_joins_meta_data()
    logger.info("Mapped joins metadata successfully.")

    logger.info("Full setup completed successfully.")


def run_full_setup(request):
    """
    Runs all necessary steps to set up the BIRD metadata database.

    Steps:
    - Deletes existing metadata
    - Populates with BIRD datamodel metadata (from sqldev)
    - Populates with BIRD report templates
    - Loads extra variables from CSV file
    - Imports hierarchies (from website)
    - Imports semantic integration (from website)
    - Creates filters and executable filters
    - Creates joins metadata
    """
    if request.GET.get('execute') == 'true':
        execute_full_setup_core()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Running Full BIRD Metadata Database Setup (This may take several minutes. Please do not navigate away from this page.)",
        "Full BIRD Metadata Database Setup completed successfully.",
        '/pybirdai/edit-cube-links/',
        "Edit Cube Links"
    )


def test_report_view(request):
    """Summary page for displaying BIRD test reports."""
    results_dir = ensure_results_directory()
    templates = process_test_results_files(results_dir)

    context = {
        'templates': list(templates.values())
    }
    return render(request, 'pybirdai/tests/test_report_view.html', context)
