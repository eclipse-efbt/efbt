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
Process execution views for running various data processing tasks.
"""
import logging
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from pybirdai.entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from pybirdai.entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
from pybirdai.entry_points.create_filters import RunCreateFilters
from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata
from pybirdai.entry_points.delete_joins_metadata import RunDeleteJoinsMetadata
from pybirdai.entry_points.delete_semantic_integrations import RunDeleteSemanticIntegrations
from pybirdai.entry_points.delete_output_concepts import RunDeleteOutputConcepts
from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins
from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
from pybirdai.entry_points.execute_datapoint import RunExecuteDataPoint
from pybirdai.entry_points.create_django_models import RunCreateDjangoModels
from pybirdai.entry_points.convert_ldm_to_sdd_hierarchies import RunConvertLDMToSDDHierarchies
from pybirdai.entry_points.upload_sqldev_eil_files import UploadSQLDevEILFiles
from pybirdai.entry_points.upload_sqldev_eldm_files import UploadSQLDevELDMFiles
from pybirdai.entry_points.upload_technical_export_files import UploadTechnicalExportFiles
from pybirdai.entry_points.upload_joins_configuration import UploadJoinsConfiguration

from .loading_helpers import create_response_with_loading

logger = logging.getLogger(__name__)


def run_create_joins_meta_data(request):
    """Create joins metadata."""
    if request.GET.get('execute') == 'true':
        app_config = RunCreateJoinsMetadata('pybirdai', 'birds_nest')
        app_config.run_create_joins_meta_data()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Creating Joins Metadata (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Joins Metadata created successfully.",
        '/pybirdai/create-transformation-rules-in-smcubes',
        "Create Transformations Rules MetaData"
    )


def create_django_models(request):
    """Create Django ORM models."""
    if request.GET.get('execute') == 'true':
        app_config = RunCreateDjangoModels('pybirdai', 'birds_nest')
        app_config.ready()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Creating Django Models",
        "Created Django Models successfully.",
        '/pybirdai/create-bird-database',
        "Create BIRD Database"
    )


def run_create_python_joins(request):
    """Generate Python join code."""
    if request.GET.get('execute') == 'true':
        app_config = RunCreateExecutableJoins('pybirdai', 'birds_nest')
        app_config.create_python_joins()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Creating Python Joins (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Created Executable Joins in Python",
        '/pybirdai/workflow/task/4/do/',
        "Do"
    )


def run_delete_joins_meta_data(request):
    """Delete joins metadata."""
    if request.GET.get('execute') == 'true':
        app_config = RunDeleteJoinsMetadata('pybirdai', 'birds_nest')
        app_config.run_delete_joins_meta_data()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Deleting Joins Metadata",
        "Deleted Transformation Metadata successfully",
        '/pybirdai/workflow/task/4/review/',
        "Review"
    )


def run_delete_mappings(request):
    """Delete mappings metadata."""
    if request.GET.get('execute') == 'true':
        app_config = RunDeleteSemanticIntegrations('pybirdai', 'birds_nest')
        app_config.run_delete_semantic_integrations()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Deleting Mappings Metadata",
        "Deleted Mappings Metadata successfully",
        '/pybirdai/workflow/task/3/review/',
        "Review"
    )


def run_delete_output_concepts(request):
    """Delete output concepts."""
    if request.GET.get('execute') == 'true':
        app_config = RunDeleteOutputConcepts('pybirdai', 'birds_nest')
        app_config.run_delete_output_concepts()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Deleting Output Concepts Metadata",
        "Deleted Output Concepts  successfully",
        '/pybirdai/workflow/task/3/review/',
        "Review"
    )


def delete_existing_contents_of_bird_metadata_database(request):
    """Clear entire BIRD metadata database."""
    if request.GET.get('execute') == 'true':
        app_config = RunDeleteBirdMetadataDatabase('pybirdai', 'birds_nest')
        app_config.run_delete_bird_metadata_database()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Deleting Bird Metadata Database",
        "Deleted Bird Metadata Database",
        '/pybirdai/workflow/task/3/do/',
        "Do"
    )


def run_import_semantic_integrations_from_website(request):
    """Import semantic integrations from BIRD website."""
    if request.GET.get('execute') == 'true':
        app_config = RunImportSemanticIntegrationsFromWebsite('pybirdai', 'birds_nest')
        app_config.import_mappings_from_website()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Importing Semantic Integrations (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import Semantic Integrations completed successfully.",
        '/pybirdai/workflow/task/3/do/',
        "Do"
    )


def run_import_input_model_from_sqldev(request):
    """Import input model from SQL Developer files."""
    if request.GET.get('execute') == 'true':
        app_config = RunImportInputModelFromSQLDev('pybirdai', 'birds_nest')
        app_config.ready()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Importing Input Model from SQLDev (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import Input Model from SQLDev process completed successfully",
        '/pybirdai/workflow/task/3/do/',
        "Do"
    )


def run_import_hierarchies(request):
    """Import hierarchies from website."""
    if request.GET.get('execute') == 'true':
        app_config = RunImportHierarchiesFromWebsite('pybirdai', 'birds_nest')
        app_config.import_hierarchies()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Importing Hierarchies (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import hierarchies completed successfully.",
        '/pybirdai/workflow/task/3/do/',
        "Do"
    )


def import_report_templates(request):
    """Import report templates from website."""
    if request.GET.get('execute') == 'true':
        app_config = RunImportReportTemplatesFromWebsite('pybirdai', 'birds_nest')
        app_config.run_import()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Importing Report Templates (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import Report templates from website completed successfully.",
        '/pybirdai/workflow/task/3/do/',
        "Do"
    )


def run_create_filters(request):
    """Create filters metadata."""
    if request.GET.get('execute') == 'true':
        app_config = RunCreateFilters('pybirdai', 'birds_nest')
        app_config.run_create_filters()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Creating Filters (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Filters created successfully.",
        '/pybirdai/workflow/task/4/do/',
        "Do"
    )


def run_create_executable_filters(request):
    """Create executable filter code."""
    if request.GET.get('execute') == 'true':
        app_config = RunCreateExecutableFilters('pybirdai', 'birds_nest')
        app_config.run_create_executable_filters()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Creating Executable Filters (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Create executable filters process completed successfully",
        '/pybirdai/workflow/task/4/do/',
        "Do"
    )


def run_create_executable_filters_from_db(request):
    """Create executable filters from database."""
    if request.GET.get('execute') == 'true':
        app_config = RunCreateExecutableFilters('pybirdai', 'birds_nest')
        app_config.run_create_executable_filters_from_db()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Creating Executable Filters from Database (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Create executable filters from database process completed successfully",
        '/pybirdai/workflow/task/4/do/',
        "Do"
    )


def run_create_python_joins_from_db(request):
    """Create Python joins from database."""
    if request.GET.get('execute') == 'true':
        app_config = RunCreateExecutableJoins('pybirdai', 'birds_nest')
        app_config.create_python_joins_from_db()
        return JsonResponse({'status': 'success'})

    return create_response_with_loading(
        request,
        "Creating Python Joins from Database (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Created Executable Joins from Database in Python",
        '/pybirdai/workflow/task/4/do/',
        "Do"
    )


def run_create_python_transformations_from_db(request):
    """
    Run both Python filters and joins generation from database sequentially.
    Used as the third task in automode after database and transformations setup.
    """
    if request.GET.get('execute') == 'true':
        logger.info("Starting Python transformations generation from database...")

        try:
            # Step 1: Create executable filters from database
            logger.info("Step 1: Creating executable filters from database...")
            filters_config = RunCreateExecutableFilters('pybirdai', 'birds_nest')
            filters_config.run_create_executable_filters_from_db()
            logger.info("Successfully created executable filters from database.")

            # Step 2: Create Python joins from database
            logger.info("Step 2: Creating Python joins from database...")
            joins_config = RunCreateExecutableJoins('pybirdai', 'birds_nest')
            joins_config.create_python_joins_from_db()
            logger.info("Successfully created Python joins from database.")

            logger.info("Python transformations generation completed successfully.")
            return JsonResponse({'status': 'success'})

        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            logger.error(f"Python transformations generation failed: {str(e)}")
            return SecureErrorHandler.secure_json_response(e, 'Python transformations generation', request)

    return create_response_with_loading(
        request,
        "Creating Python Transformations from Database (Creating executable filters and Python joins - approx 2 minutes, please don't navigate away)",
        "Python transformations generation completed successfully! Executable filters and Python joins have been created from the database.",
        '/pybirdai/automode',
        "Back to Automode"
    )


def convert_ldm_to_sdd_hierarchies(request):
    """View for converting LDM hierarchies to SDD hierarchies."""
    if request.GET.get('execute') == 'true':
        try:
            RunConvertLDMToSDDHierarchies.run_convert_hierarchies()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            return SecureErrorHandler.secure_json_response(e, 'LDM to SDD hierarchy conversion', request)

    return create_response_with_loading(
        request,
        'Converting LDM Hierarchies to SDD Hierarchies',
        'Successfully converted LDM hierarchies to SDD hierarchies.',
        '/pybirdai/workflow/task/3/do/',
        "Do"
    )


# File upload views
def upload_sqldev_eil_files(request):
    """Upload SQL Developer EIL files."""
    if request.method == 'GET':
        return render(request, 'pybirdai/upload_sqldev_eil_files.html')
    elif request.method == 'POST':
        app_config = UploadSQLDevEILFiles('pybirdai', 'birds_nest')
        app_config.upload_sqldev_eil_files(request)

        html_response = """
        <h3>Uploaded SQLDeveloper EILFiles.</h3>
        <p> Go back to <a href="/pybirdai/create-bird-database">Create BIRD Database</a></p>
        """
        return HttpResponse(html_response)


def upload_sqldev_eldm_files(request):
    """Upload SQL Developer ELDM files."""
    if request.method == 'GET':
        return render(request, 'pybirdai/upload_sqldev_eldm_files.html')
    elif request.method == 'POST':
        app_config = UploadSQLDevELDMFiles('pybirdai', 'birds_nest')
        app_config.upload_sqldev_eldm_files(request)
        html_response = """
        <h3>Uploaded SQLDeveloper ELDMFiles.</h3>
        <p> Go back to <a href="/pybirdai/create-bird-database">Create BIRD Database</a></p>
        """
        return HttpResponse(html_response)


def upload_technical_export_files(request):
    """Upload technical export CSV files."""
    if request.method == 'GET':
        return render(request, 'pybirdai/upload_technical_export_files.html')
    elif request.method == 'POST':
        app_config = UploadTechnicalExportFiles('pybirdai', 'birds_nest')
        app_config.upload_technical_export_files(request)

        html_response = """
            <h3>Uploaded Technical Export Files.</h3>
            <p> Go back to <a href="/pybirdai/populate-bird-metadata-database">Populate BIRD Metadata Database</a></p>
        """
        return HttpResponse(html_response)


def upload_joins_configuration(request):
    """Upload joins configuration files."""
    if request.method == 'GET':
        return render(request, 'pybirdai/upload_joins_configuration.html')
    elif request.method == 'POST':
        app_config = UploadJoinsConfiguration('pybirdai', 'birds_nest')
        app_config.upload_joins_configuration(request)

        html_response = """
            <h3>Uploaded Joins Configuration Files.</h3>
            <p> Go back to <a href="/pybirdai/create-transformation-rules-configuration">Create Transformations Rules Configuration</a></p>
        """
        return HttpResponse(html_response)


# Data point execution
def execute_data_point(request, data_point_id):
    """Execute a data point and return results."""
    app_config = RunExecuteDataPoint('pybirdai', 'birds_nest')
    result = app_config.run_execute_data_point(data_point_id)

    html_response = f"""
        <h3>DataPoint Execution Results</h3>
        <p><strong>DataPoint ID:</strong> {data_point_id}</p>
        <p><strong>Result:</strong> {result}</p>
        <p><a href="/pybirdai/trails/">Go To Lineage Viewer</a></p>
    """
    return HttpResponse(html_response)
