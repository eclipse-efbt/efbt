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
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction
from django.conf import settings
from django.views.decorators.http import require_http_methods
from .bird_meta_data_model import (
    VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM, MEMBER_MAPPING, MEMBER_MAPPING_ITEM,
    CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, MAPPING_TO_CUBE, MAPPING_DEFINITION
)
from .entry_points.import_input_model import RunImportInputModelFromSQLDev

from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
from .entry_points.create_filters import RunCreateFilters
from .entry_points.create_joins_metadata import RunCreateJoinsMetadata
from .entry_points.delete_joins_metadata import RunDeleteJoinsMetadata
from .entry_points.create_executable_joins import RunCreateExecutableJoins
from .entry_points.run_create_executable_filters import RunCreateExecutableFilters
from .entry_points.execute_datapoint import RunExecuteDataPoint
from .entry_points.upload_sqldev_eil_files import UploadSQLDevEILFiles
from .entry_points.upload_technical_export_files import UploadTechnicalExportFiles
from .entry_points.create_django_models import RunCreateDjangoModels
import os
import csv
from pathlib import Path
from .process_steps.upload_files.file_uploader import FileUploader
from .entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
from .entry_points.upload_joins_configuration import UploadJoinsConfiguration
from django.template.loader import render_to_string

# Helper function for paginated modelformset views
def paginated_modelformset_view(request, model, template_name, formset_fields='__all__', order_by='id', items_per_page=20):
    page_number = request.GET.get('page', 1)
    all_items = model.objects.all().order_by(order_by)
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    ModelFormSet = modelformset_factory(model, fields=formset_fields, extra=0)
    
    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, f'{model.__name__} updated successfully.')
            return redirect(request.path)
        else:
            messages.error(request, f'There was an error updating the {model.__name__}.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, template_name, context)

def show_report(request, report_id):
    return render(request, 'pybirdai/' + report_id)

# Views for running various processes
def run_create_joins_meta_data(request):
    if request.GET.get('execute') == 'true':
        # Execute the actual task
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
    if request.GET.get('execute') == 'true':
        app_config = RunCreateExecutableJoins('pybirdai', 'birds_nest')
        app_config.create_python_joins()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Creating Python Joins (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Created Executable Joins in Python",
        '/pybirdai/create-transformation-rules-in-python',
        "Create Transformations Rules in Python"
    )
    


def run_delete_joins_meta_data(request):
    if request.GET.get('execute') == 'true':
        app_config = RunDeleteJoinsMetadata('pybirdai', 'birds_nest')
        app_config.run_delete_joins_meta_data()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Deleting Joins Metadata",
        "Deleted Transformation Metadata successfully",
        '/pybirdai/create-transformation-rules-in-smcubes',
        "Create Transformations Rules MetaData"
    )

def delete_existing_contents_of_bird_metadata_database(request):
    if request.GET.get('execute') == 'true':
        app_config = RunDeleteBirdMetadataDatabase('pybirdai', 'birds_nest')
        app_config.run_delete_bird_metadata_database()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Deleting Bird Metadata Database",
        "Deleted Bird Metadata Database",
        '/pybirdai/populate-bird-metadata-database',
        "Populate BIRD Metadata Database"
    )

def run_import_semantic_integrations_from_website(request):
    if request.GET.get('execute') == 'true':
        app_config = RunImportSemanticIntegrationsFromWebsite('pybirdai', 'birds_nest')
        app_config.import_mappings_from_website()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Importing Semantic Integrations (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import Semantic Integrations completed successfully.",
        '/pybirdai/create-transformation-rules-configuration',
        "Create Transformations Rules Configuration"
    )

def run_import_input_model_from_sqldev(request):
    if request.GET.get('execute') == 'true':
        app_config = RunImportInputModelFromSQLDev('pybirdai', 'birds_nest')
        app_config.ready()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Importing Input Model from SQLDev (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import Input Model from SQLDev process completed successfully",
        '/pybirdai/populate-bird-metadata-database',
        "Populate BIRD Metadata Database"
    )




def run_import_hierarchies(request):
    if request.GET.get('execute') == 'true':
        app_config = RunImportHierarchiesFromWebsite('pybirdai', 'birds_nest')
        app_config.import_hierarchies()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Importing Hierarchies (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import hierarchies completed successfully.",
        '/pybirdai/create-transformation-rules-configuration',
        "Create Transformations Rules Configuration"
    )
    
 
def import_report_templates(request):
    if request.GET.get('execute') == 'true':
        app_config = RunImportReportTemplatesFromWebsite('pybirdai', 'birds_nest')
        app_config.run_import()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Importing Report Templates (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Import Report templates from website completed successfully.",
        '/pybirdai/populate-bird-metadata-database',
        "Populate BIRD Metadata Database"
    )

def run_create_filters(request):
    if request.GET.get('execute') == 'true':
        # Execute the actual task
        app_config = RunCreateFilters('pybirdai', 'birds_nest')
        app_config.run_create_filters()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Creating Filters (approx 1 minute on a fast desktop, dont press the back button on this web page)",
        "Filters created successfully.",
        '/pybirdai/create-transformation-rules-in-smcubes',
        "Create Transformations Rules MetaData"
    )


def run_create_executable_filters(request):
    if request.GET.get('execute') == 'true':
        app_config = RunCreateExecutableFilters('pybirdai', 'birds_nest')
        app_config.run_create_executable_filters()
        return JsonResponse({'status': 'success'})
    
    return create_response_with_loading(
        request,
        "Creating Executable Filters (approx 10 minutes on a fast desktop, dont press the back button on this web page)",
        "Create executable filters process completed successfully",
        '/pybirdai/create-transformation-rules-in-python',
        "Create Transformations Rules in Python"
    )

def upload_sqldev_eil_files(request):
    if request.method == 'GET':
        # Show the upload form
        return render(request, 'pybirdai/upload_sqldev_eil_files.html')
    elif request.method == 'POST':
        # Handle the file upload
        
        app_config = UploadSQLDevEILFiles('pybirdai', 'birds_nest')
        app_config.upload_sqldev_eil_files(request)
        
        #result = app_config.upload_sqldev_eil_files()
        
        #if result['status'] == 'success':
        #   messages.success(request, 'Files uploaded successfully')
        #else:
        #    messages.error(request, result['message'])
        
        html_response = f"""
        <h3>Uploaded SQLDeveloper EILFiles.</h3>

        <p> Go back to <a href="{request.build_absolute_uri('/pybirdai/create-bird-database')}">Create BIRD Database</a></p>
    """
    return HttpResponse(html_response)
    

def upload_technical_export_files(request):
    if request.method == 'GET':
        # Show the upload form
        return render(request, 'pybirdai/upload_technical_export_files.html')
    elif request.method == 'POST':
        # Handle the file upload
        
        app_config = UploadTechnicalExportFiles('pybirdai', 'birds_nest')
        app_config.upload_technical_export_files(request)
        
        #result = app_config.upload_sqldev_eil_files()
        
        #if result['status'] == 'success':
        #   messages.success(request, 'Files uploaded successfully')
        #else:
        #    messages.error(request, result['message'])
            
       
        html_response = f"""
            <h3>Uploaded Technical Export Files.</h3>

            <p> Go back to <a href="{request.build_absolute_uri('/pybirdai/populate-bird-metadata-database')}">Populate BIRD Metadata Database</a></p>
        """
        return HttpResponse(html_response)
    
def upload_joins_configuration(request):
    
    if request.method == 'GET':
        # Show the upload form
        return render(request, 'pybirdai/upload_joins_configuration.html')
    elif request.method == 'POST':
        # Handle the file upload
        
        app_config = UploadJoinsConfiguration('pybirdai', 'birds_nest')
        app_config.upload_joins_configuration(request)
        
        #result = app_config.upload_sqldev_eil_files()
        
        #if result['status'] == 'success':
        #   messages.success(request, 'Files uploaded successfully')
        #else:
        #    messages.error(request, result['message'])
            
        
        html_response = f"""
            <h3>Uploaded Joins Configuration Files.</h3>

            <p> Go back to <a href="{request.build_absolute_uri('/pybirdai/create-transformation-rules-configuration')}">Create Transformations Rules Configuration</a></p>
        """
        return HttpResponse(html_response)

# Basic views
def index(request):
    return HttpResponse("Hello, world. You're at the pybirdai index.")

def home_view(request):
    return render(request, 'pybirdai/home.html')

# CRUD views for various models
def edit_variable_mappings(request):
    return paginated_modelformset_view(request, VARIABLE_MAPPING, 'pybirdai/edit_variable_mappings.html', order_by='variable_mapping_id')

def edit_variable_mapping_items(request):
    return paginated_modelformset_view(request, VARIABLE_MAPPING_ITEM, 'pybirdai/edit_variable_mapping_items.html')

def edit_member_mappings(request):
    return paginated_modelformset_view(request, MEMBER_MAPPING, 'pybirdai/edit_member_mappings.html', order_by='member_mapping_id')

def edit_member_mapping_items(request):
    return paginated_modelformset_view(request, MEMBER_MAPPING_ITEM, 'pybirdai/edit_member_mapping_items.html')

def edit_cube_links(request):
    return paginated_modelformset_view(request, CUBE_LINK, 'pybirdai/edit_cube_links.html', order_by='cube_link_id')

def edit_cube_structure_item_links(request):
    return paginated_modelformset_view(request, CUBE_STRUCTURE_ITEM_LINK, 'pybirdai/edit_cube_structure_item_links.html', order_by='cube_structure_item_link_id')

def edit_mapping_to_cubes(request):
    return paginated_modelformset_view(request, MAPPING_TO_CUBE, 'pybirdai/edit_mapping_to_cubes.html')

def edit_mapping_definitions(request):
    return paginated_modelformset_view(request, MAPPING_DEFINITION, 'pybirdai/edit_mapping_definitions.html', order_by='mapping_id')

# Delete views for various models
def delete_item(request, model, id_field, redirect_view):
    item = get_object_or_404(model, **{id_field: request.POST.get(id_field)})
    if request.method == 'POST':
        item.delete()
        messages.success(request, f'{model.__name__} deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect(f'pybirdai:{redirect_view}')

def delete_variable_mapping(request, variable_mapping_id):
    return delete_item(request, VARIABLE_MAPPING, 'variable_mapping_id', 'edit_variable_mappings')

def execute_data_point(request, data_point_id):
    app_config = RunExecuteDataPoint('pybirdai', 'birds_nest')
    result = app_config.run_execute_data_point(data_point_id)
    
    html_response = f"""

        <h3>DataPoint Execution Results</h3>
        <p><strong>DataPoint ID:</strong> {data_point_id}</p>
        <p><strong>Result:</strong> {result}</p>
        <p><a href="{request.build_absolute_uri('/pybirdai/lineage/')}">View Lineage Files</a></p>
        <p><a href="{request.build_absolute_uri('/pybirdai/report-templates/')}">Back to the PyBIRD Reports Templates Page</a></p>
    """
    return HttpResponse(html_response)

def delete_variable_mapping_item(request, item_id):
    return delete_item(request, VARIABLE_MAPPING_ITEM, 'id', 'edit_variable_mapping_items')

def delete_member_mapping(request, member_mapping_id):
    return delete_item(request, MEMBER_MAPPING, 'member_mapping_id', 'edit_member_mappings')

def delete_member_mapping_item(request, item_id):
    return delete_item(request, MEMBER_MAPPING_ITEM, 'id', 'edit_member_mapping_items')

def delete_cube_link(request, cube_link_id):
    return delete_item(request, CUBE_LINK, 'cube_link_id', 'edit_cube_links')

def delete_cube_structure_item_link(request, cube_structure_item_link_id):
    return delete_item(request, CUBE_STRUCTURE_ITEM_LINK, 'cube_structure_item_link_id', 'edit_cube_structure_item_links')

def delete_mapping_to_cube(request, mapping_to_cube_id):
    return delete_item(request, MAPPING_TO_CUBE, 'id', 'edit_mapping_to_cubes')

def delete_mapping_definition(request, mapping_id):
    return delete_item(request, MAPPING_DEFINITION, 'mapping_id', 'edit_mapping_definitions')

def list_lineage_files(request):
    lineage_dir = Path(settings.BASE_DIR) / 'results' / 'lineage'
    csv_files = []
    
    if lineage_dir.exists():
        csv_files = [f.name for f in lineage_dir.glob('*.csv')]
    
    return render(request, 'pybirdai/lineage_files.html', {'csv_files': csv_files})

def view_csv_file(request, filename):

    file_path = Path(settings.BASE_DIR) / 'results' / 'lineage' / filename
    
    if not file_path.exists() or not filename.endswith('.csv'):
        messages.error(request, 'File not found or invalid file type')
        return redirect('pybirdai:list_lineage_files')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            headers = next(csv_reader)  # Get the headers
            data = list(csv_reader)     # Get all rows
            
        # Paginate the results
        items_per_page = 50  # Adjust this number as needed
        paginator = Paginator(data, items_per_page)
        page_number = request.GET.get('page', 1)
        page_obj = paginator.get_page(page_number)
        
        # Calculate some statistics
        total_rows = len(data)
        num_columns = len(headers)
        
        context = {
            'filename': filename,
            'headers': headers,
            'page_obj': page_obj,
            'total_rows': total_rows,
            'num_columns': num_columns,
            'start_index': (page_obj.number - 1) * items_per_page + 1,
            'end_index': min(page_obj.number * items_per_page, total_rows),
        }
        return render(request, 'pybirdai/view_csv.html', context)
        
    except Exception as e:
        messages.error(request, f'Error reading file: {str(e)}')
        return redirect('pybirdai:list_lineage_files')

def create_response_with_loading(request, task_title, success_message, return_url, return_link_text):
    html_response = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                .loading-overlay {{
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(255, 255, 255, 0.8);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    flex-direction: column;
                    z-index: 9999;
                }}

                .loading-spinner {{
                    width: 50px;
                    height: 50px;
                    border: 5px solid #f3f3f3;
                    border-top: 5px solid #3498db;
                    border-radius: 50%;
                    animation: spin 1s linear infinite;
                    margin-bottom: 20px;
                }}

                @keyframes spin {{
                    0% {{ transform: rotate(0deg); }}
                    100% {{ transform: rotate(360deg); }}
                }}

                .loading-message {{
                    font-size: 18px;
                    color: #333;
                }}

                .task-info {{
                    padding: 20px;
                    max-width: 600px;
                    margin: 0 auto;
                }}

                #success-message {{
                    display: none;
                    margin-top: 20px;
                    padding: 15px;
                    background-color: #d4edda;
                    border: 1px solid #c3e6cb;
                    border-radius: 4px;
                    color: #155724;
                }}
            </style>
        </head>
        <body>
            <div class="task-info">
                <h3>{task_title}</h3>
                <div id="loading-overlay" class="loading-overlay">
                    <div class="loading-spinner"></div>
                    <div class="loading-message">Please wait while the task completes...</div>
                </div>
                <div id="success-message">
                    <p>{success_message}</p>
                    <p>Go back to <a href="{request.build_absolute_uri(return_url)}">{return_link_text}</a></p>
                </div>
            </div>
            <script>
                document.addEventListener('DOMContentLoaded', function() {{
                    // Show loading immediately
                    document.getElementById('loading-overlay').style.display = 'flex';
                    document.getElementById('success-message').style.display = 'none';
                    
                    // Start the task execution after a small delay to ensure loading is visible
                    setTimeout(() => {{
                        fetch(window.location.href + '?execute=true', {{
                            method: 'GET',
                            headers: {{
                                'X-Requested-With': 'XMLHttpRequest'
                            }}
                        }})
                        .then(response => response.json())
                        .then(data => {{
                            if (data.status === 'success') {{
                                // Hide loading and show success
                                document.getElementById('loading-overlay').style.display = 'none';
                                document.getElementById('success-message').style.display = 'block';
                            }} else {{
                                throw new Error('Task failed');
                            }}
                        }})
                        .catch(error => {{
                            console.error('Error:', error);
                            alert('An error occurred while processing the task.');
                        }});
                    }}, 100); // Small delay to ensure loading screen is visible
                }});
            </script>
        </body>
        </html>
    """
    
    # If this is the AJAX request to execute the task
    if request.GET.get('execute') == 'true':
        # Execute the actual task
        try:
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})
    
    return HttpResponse(html_response)