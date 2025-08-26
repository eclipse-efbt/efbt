"""
Copyright 2025 Arfa Digital Consulting

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""Views for ANCRDT transformation process."""

from django.shortcuts import render
from django.http import JsonResponse
from .views import create_response_with_loading
from .entry_points.ancrdt_transformation import RunANCRDTTransformation
import logging

logger = logging.getLogger(__name__)


def ancrdt_dashboard(request):
    """Main dashboard for ANCRDT transformation process"""
    return render(request, 'pybirdai/ancrdt_dashboard.html')


def ancrdt_fetch_csv(request):
    """Step 0: Fetch ANCRDT CSV data from ECB website with loading spinner"""
    if request.GET.get('execute') == 'true':
        try:
            RunANCRDTTransformation.run_step_0_fetch_ancrdt_csv()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            error_message = str(e) if str(e) else "An unknown error occurred during ANCRDT CSV fetch"
            logger.error(f"ANCRDT CSV fetch failed: {error_message}")
            return JsonResponse({'status': 'error', 'message': error_message}, status=500)
    
    return create_response_with_loading(
        request,
        "Fetching ANCRDT CSV data from ECB website (this may take several minutes, don't press the back button on this web page)",
        "ANCRDT CSV data fetched successfully. You can now proceed to import the data.",
        '/pybirdai/ancrdt/import/',
        "Import ANCRDT Data"
    )


def ancrdt_import(request):
    """Step 1: Import ANCRDT data with loading spinner"""
    if request.GET.get('execute') == 'true':
        try:
            RunANCRDTTransformation.run_step_1_import()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            error_message = str(e) if str(e) else "An unknown error occurred during ANCRDT import"
            logger.error(f"ANCRDT import failed: {error_message}")
            return JsonResponse({'status': 'error', 'message': error_message}, status=500)
    
    return create_response_with_loading(
        request,
        "Importing ANCRDT Data (this may take several minutes, don't press the back button on this web page)",
        "ANCRDT data imported successfully. You can now proceed to create joins metadata.",
        '/pybirdai/ancrdt/create-joins-metadata/',
        "Create Joins Metadata"
    )


def ancrdt_create_joins_metadata(request):
    """Step 2: Create joins metadata with loading spinner"""
    if request.GET.get('execute') == 'true':
        try:
            RunANCRDTTransformation.run_step_2_joins_metadata()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            error_message = str(e) if str(e) else "An unknown error occurred during joins metadata creation"
            logger.error(f"ANCRDT joins metadata creation failed: {error_message}")
            return JsonResponse({'status': 'error', 'message': error_message}, status=500)
    
    return create_response_with_loading(
        request,
        "Creating ANCRDT Joins Metadata (this may take several minutes, don't press the back button on this web page)",
        "ANCRDT joins metadata created successfully. You can now proceed to create executable joins.",
        '/pybirdai/ancrdt/create-executable-joins/',
        "Create Executable Joins"
    )


def ancrdt_create_executable_joins(request):
    """Step 3: Create executable joins with loading spinner"""
    if request.GET.get('execute') == 'true':
        try:
            RunANCRDTTransformation.run_step_3_executable_joins()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            error_message = str(e) if str(e) else "An unknown error occurred during executable joins creation"
            logger.error(f"ANCRDT executable joins creation failed: {error_message}")
            return JsonResponse({'status': 'error', 'message': error_message}, status=500)
    
    return create_response_with_loading(
        request,
        "Creating ANCRDT Executable Joins (this may take several minutes, don't press the back button on this web page)",
        "ANCRDT executable joins created successfully. The ANCRDT transformation process is complete.",
        '/pybirdai/ancrdt/',
        "Back to ANCRDT Dashboard"
    )