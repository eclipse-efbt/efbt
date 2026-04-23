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
"""Views for ANCRDT transformation process."""

from django.shortcuts import render
from django.http import JsonResponse
from pybirdai.views.core_views import create_response_with_loading
from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
from pybirdai.utils.secure_error_handling import SecureErrorHandler
import logging

logger = logging.getLogger(__name__)


def _internal_error_response(exception: Exception, context: str, request):
    """Hide implementation details from client-visible transformation errors."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse({'status': 'error', 'message': error_data['message']}, status=500)

def ancrdt_fetch_csv(request):
    """Step 0: Fetch ANCRDT CSV data from ECB website with loading spinner"""
    if request.GET.get('execute') == 'true':
        try:
            RunANCRDTTransformation.run_step_0_fetch_ancrdt_csv()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return _internal_error_response(e, 'fetching ANCRDT CSV data', request)

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
            return _internal_error_response(e, 'importing ANCRDT data', request)

    return create_response_with_loading(
        request,
        "Importing ANCRDT Data (this may take several minutes, don't press the back button on this web page)",
        "ANCRDT data imported successfully. You can now proceed to generate joins metadata.",
        '/pybirdai/ancrdt/create-joins-metadata/',
        "Generate Joins Metadata"
    )


def ancrdt_create_joins_metadata(request):
    """Step 2: Generate joins metadata with loading spinner"""
    if request.GET.get('execute') == 'true':
        try:
            RunANCRDTTransformation.run_step_2_joins_metadata()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return _internal_error_response(e, 'generating ANCRDT joins metadata', request)

    return create_response_with_loading(
        request,
        "Generating ANCRDT Joins Metadata (this may take several minutes, don't press the back button on this web page)",
        "ANCRDT joins metadata generated successfully. You can now review and edit the metadata before generating execution code.",
        '/pybirdai/execution-code-editing/review-joins/2/',
        "Review Joins Metadata"
    )


def ancrdt_create_executable_joins(request):
    """Step 3: Generate execution code with loading spinner"""
    if request.GET.get('execute') == 'true':
        try:
            RunANCRDTTransformation.run_step_3_executable_joins()
            return JsonResponse({'status': 'success'})
        except Exception as e:
            return _internal_error_response(e, 'generating ANCRDT execution code', request)

    return create_response_with_loading(
        request,
        "Generating ANCRDT Execution Code (this may take several minutes, don't press the back button on this web page)",
        "ANCRDT execution code generated successfully. You can now review and edit the generated code.",
        '/pybirdai/execution-code-editing/review-code/3/',
        "Review Execution Code"
    )
