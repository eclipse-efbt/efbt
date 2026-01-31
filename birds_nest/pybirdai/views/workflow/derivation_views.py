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
#    Auto-generated for derivation workflow views
"""
Views for derivation file workflow.

These views provide UI for:
- Reviewing derivation files and their sync status
- Editing derivation files in the browser
- Deploying edited files to production
"""

import os
import logging
from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from django.conf import settings

from pybirdai.views.workflow.derivation_sync import DerivationSyncManager

logger = logging.getLogger(__name__)


def derivation_review(request):
    """
    Main review page for derivation files.

    Shows all derivation files grouped by type with their sync status.
    """
    return render(request, 'pybirdai/workflow/derivation/review.html')


def derivation_editor(request):
    """
    Browser-based editor for derivation files.

    Query params:
        file: Relative path to the file to edit
        return_url: URL to return to after saving
    """
    relative_path = request.GET.get('file', '')
    return_url = request.GET.get('return_url', '')

    if not relative_path:
        return render(request, 'pybirdai/workflow/derivation/editor.html', {
            'error': 'No file specified',
            'return_url': return_url,
        })

    manager = DerivationSyncManager()
    file_info = manager.get_file_info(relative_path)

    if file_info is None:
        return render(request, 'pybirdai/workflow/derivation/editor.html', {
            'error': f'File not found: {relative_path}',
            'return_url': return_url,
        })

    content = manager.read_file(relative_path)

    return render(request, 'pybirdai/workflow/derivation/editor.html', {
        'file_info': file_info,
        'content': content,
        'relative_path': relative_path,
        'return_url': return_url or '/pybirdai/workflow/derivation/review/',
    })
