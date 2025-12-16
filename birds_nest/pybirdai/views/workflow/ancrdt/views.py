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
#
# Extracted from workflow_views.py

import logging

from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods

from pybirdai.models.workflow_model import WorkflowSession

logger = logging.getLogger(__name__)

def ancrdt_dashboard(request):
    """
    DEPRECATED: Old ANCRDT dashboard function.

    The dashboard has been removed in favor of direct navigation to individual step pages.
    This function now redirects to ANCRDT Step 0 for backward compatibility.
    """
    messages.info(request, 'The ANCRDT dashboard has been simplified. Redirecting to Step 0.')
    return redirect('pybirdai:ancrdt_step_0')


def approve_joins_metadata(request):
    """
    Approve joins metadata for ANCRDT workflow Step 2.
    Marks the joins_metadata_approved flag as True in the workflow session.
    """
    if request.method != 'POST':
        messages.warning(request, 'Invalid request method.')
        return redirect('pybirdai:ancrdt_step_2_review')

    try:
        # Get the current workflow session from Django session
        session_id = request.session.get('workflow_session_id')

        if not session_id:
            messages.error(request, 'No active workflow session found.')
            return redirect('pybirdai:ancrdt_step_2_review')

        # Get the WorkflowSession object
        session = WorkflowSession.objects.filter(session_id=session_id).first()

        if not session:
            messages.error(request, 'Workflow session not found.')
            return redirect('pybirdai:ancrdt_step_2_review')

        # Mark joins metadata as approved
        session.joins_metadata_approved = True
        session.save()

        logger.info(f"Joins metadata approved for session {session_id}")
        messages.success(request, 'Joins metadata approved successfully!')

        # Redirect back to Step 2 review page
        return redirect('pybirdai:ancrdt_step_2_review')

    except Exception as e:
        logger.error(f"Error approving joins metadata: {e}")
        messages.error(request, f'Error approving joins metadata: {str(e)}')
        return redirect('pybirdai:ancrdt_step_2_review')
