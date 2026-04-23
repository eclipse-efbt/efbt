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

from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.utils import timezone

from pybirdai.models.workflow_model import WorkflowSession, AnaCreditProcessExecution
from pybirdai.entry_points import ancrdt_transformation
from pybirdai.utils.secure_error_handling import SecureErrorHandler

logger = logging.getLogger(__name__)


def _execution_error_response(exception, context, request, status='failed'):
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    payload = {
        'success': False,
        'error': error_data['message'],
    }
    if status:
        payload['status'] = status
    return JsonResponse(payload, status=500), error_data['message']

def execute_ancrdt_step(request, step_number):
    """Execute an AnaCredit process step"""
    logger = logging.getLogger(__name__)

    try:
        # Get workflow session
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

        # Get or create AnaCredit execution record
        ancrdt_execution, created = AnaCreditProcessExecution.objects.get_or_create(
            session=workflow_session,
            step_number=step_number,
            defaults={
                'step_name': dict(AnaCreditProcessExecution.STEP_CHOICES).get(step_number, f'Step {step_number}'),
                'status': 'pending'
            }
        )

        # Mark as running
        ancrdt_execution.start_execution()

        # Execute the appropriate entry point based on step number
        # Wrap in try-finally to ensure status is ALWAYS updated, even if error handler fails
        try:
            if step_number == 0:
                # Fetch Metadata CSV
                from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
                RunANCRDTTransformation.run_step_0_fetch_ancrdt_csv()

            elif step_number == 1:
                # Import Metadata
                from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
                RunANCRDTTransformation.run_step_1_import()

            elif step_number == 2:
                # Create Joins Metadata
                from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
                RunANCRDTTransformation.run_step_2_joins_metadata()

            elif step_number == 3:
                # Create Executable Joins
                from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
                RunANCRDTTransformation.run_step_3_executable_joins()

            else:
                raise ValueError(f'Invalid AnaCredit step number: {step_number}')

            # Mark as completed
            ancrdt_execution.complete_execution({
                'completed_at': timezone.now().isoformat()
            })

            return JsonResponse({
                'success': True,
                'status': 'completed',
                'message': f'AnaCredit Step {step_number} completed successfully'
            })

        except Exception as e:
            response, safe_message = _execution_error_response(
                e,
                f'executing AnaCredit step {step_number}',
                request,
            )
            ancrdt_execution.handle_error(safe_message)
            return response
        finally:
            # Safety net: If task is still 'running' after all error handling,
            # force it to 'failed' to prevent stuck status
            # This ensures tasks can NEVER remain stuck in 'running' status
            ancrdt_execution.refresh_from_db()
            if ancrdt_execution.status == 'running':
                logger.error(f"AnaCredit Step {step_number} was still 'running' after execution - forcing to 'failed'")
                ancrdt_execution.status = 'failed'
                if not ancrdt_execution.error_message:
                    ancrdt_execution.error_message = 'Execution interrupted unexpectedly'
                ancrdt_execution.completed_at = timezone.now()
                ancrdt_execution.save(update_fields=['status', 'error_message', 'completed_at'])

    except Exception as e:
        response, _ = _execution_error_response(
            e,
            f'preparing AnaCredit step {step_number} execution',
            request,
            status=None,
        )
        return response


def get_ancrdt_status(request):
    """Get AnaCredit execution status"""
    try:
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)
        ancrdt_grid = get_ancrdt_task_grid(workflow_session)

        return JsonResponse({
            'success': True,
            'ancrdt_status': ancrdt_grid
        })

    except Exception as e:
        response, _ = _execution_error_response(
            e,
            'reading AnaCredit execution status',
            request,
            status=None,
        )
        return response
