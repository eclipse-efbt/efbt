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
import traceback

from django.shortcuts import get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.utils import timezone

from pybirdai.models.workflow_model import WorkflowSession, AnaCreditProcessExecution
from pybirdai.entry_points import ancrdt_transformation

logger = logging.getLogger(__name__)

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
            # Fetch test suite for ANCRDT workflow (on first step)
            if step_number == 1:
                from pybirdai.models.workflow_model import AutomodeConfiguration
                from pybirdai.api.workflow_api import AutomodeConfigurationService
                from pybirdai.views.workflow.github import _get_github_token
                config = AutomodeConfiguration.get_active_configuration()
                # Use configured URL or default ANCRDT test suite
                test_suite_url = (getattr(config, 'test_suite_url_ancrdt', None) if config else None) or 'https://github.com/benjamin-arfa/bird-ancrdt-test-suite'
                logger.info(f"Fetching ANCRDT test suite from: {test_suite_url}")
                workflow_service = AutomodeConfigurationService()
                # Pass stored GitHub token for private repo access
                workflow_service._fetch_test_suite_from_github(test_suite_url, token=_get_github_token())

            # Initialize execution data with steps_completed tracking
            execution_data = {
                'steps_completed': [],
                'completed_at': None
            }

            if step_number == 1:
                # Import Metadata
                # First, delete previous ANCRDT framework data to ensure clean state
                # This preserves other frameworks (FINREP, COREP, etc.) and input model
                from pybirdai.entry_points.delete_framework_data import RunDeleteFrameworkData
                logger.info("Cleaning up previous ANCRDT framework data before import...")
                try:
                    cleanup_result = RunDeleteFrameworkData.run_delete_ancrdt()
                    logger.info(f"ANCRDT cleanup completed: {cleanup_result}")
                    execution_data['steps_completed'].append('cleanup_previous_data')
                    ancrdt_execution.update_progress(25, 'cleanup', {'result': cleanup_result})
                except Exception as cleanup_error:
                    logger.warning(f"ANCRDT cleanup warning (continuing): {cleanup_error}")
                    execution_data['steps_completed'].append('cleanup_skipped')

                from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
                RunANCRDTTransformation.run_step_1_import()
                execution_data['steps_completed'].append('import_metadata')
                ancrdt_execution.update_progress(100, 'import', {'status': 'completed'})

            elif step_number == 2:
                # Create Joins Metadata
                from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
                ancrdt_execution.update_progress(10, 'starting', {'step': 'create_joins_metadata'})
                RunANCRDTTransformation.run_step_2_joins_metadata()
                execution_data['steps_completed'].extend([
                    'cube_links_created',
                    'cube_structure_item_links_created',
                    'member_links_created'
                ])
                ancrdt_execution.update_progress(100, 'joins_metadata', {'status': 'completed'})

            elif step_number == 3:
                # Create Executable Joins
                from pybirdai.entry_points.ancrdt_transformation import RunANCRDTTransformation
                ancrdt_execution.update_progress(10, 'starting', {'step': 'create_executable_joins'})
                RunANCRDTTransformation.run_step_3_executable_joins()
                execution_data['steps_completed'].extend([
                    'output_tables_generated',
                    'logic_files_generated'
                ])
                ancrdt_execution.update_progress(100, 'code_generation', {'status': 'completed'})

            elif step_number == 4:
                # Run Test Suite - dynamically discover ANCRDT test suite
                from pybirdai.entry_points.run_ancrdt_tests import RunANCRDTTests
                from pybirdai.utils.test_discovery import get_ancrdt_test_suite

                ancrdt_execution.update_progress(10, 'discovering', {'step': 'find_test_suite'})

                config_path, suite_name = get_ancrdt_test_suite()
                if not config_path:
                    raise FileNotFoundError(
                        "No ANCRDT test suite found. Please ensure a test suite with "
                        "test_type='ancrdt' exists in the tests/ directory."
                    )

                execution_data['steps_completed'].append('test_suite_discovered')
                execution_data['suite_name'] = suite_name
                ancrdt_execution.update_progress(20, 'suite_found', {'suite': suite_name})

                logger.info(f"Running ANCRDT tests from discovered suite: {suite_name}")
                RunANCRDTTests.run_tests(
                    config_file_path=config_path,
                    suite_name=suite_name,
                    use_uv=True
                )
                execution_data['steps_completed'].extend([
                    'tests_executed',
                    'results_generated'
                ])
                ancrdt_execution.update_progress(100, 'tests_complete', {'status': 'completed'})

            else:
                raise ValueError(f'Invalid AnaCredit step number: {step_number}. Valid steps are 1-4.')

            # Mark as completed with execution data
            execution_data['completed_at'] = timezone.now().isoformat()
            ancrdt_execution.complete_execution(execution_data)

            return JsonResponse({
                'success': True,
                'status': 'completed',
                'message': f'AnaCredit Step {step_number} completed successfully'
            })

        except Exception as e:
            logger.error(f"AnaCredit Step {step_number} execution failed: {e}")
            ancrdt_execution.handle_error(str(e))
            return JsonResponse({
                'success': False,
                'error': str(e),
                'status': 'failed'
            })
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
        logger.error(f"Error executing AnaCredit step: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })


@require_http_methods(["POST"])
def fetch_ancrdt_artifacts(request):
    """
    Fetch ANCRDT-specific artifacts from the configured GitHub repository.
    This endpoint only fetches ANCRDT framework files, not the main IL data.
    """
    try:
        from pybirdai.api.workflow_api import AutomodeConfigurationService
        from pybirdai.views.workflow.github import _get_github_token

        logger.info("Starting ANCRDT artifact fetch...")

        # Get GitHub token from request if provided, otherwise try stored token
        github_token = request.POST.get('github_token', None)
        if not github_token:
            github_token = _get_github_token()  # Try stored token
            if github_token:
                logger.info("Using stored GitHub token for authentication")

        # Fetch ANCRDT-specific files
        service = AutomodeConfigurationService()
        result = service.fetch_files_for_framework(
            'ANCRDT',
            github_token=github_token,
            branch='main'
        )

        # Check for success: no errors and at least some files fetched
        # Note: technical_export and config_files are set to the same value in fetch_files_for_framework
        errors = result.get('errors', [])
        files_count = result.get('technical_export', 0)

        if not errors and files_count > 0:
            logger.info(f"ANCRDT artifacts fetched successfully: {files_count} files")
            return JsonResponse({
                'success': True,
                'message': 'ANCRDT artifacts fetched successfully',
                'files_count': files_count,
                'details': result
            })
        elif not errors and files_count == 0:
            # No errors but no files - might be okay for some cases
            logger.warning("ANCRDT artifact fetch completed but no files were copied")
            return JsonResponse({
                'success': True,
                'message': 'ANCRDT artifacts fetch completed (no new files)',
                'files_count': 0,
                'details': result
            })
        else:
            error_msg = '; '.join(errors) if errors else 'Unknown error during ANCRDT fetch'
            logger.error(f"ANCRDT artifact fetch failed: {error_msg}")
            return JsonResponse({
                'success': False,
                'error': error_msg
            })

    except Exception as e:
        logger.error(f"Error fetching ANCRDT artifacts: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })


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
        return JsonResponse({
            'success': False,
            'error': str(e)
        })
