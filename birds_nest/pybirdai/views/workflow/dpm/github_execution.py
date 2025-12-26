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
#
# GitHub-based DPM workflow execution views (4-step flow)

import os
import json
import logging

from django.shortcuts import get_object_or_404, render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.conf import settings
from django.utils import timezone

from pybirdai.models.workflow_model import (
    WorkflowSession, DPMProcessExecution, AutomodeConfiguration
)
from pybirdai.api.workflow_api import AutomodeConfigurationService
from pybirdai.services.github_service import GitHubService

logger = logging.getLogger(__name__)


def execute_github_dpm_step(request, step_number):
    """
    Execute a GitHub-based DPM workflow step.

    GitHub Source 4-Step Flow:
    Step 1: Import Data - Fetch CSVs from GitHub package
    Step 2: Generate Structure Links - Create joins metadata
    Step 3: Generate Executable Code - Generate Python filters/joins
    Step 4: Run Tests - Execute test suite
    """
    logger.info(f'Executing GitHub DPM Step {step_number}')

    try:
        # Get workflow session
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

        # Validate step number for GitHub workflow (1-4)
        if step_number < 1 or step_number > 4:
            return JsonResponse({
                'success': False,
                'error': f'Invalid step number for GitHub DPM workflow: {step_number}. Valid range is 1-4.'
            })

        # Get GitHub package URL and branch from request or configuration
        github_url = request.POST.get('github_url', '')
        github_branch = request.POST.get('github_branch', 'main')
        github_token = request.POST.get('github_token', '')

        # If not provided in request, try to get from configuration
        if not github_url:
            config = AutomodeConfiguration.get_active_configuration()
            if config and config.pipeline_url_dpm:
                github_url = config.pipeline_url_dpm
            else:
                github_url = 'https://github.com/regcommunity/FreeBIRD_COREP'

        # Get or create DPM execution record for GitHub source
        dpm_execution, created = DPMProcessExecution.get_or_create_for_github(
            session=workflow_session,
            step_number=step_number,
            github_url=github_url,
            branch=github_branch
        )

        # Mark as running
        dpm_execution.start_execution()

        try:
            if step_number == 1:
                # Step 1: Import Data from GitHub package
                result = _execute_github_step1_import(
                    dpm_execution, github_url, github_branch, github_token
                )

            elif step_number == 2:
                # Step 2: Generate Structure Links (joins metadata)
                result = _execute_github_step2_structure_links(dpm_execution)

            elif step_number == 3:
                # Step 3: Generate Executable Code (Python)
                result = _execute_github_step3_generate_code(dpm_execution)

            elif step_number == 4:
                # Step 4: Run Tests
                result = _execute_github_step4_run_tests(dpm_execution)

            else:
                raise ValueError(f'Invalid GitHub DPM step number: {step_number}')

            # Mark as completed
            dpm_execution.complete_execution({
                'completed_at': timezone.now().isoformat(),
                'result': result
            })

            return JsonResponse({
                'success': True,
                'status': 'completed',
                'message': f'GitHub DPM Step {step_number} completed successfully',
                'result': result
            })

        except Exception as e:
            logger.error(f"GitHub DPM Step {step_number} execution failed: {e}")
            dpm_execution.handle_error(str(e))
            return JsonResponse({
                'success': False,
                'error': str(e),
                'status': 'failed'
            })

        finally:
            # Safety net: If task is still 'running' after all error handling,
            # force it to 'failed' to prevent stuck status
            dpm_execution.refresh_from_db()
            if dpm_execution.status == 'running':
                logger.error(f"GitHub DPM Step {step_number} was still 'running' - forcing to 'failed'")
                dpm_execution.status = 'failed'
                if not dpm_execution.error_message:
                    dpm_execution.error_message = 'Execution interrupted unexpectedly'
                dpm_execution.completed_at = timezone.now()
                dpm_execution.save(update_fields=['status', 'error_message', 'completed_at'])

    except Exception as e:
        logger.error(f"Error executing GitHub DPM step: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })


def _execute_github_step1_import(dpm_execution, github_url, github_branch, github_token):
    """
    Step 1: Import Data from GitHub package.

    Fetches CSV files from the GitHub repository and imports them into the database.
    Similar to how clone mode works for FINREP.
    """
    logger.info(f"GitHub DPM Step 1: Importing data from {github_url}")

    from pybirdai.utils.clone_mode import import_from_metadata_export
    from django.core.management import call_command
    from io import StringIO

    # Validate GitHub URL
    github_service = GitHubService(token=github_token if github_token else None)
    validation = github_service.is_allowed_load_repo(github_url)

    if not validation.get('allowed', False):
        raise ValueError(f"GitHub repository not allowed: {validation.get('error', 'Unknown error')}")

    # Use the load_clone_state management command to import from GitHub
    args = ['--repo-url', github_url, '--branch', github_branch]
    if github_token:
        args.extend(['--token', github_token])
    args.append('--force')  # Force overwrite existing data

    out = StringIO()
    call_command('load_clone_state', *args, stdout=out, verbosity=1)

    output = out.getvalue()
    logger.info(f"GitHub DPM Step 1 output: {output}")

    # Import IL (Input Layer) if files exist
    il_imported = False
    il_dir = os.path.join(settings.BASE_DIR, 'resources', 'il')
    if os.path.exists(il_dir) and any(f.endswith('.csv') for f in os.listdir(il_dir)):
        logger.info("IL files detected - importing input layer...")
        from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev

        il_config = RunImportInputModelFromSQLDev("pybirdai", "birds_nest")
        il_config.ready()
        il_imported = True
        logger.info("IL import completed")
    else:
        logger.info("No IL files found in resources/il/ - skipping input layer import")

    # Update execution data with import details
    dpm_execution.github_package_url = github_url
    dpm_execution.github_branch = github_branch
    dpm_execution.save(update_fields=['github_package_url', 'github_branch'])

    return {
        'source': 'github',
        'url': github_url,
        'branch': github_branch,
        'il_imported': il_imported,
        'message': 'Data imported successfully from GitHub'
    }


def _execute_github_step2_structure_links(dpm_execution):
    """
    Step 2: Generate Structure Links (joins metadata).

    Creates transformation rules including filters and joins metadata.
    Equivalent to DPM EBA Step 4.
    """
    logger.info("GitHub DPM Step 2: Creating structure links (joins metadata)")

    from pybirdai.entry_points.create_filters import RunCreateFilters
    from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata

    # Get frameworks from the execution record
    frameworks_to_process = dpm_execution.selected_frameworks or ['FINREP']

    execution_data = {
        'filters_created': False,
        'joins_metadata_created': False,
        'steps_completed': [],
        'frameworks_processed': []
    }

    # Generate filters for each framework
    logger.info(f"Generating filters for frameworks: {frameworks_to_process}")
    for framework in frameworks_to_process:
        framework_ref = f"{framework}_REF" if not framework.endswith('_REF') else framework
        version = "4.0"  # Default version
        logger.info(f"Generating filters for framework: {framework_ref}, version: {version}")
        RunCreateFilters.run_create_filters(framework=framework_ref, version=version)
        execution_data['frameworks_processed'].append(framework)

    execution_data['filters_created'] = True
    execution_data['steps_completed'].append('Filters creation')

    # Create joins metadata
    logger.info("Creating joins metadata...")
    RunCreateJoinsMetadata.run_create_joins_meta_data()
    execution_data['joins_metadata_created'] = True
    execution_data['steps_completed'].append('Joins metadata creation')

    logger.info("GitHub DPM Step 2 completed: Structure links created successfully")

    return execution_data


def _execute_github_step3_generate_code(dpm_execution):
    """
    Step 3: Generate Executable Code (Python).

    Generates executable Python code from metadata.
    Equivalent to DPM EBA Step 5.
    """
    logger.info("GitHub DPM Step 3: Generating Python code")

    from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
    from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins

    execution_data = {
        'filter_code_generated': False,
        'join_code_generated': False,
        'steps_completed': [],
        'frameworks_processed': []
    }

    # Get frameworks from the execution record
    frameworks_to_process = dpm_execution.selected_frameworks or ['FINREP']

    # Generate filter code for each framework
    logger.info(f"Generating executable filter code for frameworks: {frameworks_to_process}")
    for framework in frameworks_to_process:
        logger.info(f"Generating filter code for framework: {framework}")
        RunCreateExecutableFilters.run_create_executable_filters_from_db(framework=framework)

    execution_data['filter_code_generated'] = True
    execution_data['steps_completed'].append('Executable filter code generation')

    # Generate join code for each framework
    logger.info(f"Generating executable join code for frameworks: {frameworks_to_process}")
    for framework in frameworks_to_process:
        logger.info(f"Generating joins for framework: {framework}")
        RunCreateExecutableJoins.run_create_executable_joins(framework_id=framework)
        execution_data['frameworks_processed'].append(framework)

    execution_data['join_code_generated'] = True
    execution_data['steps_completed'].append('Executable join code generation')

    logger.info("GitHub DPM Step 3 completed: Python code generated successfully")

    return execution_data


def _execute_github_step4_run_tests(dpm_execution):
    """
    Step 4: Run Tests.

    Executes the test suite for the DPM workflow.
    Equivalent to DPM EBA Step 6.
    """
    logger.info("GitHub DPM Step 4: Running tests")

    from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

    # Get frameworks from the execution record
    frameworks_to_process = dpm_execution.selected_frameworks or ['FINREP']
    test_framework = frameworks_to_process[0]  # Use first selected framework for tests

    execution_data = {
        'tests_executed': False,
        'steps_completed': [],
        'framework_used': test_framework
    }

    # Look for DPM-specific test suite in tests/dpm/ directory
    tests_dir = 'tests/dpm'
    config_file_path = os.path.join(tests_dir, 'configuration_file_tests.json')

    if os.path.exists(config_file_path):
        logger.info(f"Found test suite configuration: {config_file_path}")

        # Create test runner instance
        test_runner = RegulatoryTemplateTestRunner(False)

        # Configure test runner
        test_runner.args.uv = "False"
        test_runner.args.config_file = config_file_path
        test_runner.args.dp_value = None
        test_runner.args.reg_tid = None
        test_runner.args.dp_suffix = None
        test_runner.args.scenario = None
        test_runner.args.suite_name = 'dpm'
        test_runner.args.framework = test_framework

        # Execute tests
        logger.info(f"Executing tests for framework '{test_framework}'")
        test_runner.main()
        logger.info("Test suite completed")

        execution_data['tests_executed'] = True
        execution_data['steps_completed'].append('Test suite executed')
    else:
        logger.warning(f"No test configuration found at {config_file_path}")
        logger.info("Step 4 completed without test execution (no test suite configured)")
        execution_data['tests_executed'] = False
        execution_data['steps_completed'].append('No test suite found - skipped')

    logger.info("GitHub DPM Step 4 completed")

    return execution_data


def get_github_dpm_status(request):
    """Get GitHub DPM workflow execution status"""
    try:
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

        # Get all GitHub-source DPM executions
        github_executions = DPMProcessExecution.objects.filter(
            session=workflow_session,
            source_type='github'
        ).order_by('step_number')

        # Build status grid for 4 steps
        github_dpm_grid = []
        for step_num in range(1, 5):
            step_name = dict(DPMProcessExecution.GITHUB_STEP_CHOICES).get(step_num, f'Step {step_num}')

            try:
                execution = github_executions.get(step_number=step_num)
                status = execution.status
                error_message = execution.error_message
            except DPMProcessExecution.DoesNotExist:
                status = 'pending'
                error_message = None

            github_dpm_grid.append({
                'step_number': step_num,
                'step_name': step_name,
                'status': status,
                'error_message': error_message
            })

        return JsonResponse({
            'success': True,
            'github_dpm_status': github_dpm_grid
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        })


def workflow_github_dpm_review(request, step_number):
    """Review page for GitHub DPM workflow step"""
    try:
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return render(request, 'pybirdai/error.html', {
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

        # Get the execution record for this step
        try:
            execution = DPMProcessExecution.objects.get(
                session=workflow_session,
                step_number=step_number,
                source_type='github'
            )
        except DPMProcessExecution.DoesNotExist:
            execution = None

        step_name = dict(DPMProcessExecution.GITHUB_STEP_CHOICES).get(step_number, f'Step {step_number}')

        context = {
            'step_number': step_number,
            'step_name': step_name,
            'execution': execution,
            'source_type': 'github'
        }

        return render(request, 'pybirdai/workflow/dpm_workflow/github_source/review.html', context)

    except Exception as e:
        logger.error(f"Error in workflow_github_dpm_review: {e}")
        return render(request, 'pybirdai/error.html', {
            'error': str(e)
        })


@require_http_methods(["POST"])
def configure_github_dpm_source(request):
    """Configure GitHub source for DPM workflow"""
    try:
        github_url = request.POST.get('github_url', '').strip()
        github_branch = request.POST.get('github_branch', 'main').strip()
        github_token = request.POST.get('github_token', '').strip()

        if not github_url:
            return JsonResponse({
                'success': False,
                'error': 'GitHub URL is required'
            }, status=400)

        # Validate the repository
        github_service = GitHubService(token=github_token if github_token else None)
        validation = github_service.is_allowed_load_repo(github_url)

        if not validation.get('allowed', False):
            return JsonResponse({
                'success': False,
                'error': validation.get('error', 'Repository not allowed')
            }, status=403)

        # Save configuration
        session_id = request.session.get('workflow_session_id')
        if session_id:
            try:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)
                # Store GitHub config in session configuration
                config = workflow_session.configuration or {}
                config['github_dpm'] = {
                    'url': github_url,
                    'branch': github_branch,
                    'configured_at': timezone.now().isoformat()
                }
                workflow_session.configuration = config
                workflow_session.save(update_fields=['configuration'])
            except WorkflowSession.DoesNotExist:
                pass

        return JsonResponse({
            'success': True,
            'message': 'GitHub source configured successfully',
            'config': {
                'url': github_url,
                'branch': github_branch
            }
        })

    except Exception as e:
        logger.error(f"Error configuring GitHub DPM source: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def validate_github_dpm_package(request):
    """Validate a GitHub DPM package before import"""
    try:
        github_url = request.POST.get('github_url', '').strip()
        github_token = request.POST.get('github_token', '').strip()

        if not github_url:
            return JsonResponse({
                'success': False,
                'error': 'GitHub URL is required'
            }, status=400)

        # Validate the repository
        github_service = GitHubService(token=github_token if github_token else None)

        # Check if repo is allowed
        validation = github_service.is_allowed_load_repo(github_url)

        if not validation.get('allowed', False):
            return JsonResponse({
                'success': False,
                'valid': False,
                'error': validation.get('error', 'Repository not allowed')
            })

        # Check if repo exists and has required structure
        # (This would check for process_metadata.json, CSV files, etc.)
        repo_info = github_service.validate_for_operation(
            repo_url=github_url,
            operation='load'
        )

        return JsonResponse({
            'success': True,
            'valid': repo_info.get('valid', False),
            'source_type': validation.get('source_type', 'unknown'),
            'details': repo_info
        })

    except Exception as e:
        logger.error(f"Error validating GitHub DPM package: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def get_github_dpm_task_grid(workflow_session):
    """
    Build the task grid for GitHub DPM workflow (4 steps).

    Returns a list of dictionaries with step information and status.
    """
    github_dpm_grid = []

    for step_num in range(1, 5):
        step_name = dict(DPMProcessExecution.GITHUB_STEP_CHOICES).get(step_num, f'Step {step_num}')

        try:
            execution = DPMProcessExecution.objects.get(
                session=workflow_session,
                step_number=step_num,
                source_type='github'
            )
            status = execution.status
            error_message = execution.error_message
            execution_data = execution.execution_data
        except DPMProcessExecution.DoesNotExist:
            status = 'pending'
            error_message = None
            execution_data = {}

        github_dpm_grid.append({
            'step_number': step_num,
            'step_name': step_name,
            'status': status,
            'error_message': error_message,
            'execution_data': execution_data
        })

    return github_dpm_grid
