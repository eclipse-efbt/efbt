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

import os
import json
import logging
import glob
import traceback
import time

from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.conf import settings
from django.utils import timezone

from pybirdai.models.workflow_model import WorkflowTaskExecution, WorkflowSession
from pybirdai.entry_points import (
    create_filters,
    create_joins_metadata,
    execute_datapoint,
)
from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner
from ..core_views import create_response_with_loading

from .helpers import encode_file_list, _discover_test_suites

logger = logging.getLogger(__name__)


def workflow_task_substep(request, task_number, substep_name):
    """Handle individual substep execution for workflow tasks"""

    # Validate task number
    if task_number < 0 or task_number > 4:
        return JsonResponse({
            'success': False,
            'message': 'Invalid task number. Substeps are only available for tasks 1-4.'
        }, status=400)

    # Get or create task execution record
    try:
        session_id = request.session.get("workflow_session_id")
        if not session_id:
            return JsonResponse({
                'success': False,
                'message': 'No workflow session found'
            }, status=400)

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)
        task_execution, _ = WorkflowTaskExecution.objects.get_or_create(
            task_number=task_number,
            operation_type='do',
            defaults={'status': 'running'}
        )

        # Update status to running if not already
        if task_execution.status != 'running':
            task_execution.status = 'running'
            task_execution.started_at = timezone.now()
            task_execution.save()

    except Exception as e:
        logger.error(f"Error getting workflow session: {e}")
        return JsonResponse({
            'success': False,
            'message': 'Failed to get workflow session'
        }, status=500)

    # Route to appropriate substep handler
    try:

        if task_number == 1:
            return _execute_task1_substep(request, substep_name, task_execution, workflow_session)
        elif task_number == 2:
            return _execute_task2_substep(request, substep_name, task_execution, workflow_session)
        elif task_number == 3:
            return _execute_task3_substep(request, substep_name, task_execution, workflow_session)
        elif task_number == 4:
            return _execute_task4_substep(request, substep_name, task_execution, workflow_session)
        else:
            return JsonResponse({
                'success': False,
                'message': f'No substep handler for task {task_number}'
            }, status=400)

    except Exception as e:
        logger.error(f"Error executing substep {substep_name} for task {task_number}: {e}")
        return JsonResponse({
            'success': False,
            'message': 'Failed to execute substep. Please check system logs for details.'
        }, status=500)


def _execute_database_creation_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 2: Database Creation"""

    if substep_name == 'start':
        try:
            from pybirdai.entry_points.database_setup import RunApplicationSetup
            app_config = RunApplicationSetup('pybirdai', 'birds_nest')
            results = app_config.run_automode_setup()

            # Update execution data
            execution_data = task_execution.execution_data or {}
            execution_data['database_models_created'] = True
            execution_data['requires_restart'] = results.get('requires_restart', False)
            task_execution.execution_data = execution_data
            task_execution.save()

            return JsonResponse({
                'success': True,
                'message': 'Database models created successfully',
                'requires_restart': results.get('requires_restart', False)
            })

        except Exception as e:
            logger.error(f"Database creation substep failed: {e}")
            return JsonResponse({
                'success': False,
                'message': 'Operation failed. Please check system logs for details.'
            }, status=500)

    elif substep_name == 'continue':
        try:
            from pybirdai.entry_points.database_setup import RunApplicationSetup
            app_config = RunApplicationSetup('pybirdai', 'birds_nest')
            app_config.run_post_setup()

            # Update execution data
            execution_data = task_execution.execution_data or {}
            execution_data['migrations_applied'] = True
            task_execution.execution_data = execution_data
            task_execution.status = 'completed'
            task_execution.completed_at = timezone.now()
            task_execution.save()

            return JsonResponse({
                'success': True,
                'message': 'Migrations applied successfully'
            })

        except Exception as e:
            logger.error(f"Migration substep failed: {e}")
            return JsonResponse({
                'success': False,
                'message': 'Operation failed. Please check system logs for details.'
            }, status=500)

    else:
        return JsonResponse({
            'success': False,
            'message': f'Unknown substep: {substep_name}'
        }, status=400)


def _execute_task1_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 1: SMCubes Core Creation"""

    try:
        # Import necessary modules
        from pybirdai.entry_points.convert_ldm_to_sdd_hierarchies import RunConvertLDMToSDDHierarchies
        from pybirdai.entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
        from pybirdai.entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
        from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
        from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
        from pybirdai.entry_points.delete_framework_data import RunDeleteFrameworkData

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        success_message = ''

        if substep_name == 'cleanup_finrep':
            logger.info("Executing FINREP framework cleanup substep...")
            try:
                cleanup_result = RunDeleteFrameworkData.run_delete_finrep()
                logger.info(f"FINREP cleanup completed: {cleanup_result}")
            except Exception as cleanup_error:
                logger.warning(f"FINREP cleanup warning (continuing): {cleanup_error}")
            execution_data['framework_cleanup'] = True
            execution_data['steps_completed'].append('FINREP framework cleanup')
            success_message = 'FINREP framework data cleaned up successfully'

        elif substep_name == 'import_input_model':
            logger.info("Executing import input model substep...")
            app_config = RunImportInputModelFromSQLDev("pybirdai", "birds_nest")
            app_config.ready()
            execution_data['input_model_imported'] = True
            execution_data['steps_completed'].append('Input model import')
            success_message = 'Input model imported successfully'

        elif substep_name == 'generate_templates':
            logger.info("Executing generate templates substep...")
            RunImportReportTemplatesFromWebsite.run_import()
            execution_data['report_templates_created'] = True
            execution_data['steps_completed'].append('Report templates import')
            success_message = 'Report templates imported successfully'

        elif substep_name == 'import_hierarchy_analysis':
            logger.info("Executing import hierarchy analysis substep...")
            RunImportHierarchiesFromWebsite.import_hierarchies()
            execution_data['hierarchy_analysis_imported'] = True
            execution_data['steps_completed'].append('Hierarchy analysis import')
            success_message = 'Hierarchy analysis imported successfully'

        elif substep_name == 'process_semantic':
            logger.info("Executing process semantic substep...")
            RunImportSemanticIntegrationsFromWebsite.import_mappings_from_website()
            execution_data['semantic_integrations_processed'] = True
            execution_data['steps_completed'].append('Semantic integrations import')
            success_message = 'Semantic integrations processed successfully'

        else:
            return JsonResponse({
                'success': False,
                'message': f'Unknown substep: {substep_name}'
            }, status=400)

        # Check if all subtasks are completed before marking main task as completed
        all_subtasks_completed = (
            execution_data.get('framework_cleanup', False) and
            execution_data.get('input_model_imported', False) and
            execution_data.get('report_templates_created', False) and
            execution_data.get('hierarchy_analysis_imported', False) and
            execution_data.get('semantic_integrations_processed', False)
        )

        any_subtasks_completed = (
            execution_data.get('framework_cleanup', False) or
            execution_data.get('input_model_imported', False) or
            execution_data.get('report_templates_created', False) or
            execution_data.get('hierarchy_analysis_imported', False) or
            execution_data.get('semantic_integrations_processed', False)
        )

        # Update task execution
        task_execution.execution_data = execution_data
        if any_subtasks_completed:
            task_execution.status = "running"
            task_execution.completed_at = timezone.now()
        if all_subtasks_completed:
            task_execution.status = "completed"
            task_execution.completed_at = timezone.now()
        task_execution.save()

        return JsonResponse({
            'success': True,
            'message': success_message,
            'steps_completed': len(execution_data.get('steps_completed', []))
        })

    except Exception as e:
        traceback.print_exc()
        logger.error(f"Task 1 substep {substep_name} failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


def _execute_task2_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 2: SMCubes Transformation Rules"""

    try:
        from pybirdai.entry_points.create_filters import RunCreateFilters
        from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        success_message = ''

        if substep_name == 'generate_all_filters':
            logger.info("Executing generate filters substep...")
            RunCreateFilters.run_create_filters()
            execution_data['filters_created'] = True
            execution_data['steps_completed'].append('Filters creation')
            success_message = 'Filters created successfully'

        elif substep_name == 'create_joins_metadata':
            logger.info("Executing create joins metadata substep...")
            RunCreateJoinsMetadata.run_create_joins_meta_data()
            execution_data['joins_metadata_created'] = True
            execution_data['steps_completed'].append('Joins metadata creation')
            success_message = 'Joins metadata created successfully'

        else:
            return JsonResponse({
                'success': False,
                'message': f'Unknown substep: {substep_name}'
            }, status=400)

        # Check if all subtasks are completed before marking main task as completed
        all_subtasks_completed = (
            execution_data.get('filters_created', False) and
            execution_data.get('joins_metadata_created', False)
        )

        any_subtasks_completed = (
            execution_data.get('filters_created', False) or
            execution_data.get('joins_metadata_created', False)
        )

        # Update task execution
        task_execution.execution_data = execution_data
        if any_subtasks_completed:
            task_execution.status = "running"
            task_execution.completed_at = timezone.now()
        if all_subtasks_completed:
            task_execution.status = "completed"
            task_execution.completed_at = timezone.now()
        task_execution.save()

        return JsonResponse({
            'success': True,
            'message': success_message,
            'steps_completed': len(execution_data.get('steps_completed', []))
        })

    except Exception as e:
        traceback.print_exc()
        logger.error(f"Task 2 substep {substep_name} failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


def _execute_task3_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 3: Python Transformation Rules"""

    try:
        from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
        from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        success_message = ''

        if substep_name == 'generate_filter_code':
            logger.info("Executing generate filter code substep...")
            RunCreateExecutableFilters.run_create_executable_filters_from_db()
            execution_data['filter_code_generated'] = True
            execution_data['steps_completed'].append('Executable filter code generation')
            success_message = 'Filter code generated successfully'

        elif substep_name == 'generate_join_code':
            logger.info("Executing generate join code substep...")
            RunCreateExecutableJoins.create_python_joins_from_db()
            execution_data['join_code_generated'] = True
            execution_data['steps_completed'].append('Join code generation')
            success_message = 'Join code generated successfully'

        else:
            return JsonResponse({
                'success': False,
                'message': f'Unknown substep: {substep_name}'
            }, status=400)

        # Check if all subtasks are completed before marking main task as completed
        all_subtasks_completed = (
            execution_data.get('filter_code_generated', False) and
            execution_data.get('join_code_generated', False)
        )

        any_subtasks_completed = (
            execution_data.get('filter_code_generated', False) or
            execution_data.get('join_code_generated', False)
        )

        # Update task execution
        task_execution.execution_data = execution_data
        if any_subtasks_completed:
            task_execution.status = "running"
            task_execution.completed_at = timezone.now()
        if all_subtasks_completed:
            task_execution.status = "completed"
            task_execution.completed_at = timezone.now()
        task_execution.save()

        return JsonResponse({
            'success': True,
            'message': success_message,
            'steps_completed': len(execution_data.get('steps_completed', []))
        })

    except Exception as e:
        traceback.print_exc()
        logger.error(f"Task 3 substep {substep_name} failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


def _execute_task4_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 4: Test Suite Execution"""

    try:
        from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

        # Get or initialize execution data with complete structure
        execution_data = task_execution.execution_data or {
            'test_mode': 'test_suite',
            'steps_completed': [],
            'test_suites': [],
            'tests_executed': False
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []
        if 'test_suites' not in execution_data:
            execution_data['test_suites'] = []
        if 'test_mode' not in execution_data:
            execution_data['test_mode'] = 'test_suite'

        if substep_name == 'run_tests':
            logger.info("Executing run tests substep...")

            # Track start time for execution time calculation
            start_time = timezone.now()

            # Discover all test suites
            test_suites = _discover_test_suites()

            if not test_suites:
                logger.warning("No test suites found in tests/ directory")
                execution_data['steps_completed'].append('Test suite execution (no suites found)')
                success_message = 'No test suites found to execute'
            else:
                logger.info(f"Found {len(test_suites)} test suite(s): {', '.join(test_suites)}")

                # Clear previous test_suites to avoid duplicates
                execution_data['test_suites'] = []

                # Run tests for each suite
                for suite_name in test_suites:
                    logger.info(f"Running tests for suite: {suite_name}")

                    try:
                        # Create test runner instance
                        test_runner = RegulatoryTemplateTestRunner(False)

                        # Configure test runner for this suite
                        config_file = f'tests/{suite_name}/configuration_file_tests.json'
                        test_runner.args.uv = "False"
                        test_runner.args.config_file = config_file
                        test_runner.args.dp_value = None
                        test_runner.args.reg_tid = None
                        test_runner.args.dp_suffix = None
                        test_runner.args.scenario = None
                        test_runner.args.framework = "FINREP"

                        # Execute tests
                        logger.info(f"Executing tests for suite: {suite_name}")
                        test_runner.main()

                        execution_data['test_suites'].append(suite_name)
                        logger.info(f"Successfully executed tests for suite: {suite_name}")

                    except Exception as suite_error:
                        logger.error(f"Error running tests for suite '{suite_name}': {str(suite_error)}")
                        execution_data['steps_completed'].append(f'Test suite execution error for {suite_name}: {str(suite_error)}')

                execution_data['tests_executed'] = True

                # Remove duplicate completion messages and add a clean one
                execution_data['steps_completed'] = [
                    step for step in execution_data.get('steps_completed', [])
                    if not step.startswith('Test suite execution completed')
                ]
                execution_data['steps_completed'].append(
                    f'Test suite execution completed for {len(execution_data["test_suites"])} suite(s): {", ".join(execution_data["test_suites"])}'
                )

                # Calculate execution time
                end_time = timezone.now()
                execution_time = end_time - start_time
                execution_data['execution_time'] = str(execution_time).split('.')[0]

                success_message = f'Tests executed successfully for {len(execution_data["test_suites"])} suite(s): {", ".join(execution_data["test_suites"])}'

        else:
            return JsonResponse({
                'success': False,
                'message': f'Unknown substep: {substep_name}'
            }, status=400)

        # Check if all subtasks are completed before marking main task as completed
        all_subtasks_completed = execution_data.get('tests_executed', False)

        # Update task execution
        task_execution.execution_data = execution_data
        if all_subtasks_completed:
            task_execution.status = "completed"
            task_execution.completed_at = timezone.now()
        task_execution.save()

        return JsonResponse({
            'success': True,
            'message': success_message,
            'steps_completed': len(execution_data.get('steps_completed', []))
        })

    except Exception as e:
        traceback.print_exc()
        logger.error(f"Task 4 substep {substep_name} failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        }, status=500)


def workflow_task_substep_with_loading(request, task_number, substep_name):
    """
    Execute workflow substeps using the loading pattern instead of AJAX refresh.
    This eliminates infinite loop issues by avoiding complex session management.
    """
    logger.info(f"Loading-based substep execution: Task {task_number}, Substep {substep_name}")

    if request.GET.get('execute') == 'true':
        logger.info(f"Executing substep {substep_name} for Task {task_number}")

        try:
            # Get or create task execution record
            session_id = request.session.get("workflow_session_id")
            if not session_id:
                return JsonResponse({
                    'status': 'error',
                    'message': 'No workflow session found'
                }, status=400)

            workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)
            task_execution, _ = WorkflowTaskExecution.objects.get_or_create(
                task_number=task_number,
                operation_type='do',
                defaults={'status': 'running'}
            )

            # Delegate to appropriate task-specific substep handler
            if task_number == 1:
                result = _execute_task1_substep(request, substep_name, task_execution, workflow_session)
            elif task_number == 2:
                result = _execute_task2_substep(request, substep_name, task_execution, workflow_session)
            elif task_number == 3:
                result = _execute_task3_substep(request, substep_name, task_execution, workflow_session)
            elif task_number == 4:
                result = _execute_task4_substep(request, substep_name, task_execution, workflow_session)
            else:
                logger.error(f"No substep handler for task {task_number}")
                return JsonResponse({
                    'status': 'error',
                    'message': f'No substep handler for task {task_number}'
                }, status=400)

            # Convert result to loading pattern response
            if isinstance(result, JsonResponse):
                result_data = json.loads(result.content.decode('utf-8'))
                if result_data.get('success'):
                    return JsonResponse({'status': 'success'})
                else:
                    return JsonResponse({
                        'status': 'error',
                        'message': result_data.get('message', 'Substep failed')
                    }, status=500)
            else:
                logger.error(f"Unexpected result type from substep handler: {type(result)}")
                return JsonResponse({
                    'status': 'error',
                    'message': 'Unexpected error in substep execution'
                }, status=500)


        except Exception as e:
            logger.error(f"Error executing substep {substep_name}: {str(e)}")
            return JsonResponse({
                'status': 'error',
                'message': f'Failed to execute {substep_name}: {str(e)}'
            }, status=500)

    # Show loading screen for the substep
    substep_display_names = {
        # Task 1 substeps
        'cleanup_finrep': 'FINREP Framework Cleanup',
        'import_input_model': 'Input Model Import',
        'generate_templates': 'Report Templates Generation',
        'import_hierarchy_analysis': 'Hierarchy Analysis Import',
        'process_semantic': 'Semantic Integrations Processing',
        # Task 2 substeps
        'generate_all_filters': 'Filter Generation',
        'create_joins_metadata': 'Join Metadata Creation',
        # Task 3 substeps
        'generate_filter_code': 'Filter Code Generation',
        'generate_join_code': 'Join Code Generation',
        # Task 4 substeps
        'run_tests': 'Test Suite Execution'
    }

    task_display_names = {
        1: 'SMCubes Core Creation',
        2: 'SMCubes Transformation Rules Creation',
        3: 'Python Transformation Rules Creation',
        4: 'Full Execution with Test Suite'
    }

    substep_display = substep_display_names.get(substep_name, substep_name.replace('_', ' ').title())
    task_display = task_display_names.get(task_number, f'Task {task_number}')

    # Check if task might be completed after this substep to determine return URL
    try:
        current_task_execution = WorkflowTaskExecution.objects.get(
            task_number=task_number,
            operation_type='do'
        )
        current_execution_data = current_task_execution.execution_data or {}

        # Check how many substeps will be completed after this one
        upcoming_completed_count = sum([
            current_execution_data.get('framework_cleanup', False),
            current_execution_data.get('input_model_imported', False),
            current_execution_data.get('report_templates_created', False),
            current_execution_data.get('hierarchy_analysis_imported', False),
            current_execution_data.get('semantic_integrations_processed', False)
        ]) + 1  # +1 for the current substep that will complete

        # If this will be the last substep, redirect to review
        if upcoming_completed_count >= 5:
            return_url = f'/pybirdai/workflow/task/{task_number}/review/'
            return_text = f"Review {task_display}"
            success_message = f"{substep_display} completed successfully. Task  is now complete!"
        else:
            return_url = f'/pybirdai/workflow/task/{task_number}/do/'
            return_text = f"Back to {task_display}"
            success_message = f"{substep_display} completed successfully"

    except WorkflowTaskExecution.DoesNotExist:
        # Fallback to default
        return_url = f'/pybirdai/workflow/task/{task_number}/do/'
        return_text = f"Back to {task_display}"
        success_message = f"{substep_display} completed successfully"

    return create_response_with_loading(
        request,
        f"Executing {substep_display} for {task_display}",
        success_message,
        return_url,
        return_text
    )
