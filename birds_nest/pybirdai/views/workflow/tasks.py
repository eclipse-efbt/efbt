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
import traceback
import time
import glob

from django.shortcuts import render, redirect
from django.http import JsonResponse
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
from pybirdai.forms import SMCubesCoreForm

from .helpers import encode_file_list, refresh_complete_status, load_test_results

logger = logging.getLogger(__name__)

def workflow_task_router(request, task_number, operation):
    """Route to appropriate task handler based on task number and operation"""

    if task_number < 1 or task_number > 6:
        messages.error(request, "Invalid task number")
        return redirect('pybirdai:workflow_dashboard')

    if operation not in ['do', 'review', 'compare']:
        messages.error(request, "Invalid operation type")
        return redirect('pybirdai:workflow_dashboard')



    # Get or create task execution record with enhanced error handling
    session_id = request.session.get("workflow_session_id")
    if not session_id:
        logger.warning("No workflow session ID found in request session")
        messages.warning(request, "No active workflow session found. Starting new session.")
        return redirect('pybirdai:workflow_dashboard')

    # Enhanced workflow session retrieval with fallback
    try:
        workflow_session = WorkflowSession.objects.get(session_id=session_id)
    except WorkflowSession.DoesNotExist:
        logger.warning(f"Workflow session {session_id} not found in database, attempting recovery")

        # Try to recreate session with basic configuration
        try:
            workflow_session = WorkflowSession.objects.create(
                session_id=session_id,
                configuration={},
                current_task=task_number
            )
            logger.info(f"Recreated workflow session {session_id}")
            messages.info(request, "Workflow session was recreated. You may need to reconfigure some settings.")
        except Exception as create_error:
            logger.error(f"Failed to recreate workflow session: {str(create_error)}")
            messages.error(request, "Unable to restore workflow session. Please start a new session.")
            return redirect('pybirdai:workflow_dashboard')


    # Enhanced task execution retrieval with error handling
    try:
        task_execution, created = WorkflowTaskExecution.objects.get_or_create(
            task_number=task_number,
            operation_type=operation,
            defaults={"status": "pending"},
        )
        if created:
            logger.info(f"Created new task execution for Task {task_number} - {operation}")
    except Exception as task_error:
        logger.error(f"Failed to get/create task execution: {str(task_error)}")
        messages.error(request, "Unable to access task execution records. Please try again.")
        return redirect('pybirdai:workflow_dashboard')

    # # Check if task can be executed
    # if operation == "do" and not task_execution.can_execute():
    #     messages.error(request, "Previous tasks must be completed first")
    #     return redirect('pybirdai:workflow_dashboard')

    # Route to appropriate handler
    task_handlers = {
        1: task1_smcubes_core,
        2: task2_smcubes_rules,
        3: task3_python_rules,
        4: task4_full_execution,
    }

    handler = task_handlers.get(task_number)
    if handler:
        return handler(request, operation, task_execution, workflow_session)
    else:
        messages.error(request, "Task handler not implemented")
        return redirect("pybirdai:workflow_dashboard")


def task1_smcubes_core(request, operation, task_execution, workflow_session):
    """Handle Task 1: SMCubes Core Creation operations"""

    if operation == 'do':
        if request.method == "GET":
            steps_completed = sum([_ for _ in task_execution.execution_data.values() if isinstance(_,bool)])

            if steps_completed == 5:
                task_execution.status = "completed"
                task_execution.save()

        if request.method == 'POST':
            # Check if this is an AJAX request (handle MockRequest objects)
            is_ajax = hasattr(request, 'headers') and request.headers.get('X-Requested-With') == 'XMLHttpRequest'

            # Start SMCubes core creation
            task_execution.status = "running task1_smcubes_core"
            task_execution.started_at = timezone.now()
            task_execution.save()

            logger = logging.getLogger(__name__)

            try:
                # Fetch FINREP content and test suite files using framework-specific URLs
                import json
                import os
                from django.conf import settings
                from pybirdai.api.workflow_api import AutomodeConfigurationService
                from pybirdai.views.workflow.github import _get_github_token

                # Load config from JSON file (same as dashboard)
                config = {}
                base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                config_path = os.path.join(base_dir, 'automode_config.json')
                if os.path.exists(config_path):
                    with open(config_path, "r") as f:
                        config = json.load(f)
                    logger.info(f"Loaded config from {config_path}")
                else:
                    logger.warning(f"Config file not found at {config_path}")

                workflow_service = AutomodeConfigurationService()
                token = _get_github_token()
                branch = config.get('bird_content_branch', 'main')

                # Detect pipeline from selected frameworks
                # Priority: 1) request POST params, 2) config file, 3) default to empty
                from pybirdai.services.pipeline_repo_service import detect_pipeline

                # Try to get frameworks from request first (for direct task execution)
                selected_frameworks = []
                if hasattr(request, 'POST'):
                    selected_frameworks = request.POST.getlist('selected_frameworks')
                    if not selected_frameworks:
                        single_framework = request.POST.get('selected_frameworks', '')
                        if single_framework:
                            selected_frameworks = [single_framework]

                # Fall back to config file if not in request
                if not selected_frameworks:
                    selected_frameworks = config.get('selected_frameworks', []) or []

                # Detect pipeline based on frameworks
                pipeline_name = detect_pipeline(selected_frameworks) if selected_frameworks else 'main'
                logger.info(f"Detected pipeline '{pipeline_name}' from frameworks: {selected_frameworks}")

                # Log warning if no frameworks specified (helps debug configuration issues)
                if not selected_frameworks:
                    logger.warning("No frameworks specified - defaulting to 'main' pipeline. "
                                   "Ensure 'selected_frameworks' is set in config or passed via request.")

                # 1. Fetch content files using framework-specific pipeline URL
                # Use mirror mode (non-destructive) for workflow execution to preserve generated code
                pipeline_url = config.get(f'pipeline_url_{pipeline_name}') or config.get('pipeline_url_main')
                if pipeline_url:
                    logger.info(f"Fetching {pipeline_name.upper()} content from: {pipeline_url} (mirror mode)")
                    workflow_service._fetch_from_github(pipeline_url, token=token, branch=branch, use_mirror=True)

                # 2. Fetch test suite using framework-specific URL (mirror mode to preserve existing results)
                test_suite_url = config.get(f'test_suite_url_{pipeline_name}') or config.get('test_suite_url_main') or config.get('test_suite_github_url')
                logger.info(f"Test suite URL for {pipeline_name}: {test_suite_url}")
                if test_suite_url:
                    logger.info(f"Fetching {pipeline_name.upper()} test suite from: {test_suite_url} (mirror mode)")
                    workflow_service._fetch_test_suite_from_github(test_suite_url, token=token, use_mirror=True)
                else:
                    logger.warning(f"No test suite URL configured for {pipeline_name.upper()} workflow")

                # Import real entry point modules (with correct class names)
                from pybirdai.entry_points.convert_ldm_to_sdd_hierarchies import RunConvertLDMToSDDHierarchies
                from pybirdai.entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
                from pybirdai.entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
                from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
                from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
                execution_data = {
                    "framework_cleanup": False,
                    "hierarchy_analysis_imported": False,
                    "semantic_integrations_processed": False,
                    "input_model_imported": False,
                    "report_templates_created": False,
                }

                # Execute subtasks based on selections or run all by default
                run_all = not any([
                    request.POST.get('cleanup_finrep'),
                    request.POST.get('import_input_model'),
                    request.POST.get('generate_templates'),
                    request.POST.get('import_hierarchy_analysis'),
                    request.POST.get('process_semantic'),

                ])


                # Framework-specific cleanup (preserves other frameworks and input model)
                # Full database reset is available separately via the Reset Database button on dashboard
                if request.POST.get("cleanup_finrep") or run_all:
                    logger.info("Cleaning up FINREP framework data (preserving other frameworks and input model)...")
                    from pybirdai.entry_points.delete_framework_data import RunDeleteFrameworkData
                    try:
                        cleanup_result = RunDeleteFrameworkData.run_delete_finrep()
                        logger.info(f"FINREP cleanup completed: {cleanup_result}")
                        execution_data['framework_cleanup'] = True
                    except Exception as cleanup_error:
                        logger.warning(f"FINREP cleanup warning (continuing): {cleanup_error}")

                 # Import input model using ready() method (creates cubes and structures)
                if request.POST.get('import_input_model') or run_all:
                    logger.info("Importing input model...")
                    app_config = RunImportInputModelFromSQLDev("pybirdai", "birds_nest")
                    app_config.ready()  # Call ready() method since no static method exists
                    execution_data['input_model_imported'] = True

                # Import report templates
                if request.POST.get("generate_templates") or run_all:
                    logger.info("Importing report templates from website...")
                    RunImportReportTemplatesFromWebsite.run_import()
                    execution_data['report_templates_created'] = True

                # Import hierarchies from website
                if request.POST.get("import_hierarchy_analysis") or run_all:
                    logger.info("Importing hierarchies from website...")
                    RunImportHierarchiesFromWebsite.import_hierarchies()
                    execution_data['hierarchy_analysis_imported'] = True

                # Import semantic integrations
                if request.POST.get("process_semantic") or run_all:
                    logger.info("Importing semantic integrations from website...")
                    RunImportSemanticIntegrationsFromWebsite.import_mappings_from_website()
                    execution_data['semantic_integrations_processed'] = True



                # Store results
                task_execution.execution_data = execution_data
                task_execution.status = "completed"
                task_execution.completed_at = timezone.now()
                task_execution.save()

                # Count completed steps
                steps_completed = sum([
                    execution_data.get('framework_cleanup', False),
                    execution_data.get('hierarchy_analysis_imported', False),
                    execution_data.get('semantic_integrations_processed', False),
                    execution_data.get('input_model_imported', False),
                    execution_data.get('report_templates_created', False),
                ])

                if steps_completed == 5:
                    task_execution.status = "completed"
                    task_execution.save()


                success_message = f"SMCubes core creation completed successfully. {steps_completed} steps completed."

                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_message,
                        'steps_completed': steps_completed,
                        'execution_data': execution_data
                    })

                # Only use messages for real requests, not automode MockRequest
                if hasattr(request, "_messages"):
                    messages.success(request, success_message)
                    return redirect(
                        "pybirdai:workflow_task", task_number=3, operation="review"
                    )
                # For automode, just return None (no redirect needed)

            except Exception as e:
                traceback.print_exc()
                logger.error(f"SMCubes core creation failed: {e}")
                task_execution.status = "failed"
                task_execution.error_message = str(e)
                task_execution.save()

                if is_ajax:
                    return JsonResponse({
                        'success': False,
                        'message': f"SMCubes core creation failed: {e}"
                    })

                if hasattr(request, '_messages'):
                    messages.error(request, f"SMCubes core creation failed: {e}")

        # Only render template for real requests, not automode MockRequest
        if hasattr(request, "_messages"):
            # Create form instance for GET requests
            if request.method == "GET":
                form = SMCubesCoreForm()
            return render(
                request,
                "pybirdai/workflow/main_workflow/task1/do.html",
                {
                    "form": form if request.method == "GET" else SMCubesCoreForm(),
                    "task_execution": task_execution,
                    "workflow_session": workflow_session,
                },
            )
        else:
            # For automode, return None (no template rendering needed)
            return None

    elif operation == 'review':
        refresh_complete_status(task=1,all=False)
        # Handle POST request for marking as reviewed
        if request.method == 'POST' and 'mark_reviewed' in request.POST:
            # Update task execution to mark as reviewed
            task_execution.reviewed_at = timezone.now()
            if not task_execution.execution_data:
                task_execution.execution_data = {}
            task_execution.execution_data['reviewed'] = True
            task_execution.save()

            messages.success(request, "Task 1: SMCubes Core Creation marked as reviewed successfully")
            return redirect('pybirdai:workflow_dashboard')

        # Fetch execution data from the 'do' operation
        do_execution = WorkflowTaskExecution.objects.filter(
            task_number=1,
            operation_type='do'
        ).first()

        execution_data = do_execution.execution_data if do_execution and do_execution.execution_data else {}

        return render(request, 'pybirdai/workflow/main_workflow/task1/review.html', {
            'task_execution': task_execution,
            'workflow_session': workflow_session,
            'execution_data': execution_data,
        })


def task2_smcubes_rules(request, operation, task_execution, workflow_session):
    """Handle Task 4: SMCubes Transformation Rules Creation operations"""

    if operation == 'do':
        if request.method == 'POST':
            # Check if this is an AJAX request (handle MockRequest objects)
            is_ajax = hasattr(request, 'headers') and request.headers.get('X-Requested-With') == 'XMLHttpRequest'

            # Start transformation rules creation
            task_execution.status = "running"
            task_execution.started_at = timezone.now()
            task_execution.save()

            try:
                # Import real entry point classes (using the correct class names)
                from pybirdai.entry_points.create_filters import RunCreateFilters
                from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata

                execution_data = {
                    "current_step": "filters",
                    "filters_created": False,
                    "joins_metadata_created": False,
                    "steps_completed": [],
                }

                # Execute all steps by default or based on selections
                run_all = not any([
                    request.POST.get('generate_all_filters'),
                    request.POST.get('create_joins_metadata'),
                ])

                # Create filters
                if request.POST.get("generate_all_filters") or run_all:
                    logger.info("Creating filters...")
                    execution_data["current_step"] = "filters"
                    RunCreateFilters.run_create_filters()
                    execution_data['filters_created'] = True
                    execution_data['steps_completed'].append('Filters creation')

                # Create join metadata
                if request.POST.get('create_joins_metadata') or run_all:
                    logger.info("Creating joins metadata...")
                    execution_data["current_step"] = "joins_metadata"
                    RunCreateJoinsMetadata.run_create_joins_meta_data()  # Correct method name
                    execution_data['joins_metadata_created'] = True
                    execution_data['steps_completed'].append('Joins metadata creation')


                execution_data['current_step'] = 'completed'

                # Check if all subtasks are completed before marking main task as completed
                all_subtasks_completed = (
                    execution_data.get('filters_created', False) and
                    execution_data.get('joins_metadata_created', False)
                )

                # Store results
                task_execution.execution_data = execution_data
                if all_subtasks_completed:
                    task_execution.status = "completed"
                    task_execution.completed_at = timezone.now()
                task_execution.save()

                steps_completed = len(execution_data.get('steps_completed', []))
                if all_subtasks_completed:
                    success_message = f"Transformation rules created successfully. {steps_completed} steps completed."
                else:
                    success_message = f"Transformation rules partially completed. {steps_completed} steps completed."

                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_message,
                        'steps_completed': steps_completed,
                        'execution_data': execution_data,
                        'all_completed': all_subtasks_completed
                    })

                if hasattr(request, '_messages'):
                    messages.success(request, success_message)
                    if all_subtasks_completed:
                        return redirect('pybirdai:workflow_task', task_number=2, operation='review')
                    else:
                        return redirect('pybirdai:workflow_task', task_number=2, operation='do')

            except Exception as e:
                logger.error(f"Transformation rules creation failed: {e}")
                task_execution.status = "failed"
                task_execution.error_message = str(e)
                task_execution.save()

                if is_ajax:
                    return JsonResponse({
                        'success': False,
                        'message': f"Transformation rules creation failed: {e}"
                    })

                if hasattr(request, '_messages'):
                    messages.error(request, f"Transformation rules creation failed: {e}")

        if hasattr(request, '_messages'):
            return render(request, 'pybirdai/workflow/main_workflow/task2/do.html', {
                'task_execution': task_execution,
                'workflow_session': workflow_session,
            })
        else:
            return None

    elif operation == 'review':

        refresh_complete_status(task=2,all=False)
        # Fetch execution data from the 'do' operation
        do_execution = WorkflowTaskExecution.objects.filter(
            task_number=2,
            operation_type='do'
        ).first()

        if do_execution.status == "completed":
            task_execution.status = "completed"

        execution_data = do_execution.execution_data if do_execution and do_execution.execution_data else {}

        return render(request, 'pybirdai/workflow/main_workflow/task2/review.html', {
            'task_execution': task_execution,
            'workflow_session': workflow_session,
            'execution_data': execution_data,
        })


def task3_python_rules(request, operation, task_execution, workflow_session):
    """Handle Task 5: Python Transformation Rules Creation operations"""

    if operation == 'do':
        if request.method == 'POST':
            # Check if this is an AJAX request (handle MockRequest objects)
            is_ajax = hasattr(request, 'headers') and request.headers.get('X-Requested-With') == 'XMLHttpRequest'

            # Start Python code generation
            task_execution.status = "running"
            task_execution.started_at = timezone.now()
            task_execution.save()

            try:
                # Import real Python code generation entry points
                from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
                from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins

                # Get framework from config
                config = {}
                base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                config_path = os.path.join(base_dir, 'automode_config.json')
                if os.path.exists(config_path):
                    with open(config_path, "r") as f:
                        config = json.load(f)

                selected_frameworks = config.get('selected_frameworks', ['FINREP'])
                framework = selected_frameworks[0] if selected_frameworks else 'FINREP'
                # Remove _REF suffix if present (entry point will add it)
                framework = framework.replace('_REF', '')
                logger.info(f"Using framework '{framework}' for Python code generation")

                execution_data = {
                    "current_phase": "filters",
                    "filter_code_generated": False,
                    "join_code_generated": False,
                    "steps_completed": [],
                }

                # Execute Python code generation steps
                run_all = not any(
                    [
                        request.POST.get("generate_filter_code"),
                        request.POST.get("generate_join_code"),
                    ]
                )

                # Generate executable filter code
                if request.POST.get("generate_filter_code") or run_all:
                    logger.info(f"Generating executable filter Python code for {framework}...")
                    execution_data["current_phase"] = "filters"
                    RunCreateExecutableFilters.run_create_executable_filters_from_db(framework=framework)
                    execution_data["filter_code_generated"] = True
                    execution_data["steps_completed"].append(
                        f"Executable filter code generation ({framework})"
                    )

                # Note: Join and transformation code generation would use different entry points
                # For now, marking as completed to indicate the workflow step is done
                if request.POST.get("generate_join_code") or run_all:
                    logger.info("Join code generation (using filter infrastructure)...")
                    execution_data["current_phase"] = "joins"
                    RunCreateExecutableJoins.create_python_joins_from_db(framework_id=workflow_session.framework_id)
                    execution_data['join_code_generated'] = True
                    execution_data['steps_completed'].append('Join code infrastructure ready')



                execution_data['current_phase'] = 'completed'

                # Store results
                task_execution.execution_data = execution_data
                task_execution.status = "completed"
                task_execution.completed_at = timezone.now()
                task_execution.save()

                steps_completed = len(execution_data.get('steps_completed', []))
                success_message = f"Python code generation completed. {steps_completed} steps completed."

                if is_ajax:
                    return JsonResponse({
                        'success': True,
                        'message': success_message,
                        'steps_completed': steps_completed,
                        'execution_data': execution_data
                    })

                if hasattr(request, '_messages'):
                    messages.success(request, success_message)
                    return redirect('pybirdai:workflow_task', task_number=3, operation='review')

            except Exception as e:
                logger.error(f"Python code generation failed: {e}")
                task_execution.status = "failed"
                task_execution.error_message = str(e)
                task_execution.save()

                if is_ajax:
                    return JsonResponse({
                        'success': False,
                        'message': f"Python code generation failed: {e}"
                    })

                if hasattr(request, '_messages'):
                    messages.error(request, f"Python code generation failed: {e}")

        if hasattr(request, '_messages'):
            return render(request, 'pybirdai/workflow/main_workflow/task3/do.html', {
                'task_execution': task_execution,
                'workflow_session': workflow_session,
            })
        else:
            return None

    elif operation == 'review':
        refresh_complete_status(task=3,all=False)
        # Fetch execution data from the 'do' operation
        do_execution = WorkflowTaskExecution.objects.filter(
            task_number=3,
            operation_type='do'
        ).first()

        execution_data = do_execution.execution_data if do_execution and do_execution.execution_data else {}

        if do_execution.status == "completed":
            task_execution.status = "completed"

        # Generate encoded file list for Filter Code Editor (FINREP files only)
        # New structure: logic files are in filter_code/logic/templates/
        filter_code_base = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
        logic_templates_dir = os.path.join(filter_code_base, 'logic', 'templates')

        # Look in logic/templates for F_*.py files, fallback to old location
        finrep_files = [os.path.basename(f) for f in glob.glob(os.path.join(logic_templates_dir, 'F_*.py'))]
        if not finrep_files:
            finrep_files = [os.path.basename(f) for f in glob.glob(os.path.join(filter_code_base, 'F_*.py'))]
        finrep_files.sort()  # Sort alphabetically for consistency
        encoded_files = encode_file_list(finrep_files)

        return render(request, 'pybirdai/workflow/main_workflow/task3/review.html', {
            'task_execution': task_execution,
            'workflow_session': workflow_session,
            'execution_data': execution_data,
            'encoded_file_filter': encoded_files,
        })


def task4_full_execution(request, operation, task_execution, workflow_session):
    """Handle Task 4: Test Suite Execution operations"""

    if operation == 'do':
        if request.method == 'POST':
            # Start test execution
            task_execution.status = 'running'
            task_execution.started_at = timezone.now()
            task_execution.save()

            try:
                execution_data = {
                    'current_stage': 'test_execution',
                    'test_mode': 'config_file',
                    'tests_executed': False,
                    'reports_generated': False,
                    'results_validated': False,
                    'steps_completed': []
                }

                # Execute subtasks based on selections or run all by default
                run_all = not any([
                    request.POST.get('use_config_file'),
                    request.POST.get('generate_reports'),
                    request.POST.get('validate_results'),
                ])

                # Run configuration file tests
                if request.POST.get('use_config_file') or run_all:
                    logger.info("Starting test suite execution...")
                    execution_data['steps_completed'].append('Test suite execution started')

                    # Use framework-aware test discovery for FINREP
                    from pybirdai.utils.test_discovery import get_test_suite_for_framework

                    test_suites = []
                    config_path, suite_name = get_test_suite_for_framework('FINREP')

                    if config_path:
                        test_suites.append({
                            'name': suite_name,
                            'config_path': config_path
                        })
                        logger.info(f"Discovered FINREP test suite: {suite_name}")

                    if not test_suites:
                        logger.error("No FINREP test suites found. Ensure test suite has 'test_type': 'finrep' in configuration_file_tests.json")
                        raise Exception("No FINREP test suites found. Ensure test suite has 'test_type': 'finrep' in configuration_file_tests.json")

                    # Run tests for each discovered suite
                    for suite in test_suites:
                        logger.info(f"Running test suite: {suite['name']}")

                        # Create test runner instance for this suite
                        test_runner = RegulatoryTemplateTestRunner(False)

                        # Configure test runner
                        test_runner.args.uv = "False"
                        test_runner.args.config_file = suite['config_path']
                        test_runner.args.dp_value = None
                        test_runner.args.reg_tid = None
                        test_runner.args.dp_suffix = None
                        test_runner.args.scenario = None
                        test_runner.args.suite_name = suite['name']
                        test_runner.args.framework = "FINREP"

                        # Execute tests
                        logger.info(f"Executing tests from config: {suite['config_path']}")
                        test_runner.main()
                        logger.info(f"Completed test suite: {suite['name']}")

                    execution_data['test_mode'] = 'config_file'
                    execution_data['test_suites'] = [s['name'] for s in test_suites]
                    execution_data['tests_executed'] = True
                    execution_data['steps_completed'].append(f'Configuration file tests completed for {len(test_suites)} suite(s)')

                # Generate test reports
                if request.POST.get('generate_reports') or run_all:
                    logger.info("Generating test reports...")
                    # Note: Test report generation is typically handled by the test runner itself
                    execution_data['reports_generated'] = True
                    execution_data['steps_completed'].append('Test reports generated')

                # Validate test results
                if request.POST.get('validate_results') or run_all:
                    logger.info("Validating test results...")
                    # Note: Result validation is typically handled by the test runner itself
                    execution_data['results_validated'] = True
                    execution_data['steps_completed'].append('Test results validated')

                execution_data['current_stage'] = 'completed'

                # Calculate execution time
                execution_time = timezone.now() - task_execution.started_at
                execution_data['execution_time'] = str(execution_time).split('.')[0]

                # Check if all selected subtasks are completed before marking main task as completed
                # Determine which tasks were requested
                requested_tasks = []
                if request.POST.get('use_config_file') or run_all:
                    requested_tasks.append('tests_executed')
                if request.POST.get('generate_reports') or run_all:
                    requested_tasks.append('reports_generated')
                if request.POST.get('validate_results') or run_all:
                    requested_tasks.append('results_validated')

                # Check if all requested tasks are completed
                all_subtasks_completed = all(
                    execution_data.get(task, False) for task in requested_tasks
                )

                # Store results
                task_execution.execution_data = execution_data
                if all_subtasks_completed:
                    task_execution.status = "completed"
                    task_execution.completed_at = timezone.now()
                task_execution.save()

                if hasattr(request, '_messages'):
                    if all_subtasks_completed:
                        messages.success(request, "Test suite execution completed successfully!")
                        return redirect('pybirdai:workflow_task', task_number=4, operation='review')
                    else:
                        steps_completed = len(execution_data.get('steps_completed', []))
                        messages.success(request, f"Test suite partially completed. {steps_completed} steps completed.")
                        return redirect('pybirdai:workflow_task', task_number=4, operation='do')

            except Exception as e:
                logger.error(f"Test execution failed: {e}")
                task_execution.status = 'failed'
                task_execution.error_message = str(e)
                task_execution.save()
                if hasattr(request, '_messages'):
                    messages.error(request, f"Test execution failed: {e}")

        if hasattr(request, '_messages'):
            return render(request, 'pybirdai/workflow/main_workflow/task4/do.html', {
                'task_execution': task_execution,
                'workflow_session': workflow_session,
            })
        else:
            return None

    elif operation == 'review':
        refresh_complete_status(task=4,all=False)
        # Load test results from JSON files
        test_results = load_test_results()

        # Calculate summary statistics
        total_tests = len(test_results)
        passed_tests = 0
        failed_tests = 0

        for result in test_results:
            test_data = result.get('test_results', {})
            passed_list = test_data.get('passed', [])
            failed_list = test_data.get('failed', [])

            # Count actual test results
            if passed_list:
                passed_tests += len(passed_list) if isinstance(passed_list, list) else 1
            if failed_list:
                failed_tests += len(failed_list) if isinstance(failed_list, list) else 1

        logger.info(f"Test summary - Total: {total_tests}, Passed: {passed_tests}, Failed: {failed_tests}")

        # Group results by regulatory template and scenario
        grouped_results = {}
        for result in test_results:
            test_info = result.get('test_information', {})
            template_id = test_info.get('regulatory_template_id', 'Unknown')
            scenario = test_info.get('scenario_name', 'Unknown')

            if template_id not in grouped_results:
                grouped_results[template_id] = {}
            if scenario not in grouped_results[template_id]:
                grouped_results[template_id][scenario] = []

            grouped_results[template_id][scenario].append(result)

        # Fetch execution data from the 'do' operation
        do_execution = WorkflowTaskExecution.objects.filter(
            task_number=4,
            operation_type='do'
        ).first()

        execution_data = do_execution.execution_data if do_execution and do_execution.execution_data else {}

        if do_execution.status == "completed":
            task_execution.status = "completed"

        return render(request, 'pybirdai/workflow/main_workflow/task4/review.html', {
            'task_execution': task_execution,
            'workflow_session': workflow_session,
            'execution_data': execution_data,
            'test_results': test_results,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'grouped_results': grouped_results,
        })
