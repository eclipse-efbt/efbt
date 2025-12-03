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

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.utils import timezone
from django.urls import reverse
from django.conf import settings
from django.db import OperationalError
import uuid
import logging
import os
import threading
import time
import datetime
import json
import glob
import subprocess
import requests
import zlib
import binascii

from pybirdai.models.workflow_model import WorkflowTaskExecution, WorkflowSession, DPMProcessExecution, AnaCreditProcessExecution
from .core_views import create_response_with_loading
from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from pybirdai.entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
from pybirdai.entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from ..api.workflow_api import AutomodeConfigurationService
from ..forms import (
    AutomodeConfigurationSessionForm,
    ResourceDownloadForm,
    SMCubesCoreForm,
    SMCubesRulesForm,
    PythonRulesForm,
    FullExecutionForm,
)
from pybirdai.entry_points import (
    automode_database_setup,
    create_filters,
    import_dpm_data,
    dpm_output_layer_creation,
    ancrdt_transformation,
    create_joins_metadata,
    execute_datapoint,
)
# Import the test runner
from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner
import traceback
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def encode_file_list(file_list):
    """
    Compress and hex-encode a list of filenames for URL transmission.

    Args:
        file_list: List of filenames (strings)

    Returns:
        Hex-encoded string representing compressed file list
    """
    if not file_list:
        return ""

    # Join filenames with pipe separator
    file_string = "|".join(file_list)

    # Compress using zlib
    compressed = zlib.compress(file_string.encode('utf-8'))

    # Convert to hex string
    hex_string = binascii.hexlify(compressed).decode('ascii')

    return hex_string


def refresh_complete_status(task:int=3,all:bool=True):

    try:

        task_to_complete_mapping = {
            1:5,
            2:2,
            3:2,
            4:1
        }

        def check_one_task(execution,task:int=3):
            steps_completed = sum([_ for _ in execution.execution_data.values() if isinstance(_,bool)])
            if (execution.task_number == task) and (steps_completed == task_to_complete_mapping[task]):
                execution.status = "completed"
            return execution

        if all:
            task_executions = WorkflowTaskExecution.objects.all()
            for task_number,_ in task_to_complete_mapping.items():
                for execution in task_executions:
                    execution = check_one_task(execution,task_number)
                    execution.save()
            return

        task_executions = WorkflowTaskExecution.objects.filter(
            task_number=task,
            operation_type='do'
        ).first()
        if isinstance(task_executions,WorkflowTaskExecution):
            task_executions = [task_executions]
        for execution in task_executions:
            execution = check_one_task(execution,task)
            execution.save()
        return
    except OperationalError:
        return


def load_test_results():
    """Load and parse test results from JSON files across all test suites"""
    test_results = []
    # Use Django's BASE_DIR to construct the full path
    base_dir = getattr(settings, 'BASE_DIR', os.getcwd())

    try:
        # Discover all test suites
        test_suites = _discover_test_suites()

        if not test_suites:
            logger.warning("No test suites found")
            return test_results

        logger.info(f"Discovered {len(test_suites)} test suite(s): {', '.join(test_suites)}")

        # Load test results from each suite
        for suite_name in test_suites:
            json_files_path = os.path.join(base_dir, 'tests', suite_name, 'tests', 'test_results', 'json', '*.json')
            logger.info(f"Looking for test results in suite '{suite_name}': {json_files_path}")

            json_files = glob.glob(json_files_path)
            logger.info(f"Found {len(json_files)} JSON file(s) in suite '{suite_name}'")

            for json_file in json_files:
                try:
                    logger.debug(f"Loading test result file: {json_file}")
                    with open(json_file, 'r', encoding='utf-8') as f:
                        result_data = json.load(f)
                        # Add filename and suite name for reference
                        result_data['filename'] = os.path.basename(json_file)
                        result_data['suite_name'] = suite_name
                        test_results.append(result_data)
                        logger.debug(f"Successfully loaded {json_file}")
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"Error loading test result file {json_file}: {e}")
                    continue

        # Sort by timestamp (newest first)
        test_results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        logger.info(f"Loaded {len(test_results)} test result(s) successfully from {len(test_suites)} suite(s)")

    except Exception as e:
        logger.error(f"Error loading test results: {e}")

    return test_results

# In-memory storage for GitHub token (not persisted to database or file)
_in_memory_github_token = None

# In-memory storage for migration status (not persisted to database or file)
_migration_status = {
    "running": False,
    "completed": False,
    "success": False,
    "error": None,
    "message": "",
    "started_at": None,
    "completed_at": None,
}

# In-memory storage for database setup status (not persisted to database or file)
_database_setup_status = {
    "running": False,
    "completed": False,
    "success": False,
    "error": None,
    "message": "",
    "started_at": None,
    "completed_at": None,
    "current_task": None,
    "completed_tasks": [],
}

# In-memory storage for automode status (not persisted to database or file)
_automode_status = {
    "running": False,
    "completed": False,
    "success": False,
    "error": None,
    "message": "",
    "started_at": None,
    "completed_at": None,
    "current_task": None,
    "target_task": None,
    "completed_tasks": [],
    "task_errors": [],
}


def _get_github_token():
    """Get GitHub token from in-memory storage or environment variable."""
    global _in_memory_github_token
    return _in_memory_github_token or os.environ.get("GITHUB_TOKEN", "")


def _set_github_token(token):
    """Set GitHub token in in-memory storage."""
    global _in_memory_github_token
    _in_memory_github_token = token.strip() if token else None


def _clear_github_token():
    """Clear GitHub token from in-memory storage."""
    global _in_memory_github_token
    _in_memory_github_token = None


def _run_migrations_async():
    """Run migrations in background thread."""
    global _migration_status

    try:
        logger.info("Starting background migration process...")
        _migration_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'error': None,
            'message': 'Running database migrations...',
            'started_at': time.time()
        })

        # Run the actual migration - this should ONLY run Django migrations, no file operations


        from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
        app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest', token=_in_memory_github_token)

        logger.info("About to call run_migrations_after_restart() - this should NOT download or delete any files")
        migration_results = app_config.run_migrations_after_restart()
        logger.info("run_migrations_after_restart() completed - no files should have been modified")

        # Update status on success
        _migration_status.update({
            'running': False,
            'completed': True,
            'success': True,
            'error': None,
            'message': migration_results.get('message', 'Database migrations completed successfully'),
            'completed_at': time.time()
        })

        logger.info("Background migration process completed successfully")

        time.sleep(6)

        os._exit(0)

    except Exception as e:
        logger.error(f"Background migration process failed: {e}")
        _migration_status.update(
            {
                "running": False,
                "completed": True,
                "success": False,
                "error": "Database migration error occurred",
                "message": "Database migrations failed",
                "completed_at": time.time(),
            }
        )


def _reset_migration_status():
    """Reset migration status to initial state."""
    global _migration_status
    _migration_status.update(
        {
            "running": False,
            "completed": False,
            "success": False,
            "error": None,
            "message": "",
            "started_at": None,
            "completed_at": None,
        }
    )


def _reset_database_setup_status():
    """Reset database setup status to initial state."""
    global _database_setup_status
    _database_setup_status.update(
        {
            "running": False,
            "completed": False,
            "success": False,
            "error": None,
            "message": "",
            "started_at": None,
            "completed_at": None,
            "current_task": None,
            "completed_tasks": [],
        }
    )


def _reset_automode_status():
    """Reset automode status to initial state"""
    global _automode_status
    _automode_status.update(
        {
            "running": False,
            "completed": False,
            "success": False,
            "error": None,
            "message": "Ready to start automode",
            "started_at": None,
            "completed_at": None,
            "current_task": 1,
            "completed_tasks": [],
        }
    )


def _load_task1_completion_from_marker():
    """Load Task 1 completion state from marker file after database restart"""
    try:
        import os
        import json
        from django.conf import settings
        from django.utils import timezone
        from pybirdai.models.workflow_model import WorkflowTaskExecution

        base_dir = getattr(
            settings,
            "BASE_DIR",
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        task1_marker_path = os.path.join(base_dir, ".task1_completed_marker")

        if os.path.exists(task1_marker_path):
            with open(task1_marker_path, "r") as f:
                completion_data = json.load(f)

            # Create or update Task 1 execution record
            task1_execution, created = WorkflowTaskExecution.objects.get_or_create(
                task_number=1,
                operation_type='do',
                defaults={
                    'status': 'completed',
                    'started_at': timezone.now(),
                    'completed_at': completion_data.get('completed_at', timezone.now()),
                    'execution_data': completion_data.get('execution_data', {})
                }
            )
            if not created and task1_execution.status != 'completed':
                task1_execution.status = 'completed'
                task1_execution.completed_at = completion_data.get('completed_at', timezone.now())
                task1_execution.execution_data = completion_data.get('execution_data', {})
                task1_execution.save()

            # Clean up marker file
            os.remove(task1_marker_path)
            logger.info("Task 1 completion state loaded from marker file and saved to database")
            return True

    except Exception as e:
        logger.warning(f"Could not load Task 1 completion from marker: {e}")

    return False


def _run_database_setup_async():
    """Run database setup (Tasks 1-2) in background thread."""
    global _database_setup_status

    try:
        logger.info("Starting background database setup process...")
        _database_setup_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'error': None,
            'message': 'Starting database setup...',
            'started_at': time.time(),
            'current_task': 1,
            'completed_tasks': []
        })

        # Import necessary modules
        import json
        import os
        from django.conf import settings
        from pybirdai.models.workflow_model import AutomodeConfiguration
        from pybirdai.api.workflow_api import AutomodeConfigurationService

        # Task 1: Resource Download
        _database_setup_status['message'] = 'Running Task 1: Resource Download...'

        # Load configuration
        config_data = {}
        base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, 'automode_config.json')

        base_dir_str = str(base_dir)
        marker_path = os.path.join(base_dir_str, ".setup_ready_marker")
        if os.path.exists(marker_path):
            os.remove(marker_path)

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config_data = json.load(f)

        # Create config object
        config = AutomodeConfiguration(
            data_model_type=config_data.get("data_model_type", "EIL"),
            technical_export_source=config_data.get(
                "technical_export_source", "BIRD_WEBSITE"
            ),
            technical_export_github_url=config_data.get(
                "technical_export_github_url", ""
            ),
            config_files_source=config_data.get("config_files_source", "MANUAL"),
            config_files_github_url=config_data.get("config_files_github_url", ""),
            test_suite_source=config_data.get("test_suite_source", "GITHUB"),
            test_suite_github_url=config_data.get("test_suite_github_url", ""),
            when_to_stop=config_data.get("when_to_stop", "RESOURCE_DOWNLOAD"),
        )
        # Add github_branch as a dynamic attribute since it's not in the model
        config.github_branch = config_data.get("github_branch", "main")

        service = AutomodeConfigurationService()
        github_token = _get_github_token()

        # Only force refresh if explicitly needed - preserve existing files
        # This prevents unnecessary file deletion that can cause issues during model creation
        task1_results = service.fetch_files_from_source(
            config=config,
            github_token=github_token,
            force_refresh=False,  # Changed to False to preserve existing files
        )

        _database_setup_status['completed_tasks'].append('Task 1: Resource Download')
        logger.info("Task 1 completed successfully")

        # Task 2: Database Creation
        _database_setup_status.update({
            'current_task': 2,
            'message': 'Running  Artfacts Retrieval...'
        })

        from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
        app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')

        # This creates models and runs migrations
        db_results = app_config.run_automode_database_setup()

        # Additional cleanup - remove results admin.py if it exists to prevent future duplicates
        results_admin_path = os.path.join(
            base_dir, "results", "database_configuration_files", "admin.py"
        )
        try:
            if os.path.exists(results_admin_path):
                os.remove(results_admin_path)
                logger.info(f"Cleaned up results admin file: {results_admin_path}")
        except (OSError, PermissionError) as e:
            logger.warning(f"Could not clean up results admin file {results_admin_path}: {e}")

        # Check if restart is required (check both field names for compatibility)
        if db_results.get("requires_restart") or db_results.get(
            "server_restart_required"
        ):
            # First, update status to show completion BEFORE triggering restart
            _database_setup_status.update({
                'running': False,
                'completed': True,
                'success': True,
                'error': None,
                'message': 'Database setup completed. Server restart required for migrations.',
                'completed_at': time.time(),
                'completed_tasks': ['Task 1: Resource Download', 'Task 2: Database models created'],
                'server_restart_required': True,
                'restart_info': db_results.get('estimated_restart_time', 'Server will restart automatically')
            })

            # Give frontend time to poll and get the completion status before restart
            logger.info("Database setup completed - waiting briefly for frontend to receive status...")
            logger.info("Status updated with server_restart_required=True. Frontend should detect this in next poll.")

            # Wait time to ensure frontend gets the status before restart
            # Server restart takes ~5 seconds, so we wait 4 seconds before triggering it
            # restart_delay = 10
            # for i in range(restart_delay):
            #     time.sleep(1)
            #     logger.info(f"Waiting {i+1}/{restart_delay} seconds before triggering restart...")



            # Create marker file FIRST (before restart) so it exists when page refreshes
            marker_path = os.path.join(base_dir, ".migration_ready_marker")
            with open(marker_path, "w") as f:
                f.write("ready")
            logger.info(f"Created migration ready marker at: {marker_path}")

            # Now trigger the file operations that will cause Django restart
            logger.info(
                "Now triggering post-setup operations that will cause Django restart..."
            )
            try:
                from pybirdai.entry_points.automode_database_setup import (
                    RunAutomodeDatabaseSetup,
                )

                app_config = RunAutomodeDatabaseSetup("pybirdai", "birds_nest")
                app_config.run_post_setup_operations()
                logger.info(
                    "Post-setup operations completed - Django should restart now."
                )
            except Exception as e:
                logger.error(f"Post-setup operations failed: {e}")
                # Continue anyway - the main setup was successful

            # Add final log message that frontend can detect
            logger.warning("The restart process has been initiated. Please wait for the server to come back online.")

            time.sleep(10)

            os._exit(0)
        else:
            # No restart required, setup is complete
            _database_setup_status.update({
                'running': False,
                'completed': True,
                'success': True,
                'error': None,
                'message': 'Database setup completed successfully!',
                'completed_at': time.time(),
                'completed_tasks': ['Task 1: Resource Download', 'Task 2: Database Creation']
            })

        logger.info("Background database setup process completed successfully")

    except Exception as e:
        logger.error(f"Background database setup process failed: {e}")
        _database_setup_status.update(
            {
                "running": False,
                "completed": True,
                "success": False,
                "error": "Database setup error occurred",
                "message": f"Database setup failed at Task {_database_setup_status.get('current_task', '?')}",
                "completed_at": time.time(),
            }
        )


def _run_automode_async(target_task, session_data):
    """Run automode (Tasks 1-4) in background thread."""
    global _automode_status

    try:
        logger.info(f"Starting background automode process up to task {target_task}...")
        _automode_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'error': None,
            'message': f'Starting automode from Task 1 to Task {target_task}...',
            'started_at': time.time(),
            'current_task': 3,
            'target_task': target_task,
            'completed_tasks': [],
            'task_errors': []
        })

        # Map task numbers to their handler functions
        task_handlers = {
            1: task1_smcubes_core,
            2: task2_smcubes_rules,
            3: task3_python_rules,
            4: task4_full_execution,
        }

        # Create a mock request object
        class MockRequest:
            def __init__(self, method="POST", post_data=None):
                self.method = method
                self.POST = post_data or {}
                self.session = session_data
                self.user = None
                self.META = {}
                self.headers = {'X-Requested-With': 'XMLHttpRequest'}

        # Execute tasks sequentially


        for task_num in range(1, target_task + 1):

            try:
                _automode_status.update({
                    'current_task': task_num,
                    'message': f'Running Task {task_num}...'
                })

                # Get or create task execution record (if database is available)
                task_execution = None
                workflow_session = None

                try:
                    from django.db import connection

                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")  # Test database connection

                    # Database is available, create records
                    task_execution, _ = WorkflowTaskExecution.objects.get_or_create(
                        task_number=task_num, operation_type="do"
                    )

                    session_id = session_data.get('workflow_session_id')
                    if session_id:
                        workflow_session = WorkflowSession.objects.filter(
                            session_id=session_id
                        ).first()
                except Exception as db_error:
                    logger.warning(
                        f"Database not available for task {task_num}: {db_error}"
                    )
                    # Continue without database records

                # Get the appropriate task handler
                handler = task_handlers.get(task_num)
                if not handler:
                    raise Exception(f"No handler found for task {task_num}")

                # Create mock request
                mock_request = MockRequest(post_data={})

                # Call the handler
                logger.info(f"Executing handler for task {task_num}")
                result = handler(mock_request, 'do', task_execution, workflow_session)

                # Check if it's a JsonResponse indicating success
                if hasattr(result, "content"):
                    import json

                    response_data = json.loads(result.content)
                    if response_data.get("success"):
                        _automode_status["completed_tasks"].append(f"Task {task_num}")
                        logger.info(f"Task {task_num} completed successfully")
                    else:
                        raise Exception(
                            response_data.get("message", f"Task {task_num} failed")
                        )
                else:
                    # If no JsonResponse, assume success
                    _automode_status["completed_tasks"].append(f"Task {task_num}")
                    logger.info(f"Task {task_num} completed")

            except Exception as task_error:
                logger.error(f"Task {task_num} failed: {task_error}")
                _automode_status["task_errors"].append(
                    {"task": task_num, "error": str(task_error)}
                )


        # Update final status
        if _automode_status["task_errors"]:
            _automode_status.update(
                {
                    "running": False,
                    "completed": True,
                    "success": False,
                    "error": f"Some tasks failed: {len(_automode_status['task_errors'])} errors",
                    "message": f"Automode completed with errors. Successfully completed: {', '.join(_automode_status['completed_tasks'])}",
                    "completed_at": time.time(),
                }
            )
        else:
            _automode_status.update({
                'running': False,
                'completed': True,
                'success': True,
                'error': None,
                'message': f"Automode completed successfully! Tasks completed: {', '.join(_automode_status['completed_tasks'])}",
                'completed_at': time.time()
            })

        logger.info("Background automode process completed")

    except Exception as e:
        logger.error(f"Background automode process failed: {e}")
        _automode_status.update(
            {
                "running": False,
                "completed": True,
                "success": False,
                "error": "Automode execution error occurred",
                "message": f"Automode failed at Task {_automode_status.get('current_task', '?')}",
                "completed_at": time.time(),
            }
        )


def get_dpm_task_grid(session):
    """Get DPM process status grid"""
    if not session:
        return []

    dpm_steps = [
        (1, 'Extract DPM Metadata'),
        (2, 'Process & Import Selected Tables'),
        (3, 'Create Output Layers'),
        (4, 'Create Transformation Rules'),
        (5, 'Generate Python Code'),
        (6, 'Execute DPM Tests'),
    ]

    grid = []
    for step_number, step_name in dpm_steps:
        try:
            execution = DPMProcessExecution.objects.get(
                session=session,
                step_number=step_number
            )
            status = execution.status
        except DPMProcessExecution.DoesNotExist:
            # Create default pending entry
            execution = DPMProcessExecution.objects.create(
                session=session,
                step_number=step_number,
                step_name=step_name,
                status='pending'
            )
            status = 'pending'

        grid.append({
            'step_number': step_number,
            'step_name': step_name,
            'status': status,
            'started_at': execution.started_at if execution else None,
            'completed_at': execution.completed_at if execution else None,
        })

    return grid


def get_ancrdt_task_grid(session):
    """Get AnaCredit process status grid"""
    if not session:
        return []

    ancrdt_steps = [
        (0, 'Fetch Metadata CSV'),
        (1, 'Import Metadata'),
        (2, 'Create Joins Metadata'),
        (3, 'Create Executable Joins'),
        (4, 'Full Execution with Test Suite'),
        (5, 'Execute Tables'),
    ]

    grid = []
    for step_number, step_name in ancrdt_steps:
        try:
            execution = AnaCreditProcessExecution.objects.get(
                session=session,
                step_number=step_number
            )
            status = execution.status
        except AnaCreditProcessExecution.DoesNotExist:
            # Create default pending entry
            execution = AnaCreditProcessExecution.objects.create(
                session=session,
                step_number=step_number,
                step_name=step_name,
                status='pending'
            )
            status = 'pending'

        grid.append({
            'step_number': step_number,
            'step_name': step_name,
            'status': status,
            'started_at': execution.started_at if execution else None,
            'completed_at': execution.completed_at if execution else None,
        })

    return grid


def get_workflow_progress_summary(session):
    """Get progress summary for all workflows"""
    if not session:
        return {
            'main': {'completed': 0, 'total': 4, 'active': False, 'current': 0},
            'dpm': {'completed': 0, 'total': 3, 'active': False, 'current': 0},
            'ancrdt': {'completed': 0, 'total': 5, 'active': False, 'current': 0},
        }

    # Main workflow progress
    main_completed = WorkflowTaskExecution.objects.filter(
        operation_type='do',
        status='completed'
    ).count()
    main_active = WorkflowTaskExecution.objects.filter(
        operation_type='do',
        status='running'
    ).exists()
    main_current = WorkflowTaskExecution.objects.filter(
        operation_type='do',
        status='running'
    ).first()
    main_current_num = main_current.task_number if main_current else 0

    # DPM workflow progress
    dpm_completed = DPMProcessExecution.objects.filter(
        session=session,
        status='completed'
    ).count()
    dpm_active = DPMProcessExecution.objects.filter(
        session=session,
        status='running'
    ).exists()
    dpm_current = DPMProcessExecution.objects.filter(
        session=session,
        status='running'
    ).first()
    dpm_current_num = dpm_current.step_number if dpm_current else 0

    # AnaCredit workflow progress
    ancrdt_completed = AnaCreditProcessExecution.objects.filter(
        session=session,
        status='completed'
    ).count()
    ancrdt_active = AnaCreditProcessExecution.objects.filter(
        session=session,
        status='running'
    ).exists()
    ancrdt_current = AnaCreditProcessExecution.objects.filter(
        session=session,
        status='running'
    ).first()
    ancrdt_current_num = ancrdt_current.step_number if ancrdt_current else 0

    return {
        'main': {
            'completed': main_completed,
            'total': 4,
            'active': main_active,
            'current': main_current_num,
        },
        'dpm': {
            'completed': dpm_completed,
            'total': 3,
            'active': dpm_active,
            'current': dpm_current_num,
        },
        'ancrdt': {
            'completed': ancrdt_completed,
            'total': 4,
            'active': ancrdt_active,
            'current': ancrdt_current_num,
        },
    }


def workflow_dashboard(request):
    """Main dashboard showing all tasks and their status"""
    import json
    import os
    from django.conf import settings
    from django.db import connection
    from django.db.utils import OperationalError, ProgrammingError

    if not os.path.exists("automode_config.json"):
        # Auto-create config file silently if it doesn't exist
        with open("automode_config.json", "w") as f:
            f.write("""{
              "data_model_type": "EIL",
              "clone_mode": "false",
              "technical_export_source": "GITHUB",
              "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD_IL",
              "config_files_source": "GITHUB",
              "config_files_github_url": "https://github.com/regcommunity/FreeBIRD_IL",
              "test_suite_source": "GITHUB",
              "test_suite_github_url": " https://github.com/regcommunity/bird-default-test-suite",
              "github_branch": "main",
              "when_to_stop": "RESOURCE_DOWNLOAD",
              "enable_lineage_tracking": true
            }""")

    # Check if database tables exist
    database_ready = False
    workflow_session = None
    session_id = None

    try:
        # Try to access session data only if database is available
        with connection.cursor() as cursor:
            # Check if django_session table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='django_session';"
            )
            session_table_exists = cursor.fetchone() is not None

            # Check if WorkflowSession table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='pybirdai_workflowsession';"
            )
            workflow_table_exists = cursor.fetchone() is not None

        if session_table_exists and workflow_table_exists:
            database_ready = True
            session_id = request.session.get('workflow_session_id')

            if not session_id:
                session_id = str(uuid.uuid4())
                request.session["workflow_session_id"] = session_id
                workflow_session = WorkflowSession.objects.create(session_id=session_id)
            else:
                workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)

    except (OperationalError, ProgrammingError):
        # Database doesn't exist or tables don't exist - this is OK
        database_ready = False

    # Load configuration from temporary file
    config = {}
    github_token = ""
    migration_ready = False
    try:
        base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, 'automode_config.json')

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = json.load(f)
                # Remove github_token from config if it exists (should not be persisted)
                github_token_existed = config.pop('github_token', None) is not None

                # If we removed a github_token, save the cleaned config back to file
                if github_token_existed:
                    with open(config_path, "w") as f:
                        json.dump(config, f, indent=2)
                    logger.info("Removed GitHub token from persistent config file for security")

        # Get GitHub token from in-memory storage or environment variable
        github_token = _get_github_token()

        # Check if we're waiting for step 2 migrations
        marker_path = os.path.join(base_dir, ".migration_ready_marker")
        migration_ready = os.path.exists(marker_path)

        setup_marker_path = os.path.join(base_dir, '.setup_ready_marker')
        setup_ready = os.path.exists(setup_marker_path)

    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        # Use defaults if config cannot be loaded
        config = {
            "data_model_type": "EIL",
            "clone_mode": "false",
            "technical_export_source": "BIRD_WEBSITE",
            "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD_IL",
            "config_files_source": "MANUAL",
            "config_files_github_url": "",
            "test_suite_source": "GITHUB",
            "test_suite_github_url": "",
            "github_branch": "main",
            "when_to_stop": "RESOURCE_DOWNLOAD",
            "enable_lineage_tracking": True,
        }

    # Create context - handle missing database gracefully
    context = {
        'config': config,
        'github_token': github_token,
        'database_ready': database_ready,
        'migration_ready': migration_ready,
        'setup_ready': setup_ready,
    }

    if database_ready and workflow_session:
        try:
            # Load Task 1 completion state from marker file if it exists
            _load_task1_completion_from_marker()

            # Only include database-dependent data if database is available
            context.update(
                {
                    "workflow_session": workflow_session,
                    "task_grid": workflow_session.get_task_status_grid(),
                    "progress": workflow_session.get_progress_percentage(),
                    "dpm_task_grid": get_dpm_task_grid(workflow_session),
                    "ancrdt_task_grid": get_ancrdt_task_grid(workflow_session),
                    "workflow_summaries": get_workflow_progress_summary(workflow_session),
                }
            )

            refresh_complete_status()
        except:
            context.update({
                'workflow_session': None,
                'task_grid': [],
                'progress': 0,
                'session_id': session_id or 'no-database',
                'dpm_task_grid': [],
                'ancrdt_task_grid': [],
                'workflow_summaries': {
                    'main': {'completed': 0, 'total': 4, 'active': False, 'current': 0},
                    'dpm': {'completed': 0, 'total': 3, 'active': False, 'current': 0},
                    'ancrdt': {'completed': 0, 'total': 5, 'active': False, 'current': 0},
                },
            })
    else:
        # Provide default data when no database is available
        context.update({
            'workflow_session': None,
            'task_grid': [],
            'progress': 0,
            'session_id': session_id or 'no-database',
            'dpm_task_grid': [],
            'ancrdt_task_grid': [],
            'workflow_summaries': {
                'main': {'completed': 0, 'total': 4, 'active': False, 'current': 0},
                'dpm': {'completed': 0, 'total': 3, 'active': False, 'current': 0},
                'ancrdt': {'completed': 0, 'total': 5, 'active': False, 'current': 0},
            },
        })




    return render(request, 'pybirdai/workflow/dashboard.html', context)



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

            try:
                # Import real entry point modules (with correct class names)
                from pybirdai.entry_points.convert_ldm_to_sdd_hierarchies import RunConvertLDMToSDDHierarchies
                from pybirdai.entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
                from pybirdai.entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
                from pybirdai.entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
                from pybirdai.entry_points.import_input_model import RunImportInputModelFromSQLDev
                from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

                execution_data = {
                    "database_deleted": False,
                    "hierarchy_analysis_imported": False,
                    "semantic_integrations_processed": False,
                    "input_model_imported": False,
                    "report_templates_created": False,
                }

                # Execute subtasks based on selections or run all by default
                run_all = not any([
                    request.POST.get('delete_database'),
                    request.POST.get('import_input_model'),
                    request.POST.get('generate_templates'),
                    request.POST.get('import_hierarchy_analysis'),
                    request.POST.get('process_semantic'),

                ])


                # Delete database if requested (should run first)
                if request.POST.get("delete_database") or run_all:
                    logger.info("Deleting existing database...")
                    app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
                    app_config.run_delete_bird_metadata_database()
                    execution_data['database_deleted'] = True

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

                steps_completed = sum([_ for _ in execution_data.values() if isinstance(_,bool)])

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
                "pybirdai/workflow/task1/do.html",
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

        return render(request, 'pybirdai/workflow/task1/review.html', {
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
            return render(request, 'pybirdai/workflow/task2/do.html', {
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

        return render(request, 'pybirdai/workflow/task2/review.html', {
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

                execution_data = {
                    'current_phase': 'filters',
                    'filter_code_generated': False,
                    'join_code_generated': False,
                    'transformation_code_generated': False,
                    'steps_completed': []
                }

                # Execute Python code generation steps
                run_all = not any([
                    request.POST.get('generate_filter_code'),
                    request.POST.get('generate_join_code'),
                ])

                # Generate executable filter code
                if request.POST.get('generate_filter_code') or run_all:
                    logger.info("Generating executable filter Python code...")
                    execution_data['current_phase'] = 'filters'
                    RunCreateExecutableFilters.run_create_executable_filters_from_db()
                    execution_data['filter_code_generated'] = True
                    execution_data['steps_completed'].append('Executable filter code generation')

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
                    logger.info("Generating executable filter Python code...")
                    execution_data["current_phase"] = "filters"
                    RunCreateExecutableFilters.run_create_executable_filters_from_db()
                    execution_data["filter_code_generated"] = True
                    execution_data["steps_completed"].append(
                        "Executable filter code generation"
                    )

                # Note: Join and transformation code generation would use different entry points
                # For now, marking as completed to indicate the workflow step is done
                if request.POST.get("generate_join_code") or run_all:
                    logger.info("Join code generation (using filter infrastructure)...")
                    execution_data["current_phase"] = "joins"
                    RunCreateExecutableJoins.create_python_joins_from_db()  # Correct method name
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
            return render(request, 'pybirdai/workflow/task3/do.html', {
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
        filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
        finrep_files = [os.path.basename(f) for f in glob.glob(os.path.join(filter_code_dir, 'F_*.py'))]
        finrep_files.sort()  # Sort alphabetically for consistency
        encoded_files = encode_file_list(finrep_files)

        return render(request, 'pybirdai/workflow/task3/review.html', {
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

                    # Auto-discover test suites in tests/ directory
                    tests_dir = 'tests'
                    test_suites = []

                    if os.path.exists(tests_dir):
                        for entry in os.listdir(tests_dir):
                            suite_path = os.path.join(tests_dir, entry)
                            # Check if this is a directory and contains a configuration file
                            if os.path.isdir(suite_path):
                                config_file_path = os.path.join(suite_path, 'configuration_file_tests.json')
                                if os.path.exists(config_file_path):
                                    test_suites.append({
                                        'name': entry,
                                        'config_path': config_file_path
                                    })
                                    logger.info(f"Discovered test suite: {entry}")

                    if not test_suites:
                        logger.error("No test suites found in tests/ directory")
                        raise Exception("No test suites found in tests/ directory")

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
            return render(request, 'pybirdai/workflow/task4/do.html', {
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

        return render(request, 'pybirdai/workflow/task4/review.html', {
            'task_execution': task_execution,
            'workflow_session': workflow_session,
            'execution_data': execution_data,
            'test_results': test_results,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'grouped_results': grouped_results,
        })




@require_http_methods(["POST"])
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


def _execute_task2_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 2: Database Creation"""

    if substep_name == 'start':
        try:
            from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
            app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')
            results = app_config.run_automode_database_setup()

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
            from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
            app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')
            app_config.run_post_setup_operations()

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
        from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        success_message = ''

        if substep_name == 'delete_database':
            logger.info("Executing delete database substep via old AJAX method...")
            logger.warning("Note: Consider using the new loading-based endpoint for better user experience")
            app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
            app_config.run_delete_bird_metadata_database()
            execution_data['database_deleted'] = True
            execution_data['steps_completed'].append('Database deletion')
            success_message = 'Database deleted successfully'

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
        #
        all_subtasks_completed = (
            execution_data.get('database_deleted', False) and
            execution_data.get('input_model_imported', False) and
            execution_data.get('report_templates_created', False) and
            execution_data.get('hierarchy_analysis_imported', False) and
            execution_data.get('semantic_integrations_processed', False)
        )

        any_subtasks_completed = (
            execution_data.get('database_deleted', False) or
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


def _discover_test_suites() -> list:
    """
    Discover test suites in the tests/ directory.
    Looks for directories containing suite_manifest.yaml files.

    Returns:
        List of test suite directory names
    """
    test_suites = []
    tests_dir = "tests"

    # Scan for directories with suite_manifest.yaml
    for item in os.listdir(tests_dir):
        item_path = os.path.join(tests_dir, item)

        # Check if it's a directory
        if not os.path.isdir(item_path):
            continue

        # Check for suite_manifest.yaml
        manifest_bool = os.path.exists(os.path.join(item_path, "suite_manifest.json")) or os.path.exists(os.path.join(item_path, "suite_manifest.yaml"))
        if manifest_bool:
            test_suites.append(item)
            logger.info(f"Discovered test suite: {item}")

    return test_suites


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



@require_http_methods(["POST"])
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


def _execute_task2_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 2: Database Creation"""

    if substep_name == 'start':
        try:
            from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
            app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')
            results = app_config.run_automode_database_setup()

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
            from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
            app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')
            app_config.run_post_setup_operations()

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
        from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        success_message = ''

        if substep_name == 'delete_database':
            logger.info("Executing delete database substep via old AJAX method...")
            logger.warning("Note: Consider using the new loading-based endpoint for better user experience")
            app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
            app_config.run_delete_bird_metadata_database()
            execution_data['database_deleted'] = True
            execution_data['steps_completed'].append('Database deletion')
            success_message = 'Database deleted successfully'

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
        #
        all_subtasks_completed = (
            execution_data.get('database_deleted', False) and
            execution_data.get('input_model_imported', False) and
            execution_data.get('report_templates_created', False) and
            execution_data.get('hierarchy_analysis_imported', False) and
            execution_data.get('semantic_integrations_processed', False)
        )

        any_subtasks_completed = (
            execution_data.get('database_deleted', False) or
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
            'message': 'An internal error has occurred.'
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


@require_http_methods(["POST"])
def workflow_automode(request):
    """Start automode tasks 1-4 in background thread"""
    global _automode_status
    from django.db import connection
    from django.db.utils import OperationalError, ProgrammingError

    # Check if automode is already running
    if _automode_status['running']:
        return JsonResponse({
            'success': False,
            'message': 'Automode is already running. Please wait for completion.',
            'status': 'already_running'
        })

    # Check if automode was recently completed
    if _automode_status["completed"]:
        # Reset status for new run
        _reset_automode_status()

    target_task = int(request.POST.get('target_task', 4))

    # Ensure target task is at least 1 since we start from Task 1
    if target_task < 1:
        return JsonResponse({
            'success': False,
            'message': 'Target task must be 1 or higher',
            'status': 'invalid_target'
        }, status=400)

    # Check if database is available (required for automode tasks 1-4)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='pybirdai_workflowsession';"
            )
            if not cursor.fetchone():
                return JsonResponse(
                    {
                        "success": False,
                        "message": 'Database not available. Please run Retrieve Artifacts and Setup Database first.',
                        "status": "database_missing",
                    },
                    status=400,
                )
    except (OperationalError, ProgrammingError):
        return JsonResponse({
            'success': False,
            'message': 'Database not available. Please run Retrieve Artifacts and Setup Database first.',
            'status': 'database_error'
        }, status=400)

    # Copy session data for background thread
    session_data = dict(request.session)

    try:
        # Start automode in background thread
        automode_thread = threading.Thread(
            target=_run_automode_async,
            args=(target_task, session_data),
            daemon=True
        )
        automode_thread.start()

        return JsonResponse({
            'success': True,
            'message': f'Automode started in background (tasks 1 to {target_task}). Use /workflow/automode-status/ to check progress.',
            'status': 'started',
            'check_status_url': '/pybirdai/workflow/automode-status/'
        })

    except Exception as e:
        logger.error(f"Failed to start automode thread: {e}")
        return JsonResponse(
            {
                "success": False,
                "message": f"Failed to start automode: {str(e)}",
                "status": "failed",
            },
            status=500,
        )


@require_http_methods(["GET"])
def workflow_automode_status(request):
    """Check the status of running automode"""
    global _automode_status

    status_copy = _automode_status.copy()

    # Calculate elapsed time if running
    if status_copy['running'] and status_copy['started_at']:
        status_copy['elapsed_time'] = time.time() - status_copy['started_at']
    elif status_copy['completed'] and status_copy['started_at'] and status_copy['completed_at']:
        status_copy['elapsed_time'] = status_copy['completed_at'] - status_copy['started_at']

    return JsonResponse({
        'success': True,
        'automode_status': status_copy
    })

def workflow_task_status(request, task_number):
    """Get current status of a specific task"""
    task_executions = WorkflowTaskExecution.objects.filter(task_number=task_number)

    status_data = {}
    for execution in task_executions:

        status_data[execution.operation_type] = {
            "status": execution.status,
            "started_at": execution.started_at.isoformat()
            if execution.started_at
            else None,
            "completed_at": execution.completed_at.isoformat()
            if execution.completed_at
            else None,
            "error_message": execution.error_message,
        }

    return JsonResponse(status_data)


@require_http_methods(["POST"])
def workflow_save_config(request):
    """Save workflow configuration to temporary file"""
    import json
    import os
    from django.conf import settings

    try:
        # Get configuration data from request
        technical_export_github_url = request.POST.get("technical_export_github_url", "")

        config_data = {
            "data_model_type": request.POST.get("data_model_type", "EIL"),
            "clone_mode": request.POST.get("clone_mode", "false"),
            "technical_export_source": request.POST.get(
                "technical_export_source", "BIRD_WEBSITE"
            ),
            "technical_export_github_url": technical_export_github_url,
            "config_files_source": "GITHUB",  # Always use GitHub
            "config_files_github_url": technical_export_github_url,  # Always use same URL as BIRD Content Repository
            "test_suite_source": "GITHUB",  # Always use GitHub
            "test_suite_github_url": request.POST.get("test_suite_github_url", ""),
            "bird_content_branch": request.POST.get("bird_content_branch", "main"),
            "test_suite_branch": request.POST.get("test_suite_branch", "main"),
            "github_branch": request.POST.get("bird_content_branch", "main"),  # Keep for backwards compatibility
            "when_to_stop": "RESOURCE_DOWNLOAD",  # Default for workflow
            "enable_lineage_tracking": request.POST.get("enable_lineage_tracking") == "true",
        }

        # Store GitHub token in memory only, don't persist to file
        github_token = request.POST.get("github_token", "")
        if github_token:
            # Store in module-level variable for in-memory use (no database required)
            _set_github_token(github_token)

        # Save to temporary file (reuse the automode config file)
        # Note: GitHub token is NOT persisted to file for security
        base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, 'automode_config.json')

        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)

        # Delete the migration ready marker file since configuration has changed
        # User will need to run database setup again with the new configuration
        """
        marker_path = os.path.join(base_dir, '.migration_ready_marker')
        marker_removed = False
        try:
            if os.path.exists(marker_path):
                os.remove(marker_path)
                marker_removed = True
                logger.info("Removed migration ready marker due to configuration change")

                # Also reset any in-memory status that might be stale
                global _database_setup_status, _migration_status
                _reset_database_setup_status()
                _reset_migration_status()
                logger.info("Reset workflow status due to configuration change")


        except (OSError, PermissionError) as e:
            logger.warning(f"Could not remove migration ready marker: {e}")
            # Don't fail the config save for this
            """
        # Provide appropriate success message
        marker_removed = None
        message = 'Configuration saved successfully'
        if marker_removed:
            message += '. Previous database setup status reset - you may need to run database setup again.'

        return JsonResponse({
            'success': True,
            'message': message
        })

    except Exception as e:
        logger.error(f"Error saving workflow configuration: {str(e)}")
        return JsonResponse({"success": False, "error": "Download preparation failed. Please check system logs."}, status=500)


@require_http_methods(["POST"])
def workflow_run_migrations(request):
    """Start Migratioin step: Database migrations in background thread"""
    global _migration_status

    # Check if migrations are already running
    if _migration_status['running']:
        return JsonResponse({
            'success': False,
            'message': 'Migrations are already running. Please wait for completion.',
            'status': 'already_running'
        })

    # Check if migrations were recently completed
    if _migration_status["completed"]:
        # Reset status for new run
        _reset_migration_status()

    try:
        # Start migrations in background thread
        migration_thread = threading.Thread(target=_run_migrations_async, daemon=True)
        migration_thread.start()

        return JsonResponse({
            'success': True,
            'message': 'Database migrations started in background. Use /workflow/migration-status/ to check progress.',
            'status': 'started',
            'check_status_url': '/pybirdai/workflow/migration-status/'
        })

    except Exception as e:
        logger.error(f"Failed to start migration thread: {e}")
        return JsonResponse(
            {
                "success": False,
                "message": f"Failed to start migrations: {str(e)}",
                "status": "failed",
            },
            status=500,
        )


@require_http_methods(["GET"])
def workflow_migration_status(request):
    """Check the status of running migrations"""
    global _migration_status

    status_copy = _migration_status.copy()

    # Calculate elapsed time if running
    if status_copy['running'] and status_copy['started_at']:
        status_copy['elapsed_time'] = time.time() - status_copy['started_at']
    elif status_copy['completed'] and status_copy['started_at'] and status_copy['completed_at']:
        status_copy['elapsed_time'] = status_copy['completed_at'] - status_copy['started_at']

    return JsonResponse({
        'success': True,
        'migration_status': status_copy
    })


@require_http_methods(["POST"])
def workflow_database_setup(request):
    """Start tasks for  database setup in background thread"""
    global _database_setup_status

    # Check if setup is already running
    if _database_setup_status['running']:
        return JsonResponse({
            'success': False,
            'message': 'Database setup is already running. Please wait for completion.',
            'status': 'already_running'
        })

    # Check if setup was recently completed
    if _database_setup_status["completed"]:
        # Reset status for new run
        _reset_database_setup_status()

    # Check configuration exists
    import json
    import os
    from django.conf import settings

    try:
        base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, 'automode_config.json')

        if not os.path.exists(config_path):
            return JsonResponse(
                {
                    "success": False,
                    "message": "Configuration not found. Please save configuration first.",
                    "status": "config_missing",
                },
                status=400,
            )
    except Exception as e:
        logger.error(f"Error checking configuration: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Error checking configuration: {str(e)}',
            'status': 'failed'
        }, status=500)

    try:
        # Start database setup in background thread
        setup_thread = threading.Thread(target=_run_database_setup_async, daemon=True)
        setup_thread.start()

        return JsonResponse({
            'success': True,
            'message': 'Database setup started in background. Use /workflow/database-setup-status/ to check progress.',
            'status': 'started',
            'check_status_url': '/pybirdai/workflow/database-setup-status/'
        })

    except Exception as e:
        logger.error(f"Failed to start database setup thread: {e}")
        return JsonResponse(
            {
                "success": False,
                "message": f"Failed to start database setup: {str(e)}",
                "status": "failed",
            },
            status=500,
        )


@require_http_methods(["GET"])
def workflow_database_setup_status(request):
    """Check the status of running database setup"""
    global _database_setup_status

    status_copy = _database_setup_status.copy()

    # Calculate elapsed time if running
    if status_copy['running'] and status_copy['started_at']:
        status_copy['elapsed_time'] = time.time() - status_copy['started_at']
    elif status_copy['completed'] and status_copy['started_at'] and status_copy['completed_at']:
        status_copy['elapsed_time'] = status_copy['completed_at'] - status_copy['started_at']

    return JsonResponse({
        'success': True,
        'database_setup_status': status_copy
    })


@require_http_methods(["POST"])
def workflow_clone_import(request):
    """Import CSV files from the technical_export directory"""
    import os
    import glob
    from django.conf import settings
    from django.db import connection
    from django.db.utils import OperationalError, ProgrammingError

    try:
        # Check if database is available
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except (OperationalError, ProgrammingError):
            return JsonResponse({
                'success': False,
                'message': 'Database not available. Please run database setup first.',
                'error': 'Database connection failed'
            }, status=400)

        # Get the base directory
        base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        technical_export_dir = os.path.join(base_dir, 'resources', 'technical_export')

        # Check if directory exists
        if not os.path.exists(technical_export_dir):
            return JsonResponse({
                'success': False,
                'message': 'Technical export directory not found',
                'error': f'Directory not found: {technical_export_dir}'
            }, status=400)

        # Get all CSV files in the directory
        csv_files = glob.glob(os.path.join(technical_export_dir, '*.csv'))

        if not csv_files:
            return JsonResponse({
                'success': False,
                'message': 'No CSV files found in technical_export directory',
                'error': 'No CSV files to import'
            }, status=400)

        # Read CSV files and prepare data for import
        csv_data = {}
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            try:
                with open(csv_file, "r", encoding="utf-8") as f:
                    csv_data[filename] = f.read()
            except Exception as e:
                logger.error(f"Error reading CSV file {filename}: {e}")
                # Continue with other files even if one fails

        if not csv_data:
            return JsonResponse({
                'success': False,
                'message': 'Could not read any CSV files',
                'error': 'Failed to read CSV files'
            }, status=500)

        # Import the CSV data using the existing import functionality
        try:
            from pybirdai.utils.clone_mode import import_from_metadata_export

            # Use ordered import to maintain ID mappings across files
            importer = import_from_metadata_export.CSVDataImporter()
            results = importer.import_from_csv_strings_ordered(csv_data)

            # Count successful imports
            successful_imports = sum(1 for result in results.values() if result.get('success', False))
            total_objects = sum(result.get('imported_count', 0) for result in results.values() if result.get('success', False))

            # Create summary message
            message = f'Successfully imported {successful_imports}/{len(results)} CSV files'
            details = f'Total objects imported: {total_objects}'

            # Check if all imports were successful
            all_successful = successful_imports == len(results)

            # Log any errors
            for filename, result in results.items():
                if not result.get('success', False):
                    logger.error(f"Failed to import {filename}: {result.get('error', 'Unknown error')}")

            # If clone was successful, mark tasks 1 and 2 as completed
            if all_successful:
                try:
                    # Mark Task 1 (SMCubes Core Creation) as completed
                    task1_do, created = WorkflowTaskExecution.objects.get_or_create(
                        task_number=1,
                        operation_type='do',
                        defaults={
                            'status': 'completed',
                            'started_at': timezone.now(),
                            'completed_at': timezone.now(),
                            'execution_data': {'source': 'clone_import'}
                        }
                    )
                    if not created and task1_do.status != 'completed':
                        task1_do.status = 'completed'
                        task1_do.completed_at = timezone.now()
                        task1_do.execution_data = {'source': 'clone_import'}
                        task1_do.save()

                    # Mark Task 2 (SMCubes Transformation Rules Creation) as completed
                    task2_do, created = WorkflowTaskExecution.objects.get_or_create(
                        task_number=2,
                        operation_type='do',
                        defaults={
                            'status': 'completed',
                            'started_at': timezone.now(),
                            'completed_at': timezone.now(),
                            'execution_data': {'source': 'clone_import'}
                        }
                    )
                    if not created and task2_do.status != 'completed':
                        task2_do.status = 'completed'
                        task2_do.completed_at = timezone.now()
                        task2_do.execution_data = {'source': 'clone_import'}
                        task2_do.save()

                    logger.info("Clone import completed: Tasks 1 and 2 marked as completed")
                    message += " (Tasks 1 & 2 marked as completed)"

                except Exception as e:
                    logger.error(f"Error marking tasks as completed after clone: {e}")
                    # Don't fail the whole operation if task marking fails

            return JsonResponse({
                'success': all_successful,
                'message': message,
                'details': details,
                'results': {
                    'successful_imports': successful_imports,
                    'total_files': len(results),
                    'total_objects': total_objects
                },
                'refresh_recommended': True
            })

        except Exception as e:
            logger.error(f"Error during CSV import: {e}")
            return JsonResponse({
                'success': False,
                'message': 'Failed to import CSV files',
                'error': str(e)
            }, status=500)

    except Exception as e:
        logger.error(f"Unexpected error in workflow_clone_import: {e}")
        return JsonResponse({
            'success': False,
            'message': 'An unexpected error occurred',
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def workflow_session_check(request):
    """
    Check if the current workflow session is valid and accessible.
    Used by frontend JavaScript to validate session state before page reloads.
    """
    try:
        # Check if session has workflow_session_id
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'message': 'No workflow session ID found'
            }, status=400)

        # Try to access the workflow session
        try:
            workflow_session = WorkflowSession.objects.get(session_id=session_id)
        except WorkflowSession.DoesNotExist:
            return JsonResponse({
                'success': False,
                'message': 'Workflow session not found in database'
            }, status=404)

        # Check if session is still active
        if not request.session.session_key:
            return JsonResponse({
                'success': False,
                'message': 'Django session expired'
            }, status=401)

        # All checks passed
        return JsonResponse({
            'success': True,
            'message': 'Session valid',
            'session_id': session_id,
            'current_task': workflow_session.current_task
        })

    except Exception as e:
        logger.error(f"Error in workflow_session_check: {str(e)}")
        return JsonResponse({
            'success': False,
            'message': 'Session validation error',
            'error': str(e)
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
        # Task 31 substeps
        'delete_database': 'Database Deletion',
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
            current_execution_data.get('database_deleted', False),
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



@require_http_methods(["POST"])
def workflow_reset_session_full(request):
    """
    Reset the entire workflow session (full reset).
    Removes all marker files and resets all tasks (1-4).
    """
    logger.info("Full workflow session reset requested")

    try:
        # Reset all internal status
        _reset_database_setup_status()
        _reset_migration_status()
        _reset_automode_status()

        # Get current session
        session_id = request.session.get('workflow_session_id')
        if session_id:
            try:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)
                workflow_session.current_task = 1
                workflow_session.updated_at = timezone.now()
                workflow_session.save()
                logger.info(f"Reset workflow session {session_id} current_task to 1")
            except WorkflowSession.DoesNotExist:
                logger.warning(f"Workflow session {session_id} not found during reset")

        # Delete all task executions
        deleted_count = WorkflowTaskExecution.objects.all().delete()[0]
        logger.info(f"Deleted {deleted_count} task executions")

        # Remove all marker files
        base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        marker_files = [
            '.setup_ready_marker',
            '.migration_ready_marker',
            '.task1_completed_marker',
            '.task2_completed_marker',
            '.task3_completed_marker',
            '.task4_completed_marker'
        ]

        removed_markers = []
        for marker_file in marker_files:
            marker_path = os.path.join(base_dir, marker_file)
            if os.path.exists(marker_path):
                try:
                    os.remove(marker_path)
                    removed_markers.append(marker_file)
                    logger.info(f"Removed marker file: {marker_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove marker file {marker_file}: {e}")

        # Remove temporary directories if they exist
        temp_dirs = [
            os.path.join(base_dir, 'results', 'generated_hierarchy_warnings', 'tmp'),
            os.path.join(base_dir, 'results', 'generated_html', 'tmp'),
            os.path.join(base_dir, 'results', 'generated_mapping_warnings', 'tmp'),
            os.path.join(base_dir, 'results', 'lineage', 'tmp'),
            os.path.join(base_dir, 'tests', 'test_results', 'json'),
            os.path.join(base_dir, 'tests', 'test_results', 'txt')
        ]

        removed_dirs = []
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                try:
                    import shutil
                    shutil.rmtree(temp_dir)
                    removed_dirs.append(temp_dir)
                    logger.info(f"Removed temporary directory: {temp_dir}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary directory {temp_dir}: {e}")

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': True,
                'message': 'Full workflow session reset completed successfully',
                'details': {
                    'removed_markers': removed_markers,
                    'removed_directories': removed_dirs,
                    'deleted_executions': deleted_count
                }
            })
        else:
            messages.success(request, 'Full workflow session reset completed successfully')
            return redirect('pybirdai:workflow_dashboard')

    except Exception as e:
        logger.error(f"Error during full workflow session reset: {e}")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': False,
                'message': 'Failed to reset full workflow session',
                'error': str(e)
            }, status=500)
        else:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'workflow session reset')
            return redirect('pybirdai:workflow_dashboard')


@require_http_methods(["POST"])
def workflow_reset_session_partial(request):
    """
    Reset workflow session from task 1 onwards (partial reset).

    """
    logger.info("Partial workflow session reset requested (tasks 1-4) but not database reset")

    try:
        # Reset only automode status (tasks 1-4)
        _reset_automode_status()

        # Get current session
        session_id = request.session.get('workflow_session_id')
        if session_id:
            try:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)
                workflow_session.current_task = 1
                workflow_session.updated_at = timezone.now()
                workflow_session.save()
                logger.info(f"Reset workflow session {session_id} current_task to 1")
            except WorkflowSession.DoesNotExist:
                logger.warning(f"Workflow session {session_id} not found during reset")

        # Delete only task executions for tasks 1-4
        deleted_count = WorkflowTaskExecution.objects.filter(
            task_number__in=[1, 2, 3, 4]
        ).delete()[0]
        logger.info(f"Deleted {deleted_count} task executions for tasks 1-4")

        # Remove only marker files for tasks 1-4
        base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        marker_files = [
            '.task1_completed_marker',
            '.task2_completed_marker',
            '.task3_completed_marker',
            '.task4_completed_marker'
        ]

        removed_markers = []
        for marker_file in marker_files:
            marker_path = os.path.join(base_dir, marker_file)
            if os.path.exists(marker_path):
                try:
                    os.remove(marker_path)
                    removed_markers.append(marker_file)
                    logger.info(f"Removed marker file: {marker_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove marker file {marker_file}: {e}")

        # Remove temporary directories if they exist
        temp_dirs = [
            os.path.join(base_dir, 'results', 'generated_hierarchy_warnings', 'tmp'),
            os.path.join(base_dir, 'results', 'generated_html', 'tmp'),
            os.path.join(base_dir, 'results', 'generated_mapping_warnings', 'tmp'),
            os.path.join(base_dir, 'results', 'lineage', 'tmp'),
            os.path.join(base_dir, 'tests', 'test_results', 'json'),
            os.path.join(base_dir, 'tests', 'test_results', 'txt')
        ]

        removed_dirs = []
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                try:
                    import shutil
                    shutil.rmtree(temp_dir)
                    removed_dirs.append(temp_dir)
                    logger.info(f"Removed temporary directory: {temp_dir}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary directory {temp_dir}: {e}")


        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': True,
                'message': 'Partial workflow session reset completed successfully (tasks 1-4)',
                'details': {
                    'removed_markers': removed_markers,
                    'removed_directories': removed_dirs,
                    'deleted_executions': deleted_count
                }
            })
        else:
            messages.success(request, 'Partial workflow session reset completed successfully (tasks 1-4)')
            return redirect('pybirdai:workflow_dashboard')

    except Exception as e:
        logger.error(f"Error during partial workflow session reset: {e}", exc_info=True)
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': False,
                'message': 'Failed to reset partial workflow session'
            }, status=500)
        else:
            messages.error(request, 'Failed to reset partial workflow session.')
            return redirect('pybirdai:workflow_dashboard')


def export_database_to_github(request):
    """
    Export database to GitHub repository using fork workflow with automatic branch creation and pull request.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'Only POST method allowed'})

    try:
        # Get form data
        github_token = request.POST.get('github_token', '').strip()
        repository_url = request.POST.get('repository_url', '').strip()
        organization = request.POST.get('organization', '').strip() or ""
        target_branch = request.POST.get('target_branch', 'develop').strip()
        use_fork_workflow = request.POST.get('use_fork_workflow') == 'on'

        if not github_token:
            return JsonResponse({'success': False, 'error': 'GitHub token is required'})

        # Validate repository URL format if provided
        if repository_url and not repository_url.startswith('https://github.com/'):
            return JsonResponse({'success': False, 'error': 'Repository URL must be a valid GitHub URL (https://github.com/...)'})

        # Import the GitHub integration service
        from pybirdai.api.workflow_api import GitHubIntegrationService

        # Create service instance
        github_service = GitHubIntegrationService(github_token)

        # Export database to CSV first
        from .views import _export_database_to_csv_logic
        zip_file_path, extract_dir = _export_database_to_csv_logic()

        # Determine repository URL (use automode config if not provided)
        if not repository_url:
            repository_url = github_service.get_github_url_from_automode_config() or 'https://github.com/regcommunity/FreeBIRD_IL'

        if use_fork_workflow:
            # Use new fork workflow (default behavior)
            result = github_service.fork_and_create_pr_workflow(
                source_repository_url=repository_url,
                organization=organization,
                csv_directory=extract_dir,
                target_branch=target_branch,
                pr_title="PyBIRD AI Database Export",
                pr_body="""## Database Export from PyBIRD AI

This pull request contains CSV files exported from the PyBIRD AI database using the fork workflow.

### Export Details:
- Generated automatically by PyBIRD AI's database export functionality
- Fork workflow ensures secure, isolated changes
- Files located in `export/database_export_ldm/`

### Testing:
- [ ] Verify CSV file integrity
- [ ] Check data completeness
- [ ] Validate against expected schema

This export was generated automatically by PyBIRD AI's fork workflow."""
            )

            # Prepare response data for fork workflow
            response_data = {
                'success': result['success'],
                'fork_created': result.get('fork_created', False),
                'branch_created': result.get('branch_created', False),
                'files_pushed': result.get('files_pushed', False),
                'pr_created': result.get('pr_created', False),
                'pull_request_url': result.get('pr_url'),
                'fork_url': result.get('fork_data', {}).get('html_url') if result.get('fork_data') else None,
                'message': 'Database exported via fork workflow successfully' if result['success'] else 'Fork workflow failed'
            }

            if not result['success']:
                response_data['error'] = result.get('error', 'Unknown error occurred during fork workflow')

        else:
            # Fallback to original workflow for backward compatibility
            result = github_service.export_and_push_to_github(repository_url=repository_url)

            response_data = {
                'success': result['success'],
                'branch_created': result.get('branch_created', False),
                'files_pushed': result.get('files_pushed', False),
                'pr_created': result.get('pr_created', False),
                'pull_request_url': result.get('pr_url'),
                'message': 'Database exported to GitHub successfully' if result['success'] else 'Direct push workflow failed'
            }

            if not result['success']:
                response_data['error'] = result.get('error', 'Unknown error occurred during GitHub export')

        return JsonResponse(response_data)

    except ImportError as e:
        return JsonResponse({
            'success': False,
            'error': f'GitHub integration service not available: {str(e)}'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error during GitHub export: {str(e)}'
        })


# DPM and AnaCredit Execution Endpoints

@require_http_methods(["POST"])
def execute_dpm_step(request, step_number):
    """Execute a DPM process step"""
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

        # Get or create DPM execution record
        dpm_execution, created = DPMProcessExecution.objects.get_or_create(
            session=workflow_session,
            step_number=step_number,
            defaults={
                'step_name': dict(DPMProcessExecution.STEP_CHOICES).get(step_number, f'Step {step_number}'),
                'status': 'pending'
            }
        )

        # Extract selected frameworks from request
        selected_frameworks = request.POST.getlist('frameworks')

        # Validate selected frameworks
        VALID_FRAMEWORKS = [
            'FINREP', 'COREP', 'AE', 'FP', 'SBP', 'REM', 'RES', 'PAY',
            'COVID19', 'IF', 'GSII', 'MREL', 'IMPRAC', 'ESG',
            'IPU', 'PILLAR3', 'IRRBB', 'DORA', 'FC', 'MICA'
        ]

        if selected_frameworks:
            # Check for invalid frameworks
            invalid_frameworks = [fw for fw in selected_frameworks if fw not in VALID_FRAMEWORKS]
            if invalid_frameworks:
                return JsonResponse({
                    'success': False,
                    'error': f'Invalid frameworks selected: {", ".join(invalid_frameworks)}. Valid frameworks are: {", ".join(VALID_FRAMEWORKS)}'
                })

            dpm_execution.selected_frameworks = selected_frameworks
            dpm_execution.save()

        # Mark as running
        dpm_execution.start_execution()

        # Execute the appropriate entry point based on step number
        # Wrap in try-finally to ensure status is ALWAYS updated, even if error handler fails
        try:
            if step_number == 1:
                # Step 1: Extract DPM Metadata (formerly Phase A)
                from pybirdai.entry_points.import_dpm_data import RunImportDPMData

                logger.info("Running DPM Step 1: Extracting metadata")

                result = RunImportDPMData.run_import_phase_a(frameworks=selected_frameworks or None)

                logger.info(f"Step 1 complete - {result.get('table_count', 0)} tables available for selection")

            elif step_number == 2:
                # Step 2: Process Selected Tables & Import (formerly Phase B + Step 2)
                # This step combines table processing (ordinate explosion) with database import
                from pybirdai.entry_points.import_dpm_data import RunImportDPMData
                import json

                # Check if selected_tables provided
                selected_tables_json = request.POST.get('selected_tables')
                selected_tables = json.loads(selected_tables_json) if selected_tables_json else None

                if not selected_tables:
                    # No tables selected - this should be caught by modal, but handle gracefully
                    return JsonResponse({
                        'success': False,
                        'error': 'No tables selected. Please select tables to process.'
                    })

                logger.info(f"Running DPM Step 2 with {len(selected_tables)} selected tables")
                dpm_execution.selected_tables = selected_tables
                dpm_execution.save()

                # Run Phase B: Process selected tables with ordinate explosion
                RunImportDPMData.run_import_phase_b(
                    selected_tables=selected_tables,
                    enable_table_duplication=True  # Enable Z-axis table duplication
                )

                logger.info("Table processing completed, database import already done in Phase B")

                # After importing DPM data, ensure Float subdomain exists for MTRC variable
                try:
                    from pybirdai.process_steps.output_layer_mapping_workflow.create_float_subdomain import (
                        ensure_float_subdomain_for_mtrc
                    )
                    float_result = ensure_float_subdomain_for_mtrc()
                    if float_result['success']:
                        logger.info(f"Float subdomain setup: {float_result['message']}")
                    else:
                        logger.warning(f"Float subdomain setup warning: {float_result['message']}")
                except Exception as e:
                    logger.warning(f"Float subdomain setup encountered an issue (non-critical): {str(e)}")

            elif step_number == 3:
                # Create Output Layers from DPM tables
                from pybirdai.entry_points.dpm_output_layer_creation import RunDPMOutputLayerCreation

                # Get optional parameters from request
                framework = request.POST.get('framework', '')
                version = request.POST.get('version', '')
                table_code = request.POST.get('table_code', '')
                table_codes = request.POST.get('table_codes', '')

                logger.info(f"Running output layer creation with params: framework={framework}, version={version}, table_code={table_code}, table_codes={table_codes}")

                # Call run_creation with parameters
                result = RunDPMOutputLayerCreation.run_creation(
                    framework=framework,
                    version=version,
                    table_code=table_code,
                    table_codes=table_codes
                )

                # Log results for multi-table processing
                if table_codes and isinstance(result, dict):
                    logger.info(f"Output layer creation result: {result.get('status')} - Processed: {len(result.get('processed', []))}, Errors: {len(result.get('errors', []))}")

                    # Check if there were errors
                    if result.get('status') == 'error' or (result.get('errors') and not result.get('processed')):
                        # All tables failed
                        error_details = result.get('errors', [])
                        error_msg = f"Failed to process {len(error_details)} table(s). "
                        if error_details:
                            first_error = error_details[0].get('error', 'Unknown error')
                            error_msg += f"First error: {first_error}"
                        elif result.get('message'):
                            error_msg = result.get('message')

                        logger.error(f"Output layer creation failed: {error_msg}")
                        dpm_execution.handle_error(error_msg)
                        return JsonResponse({
                            'success': False,
                            'status': 'failed',
                            'error': error_msg,
                            'details': error_details
                        })

                    elif result.get('status') == 'partial':
                        # Some tables succeeded, some failed
                        processed_count = len(result.get('processed', []))
                        error_count = len(result.get('errors', []))

                        logger.warning(f"Partial success: {processed_count} processed, {error_count} failed")
                        dpm_execution.complete_execution({
                            'completed_at': timezone.now().isoformat(),
                            'processed': processed_count,
                            'errors': error_count,
                            'details': result.get('errors', [])
                        })

                        return JsonResponse({
                            'success': True,
                            'status': 'partial',
                            'message': f'Processed {processed_count} table(s) successfully, {error_count} failed',
                            'details': {
                                'processed': processed_count,
                                'errors': error_count,
                                'error_list': result.get('errors', [])
                            }
                        })

            elif step_number == 4:
                # Create Transformation Rules - Generate filters and joins metadata for DPM output layers
                logger.info("DPM Step 4: Creating transformation rules (filters and joins metadata)...")

                from pybirdai.entry_points.create_filters import RunCreateFilters
                from pybirdai.entry_points.create_joins_metadata import RunCreateJoinsMetadata

                execution_data = {
                    'filters_created': False,
                    'joins_metadata_created': False,
                    'steps_completed': []
                }

                # Generate filters
                logger.info("Generating filters for DPM output layer cubes...")
                RunCreateFilters.run_create_filters()
                execution_data['filters_created'] = True
                execution_data['steps_completed'].append('Filters creation')

                # Create joins metadata
                logger.info("Creating joins metadata for DPM output layer cubes...")
                RunCreateJoinsMetadata.run_create_joins_meta_data()
                execution_data['joins_metadata_created'] = True
                execution_data['steps_completed'].append('Joins metadata creation')

                logger.info("DPM Step 4 completed: Transformation rules created successfully")

                # Store execution data before completing
                dpm_execution.execution_data = execution_data
                dpm_execution.save()

            elif step_number == 5:
                # Generate Python Code - Create executable Python transformations for DPM
                logger.info("DPM Step 5: Generating Python code (executable filters and joins)...")

                from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
                from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins

                execution_data = {
                    'filter_code_generated': False,
                    'join_code_generated': False,
                    'steps_completed': []
                }

                # Generate filter code
                logger.info("Generating executable filter Python code...")
                RunCreateExecutableFilters.run_create_executable_filters_from_db()
                execution_data['filter_code_generated'] = True
                execution_data['steps_completed'].append('Executable filter code generation')

                # Generate join code
                logger.info("Generating executable join Python code...")
                RunCreateExecutableJoins.run_create_executable_joins()
                execution_data['join_code_generated'] = True
                execution_data['steps_completed'].append('Executable join code generation')

                logger.info("DPM Step 5 completed: Python code generated successfully")

                # Store execution data before completing
                dpm_execution.execution_data = execution_data
                dpm_execution.save()

            elif step_number == 6:
                # Execute DPM Tests - Run DPM test suite and validate results
                logger.info("DPM Step 6: Executing DPM test suite...")

                from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

                execution_data = {
                    'tests_executed': False,
                    'steps_completed': []
                }

                # Look for DPM-specific test suite in tests/dpm/ directory
                tests_dir = 'tests/dpm'
                config_file_path = os.path.join(tests_dir, 'configuration_file_tests.json')

                if os.path.exists(config_file_path):
                    logger.info(f"Found DPM test suite configuration: {config_file_path}")

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
                    test_runner.args.framework = "FINREP"

                    # Execute tests
                    logger.info(f"Executing DPM tests from config: {config_file_path}")
                    test_runner.main()
                    logger.info("Completed DPM test suite")

                    execution_data['tests_executed'] = True
                    execution_data['steps_completed'].append('DPM test suite executed')
                else:
                    logger.warning(f"No DPM test configuration found at {config_file_path}")
                    logger.info("Step 6 completed without test execution (no test suite configured)")
                    execution_data['tests_executed'] = False
                    execution_data['steps_completed'].append('No test suite found - skipped')

                # Store execution data before completing
                dpm_execution.execution_data = execution_data
                dpm_execution.save()

                logger.info("DPM Step 6 completed")

            else:
                raise ValueError(f'Invalid DPM step number: {step_number}')

            # Mark as completed (only if no errors or not multi-table)
            dpm_execution.complete_execution({
                'completed_at': timezone.now().isoformat()
            })

            return JsonResponse({
                'success': True,
                'status': 'completed',
                'message': f'DPM Step {step_number} completed successfully'
            })

        except Exception as e:
            logger.error(f"DPM Step {step_number} execution failed: {e}")
            dpm_execution.handle_error(str(e))
            return JsonResponse({
                'success': False,
                'error': str(e),
                'status': 'failed'
            })
        finally:
            # Safety net: If task is still 'running' after all error handling,
            # force it to 'failed' to prevent stuck status
            # This ensures tasks can NEVER remain stuck in 'running' status
            dpm_execution.refresh_from_db()
            if dpm_execution.status == 'running':
                logger.error(f"DPM Step {step_number} was still 'running' after execution - forcing to 'failed'")
                dpm_execution.status = 'failed'
                if not dpm_execution.error_message:
                    dpm_execution.error_message = 'Execution interrupted unexpectedly'
                dpm_execution.completed_at = timezone.now()
                dpm_execution.save(update_fields=['status', 'error_message', 'completed_at'])

    except Exception as e:
        logger.error(f"Error executing DPM step: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })


@require_http_methods(["GET"])
def get_dpm_status(request):
    """Get DPM execution status"""
    try:
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session found'
            })

        workflow_session = get_object_or_404(WorkflowSession, session_id=session_id)
        dpm_grid = get_dpm_task_grid(workflow_session)

        return JsonResponse({
            'success': True,
            'dpm_status': dpm_grid
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        })


@require_http_methods(["POST"])
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


@require_http_methods(["GET"])
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


# Review Pages for DPM and AnaCredit

def get_cubes_for_dpm_step3():
    """
    Get all cubes generated from the output layer mapping workflow.
    Filter by the pattern: table_code_REF_version
    """
    from pybirdai.models.bird_meta_data_model import (
        CUBE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, MAPPING_TO_CUBE
    )

    # Find all cubes with '_REF_' pattern (from output layer mapping workflow)
    cubes = CUBE.objects.filter(
        cube_id__icontains='_REF_'
    ).select_related('cube_structure_id', 'framework_id')

    # Enrich with metadata
    cube_data = []
    for cube in cubes:
        # Get combination count
        combination_count = CUBE_TO_COMBINATION.objects.filter(
            cube_id=cube
        ).count()

        # Get structure item count
        item_count = 0
        if cube.cube_structure_id:
            item_count = CUBE_STRUCTURE_ITEM.objects.filter(
                cube_structure_id=cube.cube_structure_id
            ).count()

        cube_data.append({
            'cube_id': cube.cube_id,
            'cube_name': cube.name or cube.cube_id,
            'cube_code': cube.code,
            'structure_id': cube.cube_structure_id.cube_structure_id if cube.cube_structure_id else None,
            'structure_name': cube.cube_structure_id.name if cube.cube_structure_id else 'N/A',
            'framework': cube.framework_id.framework_id if cube.framework_id else 'N/A',
            'combination_count': combination_count,
            'item_count': item_count,
        })

    return cube_data


def api_dpm_cubes(request):
    """
    API endpoint to list Output Layer Mapping Workflow cubes only.
    Returns JSON array of cubes created via the Output Layer Mapping Workflow.
    Excludes ANCRDT cubes (which have their own viewer).
    """
    from pybirdai.models.bird_meta_data_model import CUBE
    from django.http import JsonResponse
    from django.db.models import Q

    try:
        # Get Output Layer Mapping Workflow cubes only
        # Pattern: {tablecode}_{framework}_{version}_CUBE (e.g., C_07_00_a_COREP_3_CUBE)
        # Also includes legacy: {table}_REF_CUBE_{timestamp}
        cubes = CUBE.objects.filter(
            cube_structure_id__isnull=False,  # Has a structure (actual output layer cube)
            cube_id__endswith='_CUBE'  # Output Layer Mapping Workflow pattern
        ).exclude(
            Q(framework_id__framework_id__icontains='ANCRDT') |  # Exclude ANCRDT cubes
            Q(cube_id='MAPPING_TO_CUBE')  # Exclude metadata cube
        ).select_related('cube_structure_id', 'framework_id').order_by('name')

        cube_list = []
        for cube in cubes:
            # Get structure item count
            item_count = 0
            if cube.cube_structure_id:
                from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM
                item_count = CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=cube.cube_structure_id
                ).count()

            cube_list.append({
                'cube_id': cube.cube_id,
                'name': cube.name or cube.cube_id,
                'code': cube.code,
                'structure_id': cube.cube_structure_id.cube_structure_id if cube.cube_structure_id else None,
                'structure_name': cube.cube_structure_id.name if cube.cube_structure_id else None,
                'item_count': item_count,
            })

        return JsonResponse({'cubes': cube_list})

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error fetching DPM cubes: {e}")
        return JsonResponse({'error': str(e)}, status=500)


def get_available_tables_for_selection(request):
    """
    API endpoint to get available tables for selection after Phase A.
    Reads table.csv and returns list of tables with metadata.
    """
    logger = logging.getLogger(__name__)

    try:
        import pandas as pd
        import os
        from django.conf import settings

        base_dir = settings.BASE_DIR
        table_csv_path = os.path.join(base_dir, 'results', 'technical_export', 'table.csv')

        if not os.path.exists(table_csv_path):
            return JsonResponse({
                'success': False,
                'error': 'table.csv not found. Please run Phase A first.'
            }, status=404)

        # Read table.csv
        tables_df = pd.read_csv(table_csv_path)

        # Convert to list of dictionaries
        tables = []
        for _, row in tables_df.iterrows():
            table_id = str(row.get('TABLE_ID', ''))
            # Use correct column names from table.csv: NAME and CODE (not TABLE_NAME and TABLE_CODE)
            table_name = str(row.get('NAME', ''))
            table_code = str(row.get('CODE', ''))
            version = str(row.get('VERSION', ''))
            description = str(row.get('DESCRIPTION', ''))

            # Extract framework from table_id (format: EBA_FRAMEWORK_CODE_VERSION)
            framework = ''
            if table_id.startswith('EBA_'):
                parts = table_id.split('_')
                if len(parts) >= 2:
                    framework = parts[1]

            tables.append({
                'table_id': table_id,
                'table_name': table_name,
                'table_code': table_code,
                'framework': framework,
                'version': version,
                'description': description
                # Note: TABLE_VID is intentionally excluded as it's an internal ID
            })

        logger.info(f"Returning {len(tables)} available tables for selection")

        return JsonResponse({
            'success': True,
            'tables': tables,
            'count': len(tables)
        })

    except Exception as e:
        logger.error(f"Error getting available tables: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def save_table_selection(request):
    """
    API endpoint to save table selection and continue with Phase B.
    Receives selected_tables and triggers Phase B execution.
    """
    logger = logging.getLogger(__name__)

    if request.method != 'POST':
        return JsonResponse({'error': 'POST required'}, status=405)

    try:
        import json

        # Get selected tables from JSON body
        data = json.loads(request.body)
        selected_tables = data.get('selected_tables', [])

        if not selected_tables:
            return JsonResponse({
                'success': False,
                'error': 'No tables selected'
            }, status=400)

        logger.info(f"Received table selection: {len(selected_tables)} tables")

        # Call execute_dpm_step with selected_tables to trigger Phase B
        # We need to create a new request object with the selected_tables
        from django.test import RequestFactory
        factory = RequestFactory()

        # Create new POST request with selected_tables
        new_request = factory.post('/workflow/dpm/execute/1/', {
            'selected_tables': json.dumps(selected_tables)
        })

        # Copy session from original request
        new_request.session = request.session

        # Execute Phase B
        response = execute_dpm_step(new_request, step_number=1)

        return response

    except Exception as e:
        logger.error(f"Error saving table selection: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def manage_table_presets(request):
    """
    API endpoint to manage table selection presets (get/save/delete).
    GET: Returns all saved presets
    POST: Saves a new preset
    DELETE: Deletes a preset
    """
    logger = logging.getLogger(__name__)

    try:
        # Get workflow session
        session_id = request.session.get('workflow_session_id')
        if not session_id:
            return JsonResponse({
                'success': False,
                'error': 'No active workflow session'
            }, status=400)

        workflow_session = WorkflowSession.objects.get(session_id=session_id)

        # Get DPM execution record (for storing presets)
        dpm_execution = DPMProcessExecution.objects.filter(
            session=workflow_session,
            step_number=1
        ).first()

        if not dpm_execution:
            return JsonResponse({
                'success': False,
                'error': 'DPM Step 1 execution not found'
            }, status=404)

        if request.method == 'GET':
            # Return all presets
            presets = dpm_execution.table_selection_presets or {}
            return JsonResponse({
                'success': True,
                'presets': presets
            })

        elif request.method == 'POST':
            # Save a new preset
            import json
            data = json.loads(request.body)
            preset_name = data.get('preset_name')
            selected_tables = data.get('table_ids', [])

            if not preset_name:
                return JsonResponse({
                    'success': False,
                    'error': 'Preset name required'
                }, status=400)

            # Get existing presets
            presets = dpm_execution.table_selection_presets or {}
            presets[preset_name] = selected_tables

            # Save to database
            dpm_execution.table_selection_presets = presets
            dpm_execution.save()

            logger.info(f"Saved preset '{preset_name}' with {len(selected_tables)} tables")

            return JsonResponse({
                'success': True,
                'message': f"Preset '{preset_name}' saved successfully"
            })

        elif request.method == 'DELETE':
            # Delete a preset
            import json
            data = json.loads(request.body)
            preset_name = data.get('preset_name')

            if not preset_name:
                return JsonResponse({
                    'success': False,
                    'error': 'Preset name required'
                }, status=400)

            presets = dpm_execution.table_selection_presets or {}
            if preset_name in presets:
                del presets[preset_name]
                dpm_execution.table_selection_presets = presets
                dpm_execution.save()

                logger.info(f"Deleted preset '{preset_name}'")

                return JsonResponse({
                    'success': True,
                    'message': f"Preset '{preset_name}' deleted successfully"
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': f"Preset '{preset_name}' not found"
                }, status=404)

        else:
            return JsonResponse({'error': 'Method not allowed'}, status=405)

    except Exception as e:
        logger.error(f"Error managing presets: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


def workflow_dpm_review(request, step_number):
    """Review page for DPM step execution results"""
    logger = logging.getLogger(__name__)

    try:
        # Get workflow session (optional for viewing artifacts)
        session_id = request.session.get('workflow_session_id')
        if session_id:
            try:
                workflow_session = WorkflowSession.objects.get(session_id=session_id)
            except WorkflowSession.DoesNotExist:
                workflow_session = None
        else:
            workflow_session = None

        # Show info message if no active session
        if not workflow_session:
            messages.info(request, 'No active workflow session. Showing available artifacts.')

        # Get DPM execution record (if it exists and we have a session)
        if workflow_session:
            try:
                dpm_execution = DPMProcessExecution.objects.get(
                    session=workflow_session,
                    step_number=step_number
                )
                execution_exists = True
            except DPMProcessExecution.DoesNotExist:
                dpm_execution = None
                execution_exists = False
                messages.info(request, f'DPM Step {step_number} has not been executed. Showing available artifacts.')
        else:
            # No workflow session, so no execution record
            dpm_execution = None
            execution_exists = False

        # Define step names for when execution doesn't exist
        STEP_NAMES = {
            1: 'Prepare DPM Data',
            2: 'Import DPM Data',
            3: 'Create Output Layers',
            4: 'Create Transformation Rules',
            5: 'Generate Python Code',
            6: 'Execute DPM Tests',
        }

        # Gather generated files based on step number
        import glob
        generated_files = []

        if step_number == 1:
            # Prepare DPM Data - check for CSV files in results/technical_export/
            csv_pattern = "results/technical_export/*.csv"
            generated_files = glob.glob(csv_pattern)
        elif step_number == 2:
            # Import DPM Data - check database records
            from pybirdai.models.bird_meta_data_model import FRAMEWORK, DOMAIN, MEMBER
            generated_files = [
                f"Frameworks: {FRAMEWORK.objects.count()} records",
                f"Domains: {DOMAIN.objects.count()} records",
                f"Members: {MEMBER.objects.count()} records",
            ]
        elif step_number == 3:
            # Create Output Layers - check for cubes and related structures
            from pybirdai.models.bird_meta_data_model import (
                CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
                COMBINATION, COMBINATION_ITEM
            )
            generated_files = [
                f"Cubes: {CUBE.objects.count()} records",
                f"Cube Structures: {CUBE_STRUCTURE.objects.count()} records",
                f"Cube Structure Items: {CUBE_STRUCTURE_ITEM.objects.count()} records",
                f"Combinations: {COMBINATION.objects.count()} records",
                f"Combination Items: {COMBINATION_ITEM.objects.count()} records",
            ]
        elif step_number == 4:
            # Create Transformation Rules - check for filters and joins metadata
            from pybirdai.models.bird_meta_data_model import (
                CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, MEMBER_LINK, TABLE_CELL
            )
            generated_files = [
                f"Cube Links: {CUBE_LINK.objects.count()} records",
                f"Cube Structure Item Links: {CUBE_STRUCTURE_ITEM_LINK.objects.count()} records",
                f"Member Links: {MEMBER_LINK.objects.count()} records",
                f"Table Cells: {TABLE_CELL.objects.count()} records",
            ]
        elif step_number == 5:
            # Generate Python Code - check for generated Python files
            filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
            join_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'join_code')

            filter_files = [os.path.basename(f) for f in glob.glob(os.path.join(filter_code_dir, 'F_*.py'))]
            join_files = [os.path.basename(f) for f in glob.glob(os.path.join(join_code_dir, 'J_*.py'))]

            filter_files.sort()
            join_files.sort()

            # Encode file lists for code editor
            encoded_filter_files = encode_file_list(filter_files)
            encoded_join_files = encode_file_list(join_files)

            generated_files = [
                f"Generated Filter Files: {len(filter_files)} Python files",
                f"Generated Join Files: {len(join_files)} Python files",
            ]

            # Add sample file names if they exist
            if filter_files:
                generated_files.append(f"Sample Filter: {filter_files[0]}")
            if join_files:
                generated_files.append(f"Sample Join: {join_files[0]}")
        elif step_number == 6:
            # Execute DPM Tests - check for test results and reports
            test_config_path = "tests/dpm/configuration_file_tests.json"
            test_results_pattern = "results/test_reports/dpm_*.html"

            test_config_exists = os.path.exists(test_config_path)
            test_reports = glob.glob(test_results_pattern)

            generated_files = [
                f"DPM Test Configuration: {'Found' if test_config_exists else 'Not Found'}",
                f"Test Reports Generated: {len(test_reports)} reports",
            ]

            # Add test execution summary from execution data if available
            if execution_exists and dpm_execution and dpm_execution.execution_data:
                tests_executed = dpm_execution.execution_data.get('tests_executed', False)
                generated_files.append(f"Tests Executed: {'Yes' if tests_executed else 'No'}")

                steps_completed = dpm_execution.execution_data.get('steps_completed', [])
                if steps_completed:
                    generated_files.append(f"Steps Completed: {len(steps_completed)}")

            # Add report file names if they exist
            if test_reports:
                for report in test_reports[:3]:  # Show first 3 reports
                    generated_files.append(f"Report: {os.path.basename(report)}")

        # Step 6: Load test results
        test_results_list = []
        total_tests = 0
        passed_tests = 0
        failed_tests = 0

        if step_number == 6:
            # Load test results from JSON files
            all_test_results = load_test_results()

            # Filter for DPM suite tests only
            test_results_list = [r for r in all_test_results if r.get('suite_name') == 'dpm']

            # Calculate statistics
            total_tests = len(test_results_list)
            for result in test_results_list:
                test_data = result.get('test_results', {})
                passed_list = test_data.get('passed', [])
                failed_list = test_data.get('failed', [])

                if passed_list:
                    passed_tests += len(passed_list) if isinstance(passed_list, list) else 1
                if failed_list:
                    failed_tests += len(failed_list) if isinstance(failed_list, list) else 1

        # Calculate duration if available
        duration = None
        if execution_exists and dpm_execution and dpm_execution.started_at and dpm_execution.completed_at:
            duration = dpm_execution.completed_at - dpm_execution.started_at

        context = {
            'step_number': step_number,
            'step_name': dpm_execution.step_name if (execution_exists and dpm_execution) else STEP_NAMES.get(step_number, f'Step {step_number}'),
            'execution': dpm_execution,
            'execution_exists': execution_exists,
            'status': dpm_execution.status if (execution_exists and dpm_execution) else 'not_executed',
            'started_at': dpm_execution.started_at if (execution_exists and dpm_execution) else None,
            'completed_at': dpm_execution.completed_at if (execution_exists and dpm_execution) else None,
            'duration': duration,
            'error_message': dpm_execution.error_message if (execution_exists and dpm_execution) else None,
            'execution_data': dpm_execution.execution_data if (execution_exists and dpm_execution) else None,
            'generated_files': generated_files,
            'workflow_type': 'dpm',
            # Step 5 specific context
            'encoded_filter_files': encoded_filter_files if step_number == 5 else None,
            'encoded_join_files': encoded_join_files if step_number == 5 else None,
            'filter_files_count': len(filter_files) if step_number == 5 else 0,
            'join_files_count': len(join_files) if step_number == 5 else 0,
            # Step 6 specific context
            'test_results': test_results_list,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
        }

        return render(request, 'pybirdai/workflow/dpm_review.html', context)

    except Exception as e:
        logger.error(f"Error in DPM review page: {e}")
        messages.error(request, f'Error loading review page: {str(e)}')
        return redirect('pybirdai:workflow_dashboard')


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
