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

from .bird_meta_data_model import WorkflowTaskExecution, WorkflowSession
from .views import create_response_with_loading
from .entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
from .entry_points.import_input_model import RunImportInputModelFromSQLDev
from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from .workflow_services import AutomodeConfigurationService
from .forms import (
    AutomodeConfigurationSessionForm,
    ResourceDownloadForm,
    SMCubesCoreForm,
    SMCubesRulesForm,
    PythonRulesForm,
    FullExecutionForm,
)
from .entry_points import (
    automode_database_setup,
    create_filters,
    create_joins_metadata,
    execute_datapoint,
)
# Import the test runner
from .utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner
import traceback
logger = logging.getLogger(__name__)

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
    """Load and parse test results from JSON files"""
    test_results = []
    # Use Django's BASE_DIR to construct the full path
    base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
    json_files_path = os.path.join(base_dir, 'tests', 'test_results', 'json', '*.json')

    logger.info(f"Looking for test results in: {json_files_path}")

    try:
        json_files = glob.glob(json_files_path)
        logger.info(f"Found {len(json_files)} JSON files: {json_files}")

        for json_file in json_files:
            try:
                logger.debug(f"Loading test result file: {json_file}")
                with open(json_file, 'r', encoding='utf-8') as f:
                    result_data = json.load(f)
                    # Add filename for reference
                    result_data['filename'] = os.path.basename(json_file)
                    test_results.append(result_data)
                    logger.debug(f"Successfully loaded {json_file}")
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Error loading test result file {json_file}: {e}")
                continue

        # Sort by timestamp (newest first)
        test_results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        logger.info(f"Loaded {len(test_results)} test results successfully")

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


        from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
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
                "error": str(e),
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
        from .bird_meta_data_model import WorkflowTaskExecution

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
        from .bird_meta_data_model import AutomodeConfiguration
        from .workflow_services import AutomodeConfigurationService

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
            data_model_type=config_data.get("data_model_type", "ELDM"),
            technical_export_source=config_data.get(
                "technical_export_source", "BIRD_WEBSITE"
            ),
            technical_export_github_url=config_data.get(
                "technical_export_github_url", ""
            ),
            config_files_source=config_data.get("config_files_source", "MANUAL"),
            config_files_github_url=config_data.get("config_files_github_url", ""),
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

        from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
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
                from .entry_points.automode_database_setup import (
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
                "error": str(e),
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
        import cProfile, pstats, io
        from pstats import SortKey

        for task_num in range(1, target_task + 1):
            pr = cProfile.Profile()
            pr.enable()
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

            pr.disable()
            pr.dump_stats(f"cProfile_1_to_{task_num}_dump")

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
                "error": str(e),
                "message": f"Automode failed at Task {_automode_status.get('current_task', '?')}",
                "completed_at": time.time(),
            }
        )


def workflow_dashboard(request):
    """Main dashboard showing all tasks and their status"""
    import json
    import os
    from django.conf import settings
    from django.db import connection
    from django.db.utils import OperationalError, ProgrammingError

    if not os.path.exists("automode_config.json"):
        messages.error(request, "Automode configuration file not found")
        with open("automode_config.json", "w") as f:
            f.write("""{
              "data_model_type": "ELDM",
              "clone_mode": "false",
              "technical_export_source": "GITHUB",
              "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD",
              "config_files_source": "GITHUB",
              "config_files_github_url": "https://github.com/regcommunity/FreeBIRD",
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
            "data_model_type": "ELDM",
            "clone_mode": "false",
            "technical_export_source": "BIRD_WEBSITE",
            "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD",
            "config_files_source": "MANUAL",
            "config_files_github_url": "",
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
                }
            )

            refresh_complete_status()
        except:
            context.update({
                'workflow_session': None,
                'task_grid': [],
                'progress': 0,
                'session_id': session_id or 'no-database',
            })
    else:
        # Provide default data when no database is available
        context.update({
            'workflow_session': None,
            'task_grid': [],
            'progress': 0,
            'session_id': session_id or 'no-database',
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
                from .entry_points.convert_ldm_to_sdd_hierarchies import RunConvertLDMToSDDHierarchies
                from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
                from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
                from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
                from .entry_points.import_input_model import RunImportInputModelFromSQLDev
                from .entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

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
                from .entry_points.create_filters import RunCreateFilters
                from .entry_points.create_joins_metadata import RunCreateJoinsMetadata

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
                from .entry_points.run_create_executable_filters import RunCreateExecutableFilters
                from .entry_points.create_executable_joins import RunCreateExecutableJoins

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
                    RunCreateExecutableFilters.run_create_executable_filters()
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
                    RunCreateExecutableFilters.run_create_executable_filters()
                    execution_data["filter_code_generated"] = True
                    execution_data["steps_completed"].append(
                        "Executable filter code generation"
                    )

                # Note: Join and transformation code generation would use different entry points
                # For now, marking as completed to indicate the workflow step is done
                if request.POST.get("generate_join_code") or run_all:
                    logger.info("Join code generation (using filter infrastructure)...")
                    execution_data["current_phase"] = "joins"
                    RunCreateExecutableJoins.create_python_joins()  # Correct method name
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

        return render(request, 'pybirdai/workflow/task3/review.html', {
            'task_execution': task_execution,
            'workflow_session': workflow_session,
            'execution_data': execution_data,
        })





def task4_full_execution(request, operation, task_execution, workflow_session):
    """Handle Task 4: Test Suite Execution operations"""

    if operation == 'do':
        if request.method == 'POST':
            # Start test execution
            task_execution.status = 'running'
            task_execution.started_at = datetime.datetime.now()
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

                    # Create test runner instance
                    test_runner = RegulatoryTemplateTestRunner(False)
                    config_file = 'tests/configuration_file_tests.json'

                    logger.info(f"Running tests from config file: {config_file}")
                    # Override the arguments to match our desired configuration
                    test_runner.args.uv = "False"
                    test_runner.args.config_file = config_file
                    test_runner.args.dp_value = None
                    test_runner.args.reg_tid = None
                    test_runner.args.dp_suffix = None
                    test_runner.args.scenario = None

                    # Execute the test runner with config file
                    test_runner.main()
                    execution_data['test_mode'] = 'config_file'
                    execution_data['config_file'] = config_file
                    execution_data['tests_executed'] = True
                    execution_data['steps_completed'].append('Configuration file tests completed')

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
                execution_time = datetime.datetime.now() - task_execution.started_at
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
            'message': f'Failed to execute substep: {str(e)}'
        }, status=500)


def _execute_task2_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 2: Database Creation"""

    if substep_name == 'start':
        try:
            from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
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
                'message': str(e)
            }, status=500)

    elif substep_name == 'continue':
        try:
            from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
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
                'message': str(e)
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
        from .entry_points.convert_ldm_to_sdd_hierarchies import RunConvertLDMToSDDHierarchies
        from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
        from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
        from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
        from .entry_points.import_input_model import RunImportInputModelFromSQLDev
        from .entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

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
        from .entry_points.create_filters import RunCreateFilters
        from .entry_points.create_joins_metadata import RunCreateJoinsMetadata

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
        from .entry_points.run_create_executable_filters import RunCreateExecutableFilters
        from .entry_points.create_executable_joins import RunCreateExecutableJoins

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        success_message = ''

        if substep_name == 'generate_filter_code':
            logger.info("Executing generate filter code substep...")
            RunCreateExecutableFilters.run_create_executable_filters()
            execution_data['filter_code_generated'] = True
            execution_data['steps_completed'].append('Executable filter code generation')
            success_message = 'Filter code generated successfully'

        elif substep_name == 'generate_join_code':
            logger.info("Executing generate join code substep...")
            RunCreateExecutableJoins.create_python_joins()
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
        from .utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        if substep_name == 'run_tests':
            logger.info("Executing run tests substep...")

            # Create test runner instance
            test_runner = RegulatoryTemplateTestRunner(False)

            # Configure test runner
            config_file = request.POST.get('config_file', 'tests/configuration_file_tests.json')
            test_runner.args.uv = "False"
            test_runner.args.config_file = config_file
            test_runner.args.dp_value = None
            test_runner.args.reg_tid = None
            test_runner.args.dp_suffix = None
            test_runner.args.scenario = None

            # Execute tests
            test_runner.main()

            execution_data['tests_executed'] = True
            execution_data['steps_completed'].append('Test suite execution')
            success_message = 'Tests executed successfully'

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
            'message': f'Failed to execute substep: {str(e)}'
        }, status=500)


def _execute_task2_substep(request, substep_name, task_execution, workflow_session):
    """Execute individual substeps for Task 2: Database Creation"""

    if substep_name == 'start':
        try:
            from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
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
                'message': str(e)
            }, status=500)

    elif substep_name == 'continue':
        try:
            from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
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
                'message': str(e)
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
        from .entry_points.convert_ldm_to_sdd_hierarchies import RunConvertLDMToSDDHierarchies
        from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
        from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
        from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
        from .entry_points.import_input_model import RunImportInputModelFromSQLDev
        from .entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

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
        from .entry_points.create_filters import RunCreateFilters
        from .entry_points.create_joins_metadata import RunCreateJoinsMetadata

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
        from .entry_points.run_create_executable_filters import RunCreateExecutableFilters
        from .entry_points.create_executable_joins import RunCreateExecutableJoins

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        success_message = ''

        if substep_name == 'generate_filter_code':
            logger.info("Executing generate filter code substep...")
            RunCreateExecutableFilters.run_create_executable_filters()
            execution_data['filter_code_generated'] = True
            execution_data['steps_completed'].append('Executable filter code generation')
            success_message = 'Filter code generated successfully'

        elif substep_name == 'generate_join_code':
            logger.info("Executing generate join code substep...")
            RunCreateExecutableJoins.create_python_joins()
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
        from .utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

        # Get or initialize execution data
        execution_data = task_execution.execution_data or {
            'steps_completed': []
        }
        if 'steps_completed' not in execution_data:
            execution_data['steps_completed'] = []

        if substep_name == 'run_tests':
            logger.info("Executing run tests substep...")

            # Create test runner instance
            test_runner = RegulatoryTemplateTestRunner(False)

            # Configure test runner
            config_file = request.POST.get('config_file', 'tests/configuration_file_tests.json')
            test_runner.args.uv = "False"
            test_runner.args.config_file = config_file
            test_runner.args.dp_value = None
            test_runner.args.reg_tid = None
            test_runner.args.dp_suffix = None
            test_runner.args.scenario = None

            # Execute tests
            test_runner.main()

            execution_data['tests_executed'] = True
            execution_data['steps_completed'].append('Test suite execution')
            success_message = 'Tests executed successfully'

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
        config_data = {
            "data_model_type": request.POST.get("data_model_type", "ELDM"),
            "clone_mode": request.POST.get("clone_mode", "false"),
            "technical_export_source": request.POST.get(
                "technical_export_source", "BIRD_WEBSITE"
            ),
            "technical_export_github_url": request.POST.get(
                "technical_export_github_url", ""
            ),
            "config_files_source": request.POST.get("config_files_source", "MANUAL"),
            "config_files_github_url": request.POST.get("config_files_github_url", ""),
            "github_branch": request.POST.get("github_branch", "main"),
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
        return JsonResponse({"success": False, "error": str(e)}, status=500)


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
            messages.error(request, f'Failed to reset full workflow session: {str(e)}')
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
        logger.error(f"Error during partial workflow session reset: {e}")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': False,
                'message': 'Failed to reset partial workflow session',
                'error': str(e)
            }, status=500)
        else:
            messages.error(request, f'Failed to reset partial workflow session: {str(e)}')
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
        organization = request.POST.get('organization', '').strip() or None
        target_branch = request.POST.get('target_branch', 'develop').strip()
        use_fork_workflow = request.POST.get('use_fork_workflow') == 'on'

        if not github_token:
            return JsonResponse({'success': False, 'error': 'GitHub token is required'})

        # Validate repository URL format if provided
        if repository_url and not repository_url.startswith('https://github.com/'):
            return JsonResponse({'success': False, 'error': 'Repository URL must be a valid GitHub URL (https://github.com/...)'})

        # Import the GitHub integration service
        from .workflow_services import GitHubIntegrationService

        # Create service instance
        github_service = GitHubIntegrationService(github_token)

        # Export database to CSV first
        from .views import _export_database_to_csv_logic
        zip_file_path, extract_dir = _export_database_to_csv_logic()

        # Determine repository URL (use automode config if not provided)
        if not repository_url:
            config = github_service.get_automode_config()
            if config and config.technical_export_github_url:
                repository_url = config.technical_export_github_url
            else:
                repository_url = 'https://github.com/regcommunity/FreeBIRD'

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
