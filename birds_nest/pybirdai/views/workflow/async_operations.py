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
import time
import json
import logging
import threading

from django.conf import settings
from django.http import JsonResponse
from django.utils import timezone

from pybirdai.models.workflow_model import WorkflowTaskExecution, WorkflowSession
from pybirdai.utils.secure_error_handling import SecureErrorHandler

from .status import (
    _migration_status, _database_setup_status, 
    _automode_status, _setup_database_models_status
)
from .github import _get_github_token, _in_memory_github_token
from .tasks import task1_smcubes_core, task2_smcubes_rules, task3_python_rules, task4_full_execution

logger = logging.getLogger(__name__)


def _async_error_response(exception, context, request, message):
    SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse({'error': message}, status=500)

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


        from pybirdai.entry_points.database_setup import RunApplicationSetup
        app_config = RunApplicationSetup('pybirdai', 'birds_nest', token=_in_memory_github_token)

        logger.info("About to call run_migrations() - this should NOT download or delete any files")
        migration_results = app_config.run_migrations()
        logger.info("run_migrations() completed - no files should have been modified")

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


def _run_setup_database_models_async():
    """Run Step 2b: Generate models + run migrations (without re-fetching artifacts)."""
    global _setup_database_models_status

    try:
        logger.info("Starting Step 2b: Setup Database Models (post-setup + migrations)...")
        _setup_database_models_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'error': None,
            'message': 'Starting model generation...',
            'started_at': time.time(),
            'current_step': 'post_setup'
        })

        import os
        from django.conf import settings
        from pybirdai.entry_points.database_setup import RunApplicationSetup

        base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        # Step 1: Run post-setup operations (generate models, copy to bird_data_model.py, update admin.py)
        _setup_database_models_status['message'] = 'Generating Django models and updating admin.py...'
        logger.info("Running post-setup operations (model generation + admin update)...")

        app_config = RunApplicationSetup('pybirdai', 'birds_nest')
        app_config.run_post_setup()

        logger.info("Post-setup operations completed. Now running migrations...")

        # Step 2: Run migrations
        _setup_database_models_status.update({
            'current_step': 'migrations',
            'message': 'Running database migrations...'
        })

        migration_results = app_config.run_migrations()

        # Update status on success
        _setup_database_models_status.update({
            'running': False,
            'completed': True,
            'success': True,
            'error': None,
            'message': migration_results.get('message', 'Database setup completed successfully!'),
            'completed_at': time.time()
        })

        logger.info("Step 2b completed successfully!")

        # Wait for frontend to get status, then restart
        time.sleep(6)
        os._exit(0)

    except Exception as e:
        error_data = SecureErrorHandler.handle_exception(
            e,
            'setting up database models asynchronously',
        )
        _setup_database_models_status.update({
            'running': False,
            'completed': True,
            'success': False,
            'error': error_data['message'],
            'message': 'Database setup failed. Please try again later.',
            'completed_at': time.time()
        })


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
    """Retrieve configured artefacts and prepare the local setup files."""
    global _database_setup_status

    try:
        logger.info("Starting background database setup process...")
        _database_setup_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'error': None,
            'message': 'Starting artefact retrieval...',
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

        # Stage 1: retrieve remote/local artefacts
        _database_setup_status['message'] = 'Retrieving configured artefacts...'

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
        # Add branch attributes as dynamic attributes since they may not be in the model
        # bird_content_branch is the primary field, github_branch is for backwards compatibility
        config.bird_content_branch = config_data.get("bird_content_branch", config_data.get("github_branch", "main"))
        config.test_suite_branch = config_data.get("test_suite_branch", "main")
        config.github_branch = config_data.get("github_branch", config_data.get("bird_content_branch", "main"))

        service = AutomodeConfigurationService()
        github_token = _get_github_token()

        # Only force refresh if explicitly needed - preserve existing files
        # This prevents unnecessary file deletion that can cause issues during model creation
        task1_results = service.fetch_files_from_source(
            config=config,
            github_token=github_token,
            force_refresh=False,  # Changed to False to preserve existing files
        )

        _database_setup_status['completed_tasks'].append('Artefacts retrieved')
        logger.info("Artefact retrieval completed successfully")

        # Stage 2: prepare local setup files from the downloaded artefacts
        _database_setup_status.update({
            'current_task': 2,
            'message': 'Preparing local setup files...'
        })

        from pybirdai.entry_points.database_setup import RunApplicationSetup
        app_config = RunApplicationSetup('pybirdai', 'birds_nest')

        # This creates models and runs migrations
        db_results = app_config.run_automode_setup()

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
                'message': 'Artefact preparation completed. Server restart required for migrations.',
                'completed_at': time.time(),
                'completed_tasks': ['Artefacts retrieved', 'Local setup files prepared'],
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
                from pybirdai.entry_points.database_setup import (
                    RunApplicationSetup,
                )

                app_config = RunApplicationSetup("pybirdai", "birds_nest")
                app_config.run_post_setup()
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
                'message': db_results.get(
                    'message',
                    'Artefact retrieval and local setup preparation completed successfully!',
                ),
                'completed_at': time.time(),
                'completed_tasks': ['Artefacts retrieved', 'Local setup files prepared']
            })

        logger.info("Background database setup process completed successfully")

    except Exception as e:
        logger.error(f"Background database setup process failed: {e}")
        _database_setup_status.update(
            {
                "running": False,
                "completed": True,
                "success": False,
                "error": "Artefact preparation error occurred",
                "message": f"Artefact preparation failed at stage {_database_setup_status.get('current_task', '?')}",
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
                break  # Stop execution if a task fails

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


def _restart_server_async():
    """Restart the server in background thread."""
    logger.info("Server restart requested - will restart in 3 seconds...")
    time.sleep(3)
    logger.info("Triggering server restart now...")
    os._exit(0)


def trigger_server_restart(request):
    """
    API endpoint to trigger a server restart.
    Used after deploying code changes to reload the Python modules.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)

    try:
        logger.info("Server restart triggered via API")

        # Start restart in background thread
        restart_thread = threading.Thread(target=_restart_server_async)
        restart_thread.daemon = True
        restart_thread.start()

        return JsonResponse({
            'success': True,
            'message': 'Server restart initiated. Please wait for the server to come back online.',
        })

    except Exception as e:
        return _async_error_response(
            e,
            'triggering server restart',
            request,
            'Server restart could not be triggered.',
        )
