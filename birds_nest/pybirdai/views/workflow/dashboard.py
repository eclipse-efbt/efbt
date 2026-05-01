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
import uuid

from django.shortcuts import render, get_object_or_404
from django.conf import settings
from django.utils import timezone

from pybirdai.models.workflow_model import (
    WorkflowTaskExecution, WorkflowSession,
    DPMProcessExecution, AnaCreditProcessExecution,
    AutomodeConfiguration
)
from pybirdai.forms import AutomodeConfigurationSessionForm

from .progress import get_dpm_task_grid, get_ancrdt_task_grid, get_workflow_progress_summary
from .helpers import load_test_results, refresh_complete_status
from .status import _migration_status
from .github import _get_github_token
from .async_operations import _load_task1_completion_from_marker

logger = logging.getLogger(__name__)

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
              "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD_EIL_67",
              "config_files_source": "GITHUB",
              "config_files_github_url": "https://github.com/regcommunity/FreeBIRD_EIL_67",
              "test_suite_source": "GITHUB",
              "test_suite_github_url": " https://github.com/regcommunity/bird-default-test-suite-eil-67",
              "bird_content_branch": "main",
              "test_suite_branch": "main",
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
    setup_ready = False
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

        from pybirdai.process_steps.database_setup.automode_orchestrator import is_setup_ready

        setup_ready = is_setup_ready(base_dir)

    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        # Use defaults if config cannot be loaded
        config = {
            "data_model_type": "EIL",
            "clone_mode": "false",
            "technical_export_source": "BIRD_WEBSITE",
            "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD_EIL_67",
            "config_files_source": "MANUAL",
            "config_files_github_url": "",
            "test_suite_source": "GITHUB",
            "test_suite_github_url": "",
            "bird_content_branch": "main",
            "test_suite_branch": "main",
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
