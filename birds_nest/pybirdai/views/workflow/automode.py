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
import threading
import time

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.conf import settings

from pybirdai.models.workflow_model import WorkflowSession, AutomodeConfiguration
from pybirdai.api.workflow_api import AutomodeConfigurationService

from .status import _automode_status, _reset_automode_status
from .async_operations import _run_automode_async
from .github import _set_github_token

logger = logging.getLogger(__name__)

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
