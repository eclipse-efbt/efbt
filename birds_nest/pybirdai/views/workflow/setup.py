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
import logging
import threading
import time

from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.conf import settings

from .status import (
    _migration_status, _setup_database_models_status, _database_setup_status,
    _reset_migration_status, _reset_setup_database_models_status, _reset_database_setup_status
)
from .async_operations import (
    _run_migrations_async, _run_setup_database_models_async, _run_database_setup_async
)
from .github import _set_github_token

logger = logging.getLogger(__name__)

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


def workflow_setup_database_models(request):
    """Step 2b: Generate Django models and run migrations (without re-fetching artifacts)"""
    global _setup_database_models_status

    # Check if already running
    if _setup_database_models_status['running']:
        return JsonResponse({
            'success': False,
            'message': 'Database model setup is already running. Please wait for completion.',
            'status': 'already_running'
        })

    # Reset status if previously completed
    if _setup_database_models_status["completed"]:
        _reset_setup_database_models_status()

    try:
        # Start in background thread
        setup_thread = threading.Thread(target=_run_setup_database_models_async, daemon=True)
        setup_thread.start()

        return JsonResponse({
            'success': True,
            'message': 'Database model setup started. Generating models and running migrations...',
            'status': 'started',
            'check_status_url': '/pybirdai/workflow/setup-database-models-status/'
        })

    except Exception as e:
        logger.error(f"Failed to start setup database models thread: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Failed to start database model setup: {str(e)}',
            'status': 'failed'
        }, status=500)


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
