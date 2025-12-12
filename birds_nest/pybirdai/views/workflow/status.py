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

import time
import logging

from django.http import JsonResponse
from django.views.decorators.http import require_http_methods

logger = logging.getLogger(__name__)

# In-memory storage for migration status
_migration_status = {
    "running": False,
    "completed": False,
    "success": False,
    "error": None,
    "message": "",
    "started_at": None,
    "completed_at": None,
}

# In-memory storage for database setup status
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

# In-memory storage for automode status
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

# In-memory storage for setup database models status
_setup_database_models_status = {
    "running": False,
    "completed": False,
    "success": False,
    "error": None,
    "message": "",
    "started_at": None,
    "completed_at": None,
    "current_step": None,
}

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


def _reset_setup_database_models_status():
    """Reset setup database models status to initial state."""
    global _setup_database_models_status
    _setup_database_models_status.update(
        {
            "running": False,
            "completed": False,
            "success": False,
            "error": None,
            "message": "",
            "started_at": None,
            "completed_at": None,
            "current_step": None,
        }
    )


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


def workflow_setup_database_models_status(request):
    """Check the status of Step 2b (model generation + migrations)"""
    global _setup_database_models_status

    status_copy = _setup_database_models_status.copy()

    # Calculate elapsed time
    if status_copy['running'] and status_copy['started_at']:
        status_copy['elapsed_time'] = time.time() - status_copy['started_at']
    elif status_copy['completed'] and status_copy['started_at'] and status_copy['completed_at']:
        status_copy['elapsed_time'] = status_copy['completed_at'] - status_copy['started_at']

    return JsonResponse({
        'success': True,
        'migration_status': status_copy  # Use migration_status for frontend compatibility
    })


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
