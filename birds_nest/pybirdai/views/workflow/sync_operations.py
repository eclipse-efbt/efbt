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
# Synchronous workflow operations (no threading)

import logging
import time

from django.utils import timezone

from pybirdai.models.workflow_model import WorkflowTaskExecution, WorkflowSession

from .tasks import task1_smcubes_core, task2_smcubes_rules, task3_python_rules, task4_full_execution

logger = logging.getLogger(__name__)


def run_automode_sync(target_task, session_data):
    """
    Run automode tasks 1 to target_task synchronously.

    Returns a dict with:
        - success: bool
        - message: str
        - completed_tasks: list of completed task names
        - errors: list of error dicts (if any)
    """
    logger.info(f"Starting synchronous automode execution up to task {target_task}...")

    completed_tasks = []
    task_errors = []

    # Map task numbers to their handler functions
    task_handlers = {
        1: task1_smcubes_core,
        2: task2_smcubes_rules,
        3: task3_python_rules,
        4: task4_full_execution,
    }

    # Create a mock request object for task handlers
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
        logger.info(f"Starting Task {task_num}...")

        try:
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
                logger.warning(f"Database not available for task {task_num}: {db_error}")
                # Continue without database records

            # Get the appropriate task handler
            handler = task_handlers.get(task_num)
            if not handler:
                raise Exception(f"No handler found for task {task_num}")

            # Create mock request
            mock_request = MockRequest(post_data={})

            # Call the handler synchronously
            logger.info(f"Executing handler for task {task_num}")
            result = handler(mock_request, 'do', task_execution, workflow_session)

            # Check if it's a JsonResponse indicating success
            if hasattr(result, "content"):
                import json
                response_data = json.loads(result.content)
                if response_data.get("success"):
                    completed_tasks.append(f"Task {task_num}")
                    logger.info(f"Task {task_num} completed successfully")
                else:
                    raise Exception(
                        response_data.get("message", f"Task {task_num} failed")
                    )
            else:
                # If no JsonResponse, assume success
                completed_tasks.append(f"Task {task_num}")
                logger.info(f"Task {task_num} completed")

        except Exception as task_error:
            logger.error(f"Task {task_num} failed: {task_error}")
            task_errors.append({
                "task": task_num,
                "error": str(task_error)
            })
            # Stop execution on first error
            break

    # Return results
    if task_errors:
        return {
            'success': False,
            'message': f"Automode completed with errors. Successfully completed: {', '.join(completed_tasks) or 'None'}",
            'completed_tasks': completed_tasks,
            'errors': task_errors
        }
    else:
        return {
            'success': True,
            'message': f"Automode completed successfully! Tasks completed: {', '.join(completed_tasks)}",
            'completed_tasks': completed_tasks,
            'errors': []
        }
