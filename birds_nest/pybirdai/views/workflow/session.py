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
import csv
import glob
import io
import json
import logging
import threading
import time
import uuid

from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.conf import settings
from django.db import close_old_connections, connection
from django.db.utils import OperationalError, ProgrammingError
from django.utils import timezone

from pybirdai.models.workflow_model import (
    WorkflowTaskExecution, WorkflowSession,
    DPMProcessExecution, AnaCreditProcessExecution
)
from pybirdai.api.workflow_api import AutomodeConfigurationService
from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
from pybirdai.utils.secure_error_handling import SecureErrorHandler

from .status import (
    _reset_migration_status, _reset_database_setup_status,
    _reset_automode_status, _reset_setup_database_models_status,
    _clone_import_status, _reset_clone_import_status
)
from .github import _set_github_token, _clear_github_token

logger = logging.getLogger(__name__)

CLONE_REQUIRED_GENERATED_ARTEFACTS = (
    'cube_link.csv',
    'cube_structure_item_link.csv',
)
CLONE_OPTIONAL_GENERATED_ARTEFACTS = (
    'member_link.csv',
)


def _session_error_response(exception, context, request, message):
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse(
        {
            'success': False,
            'message': message,
            'error': error_data['message'],
        },
        status=500,
    )


def _count_csv_data_rows(csv_content):
    reader = csv.reader(io.StringIO(csv_content or ''))

    try:
        next(reader)
    except StopIteration:
        return 0

    return sum(
        1
        for row in reader
        if any(str(cell).strip() for cell in row)
    )


def _clone_generated_artefact_summary(csv_data):
    generated_filenames = CLONE_REQUIRED_GENERATED_ARTEFACTS + CLONE_OPTIONAL_GENERATED_ARTEFACTS
    row_counts = {}

    for filename in generated_filenames:
        row_counts[filename] = _count_csv_data_rows(csv_data.get(filename, ''))

    blocking_files = [
        filename
        for filename in CLONE_REQUIRED_GENERATED_ARTEFACTS
        if row_counts[filename] == 0
    ]

    return row_counts, blocking_files


def _run_clone_test_suite():
    """Run the same test-suite service used by automode full execution."""
    return AutomodeConfigurationService()._run_tests_suite()


def _clone_task_execution_data(task_number, import_summary, test_suite_results=None):
    test_suite_results = test_suite_results or {}
    tests_executed = bool(test_suite_results.get('tests_executed', False))
    test_suites = test_suite_results.get('suites_run', [])
    test_errors = test_suite_results.get('errors', [])

    if tests_executed:
        suite_list = ', '.join(test_suites)
        test_completion_message = (
            f'Test suite execution completed for {len(test_suites)} suite(s)'
            + (f': {suite_list}' if suite_list else '')
        )
    elif test_errors:
        test_completion_message = (
            f'Test suite execution did not complete: {len(test_errors)} error(s)'
        )
    else:
        test_completion_message = 'Test suite execution did not run any suites'

    common_data = {
        'source': 'clone_import',
        'mode': 'clone',
        'import_summary': import_summary,
    }

    execution_data_by_task = {
        1: {
            **common_data,
            'database_deleted': True,
            'hierarchy_analysis_imported': True,
            'semantic_integrations_processed': True,
            'input_model_imported': True,
            'report_templates_created': True,
        },
        2: {
            **common_data,
            'current_step': 'completed',
            'filters_created': True,
            'joins_metadata_created': True,
            'steps_completed': [
                'Filters cloned from SMCubes artefacts',
                'Joins metadata cloned from SMCubes artefacts',
            ],
        },
        3: {
            **common_data,
            'current_phase': 'completed',
            'filter_code_generated': True,
            'join_code_generated': True,
            'steps_completed': [
                'Filter artefacts cloned from SMCubes artefacts',
                'Join artefacts cloned from SMCubes artefacts',
            ],
        },
        4: {
            **common_data,
            'current_stage': 'completed' if tests_executed else 'test_execution_failed',
            'test_mode': 'clone_import',
            'tests_executed': tests_executed,
            'test_suites': test_suites,
            'test_suite_results': test_suite_results,
            'test_errors': test_errors,
            'steps_completed': [
                'Completed workflow state cloned from SMCubes artefacts',
                test_completion_message,
            ],
        },
    }

    return execution_data_by_task[task_number]


def _mark_clone_tasks_completed(import_summary, test_suite_results=None):
    from django.db import transaction

    completed_task_numbers = []
    now = timezone.now()

    with transaction.atomic():
        for task_number in range(1, 5):
            execution_data = _clone_task_execution_data(
                task_number,
                import_summary,
                test_suite_results,
            )
            status = 'completed'
            error_message = None
            error_details = None
            recovery_action = 'none'

            if task_number == 4 and not execution_data.get('tests_executed', False):
                status = 'failed'
                errors = execution_data.get('test_errors') or ['No test suites were executed']
                error_message = '; '.join(errors)
                error_details = json.dumps(test_suite_results or {}, indent=2)
                recovery_action = 'retry'

            task_execution, _ = WorkflowTaskExecution.objects.get_or_create(
                task_number=task_number,
                operation_type='do',
                defaults={
                    'status': status,
                    'started_at': now,
                    'completed_at': now,
                    'execution_data': execution_data,
                    'progress_percentage': 100,
                },
            )
            task_execution.status = status
            task_execution.started_at = now
            task_execution.completed_at = now
            task_execution.execution_data = execution_data
            task_execution.progress_percentage = 100
            task_execution.error_message = error_message
            task_execution.error_details = error_details
            task_execution.recovery_action = recovery_action
            task_execution.save(
                update_fields=[
                    'status',
                    'started_at',
                    'completed_at',
                    'execution_data',
                    'progress_percentage',
                    'error_message',
                    'error_details',
                    'recovery_action',
                ]
            )
            if status == 'completed':
                completed_task_numbers.append(task_number)

    return completed_task_numbers


def _set_clone_import_progress(message, current_step=None, completed_step=None):
    """Update the in-memory clone progress shown by the polling endpoint."""
    _clone_import_status['message'] = message
    if current_step:
        _clone_import_status['current_step'] = current_step
    if completed_step:
        completed_steps = _clone_import_status.setdefault('completed_steps', [])
        if completed_step not in completed_steps:
            completed_steps.append(completed_step)


def _clone_import_failure_payload(message, error, status=400, details=None):
    payload = {
        'success': False,
        'message': message,
        'error': error,
    }
    if details is not None:
        payload['details'] = details
    return payload, status


def _clone_import_exception_payload(exception, context, message):
    error_data = SecureErrorHandler.handle_exception(exception, context)
    return _clone_import_failure_payload(message, error_data['message'], status=500)


def _perform_clone_import():
    """Import CSV files from the retrieved SMCubes artefacts directory."""
    try:
        _set_clone_import_progress(
            'Checking database availability...',
            current_step='checking_database',
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except (OperationalError, ProgrammingError):
            return _clone_import_failure_payload(
                'Database not available. Please run database setup first.',
                'Database connection failed',
                status=400,
            )

        base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        smcubes_artefacts_dir = os.path.join(base_dir, 'artefacts', 'smcubes_artefacts')

        _set_clone_import_progress(
            'Finding SMCubes artefacts...',
            current_step='finding_artefacts',
            completed_step='Database available',
        )
        if not os.path.exists(smcubes_artefacts_dir):
            return _clone_import_failure_payload(
                'SMCubes artefacts directory not found',
                'SMCubes artefacts directory not found',
                status=400,
            )

        csv_files = glob.glob(os.path.join(smcubes_artefacts_dir, '*.csv'))
        if not csv_files:
            return _clone_import_failure_payload(
                'No CSV files found in SMCubes artefacts directory',
                'No CSV files to import',
                status=400,
            )

        _set_clone_import_progress(
            f'Reading {len(csv_files)} SMCubes artefact CSV file(s)...',
            current_step='reading_csv_files',
        )
        csv_data = {}
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            try:
                with open(csv_file, "r", encoding="utf-8") as f:
                    csv_data[filename] = f.read()
            except Exception as e:
                logger.error(f"Error reading CSV file {filename}: {e}")

        if not csv_data:
            return _clone_import_failure_payload(
                'Could not read any CSV files',
                'Failed to read CSV files',
                status=500,
            )

        _set_clone_import_progress(
            'Checking generated join metadata artefacts...',
            current_step='checking_generated_artefacts',
            completed_step=f'Read {len(csv_data)} CSV file(s)',
        )
        generated_artefact_rows, empty_generated_artefacts = _clone_generated_artefact_summary(csv_data)
        if empty_generated_artefacts:
            empty_file_list = ', '.join(empty_generated_artefacts)
            return _clone_import_failure_payload(
                (
                    'Clone cannot continue because generated join metadata artefacts '
                    f'are empty or missing: {empty_file_list}. Run Automode and Create Review '
                    'so these generated artefacts are written to GitHub, then Retrieve Artefacts '
                    'and Clone again.'
                ),
                f'Empty generated artefacts: {empty_file_list}',
                status=400,
                details={
                    'generated_artefact_rows': generated_artefact_rows,
                },
            )

        try:
            from pybirdai.utils.clone_mode import import_from_metadata_export

            _set_clone_import_progress(
                'Clearing current metadata database...',
                current_step='clearing_metadata',
                completed_step='Generated artefacts checked',
            )
            importer = import_from_metadata_export.CSVDataImporter()
            deleted_table_counts = importer.clear_bird_metadata_database()

            _set_clone_import_progress(
                'Importing SMCubes artefacts into metadata database...',
                current_step='importing_metadata',
            )
            results = importer.import_from_csv_strings_ordered(csv_data)

            successful_imports = sum(1 for result in results.values() if result.get('success', False))
            total_objects = sum(
                result.get('imported_count', 0)
                for result in results.values()
                if result.get('success', False)
            )

            message = f'Successfully imported {successful_imports}/{len(results)} CSV files'
            details = f'Total objects imported: {total_objects}'
            all_successful = successful_imports == len(results)
            test_suite_results = {}

            for filename, result in results.items():
                if not result.get('success', False):
                    logger.error(f"Failed to import {filename}: {result.get('error', 'Unknown error')}")

            if all_successful:
                try:
                    _set_clone_import_progress(
                        'Running clone test suite...',
                        current_step='running_test_suite',
                        completed_step=f'Imported {successful_imports}/{len(results)} CSV file(s)',
                    )
                    logger.info("Clone import succeeded; running test suite before completing Task 4")
                    test_suite_results = _run_clone_test_suite()

                    import_summary = {
                        'successful_imports': successful_imports,
                        'total_files': len(results),
                        'total_objects': total_objects,
                        'deleted_tables': len(deleted_table_counts),
                        'deleted_objects': sum(deleted_table_counts.values()),
                        'generated_artefact_rows': generated_artefact_rows,
                    }

                    _set_clone_import_progress(
                        'Marking workflow tasks complete...',
                        current_step='marking_workflow_tasks',
                        completed_step='Test suite finished',
                    )
                    completed_task_numbers = _mark_clone_tasks_completed(
                        import_summary,
                        test_suite_results,
                    )

                    logger.info("Clone import completed: Tasks marked as completed: %s", completed_task_numbers)
                    if completed_task_numbers:
                        message += f" (Tasks {', '.join(str(task) for task in completed_task_numbers)} marked as completed)"

                except Exception as e:
                    logger.error(f"Error marking tasks as completed after clone: {e}")
                    return _clone_import_exception_payload(
                        e,
                        'marking clone workflow tasks complete',
                        'Clone imported CSV files, but workflow tasks could not be marked complete.',
                    )

            test_suite_errors = test_suite_results.get('errors', [])
            tests_executed = bool(test_suite_results.get('tests_executed', False))
            suite_count = len(test_suite_results.get('suites_run', []))
            if all_successful:
                details += f'; test suites run: {suite_count}'
                if test_suite_errors:
                    details += f'; test suite errors: {len(test_suite_errors)}'

            overall_success = all_successful and tests_executed
            if all_successful and not tests_executed:
                message = (
                    f'{message}; clone import completed, but the test suite did not execute.'
                )

            return {
                'success': overall_success,
                'message': message,
                'details': details,
                'results': {
                    'successful_imports': successful_imports,
                    'total_files': len(results),
                    'total_objects': total_objects,
                    'deleted_tables': len(deleted_table_counts),
                    'deleted_objects': sum(deleted_table_counts.values()),
                    'generated_artefact_rows': generated_artefact_rows,
                    'test_suite': test_suite_results,
                },
                'refresh_recommended': True,
            }, 200

        except Exception as e:
            return _clone_import_exception_payload(
                e,
                'workflow clone import',
                'Failed to import CSV files',
            )

    except Exception as e:
        return _clone_import_exception_payload(
            e,
            'workflow clone import',
            'An unexpected error occurred',
        )


def _run_clone_import_async():
    """Run clone import in a background thread and publish status for polling."""
    try:
        close_old_connections()
        _clone_import_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'error': None,
            'message': 'Starting clone import...',
            'started_at': time.time(),
            'completed_at': None,
            'current_step': 'starting',
            'completed_steps': [],
            'result': None,
            'http_status': None,
        })

        payload, status_code = _perform_clone_import()
    except Exception as e:
        payload, status_code = _clone_import_exception_payload(
            e,
            'workflow clone import background thread',
            'An unexpected error occurred',
        )
    finally:
        close_old_connections()

    _clone_import_status.update({
        'running': False,
        'completed': True,
        'success': bool(payload.get('success', False)),
        'error': None if payload.get('success') else payload.get('error') or payload.get('message'),
        'message': payload.get('message', ''),
        'completed_at': time.time(),
        'current_step': 'completed' if payload.get('success') else 'failed',
        'result': payload,
        'http_status': status_code,
    })
    if payload.get('success'):
        _set_clone_import_progress(
            payload.get('message', 'Clone import completed successfully'),
            current_step='completed',
            completed_step='Clone import completed',
        )


def workflow_clone_import(request):
    """Start clone import in the background so long-running imports can be polled."""
    global _clone_import_status

    if _clone_import_status['running']:
        return JsonResponse({
            'success': False,
            'message': 'Clone import is already running. Please wait for completion.',
            'status': 'already_running',
            'check_status_url': '/pybirdai/workflow/clone-import-status/',
        })

    if _clone_import_status['completed']:
        _reset_clone_import_status()

    try:
        _clone_import_status.update({
            'running': True,
            'completed': False,
            'success': False,
            'error': None,
            'message': 'Starting clone import...',
            'started_at': time.time(),
            'completed_at': None,
            'current_step': 'starting',
            'completed_steps': [],
            'result': None,
            'http_status': None,
        })
        clone_thread = threading.Thread(target=_run_clone_import_async, daemon=True)
        clone_thread.start()

        return JsonResponse({
            'success': True,
            'message': 'Clone import started in background. Use /workflow/clone-import-status/ to check progress.',
            'status': 'started',
            'check_status_url': '/pybirdai/workflow/clone-import-status/',
        })

    except Exception as e:
        _reset_clone_import_status()
        return _session_error_response(
            e,
            'starting clone import thread',
            request,
            'Failed to start clone import.',
        )


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
        return _session_error_response(
            e,
            'workflow session validation',
            request,
            'Session validation error',
        )


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
        _reset_clone_import_status()

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
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return _session_error_response(
                e,
                'full workflow session reset',
                request,
                'Failed to reset full workflow session',
            )
        else:
            SecureErrorHandler.secure_message(request, e, 'workflow session reset')
            return redirect('pybirdai:workflow_dashboard')


def workflow_reset_session_partial(request):
    """
    Reset workflow session from task 1 onwards (partial reset).

    """
    logger.info("Partial workflow session reset requested (tasks 1-4) but not database reset")

    try:
        # Reset only automode status (tasks 1-4)
        _reset_automode_status()
        _reset_clone_import_status()

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
