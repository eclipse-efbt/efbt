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
import re
import time
import uuid

from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib import messages
from django.conf import settings
from django.utils import timezone
from django.db import transaction

from pybirdai.models.workflow_model import (
    WorkflowTaskExecution, WorkflowSession,
    DPMProcessExecution, AnaCreditProcessExecution
)
from pybirdai.api.workflow_api import AutomodeConfigurationService
from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

from .status import (
    _reset_migration_status, _reset_database_setup_status,
    _reset_automode_status, _reset_setup_database_models_status
)
from .github import _set_github_token, _clear_github_token

logger = logging.getLogger(__name__)


def _get_clone_state_summary() -> dict:
    """
    Generate a summary of the clone state after a successful load.

    Returns dict with:
    - export_status: 'COMPLETE' or 'INCOMPLETE'
    - completed_tests: list of workflows that passed tests
    - last_step_completed: e.g., 'DPM_STEP2_PROCESS_TABLES'
    - workflows: status for main, dpm, anacredit
    - next_url: suggested navigation path
    """
    try:
        from pybirdai.utils.clone_mode.process_metadata import generate_process_metadata

        metadata = generate_process_metadata()
        workflows = metadata.get('workflows', {})
        export_status = metadata.get('export_status', {})

        # Build workflow summaries
        main = workflows.get('main', {})
        dpm = workflows.get('dpm', {})
        anacredit = workflows.get('anacredit', {})

        workflow_summaries = {}

        # Main workflow
        main_complete = main.get('is_complete', False)
        main_last_step = main.get('last_step_completed', 0)
        main_total = main.get('total_steps', 4)
        workflow_summaries['main'] = {
            'status': 'COMPLETE' if main_complete else f'Step {main_last_step} of {main_total}',
            'progress': f'{main_last_step}/{main_total}',
            'is_complete': main_complete,
        }

        # DPM workflow
        dpm_complete = dpm.get('is_complete', False)
        dpm_last_step = dpm.get('last_step_completed', 0)
        dpm_total = dpm.get('total_steps', 6)
        dpm_source = dpm.get('source_type', 'eba')
        workflow_summaries['dpm'] = {
            'status': 'COMPLETE' if dpm_complete else f'Step {dpm_last_step} of {dpm_total}',
            'progress': f'{dpm_last_step}/{dpm_total}',
            'is_complete': dpm_complete,
            'source_type': dpm_source,
        }

        # AnaCredit workflow
        anacredit_complete = anacredit.get('is_complete', False)
        anacredit_last_step = anacredit.get('last_step_completed', 0)
        anacredit_total = anacredit.get('total_steps', 4)
        workflow_summaries['anacredit'] = {
            'status': 'COMPLETE' if anacredit_complete else f'Step {anacredit_last_step} of {anacredit_total}',
            'progress': f'{anacredit_last_step}/{anacredit_total}',
            'is_complete': anacredit_complete,
        }

        # Get completed tests
        completed_tests = export_status.get('completed_workflows', [])
        is_export_complete = export_status.get('is_complete', False)

        # Determine next URL based on where to continue
        next_url = '/pybirdai/workflow/dashboard/'
        last_step = metadata.get('last_step_completed')

        if last_step:
            # Parse the last step to suggest next URL
            if 'DPM_STEP' in str(last_step):
                # Extract step number and suggest next step
                try:
                    step_num = int(str(last_step).split('STEP')[1].split('_')[0])
                    next_step = step_num + 1
                    if next_step <= dpm_total:
                        next_url = f'/pybirdai/workflow/dpm/step{next_step}/'
                except (ValueError, IndexError):
                    pass
            elif 'MAIN_TASK' in str(last_step):
                try:
                    task_num = int(str(last_step).split('TASK')[1].split('_')[0])
                    next_task = task_num + 1
                    if next_task <= main_total:
                        next_url = f'/pybirdai/workflow/step/{next_task}/'
                except (ValueError, IndexError):
                    pass

        return {
            'export_status': 'COMPLETE' if is_export_complete else 'INCOMPLETE',
            'completed_tests': completed_tests,
            'last_step_completed': last_step,
            'workflows': workflow_summaries,
            'next_url': next_url,
        }

    except Exception as e:
        logger.warning(f"Error generating clone state summary: {e}")
        # Return a default summary instead of None
        return {
            'export_status': 'UNKNOWN',
            'completed_tests': [],
            'last_step_completed': None,
            'workflows': {
                'main': {'status': 'Unknown', 'progress': '0/4', 'is_complete': False},
                'dpm': {'status': 'Unknown', 'progress': '0/6', 'is_complete': False, 'source_type': 'eba'},
                'anacredit': {'status': 'Unknown', 'progress': '0/4', 'is_complete': False},
            },
            'next_url': '/pybirdai/workflow/dashboard/',
        }


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
                    # Use atomic transaction to prevent race conditions
                    with transaction.atomic():
                        # Mark Task 1 (SMCubes Core Creation) as completed
                        task1_do, created = WorkflowTaskExecution.objects.select_for_update().get_or_create(
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
                            task1_do.save(update_fields=['status', 'completed_at', 'execution_data'])

                        # Mark Task 2 (SMCubes Transformation Rules Creation) as completed
                        task2_do, created = WorkflowTaskExecution.objects.select_for_update().get_or_create(
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
                            task2_do.save(update_fields=['status', 'completed_at', 'execution_data'])

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
def clone_save_local(request):
    """
    Save clone state to local directory.
    Exports database to CSV files in results/clone_export/database_export/
    """
    logger.info("Clone save to local requested")

    try:
        from django.core.management import call_command
        from io import StringIO

        force = request.POST.get('force', 'false').lower() == 'true'

        # Call the management command
        out = StringIO()
        call_command(
            'save_clone_state',
            '--local-only',
            *(['--force'] if force else []),
            stdout=out,
            verbosity=1
        )

        output = out.getvalue()
        logger.info(f"Save clone state output: {output}")

        # Get export info
        base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        export_path = os.path.join(base_dir, 'results', 'clone_export', 'database_export')

        # Count files and calculate size
        file_count = 0
        total_size = 0
        if os.path.exists(export_path):
            for f in os.listdir(export_path):
                if f.endswith('.csv') or f.endswith('.json'):
                    file_count += 1
                    total_size += os.path.getsize(os.path.join(export_path, f))

        # Format size
        if total_size > 1024 * 1024:
            size_str = f"{total_size / (1024 * 1024):.2f} MB"
        elif total_size > 1024:
            size_str = f"{total_size / 1024:.2f} KB"
        else:
            size_str = f"{total_size} bytes"

        return JsonResponse({
            'success': True,
            'message': 'Database state exported successfully',
            'details': {
                'file_count': file_count,
                'total_size': size_str,
                'export_path': export_path
            }
        })

    except Exception as e:
        logger.error(f"Error in clone_save_local: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to export database state',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_get_save_targets(request):
    """
    Get list of allowed save targets for clone mode.

    Returns the user's personal account and organizations they belong to.
    The repository name is always fixed to 'pybirdai_workplace'.
    """
    logger.info("Clone get save targets requested")

    try:
        from pybirdai.services.github_service import GitHubService

        token = request.POST.get('token', '').strip()

        if not token:
            return JsonResponse({
                'success': False,
                'message': 'GitHub token is required',
                'error': 'Missing token parameter'
            }, status=400)

        github_service = GitHubService(token=token)
        result = github_service.get_allowed_save_targets()

        if result['success']:
            return JsonResponse({
                'success': True,
                'username': result['username'],
                'targets': result['targets']
            })
        else:
            return JsonResponse({
                'success': False,
                'message': result['error'],
                'error': result['error']
            }, status=400)

    except Exception as e:
        logger.error(f"Error in clone_get_save_targets: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to get save targets',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_get_load_sources(request):
    """
    Get list of allowed load sources for clone mode.

    Returns the user's repos (if authenticated) plus default regcommunity repos.
    """
    logger.info("Clone get load sources requested")

    try:
        from pybirdai.services.github_service import GitHubService

        token = request.POST.get('token', '').strip() or None

        github_service = GitHubService(token=token)
        result = github_service.get_allowed_load_sources()

        return JsonResponse({
            'success': True,
            'sources': result['sources']
        })

    except Exception as e:
        logger.error(f"Error in clone_get_load_sources: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to get load sources',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_save_github(request):
    """
    Save clone state to GitHub repository.

    RESTRICTED: Only allows saving to user's personal pybirdai_workplace repo
    or their organization's pybirdai_workplace repo.

    Accepts 'target_owner' (username or org name) instead of full repo URL.
    The repo name is always fixed to 'pybirdai_workplace'.
    """
    logger.info("Clone save to GitHub requested")

    try:
        from django.core.management import call_command
        from io import StringIO
        from pybirdai.services.github_service import GitHubService, CLONE_MODE_REPO_NAME

        target_owner = request.POST.get('target_owner', '').strip()
        branch = request.POST.get('branch', 'main').strip()
        token = request.POST.get('token', '').strip()
        commit_message = request.POST.get('commit_message', 'Update clone state').strip()
        force = request.POST.get('force', 'false').lower() == 'true'

        if not token:
            return JsonResponse({
                'success': False,
                'message': 'GitHub token is required',
                'error': 'Missing token parameter'
            }, status=400)

        if not target_owner:
            return JsonResponse({
                'success': False,
                'message': 'Target owner is required',
                'error': 'Missing target_owner parameter'
            }, status=400)

        # Validate that the target is allowed
        github_service = GitHubService(token=token)
        validation = github_service.validate_save_target(target_owner)

        if not validation['valid']:
            logger.warning(f"Save target validation failed: {validation['error']}")
            return JsonResponse({
                'success': False,
                'message': validation['error'],
                'error': validation['error']
            }, status=403)

        # Construct the repo URL from validated target
        repo_url = validation['repo_url']
        logger.info(f"Validated save target: {repo_url} (type: {validation['owner_type']})")

        # Build command arguments
        args = ['--repo-url', repo_url, '--branch', branch]
        if token:
            args.extend(['--token', token])
        if commit_message:
            args.extend(['--commit-message', commit_message])
        if force:
            args.append('--force')

        # Call the management command
        out = StringIO()
        call_command('save_clone_state', *args, stdout=out, verbosity=1)

        output = out.getvalue()
        logger.info(f"Save clone state to GitHub output: {output}")

        # Extract commit SHA from command output
        # The command outputs: "Pushed commit <SHA[:8]> to <branch>"
        commit_match = re.search(r'Pushed commit ([a-f0-9]+) to', output)
        commit_sha = commit_match.group(1) if commit_match else 'N/A'

        return JsonResponse({
            'success': True,
            'message': 'Database state pushed to GitHub successfully',
            'details': {
                'repo_url': repo_url,
                'branch': branch,
                'commit_sha': commit_sha,
                'target_type': validation['owner_type']
            }
        })

    except Exception as e:
        logger.error(f"Error in clone_save_github: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to push to GitHub',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_load_local(request):
    """
    Load clone state from local directory.
    Imports database from CSV files.
    """
    logger.info("Clone load from local requested")

    try:
        from django.core.management import call_command
        from io import StringIO

        local_path = request.POST.get('local_path', '').strip()
        force = request.POST.get('force', 'false').lower() == 'true'
        skip_cleanup = request.POST.get('skip_cleanup', 'false').lower() == 'true'

        if not local_path:
            return JsonResponse({
                'success': False,
                'message': 'Local path is required',
                'error': 'Missing local_path parameter'
            }, status=400)

        # Build command arguments
        args = ['--local-path', local_path]
        if force:
            args.append('--force')
        if skip_cleanup:
            args.append('--skip-cleanup')

        # Call the management command
        out = StringIO()
        call_command('load_clone_state', *args, stdout=out, verbosity=1)

        output = out.getvalue()
        logger.info(f"Load clone state output: {output}")

        # Try to get import stats from the output or results file
        file_count = 0
        record_count = 0

        # Check for import results file
        base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        results_dir = os.path.join(base_dir, 'import_results')
        if os.path.exists(results_dir):
            results_files = sorted([
                f for f in os.listdir(results_dir)
                if f.startswith('ordered_import_results_')
            ], reverse=True)
            if results_files:
                latest_result = os.path.join(results_dir, results_files[0])
                try:
                    with open(latest_result, 'r') as f:
                        import_data = json.load(f)
                        for table, info in import_data.items():
                            if info.get('success'):
                                file_count += 1
                                record_count += info.get('imported_count', 0)
                except Exception as e:
                    logger.warning(f"Could not read import results: {e}")

        # Get clone state summary after successful import
        clone_state_summary = _get_clone_state_summary()

        return JsonResponse({
            'success': True,
            'message': 'Database state imported successfully',
            'details': {
                'file_count': file_count,
                'record_count': record_count
            },
            'clone_state_summary': clone_state_summary,
            'refresh_recommended': True
        })

    except Exception as e:
        logger.error(f"Error in clone_load_local: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to import database state',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_load_github(request):
    """
    Load clone state from GitHub repository.

    RESTRICTED: Only allows loading from:
    1. User's own pybirdai_workplace repo (personal or org)
    2. Official regcommunity repos (FreeBIRD, FreeBIRD_IL_66, bird-default-test-suite)

    Accepts 'repo_url' which is validated against allowed sources.
    """
    logger.info("Clone load from GitHub requested")

    try:
        from django.core.management import call_command
        from io import StringIO
        from pybirdai.services.github_service import GitHubService

        repo_url = request.POST.get('repo_url', '').strip()
        branch = request.POST.get('branch', 'main').strip()
        token = request.POST.get('token', '').strip() or None
        force = request.POST.get('force', 'false').lower() == 'true'
        skip_cleanup = request.POST.get('skip_cleanup', 'false').lower() == 'true'

        if not repo_url:
            return JsonResponse({
                'success': False,
                'message': 'Repository URL is required',
                'error': 'Missing repo_url parameter'
            }, status=400)

        # Validate that the source is allowed
        github_service = GitHubService(token=token)
        validation = github_service.is_allowed_load_repo(repo_url)

        if not validation['allowed']:
            logger.warning(f"Load source validation failed: {validation['error']}")
            return JsonResponse({
                'success': False,
                'message': validation['error'],
                'error': validation['error']
            }, status=403)

        logger.info(f"Validated load source: {repo_url} (type: {validation['source_type']})")

        # Build command arguments
        args = ['--repo-url', repo_url, '--branch', branch]
        if token:
            args.extend(['--token', token])
        if force:
            args.append('--force')
        if skip_cleanup:
            args.append('--skip-cleanup')

        # Call the management command
        out = StringIO()
        call_command('load_clone_state', *args, stdout=out, verbosity=1)

        output = out.getvalue()
        logger.info(f"Load clone state from GitHub output: {output}")

        # Try to get import stats
        record_count = 0
        base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        results_dir = os.path.join(base_dir, 'import_results')
        if os.path.exists(results_dir):
            results_files = sorted([
                f for f in os.listdir(results_dir)
                if f.startswith('ordered_import_results_')
            ], reverse=True)
            if results_files:
                latest_result = os.path.join(results_dir, results_files[0])
                try:
                    with open(latest_result, 'r') as f:
                        import_data = json.load(f)
                        for table, info in import_data.items():
                            if info.get('success'):
                                record_count += info.get('imported_count', 0)
                except Exception as e:
                    logger.warning(f"Could not read import results: {e}")

        # Get clone state summary after successful import
        clone_state_summary = _get_clone_state_summary()

        return JsonResponse({
            'success': True,
            'message': 'Database state imported from GitHub successfully',
            'details': {
                'repo_url': repo_url,
                'branch': branch,
                'record_count': record_count,
                'source_type': validation['source_type']
            },
            'clone_state_summary': clone_state_summary,
            'refresh_recommended': True
        })

    except Exception as e:
        logger.error(f"Error in clone_load_github: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to import from GitHub',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_validate_repo(request):
    """
    Validate a GitHub repository URL for clone mode operations.

    Checks:
    - URL format validity
    - Whether repository exists
    - User's access permissions
    - Whether user can create the repo if it doesn't exist

    Returns JSON with validation results and suggested action.
    """
    logger.info("Clone validate repo requested")

    try:
        from pybirdai.services.github_service import GitHubService

        repo_url = request.POST.get('repo_url', '').strip()
        token = request.POST.get('token', '').strip() or None
        operation = request.POST.get('operation', 'save').strip()

        if not repo_url:
            return JsonResponse({
                'success': False,
                'message': 'Repository URL is required',
                'error': 'Missing repo_url parameter'
            }, status=400)

        # Validate the repository using unified service
        github_service = GitHubService(token=token)
        validation_result = github_service.validate_for_operation(
            repo_url=repo_url,
            operation=operation
        )

        logger.info(f"Validation result for {repo_url}: {validation_result}")

        return JsonResponse({
            'success': True,
            'validation': validation_result
        })

    except Exception as e:
        logger.error(f"Error in clone_validate_repo: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Validation failed',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_create_repo(request):
    """
    Create a new GitHub repository for clone mode.

    Creates a new repository at the specified URL if the user has permission.
    """
    logger.info("Clone create repo requested")

    try:
        from pybirdai.services.github_service import GitHubService

        repo_url = request.POST.get('repo_url', '').strip()
        token = request.POST.get('token', '').strip()
        private = request.POST.get('private', 'true').lower() == 'true'
        description = request.POST.get(
            'description',
            'PyBIRD AI Clone Mode State Repository'
        ).strip()

        if not repo_url:
            return JsonResponse({
                'success': False,
                'message': 'Repository URL is required',
                'error': 'Missing repo_url parameter'
            }, status=400)

        if not token:
            return JsonResponse({
                'success': False,
                'message': 'GitHub token is required to create repositories',
                'error': 'Missing token parameter'
            }, status=400)

        # Parse URL to get owner and repo
        owner, repo = GitHubService.parse_url(repo_url)
        if not owner or not repo:
            return JsonResponse({
                'success': False,
                'message': 'Invalid GitHub URL format',
                'error': 'Expected: https://github.com/owner/repo'
            }, status=400)

        # Create the repository using unified service
        github_service = GitHubService(token=token)
        create_result = github_service.create_repository(
            owner=owner,
            repo=repo,
            private=private,
            description=description
        )

        if create_result['success']:
            repo_data = create_result.get('repo_data', {})
            logger.info(f"Successfully created repository: {create_result['repo_url']}")
            return JsonResponse({
                'success': True,
                'message': 'Repository created successfully',
                'details': {
                    'repo_url': create_result['repo_url'],
                    'full_name': repo_data.get('full_name'),
                    'default_branch': create_result.get('default_branch', 'main')
                }
            })
        else:
            logger.warning(f"Failed to create repository: {create_result['error']}")
            return JsonResponse({
                'success': False,
                'message': 'Failed to create repository',
                'error': create_result['error']
            }, status=400)

    except Exception as e:
        logger.error(f"Error in clone_create_repo: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to create repository',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def clone_get_user(request):
    """
    Get the authenticated GitHub user's information from their token.

    Used by the "Create Save Space" feature to determine the user's
    GitHub username for creating a personal repository.
    """
    logger.info("Clone get user requested")

    try:
        from pybirdai.services.github_service import GitHubService

        token = request.POST.get('token', '').strip()

        if not token:
            return JsonResponse({
                'success': False,
                'message': 'GitHub token is required',
                'error': 'Missing token parameter'
            }, status=400)

        # Fetch authenticated user info using unified service
        github_service = GitHubService(token=token)
        user_result = github_service.get_authenticated_user()

        if user_result['success']:
            user_data = user_result.get('user_data', {})
            username = user_result['username']
            logger.info(f"Successfully fetched GitHub user: {username}")
            return JsonResponse({
                'success': True,
                'username': username,
                'name': user_data.get('name'),
                'avatar_url': user_data.get('avatar_url')
            })
        else:
            error_msg = user_result.get('error', 'Failed to get user information')
            # Check for authentication errors
            if 'Authentication' in error_msg or 'token' in error_msg.lower():
                return JsonResponse({
                    'success': False,
                    'message': 'Invalid or expired GitHub token',
                    'error': error_msg
                }, status=401)
            return JsonResponse({
                'success': False,
                'message': 'Failed to get user information',
                'error': error_msg
            }, status=400)

    except Exception as e:
        logger.error(f"Error in clone_get_user: {e}", exc_info=True)
        return JsonResponse({
            'success': False,
            'message': 'Failed to get user information',
            'error': str(e)
        }, status=500)


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


@require_http_methods(["POST"])
def workflow_reset_database(request):
    """
    Reset the entire database (full database wipe).

    This deletes ALL data from the database including:
    - All framework data (FINREP, ANCRDT, DPM, etc.)
    - All input model data (DOMAIN, VARIABLE, MEMBER)
    - All cubes, mappings, and transformations

    This action cannot be undone.
    """
    logger.info("Full database reset requested")

    try:
        # Execute the database deletion
        logger.info("Executing RunDeleteBirdMetadataDatabase...")
        app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
        result = app_config.run_delete_bird_metadata_database()

        logger.info(f"Database reset completed: {result}")

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': True,
                'message': 'Database reset completed successfully. All framework data and input model have been deleted.',
                'details': result if isinstance(result, dict) else {'status': 'completed'}
            })
        else:
            messages.success(request, 'Database reset completed successfully.')
            return redirect('pybirdai:workflow_dashboard')

    except Exception as e:
        logger.error(f"Error during database reset: {e}", exc_info=True)
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': False,
                'message': 'Failed to reset database',
                'error': str(e)
            }, status=500)
        else:
            messages.error(request, f'Failed to reset database: {str(e)}')
            return redirect('pybirdai:workflow_dashboard')
