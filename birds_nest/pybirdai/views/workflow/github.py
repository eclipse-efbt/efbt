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
import requests
import json

from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings

# Import unified GitHub service
from pybirdai.services.github_service import GitHubService

logger = logging.getLogger(__name__)


# ============================================================================
# Token Management (delegates to GitHubService)
# ============================================================================

def _get_github_token():
    """Get GitHub token from in-memory storage or environment variable."""
    return GitHubService.get_token() or ""


def _set_github_token(token):
    """Set GitHub token in in-memory storage."""
    GitHubService.set_token(token)


def _clear_github_token():
    """Clear GitHub token from in-memory storage."""
    GitHubService.clear_token()


def export_database_to_github(request):
    """
    Export database to GitHub repository using fork workflow with automatic branch creation and pull request.

    Supports pipeline selection for isolated exports to pipeline-specific repositories.
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

        # Get pipeline selection (new feature for pipeline isolation)
        pipeline = request.POST.get('pipeline', '').strip() or None

        # Get optional framework filter(s) - can be multiple
        framework_ids = request.POST.getlist('framework_ids')
        # Filter out empty strings
        framework_ids = [fid.strip() for fid in framework_ids if fid.strip()]

        # Validate framework selection
        from pybirdai.services.framework_selection import validate_framework_selection
        is_valid, error_message = validate_framework_selection(framework_ids)
        if not is_valid:
            return JsonResponse({'success': False, 'error': error_message})

        # Auto-detect pipeline from frameworks if not explicitly set
        if not pipeline and framework_ids:
            from pybirdai.services.pipeline_repo_service import PipelineRepoService
            service = PipelineRepoService()
            pipeline = service.detect_pipeline_from_frameworks(framework_ids)
            logger.info(f"Auto-detected pipeline: {pipeline} from frameworks: {framework_ids}")

        # Default to 'main' if no pipeline detected
        pipeline = pipeline or 'main'

        # Validate pipeline
        from pybirdai.services.pipeline_repo_service import PIPELINES
        if pipeline not in PIPELINES:
            return JsonResponse({'success': False, 'error': f'Invalid pipeline: {pipeline}. Must be one of {PIPELINES}'})

        # Convert empty list to None for export logic
        framework_ids = framework_ids if framework_ids else None

        if not github_token:
            return JsonResponse({'success': False, 'error': 'GitHub token is required'})

        # Validate repository URL format if provided
        if repository_url and not repository_url.startswith('https://github.com/'):
            return JsonResponse({'success': False, 'error': 'Repository URL must be a valid GitHub URL (https://github.com/...)'})

        # Import the GitHub integration service
        from pybirdai.api.workflow_api import GitHubIntegrationService
        from datetime import datetime

        # Create service instance
        github_service = GitHubIntegrationService(github_token)

        # Export database to CSV first (with optional framework filter)
        from pybirdai.views.core.export_db import _export_database_to_csv_logic
        zip_file_path, extract_dir = _export_database_to_csv_logic(framework_ids=framework_ids)

        # If no repository URL provided, create a new repository
        if not repository_url:
            # Generate repo name based on pipeline, framework and timestamp
            timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            pipeline_suffix = f"-{pipeline}" if pipeline != 'main' else ""
            framework_suffix = f"-{'_'.join(framework_ids)}" if framework_ids else ""
            repo_name = f"pybird-export{pipeline_suffix}{framework_suffix}-{timestamp}"
            framework_display = ', '.join(framework_ids) if framework_ids else None
            pipeline_display = pipeline.upper() if pipeline != 'main' else None
            repo_description = f"PyBIRD AI Database Export"
            if pipeline_display:
                repo_description += f" ({pipeline_display} pipeline)"
            if framework_display:
                repo_description += f" for {framework_display}"

            # Create the repository
            success, repo_data, error_msg = github_service.create_repository(
                repo_name=repo_name,
                description=repo_description,
                private=True,
                organization=organization
            )

            if not success:
                return JsonResponse({
                    'success': False,
                    'error': f'Failed to create repository: {error_msg}'
                })

            # Wait a moment for GitHub to initialize the repo
            import time
            time.sleep(2)

            # Push CSV files directly to main branch
            owner = repo_data['owner']['login']
            repo = repo_data['name']
            files_pushed = github_service.push_csv_files(
                owner=owner,
                repo=repo,
                branch_name='main',
                csv_directory=extract_dir
            )

            return JsonResponse({
                'success': files_pushed,
                'repo_created': True,
                'repository_url': repo_data['html_url'],
                'files_pushed': files_pushed,
                'message': f'New repository created and files exported successfully' if files_pushed else 'Repository created but file upload failed',
                'error': None if files_pushed else 'Failed to push CSV files to new repository'
            })

        if use_fork_workflow:
            # Build PR title and body with pipeline information
            pipeline_display = pipeline.upper() if pipeline != 'main' else None
            pr_title = "PyBIRD AI Database Export"
            if pipeline_display:
                pr_title += f" ({pipeline_display} pipeline)"

            pr_body = f"""## Database Export from PyBIRD AI

This pull request contains CSV files exported from the PyBIRD AI database using the fork workflow.

### Export Details:
- **Pipeline**: {pipeline.upper()}
- Generated automatically by PyBIRD AI's database export functionality
- Fork workflow ensures secure, isolated changes
- Files located in `export/database_export_ldm/`

### Testing:
- [ ] Verify CSV file integrity
- [ ] Check data completeness
- [ ] Validate against expected schema

This export was generated automatically by PyBIRD AI's fork workflow."""

            # Use new fork workflow (default behavior)
            result = github_service.fork_and_create_pr_workflow(
                source_repository_url=repository_url,
                organization=organization,
                csv_directory=extract_dir,
                target_branch=target_branch,
                pr_title=pr_title,
                pr_body=pr_body
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
            result = github_service.export_and_push_to_github(
                repository_url=repository_url,
                framework_ids=framework_ids
            )

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
        logger.error(f"ImportError in export_database_to_github: {e}")
        return JsonResponse({
            'success': False,
            'error': f'GitHub integration service not available: {str(e)}'
        }, status=500)
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error during GitHub export: {e}")
        return JsonResponse({
            'success': False,
            'error': 'GitHub API request timed out. Please try again.'
        }, status=504)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error during GitHub export: {e}")
        return JsonResponse({
            'success': False,
            'error': 'Failed to connect to GitHub. Please check your network connection.'
        }, status=503)
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during GitHub export: {e}")
        status_code = e.response.status_code if e.response else 500
        error_msg = 'GitHub API request failed'
        if status_code == 401:
            error_msg = 'GitHub authentication failed. Please check your token.'
        elif status_code == 403:
            error_msg = 'GitHub access denied. You may not have permission for this operation.'
        elif status_code == 404:
            error_msg = 'GitHub resource not found. Please check the repository URL.'
        elif status_code == 422:
            error_msg = 'GitHub validation failed. Please check your input parameters.'
        return JsonResponse({
            'success': False,
            'error': error_msg
        }, status=status_code)
    except ValueError as e:
        logger.error(f"Validation error in export_database_to_github: {e}")
        return JsonResponse({
            'success': False,
            'error': f'Validation error: {str(e)}'
        }, status=400)
    except Exception as e:
        logger.exception(f"Unexpected error during GitHub export: {e}")
        return JsonResponse({
            'success': False,
            'error': f'An unexpected error occurred: {str(e)}'
        }, status=500)
