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

logger = logging.getLogger(__name__)

# In-memory storage for GitHub token (not persisted to database or file)
_in_memory_github_token = None

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
        organization = request.POST.get('organization', '').strip() or ""
        target_branch = request.POST.get('target_branch', 'develop').strip()
        use_fork_workflow = request.POST.get('use_fork_workflow') == 'on'

        if not github_token:
            return JsonResponse({'success': False, 'error': 'GitHub token is required'})

        # Validate repository URL format if provided
        if repository_url and not repository_url.startswith('https://github.com/'):
            return JsonResponse({'success': False, 'error': 'Repository URL must be a valid GitHub URL (https://github.com/...)'})

        # Import the GitHub integration service
        from pybirdai.api.workflow_api import GitHubIntegrationService

        # Create service instance
        github_service = GitHubIntegrationService(github_token)

        # Rebuild artefacts from the current database before any PR push.
        from pybirdai.views.core.export_db import _export_database_to_csv_enhanced
        zip_file_path, extract_dir = _export_database_to_csv_enhanced()

        # Determine repository URL (use automode config if not provided)
        if not repository_url:
            repository_url = github_service.get_github_url_from_automode_config() or 'https://github.com/regcommunity/FreeBIRD_EIL_67'

        if use_fork_workflow:
            # Use new fork workflow (default behavior)
            result = github_service.fork_and_create_pr_workflow(
                source_repository_url=repository_url,
                organization=organization,
                csv_directory=extract_dir,
                target_branch=target_branch,
                pr_title="PyBIRD AI Database Export",
                pr_body="""## Database Export from PyBIRD AI

This pull request contains files exported from the PyBIRD AI database using the fork workflow.

### Export Details:
- Generated automatically by PyBIRD AI's database export functionality
- Fork workflow ensures secure, isolated changes
- Files located in `artefacts/` directory:
  - `smcubes_artefacts/` - Database CSV files
  - `filter_code/` - Filter code (production and staging)
  - `derivation_files/` - Derivation rules and config
  - `joins_configuration/` - Joins configuration files

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
