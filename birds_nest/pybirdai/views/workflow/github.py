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
import base64
import hashlib

from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings

# Import unified GitHub service
from pybirdai.services.github_service import GitHubService

logger = logging.getLogger(__name__)


# ============================================================================
# Token Encryption (for persisting to config file)
# ============================================================================

def _get_encryption_key():
    """Generate a machine-specific encryption key."""
    import getpass
    import platform

    # Combine machine-specific values for a unique key
    machine_id = f"{platform.node()}-{getpass.getuser()}-pybird-token-key"
    # Create a 32-byte key using SHA256
    return hashlib.sha256(machine_id.encode()).digest()


def _encrypt_token(token: str) -> str:
    """Encrypt a token for storage in config file.

    Uses Fernet if cryptography is available, otherwise uses XOR + base64.
    """
    if not token:
        return ""

    try:
        # Try using cryptography library (more secure)
        from cryptography.fernet import Fernet
        key = base64.urlsafe_b64encode(_get_encryption_key())
        f = Fernet(key)
        encrypted = f.encrypt(token.encode())
        return f"fernet:{base64.urlsafe_b64encode(encrypted).decode()}"
    except ImportError:
        # Fallback: XOR with key + base64 (obfuscation, not true encryption)
        key = _get_encryption_key()
        xored = bytes(a ^ b for a, b in zip(token.encode(), (key * (len(token) // len(key) + 1))[:len(token)]))
        return f"xor:{base64.urlsafe_b64encode(xored).decode()}"


def _decrypt_token(encrypted: str) -> str:
    """Decrypt a token from config file storage."""
    if not encrypted:
        return ""

    try:
        if encrypted.startswith("fernet:"):
            from cryptography.fernet import Fernet
            key = base64.urlsafe_b64encode(_get_encryption_key())
            f = Fernet(key)
            encrypted_data = base64.urlsafe_b64decode(encrypted[7:])
            return f.decrypt(encrypted_data).decode()
        elif encrypted.startswith("xor:"):
            key = _get_encryption_key()
            xored = base64.urlsafe_b64decode(encrypted[4:])
            decrypted = bytes(a ^ b for a, b in zip(xored, (key * (len(xored) // len(key) + 1))[:len(xored)]))
            return decrypted.decode()
        else:
            # Unknown format
            logger.warning("Unknown token encryption format")
            return ""
    except Exception as e:
        logger.warning(f"Failed to decrypt token: {e}")
        return ""


def _get_token_file_path():
    """Get the path to the standalone token file in current birds_nest directory."""
    base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base_dir, '.pybird_github_token')


def _get_all_token_file_paths():
    """Get list of possible token file locations to check."""
    paths = []

    # Current directory (settings.BASE_DIR)
    base_dir = getattr(settings, 'BASE_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    paths.append(os.path.join(base_dir, '.pybird_github_token'))

    # Also check current working directory
    cwd = os.getcwd()
    cwd_path = os.path.join(cwd, '.pybird_github_token')
    if cwd_path not in paths:
        paths.append(cwd_path)

    # Check parent directories (up to 3 levels) for birds_nest folders
    for parent_level in range(1, 4):
        parent = os.path.dirname(base_dir)
        for _ in range(parent_level - 1):
            parent = os.path.dirname(parent)

        # Check for sibling birds_nest directories (like clone_branch)
        if os.path.isdir(parent):
            for item in os.listdir(parent):
                item_path = os.path.join(parent, item)
                if os.path.isdir(item_path):
                    # Check if it's a birds_nest directory
                    token_path = os.path.join(item_path, '.pybird_github_token')
                    if token_path not in paths:
                        paths.append(token_path)
                    # Also check nested birds_nest
                    nested_token = os.path.join(item_path, 'efbt', 'birds_nest', '.pybird_github_token')
                    if nested_token not in paths:
                        paths.append(nested_token)

    return paths


def _get_token_from_config():
    """Load encrypted token from standalone token file. Checks multiple locations."""
    current_path = _get_token_file_path()

    # First check current directory
    try:
        if os.path.exists(current_path):
            with open(current_path, 'r') as f:
                encrypted_token = f.read().strip()
                if encrypted_token:
                    token = _decrypt_token(encrypted_token)
                    if token:
                        return token
    except Exception as e:
        logger.warning(f"Failed to load token from {current_path}: {e}")

    # Check other possible locations
    for token_path in _get_all_token_file_paths():
        if token_path == current_path:
            continue  # Already checked
        try:
            if os.path.exists(token_path):
                with open(token_path, 'r') as f:
                    encrypted_token = f.read().strip()
                    if encrypted_token:
                        token = _decrypt_token(encrypted_token)
                        if token:
                            # Found token elsewhere - save to current directory too!
                            logger.info(f"Found token at {token_path}, copying to {current_path}")
                            _save_token_to_config(token)
                            return token
        except Exception as e:
            logger.debug(f"Could not read token from {token_path}: {e}")

    return ""


def _save_token_to_config(token: str):
    """Save encrypted token to standalone token file."""
    try:
        token_path = _get_token_file_path()
        if token:
            encrypted = _encrypt_token(token)
            with open(token_path, 'w') as f:
                f.write(encrypted)
            logger.info(f"GitHub token saved to {token_path}")
        elif os.path.exists(token_path):
            os.remove(token_path)
            logger.info(f"GitHub token file removed: {token_path}")
    except Exception as e:
        logger.warning(f"Failed to save token to file: {e}")


# ============================================================================
# Token Management (delegates to GitHubService)
# ============================================================================

def _get_github_token(request=None):
    """Get GitHub token from multiple sources in priority order.

    Priority: session → in-memory → config file (encrypted) → environment variable

    Args:
        request: Optional Django request object. If provided, checks session first.

    Returns:
        GitHub token string, or empty string if not found.
    """
    # 1. Check session first if request is provided
    if request is not None:
        try:
            session_token = getattr(request, 'session', {}).get('github_token')
            if session_token:
                # Also update in-memory storage for consistency
                GitHubService.set_token(session_token)
                return session_token
        except Exception:
            pass  # Session might not be available (no database)

    # 2. Check in-memory storage
    import pybirdai.services.github_service as gs
    if gs._github_token_storage:
        return gs._github_token_storage

    # 3. Check config file (.pybird_github_token)
    config_token = _get_token_from_config()
    if config_token:
        GitHubService.set_token(config_token)
        logger.info("Loaded GitHub token from .pybird_github_token file")
        return config_token

    # 4. Check environment variable GITHUB_TOKEN
    env_token = os.environ.get("GITHUB_TOKEN", "")
    if env_token:
        GitHubService.set_token(env_token)
        logger.info("Loaded GitHub token from GITHUB_TOKEN environment variable")
        return env_token

    return ""


def _set_github_token(token):
    """Set GitHub token in in-memory storage AND encrypted config file."""
    # Store in memory
    GitHubService.set_token(token)
    # Also persist to config file (encrypted) for server restart survival
    if token:
        _save_token_to_config(token)


def _clear_github_token():
    """Clear GitHub token from all storage locations."""
    GitHubService.clear_token()
    _save_token_to_config("")  # Remove from config file


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
        repo_private = request.POST.get('repo_private') == 'on'

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
            from pybirdai.context.framework_config import get_pipeline_for_frameworks
            pipeline = get_pipeline_for_frameworks(framework_ids)
            logger.info(f"Auto-detected pipeline: {pipeline} from frameworks: {framework_ids}")

        # Default to 'dpm' if no pipeline detected (most common for reporting frameworks)
        pipeline = pipeline or 'dpm'

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
                private=repo_private,
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
                csv_directory=extract_dir,
                pipeline=pipeline,
                framework_ids=framework_ids
            )

            visibility = 'private' if repo_private else 'public'
            return JsonResponse({
                'success': files_pushed,
                'repo_created': True,
                'repo_private': repo_private,
                'repository_url': repo_data['html_url'],
                'files_pushed': files_pushed,
                'message': f'New {visibility} repository created and files exported successfully' if files_pushed else 'Repository created but file upload failed',
                'error': None if files_pushed else 'Failed to push files to new repository'
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
                pr_body=pr_body,
                pipeline=pipeline,
                framework_ids=framework_ids
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
