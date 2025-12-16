# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
GitHub Utilities for Clone Mode.

This module provides functions for:
- Validating GitHub repository URLs
- Checking if repositories exist and are accessible
- Checking user permissions to create repositories
- Creating new repositories via GitHub API
"""
import re
import logging
import urllib.request
import urllib.error
import json
import os
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)

# GitHub API base URL
GITHUB_API_URL = "https://api.github.com"

# Regex pattern for GitHub repository URLs
GITHUB_URL_PATTERN = re.compile(
    r'^https?://github\.com/([a-zA-Z0-9_-]+)/([a-zA-Z0-9_.-]+?)(?:\.git)?/?$'
)


def parse_github_url(repo_url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a GitHub repository URL to extract owner and repo name.

    Args:
        repo_url: GitHub repository URL

    Returns:
        Tuple of (owner, repo_name) or (None, None) if invalid
    """
    if not repo_url:
        return None, None

    match = GITHUB_URL_PATTERN.match(repo_url.strip())
    if match:
        return match.group(1), match.group(2)

    return None, None


def validate_url_format(repo_url: str) -> Dict[str, Any]:
    """
    Validate that the URL is a properly formatted GitHub repository URL.

    Args:
        repo_url: URL to validate

    Returns:
        Dict with 'valid', 'owner', 'repo', and 'error' keys
    """
    owner, repo = parse_github_url(repo_url)

    if not owner or not repo:
        return {
            'valid': False,
            'owner': None,
            'repo': None,
            'error': 'Invalid GitHub URL format. Expected: https://github.com/owner/repo'
        }

    return {
        'valid': True,
        'owner': owner,
        'repo': repo,
        'error': None
    }


def _make_github_request(
    endpoint: str,
    token: Optional[str] = None,
    method: str = 'GET',
    data: Optional[Dict] = None
) -> Tuple[Optional[Dict], int, Optional[str]]:
    """
    Make a request to the GitHub API.

    Args:
        endpoint: API endpoint (e.g., '/repos/owner/repo')
        token: GitHub personal access token (optional for public repos)
        method: HTTP method
        data: Request body data (for POST/PUT/PATCH)

    Returns:
        Tuple of (response_data, status_code, error_message)
    """
    url = f"{GITHUB_API_URL}{endpoint}"

    headers = {
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'PyBIRD-AI-Clone-Mode'
    }

    if token:
        headers['Authorization'] = f'Bearer {token}'

    body = None
    if data:
        body = json.dumps(data).encode('utf-8')
        headers['Content-Type'] = 'application/json'

    request = urllib.request.Request(url, data=body, headers=headers, method=method)

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_data = json.loads(response.read().decode('utf-8'))
            return response_data, response.status, None
    except urllib.error.HTTPError as e:
        error_body = None
        try:
            error_body = json.loads(e.read().decode('utf-8'))
        except Exception:
            pass

        error_message = error_body.get('message', str(e)) if error_body else str(e)
        return error_body, e.code, error_message
    except urllib.error.URLError as e:
        return None, 0, f"Network error: {str(e)}"
    except Exception as e:
        return None, 0, f"Unexpected error: {str(e)}"


def check_repo_exists(repo_url: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Check if a GitHub repository exists and is accessible.

    Args:
        repo_url: GitHub repository URL
        token: GitHub personal access token (required for private repos)

    Returns:
        Dict with 'exists', 'accessible', 'private', 'error', and 'permissions' keys
    """
    # Validate URL format first
    url_validation = validate_url_format(repo_url)
    if not url_validation['valid']:
        return {
            'exists': False,
            'accessible': False,
            'private': None,
            'error': url_validation['error'],
            'permissions': None
        }

    owner = url_validation['owner']
    repo = url_validation['repo']

    # Try to get repo info
    response, status_code, error = _make_github_request(
        f'/repos/{owner}/{repo}',
        token=token
    )

    if status_code == 200:
        return {
            'exists': True,
            'accessible': True,
            'private': response.get('private', False),
            'error': None,
            'permissions': response.get('permissions', {}),
            'default_branch': response.get('default_branch', 'main'),
            'full_name': response.get('full_name')
        }
    elif status_code == 404:
        return {
            'exists': False,
            'accessible': False,
            'private': None,
            'error': 'Repository not found',
            'permissions': None
        }
    elif status_code == 401:
        return {
            'exists': None,  # Unknown - might exist but requires auth
            'accessible': False,
            'private': None,
            'error': 'Authentication required. Please provide a valid GitHub token.',
            'permissions': None
        }
    elif status_code == 403:
        return {
            'exists': True,  # Exists but access denied
            'accessible': False,
            'private': True,
            'error': 'Access denied. You may not have permission to access this repository.',
            'permissions': None
        }
    else:
        return {
            'exists': None,
            'accessible': False,
            'private': None,
            'error': error or f'GitHub API error (status {status_code})',
            'permissions': None
        }


def can_create_repo_at_location(owner: str, token: str) -> Dict[str, Any]:
    """
    Check if the authenticated user can create a repository at the specified location.

    This checks:
    - If owner matches the authenticated user's username
    - If owner is an organization the user belongs to with repo creation rights

    Args:
        owner: The owner/organization where the repo would be created
        token: GitHub personal access token

    Returns:
        Dict with 'can_create', 'reason', 'owner_type' keys
    """
    if not token:
        return {
            'can_create': False,
            'reason': 'GitHub token is required to create repositories',
            'owner_type': None
        }

    # Get authenticated user info
    user_response, user_status, user_error = _make_github_request(
        '/user',
        token=token
    )

    if user_status != 200:
        return {
            'can_create': False,
            'reason': user_error or 'Failed to authenticate with GitHub',
            'owner_type': None
        }

    authenticated_user = user_response.get('login')

    # Case 1: Owner is the authenticated user
    if owner.lower() == authenticated_user.lower():
        return {
            'can_create': True,
            'reason': 'You can create repositories in your personal account',
            'owner_type': 'user'
        }

    # Case 2: Check if owner is an organization the user belongs to
    # First, check if the owner is an organization
    org_response, org_status, org_error = _make_github_request(
        f'/orgs/{owner}',
        token=token
    )

    if org_status != 200:
        # Not an organization or not accessible
        return {
            'can_create': False,
            'reason': f"'{owner}' is not your username or an organization you have access to",
            'owner_type': None
        }

    # Check user's membership and permissions in the org
    membership_response, membership_status, membership_error = _make_github_request(
        f'/orgs/{owner}/memberships/{authenticated_user}',
        token=token
    )

    if membership_status != 200:
        return {
            'can_create': False,
            'reason': f"You are not a member of the '{owner}' organization",
            'owner_type': 'organization'
        }

    # Check if user has admin role or if org allows members to create repos
    role = membership_response.get('role', '')

    if role == 'admin':
        return {
            'can_create': True,
            'reason': f"You are an admin of the '{owner}' organization",
            'owner_type': 'organization'
        }

    # Check org settings for member repo creation
    if org_response.get('members_can_create_repositories', False):
        return {
            'can_create': True,
            'reason': f"Members of '{owner}' can create repositories",
            'owner_type': 'organization'
        }

    return {
        'can_create': False,
        'reason': f"You don't have permission to create repositories in '{owner}'",
        'owner_type': 'organization'
    }


def create_repository(
    repo_url: str,
    token: str,
    private: bool = True,
    description: str = "PyBIRD AI Clone Mode State Repository"
) -> Dict[str, Any]:
    """
    Create a new GitHub repository.

    Args:
        repo_url: Desired repository URL (https://github.com/owner/repo)
        token: GitHub personal access token
        private: Whether to create a private repository
        description: Repository description

    Returns:
        Dict with 'success', 'repo_url', 'error' keys
    """
    # Validate URL format
    url_validation = validate_url_format(repo_url)
    if not url_validation['valid']:
        return {
            'success': False,
            'repo_url': None,
            'error': url_validation['error']
        }

    owner = url_validation['owner']
    repo = url_validation['repo']

    # Check if we can create at this location
    permission_check = can_create_repo_at_location(owner, token)
    if not permission_check['can_create']:
        return {
            'success': False,
            'repo_url': None,
            'error': permission_check['reason']
        }

    # Determine endpoint based on owner type
    if permission_check['owner_type'] == 'user':
        endpoint = '/user/repos'
        data = {
            'name': repo,
            'private': private,
            'description': description,
            'auto_init': True  # Creates initial commit with README
        }
    else:
        endpoint = f'/orgs/{owner}/repos'
        data = {
            'name': repo,
            'private': private,
            'description': description,
            'auto_init': True
        }

    # Create the repository
    response, status_code, error = _make_github_request(
        endpoint,
        token=token,
        method='POST',
        data=data
    )

    if status_code == 201:
        return {
            'success': True,
            'repo_url': response.get('html_url'),
            'error': None,
            'full_name': response.get('full_name'),
            'default_branch': response.get('default_branch', 'main')
        }
    elif status_code == 422:
        # Validation error - repo might already exist
        return {
            'success': False,
            'repo_url': None,
            'error': error or 'Repository name already exists or is invalid'
        }
    else:
        return {
            'success': False,
            'repo_url': None,
            'error': error or f'Failed to create repository (status {status_code})'
        }


def validate_repo_for_clone(
    repo_url: str,
    token: Optional[str] = None,
    operation: str = 'save'
) -> Dict[str, Any]:
    """
    Comprehensive validation for clone mode operations.

    This is the main entry point for validating a repository before
    save or load operations.

    Args:
        repo_url: GitHub repository URL
        token: GitHub personal access token
        operation: 'save' or 'load'

    Returns:
        Dict with comprehensive validation results:
        - 'valid': bool - Overall validity for the operation
        - 'exists': bool - Whether repo exists
        - 'can_create': bool - Whether user can create repo if it doesn't exist
        - 'has_push_access': bool - Whether user can push (for save operations)
        - 'error': str - Error message if any
        - 'action_required': str - What action is needed ('none', 'create', 'error')
    """
    result = {
        'valid': False,
        'exists': False,
        'can_create': False,
        'has_push_access': False,
        'error': None,
        'action_required': 'error',
        'owner': None,
        'repo': None
    }

    # Validate URL format
    url_validation = validate_url_format(repo_url)
    if not url_validation['valid']:
        result['error'] = url_validation['error']
        return result

    result['owner'] = url_validation['owner']
    result['repo'] = url_validation['repo']

    # Check if repo exists
    repo_check = check_repo_exists(repo_url, token)
    result['exists'] = repo_check['exists']

    if repo_check['exists']:
        result['accessible'] = repo_check['accessible']

        if not repo_check['accessible']:
            result['error'] = repo_check['error']
            return result

        # For save operations, check push permissions
        if operation == 'save':
            permissions = repo_check.get('permissions', {})
            has_push = permissions.get('push', False)

            if has_push:
                result['valid'] = True
                result['has_push_access'] = True
                result['action_required'] = 'none'
                result['default_branch'] = repo_check.get('default_branch', 'main')
            else:
                result['error'] = 'You do not have push access to this repository'
        else:
            # For load operations, read access is sufficient
            result['valid'] = True
            result['action_required'] = 'none'
            result['default_branch'] = repo_check.get('default_branch', 'main')

    else:
        # Repo doesn't exist
        if operation == 'load':
            result['error'] = 'Repository not found'
            return result

        # For save operations, check if user can create the repo
        if token:
            create_check = can_create_repo_at_location(
                url_validation['owner'],
                token
            )
            result['can_create'] = create_check['can_create']

            if create_check['can_create']:
                result['action_required'] = 'create'
                result['valid'] = True  # Valid if user chooses to create
            else:
                result['error'] = create_check['reason']
        else:
            result['error'] = 'Repository not found. Provide a GitHub token to create a new repository.'

    return result
