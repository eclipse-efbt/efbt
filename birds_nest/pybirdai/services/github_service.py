# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Unified GitHub Service for PyBIRD AI.

This module provides a single, unified service for all GitHub operations:
- Authentication and token management
- User/organization info
- Repository operations (validation, creation, checking)
- File operations (fetch, download)
- Push operations (files, commits)
- Clone mode operations (export/import state)
- Fork and PR workflows
"""

import re
import os
import json
import base64
import logging
import urllib.request
import urllib.error
from typing import Tuple, Optional, Dict, Any, List

logger = logging.getLogger(__name__)


# ============================================================================
# Constants
# ============================================================================

GITHUB_API_URL = "https://api.github.com"
GITHUB_API_VERSION = "2022-11-28"
USER_AGENT = "PyBIRD-AI"

# Clone Mode Configuration
# Fixed repository name for clone mode save operations
CLONE_MODE_REPO_NAME = "pybirdai_workplace"

# Allowed repositories for loading clone state (regcommunity default repos)
ALLOWED_LOAD_REPOS = [
    "regcommunity/FreeBIRD_IL_66",
    "regcommunity/FreeBIRD_IL_66_C07",  # COREP C07.00 package
    "regcommunity/FreeBIRD",
    "regcommunity/bird-default-test-suite",
]

# Regex pattern for GitHub repository URLs
GITHUB_URL_PATTERN = re.compile(
    r'^https?://github\.com/([a-zA-Z0-9_-]+)/([a-zA-Z0-9_.-]+?)(?:\.git)?/?$'
)


# ============================================================================
# Singleton Token Storage
# ============================================================================

_github_token_storage: Optional[str] = None


class GitHubService:
    """
    Unified GitHub service for all PyBIRD AI GitHub operations.

    This service provides:
    - Token management and authentication
    - URL validation and parsing
    - Repository operations (create, check, validate)
    - File operations (fetch, download)
    - Push operations (files, commits)
    - Fork and PR workflows

    Usage:
        # With explicit token
        service = GitHubService(token="ghp_...")
        
        # Using stored token
        GitHubService.set_token("ghp_...")
        service = GitHubService()
        
        # Operations
        user = service.get_authenticated_user()
        service.create_repository("owner", "repo", private=True)
    """
    
    def __init__(self, token: Optional[str] = None):
        """
        Initialize the GitHub service.
        
        Args:
            token: GitHub personal access token. If not provided, uses:
                   1. Stored token (via set_token)
                   2. GITHUB_TOKEN environment variable
        """
        self._token = token or self.get_token()
    
    # ========================================================================
    # Token Management (Class Methods)
    # ========================================================================
    
    @classmethod
    def set_token(cls, token: Optional[str]) -> None:
        """
        Set GitHub token in memory storage.
        
        Args:
            token: GitHub personal access token (or None to clear)
        """
        global _github_token_storage
        _github_token_storage = token.strip() if token else None
    
    @classmethod
    def get_token(cls) -> Optional[str]:
        """
        Get GitHub token from storage or environment.
        
        Returns:
            Token string or None if not set
        """
        global _github_token_storage
        return _github_token_storage or os.environ.get("GITHUB_TOKEN")
    
    @classmethod
    def clear_token(cls) -> None:
        """Clear GitHub token from memory storage."""
        global _github_token_storage
        _github_token_storage = None
    
    @classmethod
    def validate_token_format(cls, token: str) -> bool:
        """
        Validate GitHub token format.
        
        Args:
            token: Token to validate
            
        Returns:
            True if token appears to be valid format
        """
        if not token:
            return False
        token = token.strip()
        # GitHub PATs start with ghp_ or github_pat_
        return token.startswith('ghp_') or token.startswith('github_pat_')
    
    @property
    def token(self) -> Optional[str]:
        """Get the token for this instance."""
        return self._token
    
    @property
    def has_token(self) -> bool:
        """Check if this instance has a token."""
        return bool(self._token)
    
    # ========================================================================
    # HTTP Request Helpers
    # ========================================================================
    
    def _get_headers(self, include_content_type: bool = False) -> Dict[str, str]:
        """
        Get headers for GitHub API requests.
        
        Args:
            include_content_type: Include Content-Type header for POST/PUT
            
        Returns:
            Headers dictionary
        """
        headers = {
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': GITHUB_API_VERSION,
            'User-Agent': USER_AGENT
        }
        
        if self._token:
            headers['Authorization'] = f'Bearer {self._token}'
        
        if include_content_type:
            headers['Content-Type'] = 'application/json'
        
        return headers
    
    def _make_request(
        self,
        endpoint: str,
        method: str = 'GET',
        data: Optional[Dict] = None,
        timeout: int = 30
    ) -> Tuple[Optional[Dict], int, Optional[str]]:
        """
        Make a request to the GitHub API.
        
        Args:
            endpoint: API endpoint (e.g., '/repos/owner/repo')
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            data: Request body data (for POST/PUT/PATCH)
            timeout: Request timeout in seconds
            
        Returns:
            Tuple of (response_data, status_code, error_message)
        """
        url = f"{GITHUB_API_URL}{endpoint}"
        headers = self._get_headers(include_content_type=data is not None)
        
        body = None
        if data:
            body = json.dumps(data).encode('utf-8')
        
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
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
    
    # ========================================================================
    # URL Parsing and Validation
    # ========================================================================
    
    @staticmethod
    def parse_url(repo_url: str) -> Tuple[Optional[str], Optional[str]]:
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
    
    @staticmethod
    def validate_url(repo_url: str) -> Dict[str, Any]:
        """
        Validate that the URL is a properly formatted GitHub repository URL.
        
        Args:
            repo_url: URL to validate
            
        Returns:
            Dict with 'valid', 'owner', 'repo', and 'error' keys
        """
        owner, repo = GitHubService.parse_url(repo_url)
        
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
    
    @staticmethod
    def build_url(owner: str, repo: str) -> str:
        """
        Build a GitHub repository URL from owner and repo.
        
        Args:
            owner: Repository owner
            repo: Repository name
            
        Returns:
            Full GitHub URL
        """
        return f"https://github.com/{owner}/{repo}"
    
    @staticmethod
    def get_raw_url(owner: str, repo: str, branch: str, path: str) -> str:
        """
        Get raw content URL for a file.

        Args:
            owner: Repository owner
            repo: Repository name
            branch: Branch name
            path: File path within repository

        Returns:
            Raw content URL
        """
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"

    @staticmethod
    def sanitize_url_for_logging(url: str) -> str:
        """
        Remove credentials and sensitive parameters from URL for safe logging.

        This prevents accidental exposure of tokens or credentials in log files.

        Args:
            url: URL that may contain sensitive information

        Returns:
            Sanitized URL safe for logging
        """
        import urllib.parse

        try:
            parsed = urllib.parse.urlparse(url)
            # Remove query params that might contain tokens
            sanitized = urllib.parse.urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                '',  # params
                '',  # query
                ''   # fragment
            ))
            return sanitized
        except Exception:
            # If parsing fails, return a generic message
            return "<URL parsing failed>"

    @staticmethod
    def validate_token(token: str) -> dict:
        """
        Validate GitHub token format and structure.

        Checks for:
        - Valid prefix (ghp_, github_pat_, ghu_, ghs_)
        - Minimum length requirements
        - No leading/trailing whitespace

        Args:
            token: Token string to validate

        Returns:
            Dict with 'valid', 'error', and 'token_type' keys
        """
        if not token:
            return {
                'valid': False,
                'error': 'Token is required',
                'token_type': None
            }

        token = token.strip()
        if len(token) < 20:
            return {
                'valid': False,
                'error': 'Token is too short (minimum 20 characters)',
                'token_type': None
            }

        # GitHub token prefixes
        token_prefixes = {
            'ghp_': 'personal_access_token',
            'github_pat_': 'fine_grained_pat',
            'ghu_': 'user_to_server_token',
            'ghs_': 'server_to_server_token',
            'gho_': 'oauth_token',
            'ghr_': 'refresh_token'
        }

        for prefix, token_type in token_prefixes.items():
            if token.startswith(prefix):
                return {
                    'valid': True,
                    'error': None,
                    'token_type': token_type
                }

        # Legacy tokens without prefix might still work
        if len(token) >= 40:
            return {
                'valid': True,
                'error': None,
                'token_type': 'legacy'
            }

        return {
            'valid': False,
            'error': 'Token does not match known GitHub token formats',
            'token_type': None
        }

    @staticmethod
    def validate_repository_url(url: str) -> dict:
        """
        Validate GitHub repository URL format and structure.

        Checks for:
        - Valid HTTPS URL
        - github.com domain
        - Valid owner/repo path structure
        - No path traversal attempts

        Args:
            url: Repository URL to validate

        Returns:
            Dict with 'valid', 'error', 'owner', 'repo' keys
        """
        import urllib.parse

        if not url:
            return {
                'valid': False,
                'error': 'Repository URL is required',
                'owner': None,
                'repo': None
            }

        url = url.strip()

        # Check for path traversal attempts
        if '..' in url:
            return {
                'valid': False,
                'error': 'URL contains invalid path traversal',
                'owner': None,
                'repo': None
            }

        # Parse URL
        try:
            parsed = urllib.parse.urlparse(url)
        except Exception:
            return {
                'valid': False,
                'error': 'Invalid URL format',
                'owner': None,
                'repo': None
            }

        # Check scheme
        if parsed.scheme not in ('http', 'https'):
            return {
                'valid': False,
                'error': 'URL must use HTTP or HTTPS',
                'owner': None,
                'repo': None
            }

        # Check domain
        if parsed.netloc != 'github.com':
            return {
                'valid': False,
                'error': 'URL must be a github.com URL',
                'owner': None,
                'repo': None
            }

        # Parse owner/repo from path
        owner, repo = GitHubService.parse_url(url)

        if not owner or not repo:
            return {
                'valid': False,
                'error': 'Invalid repository path. Expected: https://github.com/owner/repo',
                'owner': None,
                'repo': None
            }

        # Validate owner/repo names
        if len(owner) > 39:
            return {
                'valid': False,
                'error': 'Owner name exceeds maximum length (39 characters)',
                'owner': owner,
                'repo': repo
            }

        if len(repo) > 100:
            return {
                'valid': False,
                'error': 'Repository name exceeds maximum length (100 characters)',
                'owner': owner,
                'repo': repo
            }

        return {
            'valid': True,
            'error': None,
            'owner': owner,
            'repo': repo
        }
    
    # ========================================================================
    # User and Organization Info
    # ========================================================================
    
    def get_authenticated_user(self) -> Dict[str, Any]:
        """
        Get information about the authenticated user.
        
        Returns:
            Dict with 'success', 'username', 'user_data', 'error' keys
        """
        if not self._token:
            return {
                'success': False,
                'username': None,
                'user_data': None,
                'error': 'GitHub token is required'
            }
        
        response, status_code, error = self._make_request('/user')
        
        if status_code == 200:
            return {
                'success': True,
                'username': response.get('login'),
                'user_data': response,
                'error': None
            }
        
        return {
            'success': False,
            'username': None,
            'user_data': None,
            'error': error or f'Failed to get user info (status {status_code})'
        }
    
    def get_organization_info(self, org: str) -> Dict[str, Any]:
        """
        Get information about an organization.
        
        Args:
            org: Organization name
            
        Returns:
            Dict with organization info or error
        """
        response, status_code, error = self._make_request(f'/orgs/{org}')
        
        if status_code == 200:
            return {
                'success': True,
                'org_data': response,
                'error': None
            }
        
        return {
            'success': False,
            'org_data': None,
            'error': error or f'Organization not found (status {status_code})'
        }
    
    def check_org_membership(self, org: str, username: str) -> Dict[str, Any]:
        """
        Check user's membership and role in an organization.

        Args:
            org: Organization name
            username: Username to check

        Returns:
            Dict with membership info
        """
        response, status_code, error = self._make_request(
            f'/orgs/{org}/memberships/{username}'
        )

        if status_code == 200:
            return {
                'is_member': True,
                'role': response.get('role'),
                'state': response.get('state'),
                'error': None
            }

        return {
            'is_member': False,
            'role': None,
            'state': None,
            'error': error
        }

    def get_user_organizations(self) -> Dict[str, Any]:
        """
        Get list of organizations the authenticated user belongs to.

        Returns:
            Dict with 'success', 'organizations', 'error' keys.
            Each organization in the list has 'login' (name) and 'avatar_url'.
        """
        if not self._token:
            return {
                'success': False,
                'organizations': [],
                'error': 'GitHub token is required'
            }

        response, status_code, error = self._make_request('/user/orgs')

        if status_code == 200 and isinstance(response, list):
            orgs = [
                {
                    'login': org.get('login'),
                    'avatar_url': org.get('avatar_url'),
                    'description': org.get('description', '')
                }
                for org in response
            ]
            return {
                'success': True,
                'organizations': orgs,
                'error': None
            }

        return {
            'success': False,
            'organizations': [],
            'error': error or f'Failed to get organizations (status {status_code})'
        }

    def get_allowed_save_targets(self) -> Dict[str, Any]:
        """
        Get list of allowed save targets for clone mode.

        Returns only the user's personal account and organizations they belong to.
        The repository name is always fixed to CLONE_MODE_REPO_NAME.

        Returns:
            Dict with:
                - success: True if fetched successfully
                - username: Authenticated user's username
                - targets: List of allowed targets, each with:
                    - type: 'user' or 'organization'
                    - name: Username or org name
                    - display_name: Display label for UI
                    - repo_url: Full repository URL
                - error: Error message if failed
        """
        if not self._token:
            return {
                'success': False,
                'username': None,
                'targets': [],
                'error': 'GitHub token is required'
            }

        # Get authenticated user
        user_result = self.get_authenticated_user()
        if not user_result['success']:
            return {
                'success': False,
                'username': None,
                'targets': [],
                'error': user_result['error']
            }

        username = user_result['username']
        targets = []

        # Add personal account as first target
        targets.append({
            'type': 'user',
            'name': username,
            'display_name': f'My Account ({username})',
            'repo_url': f'https://github.com/{username}/{CLONE_MODE_REPO_NAME}'
        })

        # Get user's organizations
        orgs_result = self.get_user_organizations()
        if orgs_result['success']:
            for org in orgs_result['organizations']:
                org_name = org['login']
                targets.append({
                    'type': 'organization',
                    'name': org_name,
                    'display_name': f'Organization: {org_name}',
                    'repo_url': f'https://github.com/{org_name}/{CLONE_MODE_REPO_NAME}'
                })

        return {
            'success': True,
            'username': username,
            'targets': targets,
            'error': None
        }

    def validate_save_target(self, target_owner: str) -> Dict[str, Any]:
        """
        Validate that a save target is allowed for the authenticated user.

        The target must be either:
        1. The user's own username (personal account)
        2. An organization the user belongs to

        Args:
            target_owner: The owner (username or org) where repo would be saved

        Returns:
            Dict with 'valid', 'owner_type', 'repo_url', 'error' keys
        """
        if not self._token:
            return {
                'valid': False,
                'owner_type': None,
                'repo_url': None,
                'error': 'GitHub token is required'
            }

        # Get authenticated user
        user_result = self.get_authenticated_user()
        if not user_result['success']:
            return {
                'valid': False,
                'owner_type': None,
                'repo_url': None,
                'error': user_result['error']
            }

        username = user_result['username']

        # Check if target is user's personal account
        if target_owner.lower() == username.lower():
            return {
                'valid': True,
                'owner_type': 'user',
                'repo_url': f'https://github.com/{username}/{CLONE_MODE_REPO_NAME}',
                'error': None
            }

        # Check if target is an organization the user belongs to
        membership = self.check_org_membership(target_owner, username)
        if membership['is_member']:
            return {
                'valid': True,
                'owner_type': 'organization',
                'repo_url': f'https://github.com/{target_owner}/{CLONE_MODE_REPO_NAME}',
                'error': None
            }

        return {
            'valid': False,
            'owner_type': None,
            'repo_url': None,
            'error': f"You don't have access to save to '{target_owner}'. You can only save to your personal account or organizations you belong to."
        }

    def is_allowed_load_repo(self, repo_url: str) -> Dict[str, Any]:
        """
        Check if a repository URL is allowed for clone mode loading.

        Allowed sources:
        1. User's own pybirdai_workplace repo (personal or org)
        2. Official regcommunity repos (ALLOWED_LOAD_REPOS)

        Args:
            repo_url: Repository URL to check

        Returns:
            Dict with 'allowed', 'source_type', 'error' keys
        """
        owner, repo = self.parse_url(repo_url)
        if not owner or not repo:
            return {
                'allowed': False,
                'source_type': None,
                'error': 'Invalid GitHub URL format'
            }

        full_name = f'{owner}/{repo}'

        # Check if it's an allowed regcommunity repo
        if full_name in ALLOWED_LOAD_REPOS:
            return {
                'allowed': True,
                'source_type': 'default',
                'error': None
            }

        # Check if it's user's own pybirdai_workplace repo
        if repo == CLONE_MODE_REPO_NAME:
            if not self._token:
                # Without token, we can't verify ownership
                # Allow it and let the API call fail if unauthorized
                return {
                    'allowed': True,
                    'source_type': 'user',
                    'error': None
                }

            # Get authenticated user
            user_result = self.get_authenticated_user()
            if user_result['success']:
                username = user_result['username']

                # Check if owner is user's account
                if owner.lower() == username.lower():
                    return {
                        'allowed': True,
                        'source_type': 'user',
                        'error': None
                    }

                # Check if owner is an org the user belongs to
                membership = self.check_org_membership(owner, username)
                if membership['is_member']:
                    return {
                        'allowed': True,
                        'source_type': 'organization',
                        'error': None
                    }

        return {
            'allowed': False,
            'source_type': None,
            'error': f"Repository '{full_name}' is not allowed. You can only load from your own pybirdai_workplace repo or official regcommunity repos."
        }

    def get_allowed_load_sources(self) -> Dict[str, Any]:
        """
        Get list of allowed load sources for clone mode.

        Returns the user's repos (if authenticated) plus default regcommunity repos.

        Returns:
            Dict with:
                - success: True if fetched successfully
                - sources: List of source options:
                    - type: 'user', 'organization', or 'default'
                    - name: Display name
                    - repo_url: Full repository URL
                - error: Error message if failed
        """
        sources = []

        # Add default regcommunity repos
        for repo_path in ALLOWED_LOAD_REPOS:
            sources.append({
                'type': 'default',
                'name': f'Default: {repo_path}',
                'repo_url': f'https://github.com/{repo_path}'
            })

        # If we have a token, add user's potential repos
        if self._token:
            user_result = self.get_authenticated_user()
            if user_result['success']:
                username = user_result['username']

                # Add user's personal repo
                sources.insert(0, {
                    'type': 'user',
                    'name': f'My Workspace ({username})',
                    'repo_url': f'https://github.com/{username}/{CLONE_MODE_REPO_NAME}'
                })

                # Add user's org repos
                orgs_result = self.get_user_organizations()
                if orgs_result['success']:
                    for i, org in enumerate(orgs_result['organizations']):
                        org_name = org['login']
                        sources.insert(1 + i, {
                            'type': 'organization',
                            'name': f'Org: {org_name}',
                            'repo_url': f'https://github.com/{org_name}/{CLONE_MODE_REPO_NAME}'
                        })

        return {
            'success': True,
            'sources': sources,
            'error': None
        }

    # ========================================================================
    # Repository Operations
    # ========================================================================
    
    def repo_exists(self, owner: str, repo: str) -> Dict[str, Any]:
        """
        Check if a GitHub repository exists and get its details.
        
        Args:
            owner: Repository owner
            repo: Repository name
            
        Returns:
            Dict with 'exists', 'accessible', 'private', 'permissions', etc.
        """
        response, status_code, error = self._make_request(f'/repos/{owner}/{repo}')
        
        if status_code == 200:
            return {
                'exists': True,
                'accessible': True,
                'private': response.get('private', False),
                'permissions': response.get('permissions', {}),
                'default_branch': response.get('default_branch', 'main'),
                'full_name': response.get('full_name'),
                'html_url': response.get('html_url'),
                'error': None
            }
        elif status_code == 404:
            return {
                'exists': False,
                'accessible': False,
                'private': None,
                'permissions': None,
                'error': 'Repository not found'
            }
        elif status_code == 401:
            return {
                'exists': None,
                'accessible': False,
                'private': None,
                'permissions': None,
                'error': 'Authentication required'
            }
        elif status_code == 403:
            return {
                'exists': True,
                'accessible': False,
                'private': True,
                'permissions': None,
                'error': 'Access denied'
            }
        
        return {
            'exists': None,
            'accessible': False,
            'private': None,
            'permissions': None,
            'error': error or f'GitHub API error (status {status_code})'
        }
    
    def can_create_repo(self, owner: str) -> Dict[str, Any]:
        """
        Check if the authenticated user can create a repository at the specified location.
        
        Args:
            owner: The owner/organization where the repo would be created
            
        Returns:
            Dict with 'can_create', 'reason', 'owner_type' keys
        """
        if not self._token:
            return {
                'can_create': False,
                'reason': 'GitHub token is required to create repositories',
                'owner_type': None
            }
        
        # Get authenticated user info
        user_result = self.get_authenticated_user()
        if not user_result['success']:
            return {
                'can_create': False,
                'reason': user_result['error'],
                'owner_type': None
            }
        
        authenticated_user = user_result['username']
        
        # Case 1: Owner is the authenticated user
        if owner.lower() == authenticated_user.lower():
            return {
                'can_create': True,
                'reason': 'You can create repositories in your personal account',
                'owner_type': 'user'
            }
        
        # Case 2: Check if owner is an organization
        org_result = self.get_organization_info(owner)
        if not org_result['success']:
            return {
                'can_create': False,
                'reason': f"'{owner}' is not your username or an accessible organization",
                'owner_type': None
            }
        
        # Check membership
        membership = self.check_org_membership(owner, authenticated_user)
        if not membership['is_member']:
            return {
                'can_create': False,
                'reason': f"You are not a member of the '{owner}' organization",
                'owner_type': 'organization'
            }
        
        # Check permissions
        if membership['role'] == 'admin':
            return {
                'can_create': True,
                'reason': f"You are an admin of the '{owner}' organization",
                'owner_type': 'organization'
            }
        
        # Check org settings
        if org_result['org_data'].get('members_can_create_repositories', False):
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
        self,
        owner: str,
        repo: str,
        private: bool = True,
        description: str = "PyBIRD AI Repository",
        auto_init: bool = True
    ) -> Dict[str, Any]:
        """
        Create a new GitHub repository.
        
        Args:
            owner: Repository owner (user or organization)
            repo: Repository name
            private: Whether to create a private repository
            description: Repository description
            auto_init: Initialize with README
            
        Returns:
            Dict with 'success', 'repo_url', 'repo_data', 'error' keys
        """
        # Check permissions first
        permission_check = self.can_create_repo(owner)
        if not permission_check['can_create']:
            return {
                'success': False,
                'repo_url': None,
                'repo_data': None,
                'error': permission_check['reason']
            }
        
        # Determine endpoint
        if permission_check['owner_type'] == 'user':
            endpoint = '/user/repos'
        else:
            endpoint = f'/orgs/{owner}/repos'
        
        data = {
            'name': repo,
            'private': private,
            'description': description,
            'auto_init': auto_init
        }
        
        response, status_code, error = self._make_request(endpoint, method='POST', data=data)
        
        if status_code == 201:
            return {
                'success': True,
                'repo_url': response.get('html_url'),
                'repo_data': response,
                'default_branch': response.get('default_branch', 'main'),
                'error': None
            }
        elif status_code == 422:
            return {
                'success': False,
                'repo_url': None,
                'repo_data': None,
                'error': 'Repository name already exists or is invalid'
            }
        
        return {
            'success': False,
            'repo_url': None,
            'repo_data': None,
            'error': error or f'Failed to create repository (status {status_code})'
        }
    
    def validate_for_operation(
        self,
        repo_url: str,
        operation: str = 'save'
    ) -> Dict[str, Any]:
        """
        Comprehensive validation for clone mode operations.
        
        Args:
            repo_url: GitHub repository URL
            operation: 'save' or 'load'
            
        Returns:
            Dict with validation results and recommended action
        """
        result = {
            'valid': False,
            'exists': False,
            'can_create': False,
            'has_push_access': False,
            'error': None,
            'action_required': 'error',
            'owner': None,
            'repo': None,
            'default_branch': 'main'
        }
        
        # Validate URL format
        url_validation = self.validate_url(repo_url)
        if not url_validation['valid']:
            result['error'] = url_validation['error']
            return result
        
        owner = url_validation['owner']
        repo = url_validation['repo']
        result['owner'] = owner
        result['repo'] = repo
        
        # Check if repo exists
        repo_check = self.repo_exists(owner, repo)
        result['exists'] = repo_check.get('exists', False)
        
        if repo_check.get('exists'):
            if not repo_check.get('accessible'):
                result['error'] = repo_check['error']
                return result
            
            result['default_branch'] = repo_check.get('default_branch', 'main')
            
            if operation == 'save':
                permissions = repo_check.get('permissions', {})
                has_push = permissions.get('push', False)
                
                if has_push:
                    result['valid'] = True
                    result['has_push_access'] = True
                    result['action_required'] = 'none'
                else:
                    result['error'] = 'You do not have push access to this repository'
            else:
                # Load operation - read access is sufficient
                result['valid'] = True
                result['action_required'] = 'none'
        else:
            # Repo doesn't exist
            if operation == 'load':
                result['error'] = 'Repository not found'
                return result
            
            # For save, check if can create
            if self._token:
                create_check = self.can_create_repo(owner)
                result['can_create'] = create_check['can_create']
                
                if create_check['can_create']:
                    result['action_required'] = 'create'
                    result['valid'] = True
                else:
                    result['error'] = create_check['reason']
            else:
                result['error'] = 'Repository not found. Provide a token to create it.'
        
        return result
    
    # ========================================================================
    # Branch Operations
    # ========================================================================
    
    def get_branch(self, owner: str, repo: str, branch: str) -> Dict[str, Any]:
        """
        Get information about a branch.
        
        Args:
            owner: Repository owner
            repo: Repository name
            branch: Branch name
            
        Returns:
            Dict with branch info
        """
        response, status_code, error = self._make_request(
            f'/repos/{owner}/{repo}/git/refs/heads/{branch}'
        )
        
        if status_code == 200:
            return {
                'success': True,
                'sha': response['object']['sha'],
                'ref': response['ref'],
                'error': None
            }
        
        return {
            'success': False,
            'sha': None,
            'ref': None,
            'error': error or f'Branch not found (status {status_code})'
        }
    
    def create_branch(
        self,
        owner: str,
        repo: str,
        branch: str,
        from_branch: str = 'main'
    ) -> Dict[str, Any]:
        """
        Create a new branch.
        
        Args:
            owner: Repository owner
            repo: Repository name
            branch: New branch name
            from_branch: Base branch
            
        Returns:
            Dict with success status
        """
        # Get base branch SHA
        base_result = self.get_branch(owner, repo, from_branch)
        if not base_result['success']:
            return {
                'success': False,
                'error': f"Base branch '{from_branch}' not found: {base_result['error']}"
            }
        
        # Create new branch
        data = {
            'ref': f'refs/heads/{branch}',
            'sha': base_result['sha']
        }
        
        response, status_code, error = self._make_request(
            f'/repos/{owner}/{repo}/git/refs',
            method='POST',
            data=data
        )
        
        if status_code == 201:
            return {'success': True, 'error': None}
        
        return {
            'success': False,
            'error': error or f'Failed to create branch (status {status_code})'
        }
    
    # ========================================================================
    # File Operations
    # ========================================================================
    
    def fetch_file(
        self,
        owner: str,
        repo: str,
        path: str,
        branch: str = 'main'
    ) -> Dict[str, Any]:
        """
        Fetch a single file from a repository.
        
        Args:
            owner: Repository owner
            repo: Repository name
            path: File path within repository
            branch: Branch name
            
        Returns:
            Dict with 'success', 'content', 'sha', 'error' keys
        """
        response, status_code, error = self._make_request(
            f'/repos/{owner}/{repo}/contents/{path}?ref={branch}'
        )
        
        if status_code == 200:
            if response.get('type') == 'file':
                content = base64.b64decode(response['content']).decode('utf-8')
                return {
                    'success': True,
                    'content': content,
                    'sha': response['sha'],
                    'error': None
                }
            return {
                'success': False,
                'content': None,
                'sha': None,
                'error': 'Path is not a file'
            }
        
        return {
            'success': False,
            'content': None,
            'sha': None,
            'error': error or f'File not found (status {status_code})'
        }
    
    def fetch_directory(
        self,
        owner: str,
        repo: str,
        path: str = '',
        branch: str = 'main'
    ) -> Dict[str, Any]:
        """
        List contents of a directory.
        
        Args:
            owner: Repository owner
            repo: Repository name
            path: Directory path
            branch: Branch name
            
        Returns:
            Dict with 'success', 'contents', 'error' keys
        """
        endpoint = f'/repos/{owner}/{repo}/contents/{path}' if path else f'/repos/{owner}/{repo}/contents'
        endpoint += f'?ref={branch}'
        
        response, status_code, error = self._make_request(endpoint)
        
        if status_code == 200 and isinstance(response, list):
            return {
                'success': True,
                'contents': response,
                'error': None
            }
        
        return {
            'success': False,
            'contents': None,
            'error': error or f'Directory not found (status {status_code})'
        }
    
    # ========================================================================
    # Push Operations
    # ========================================================================
    
    def push_files(
        self,
        owner: str,
        repo: str,
        files: List[Dict[str, str]],
        branch: str,
        message: str,
        base_path: str = ''
    ) -> Dict[str, Any]:
        """
        Push multiple files in a single commit.
        
        Args:
            owner: Repository owner
            repo: Repository name
            files: List of dicts with 'path' and 'content' keys
            branch: Target branch
            message: Commit message
            base_path: Base path prefix for all files
            
        Returns:
            Dict with 'success', 'commit_sha', 'error' keys
        """
        try:
            # Step 1: Get current commit SHA
            branch_result = self.get_branch(owner, repo, branch)
            if not branch_result['success']:
                return {
                    'success': False,
                    'commit_sha': None,
                    'error': f"Branch '{branch}' not found"
                }
            
            current_sha = branch_result['sha']
            
            # Step 2: Get base tree
            response, status_code, error = self._make_request(
                f'/repos/{owner}/{repo}/git/commits/{current_sha}'
            )
            if status_code != 200:
                return {'success': False, 'commit_sha': None, 'error': error}
            
            base_tree_sha = response['tree']['sha']
            
            # Step 3: Create blobs for all files
            tree_items = []
            for file_info in files:
                file_path = file_info['path']
                if base_path:
                    file_path = f"{base_path}/{file_path}"
                
                # Create blob
                blob_data = {
                    'content': base64.b64encode(
                        file_info['content'].encode('utf-8') 
                        if isinstance(file_info['content'], str) 
                        else file_info['content']
                    ).decode('utf-8'),
                    'encoding': 'base64'
                }
                
                blob_response, blob_status, blob_error = self._make_request(
                    f'/repos/{owner}/{repo}/git/blobs',
                    method='POST',
                    data=blob_data
                )
                
                if blob_status != 201:
                    return {
                        'success': False,
                        'commit_sha': None,
                        'error': f"Failed to create blob for {file_path}: {blob_error}"
                    }
                
                tree_items.append({
                    'path': file_path,
                    'mode': '100644',
                    'type': 'blob',
                    'sha': blob_response['sha']
                })
            
            # Step 4: Create tree
            tree_data = {
                'tree': tree_items,
                'base_tree': base_tree_sha
            }
            
            tree_response, tree_status, tree_error = self._make_request(
                f'/repos/{owner}/{repo}/git/trees',
                method='POST',
                data=tree_data
            )
            
            if tree_status != 201:
                return {
                    'success': False,
                    'commit_sha': None,
                    'error': f"Failed to create tree: {tree_error}"
                }
            
            # Step 5: Create commit
            commit_data = {
                'tree': tree_response['sha'],
                'message': message,
                'parents': [current_sha]
            }
            
            commit_response, commit_status, commit_error = self._make_request(
                f'/repos/{owner}/{repo}/git/commits',
                method='POST',
                data=commit_data
            )
            
            if commit_status != 201:
                return {
                    'success': False,
                    'commit_sha': None,
                    'error': f"Failed to create commit: {commit_error}"
                }
            
            new_commit_sha = commit_response['sha']
            
            # Step 6: Update branch reference
            ref_data = {'sha': new_commit_sha}
            
            ref_response, ref_status, ref_error = self._make_request(
                f'/repos/{owner}/{repo}/git/refs/heads/{branch}',
                method='PATCH',
                data=ref_data
            )
            
            if ref_status != 200:
                return {
                    'success': False,
                    'commit_sha': None,
                    'error': f"Failed to update branch: {ref_error}"
                }
            
            return {
                'success': True,
                'commit_sha': new_commit_sha,
                'error': None
            }
            
        except Exception as e:
            logger.error(f"Error pushing files: {e}")
            return {
                'success': False,
                'commit_sha': None,
                'error': str(e)
            }
    
    def push_csv_directory(
        self,
        owner: str,
        repo: str,
        local_directory: str,
        branch: str,
        message: str,
        remote_path: str = ''
    ) -> Dict[str, Any]:
        """
        Push all CSV files from a local directory.
        
        Args:
            owner: Repository owner
            repo: Repository name
            local_directory: Local directory containing CSV files
            branch: Target branch
            message: Commit message
            remote_path: Remote path prefix
            
        Returns:
            Dict with 'success', 'commit_sha', 'files_pushed', 'error' keys
        """
        import glob
        
        # Files to exclude from push
        EXCLUDE_FILES = {
            "workflowtaskexecution.csv",
            "workflowsession.csv",
            "workflowtaskdependency.csv",
            "automodeconfiguration.csv"
        }
        
        csv_files = glob.glob(os.path.join(local_directory, '*.csv'))
        
        files_to_push = []
        for csv_file in csv_files:
            filename = os.path.basename(csv_file)
            if filename not in EXCLUDE_FILES:
                with open(csv_file, 'rb') as f:
                    content = f.read()
                
                file_path = f"{remote_path}/{filename}" if remote_path else filename
                files_to_push.append({
                    'path': file_path,
                    'content': content
                })
        
        if not files_to_push:
            return {
                'success': False,
                'commit_sha': None,
                'files_pushed': 0,
                'error': 'No CSV files found to push'
            }
        
        result = self.push_files(owner, repo, files_to_push, branch, message)
        result['files_pushed'] = len(files_to_push) if result['success'] else 0
        
        return result
    
    # ========================================================================
    # Fork and PR Operations
    # ========================================================================
    
    def fork_repository(
        self,
        source_owner: str,
        source_repo: str,
        organization: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fork a repository.
        
        Args:
            source_owner: Source repository owner
            source_repo: Source repository name
            organization: Target organization (or user if None)
            
        Returns:
            Dict with fork info
        """
        data = {}
        if organization:
            data['organization'] = organization
        
        response, status_code, error = self._make_request(
            f'/repos/{source_owner}/{source_repo}/forks',
            method='POST',
            data=data if data else None
        )
        
        if status_code in [201, 202]:
            return {
                'success': True,
                'fork_data': response,
                'error': None
            }
        
        return {
            'success': False,
            'fork_data': None,
            'error': error or f'Failed to fork repository (status {status_code})'
        }
    
    @staticmethod
    def generate_artifact_change_summary(change_report: dict = None) -> str:
        """
        Generate a markdown summary of linked artifact changes for PR descriptions.

        Args:
            change_report: The change report from LinkedArtifactChangeDetector.
                          If None, attempts to get from current framework context.

        Returns:
            Markdown formatted string with artifact change summary
        """
        if not change_report:
            return ''

        summary = change_report.get('summary', {})
        if not summary:
            return ''

        md = '\n\n## Linked Artifacts Changed\n\n'
        has_content = False

        # CUBE_LINK changes
        cube_link = summary.get('cube_link')
        if cube_link and cube_link.get('has_changes'):
            has_content = True
            md += '### CUBE_LINK\n'
            md += f"- Added: {cube_link.get('new_count', 0)}\n"
            md += f"- Modified: {cube_link.get('modified_count', 0)}\n"
            md += f"- Deleted: {cube_link.get('deleted_count', 0)}\n\n"

        # CUBE_STRUCTURE_ITEM_LINK changes
        csil = summary.get('cube_structure_item_link')
        if csil and csil.get('has_changes'):
            has_content = True
            md += '### CUBE_STRUCTURE_ITEM_LINK\n'
            md += f"- Added: {csil.get('new_count', 0)}\n"
            md += f"- Modified: {csil.get('modified_count', 0)}\n"
            md += f"- Deleted: {csil.get('deleted_count', 0)}\n\n"

        # MEMBER_LINK changes
        member_link = summary.get('member_link')
        if member_link and member_link.get('has_changes'):
            has_content = True
            md += '### MEMBER_LINK\n'
            md += f"- Added: {member_link.get('new_count', 0)}\n"
            md += f"- Modified: {member_link.get('modified_count', 0)}\n"
            md += f"- Deleted: {member_link.get('deleted_count', 0)}\n\n"

        # Validation status
        validation = summary.get('validation')
        if validation:
            has_content = True
            md += '### Validation Status\n'
            if validation.get('all_valid'):
                md += f"All {validation.get('total_checked', 0)} artifacts validated successfully.\n"
            else:
                md += f"{validation.get('total_invalid', 0)} of {validation.get('total_checked', 0)} "
                md += "artifacts have validation errors and will be skipped.\n"

        return md if has_content else ''

    def create_pull_request_with_artifacts(
        self,
        owner: str,
        repo: str,
        title: str,
        body: str,
        head: str,
        base: str = 'main',
        change_report: dict = None
    ) -> Dict[str, Any]:
        """
        Create a pull request with enhanced description including artifact changes.

        Args:
            owner: Repository owner
            repo: Repository name
            title: PR title
            body: PR body/description
            head: Head branch (can include fork: "user:branch")
            base: Base branch
            change_report: Optional artifact change report to include in description

        Returns:
            Dict with PR info
        """
        # Append artifact change summary to body if available
        enhanced_body = body
        artifact_summary = self.generate_artifact_change_summary(change_report)
        if artifact_summary:
            enhanced_body = body + artifact_summary

        return self.create_pull_request(
            owner=owner,
            repo=repo,
            title=title,
            body=enhanced_body,
            head=head,
            base=base
        )

    def create_pull_request(
        self,
        owner: str,
        repo: str,
        title: str,
        body: str,
        head: str,
        base: str = 'main'
    ) -> Dict[str, Any]:
        """
        Create a pull request.

        Args:
            owner: Repository owner
            repo: Repository name
            title: PR title
            body: PR body/description
            head: Head branch (can include fork: "user:branch")
            base: Base branch

        Returns:
            Dict with PR info
        """
        data = {
            'title': title,
            'body': body,
            'head': head,
            'base': base
        }
        
        response, status_code, error = self._make_request(
            f'/repos/{owner}/{repo}/pulls',
            method='POST',
            data=data
        )
        
        if status_code == 201:
            return {
                'success': True,
                'pr_url': response.get('html_url'),
                'pr_number': response.get('number'),
                'pr_data': response,
                'error': None
            }
        
        return {
            'success': False,
            'pr_url': None,
            'pr_number': None,
            'pr_data': None,
            'error': error or f'Failed to create PR (status {status_code})'
        }
    
    def download_archive(
        self,
        owner: str,
        repo: str,
        branch: str = 'main',
        output_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Download and extract a repository archive (ZIP).

        Args:
            owner: Repository owner
            repo: Repository name
            branch: Branch to download (default: main)
            output_dir: Output directory (default: creates temp directory)

        Returns:
            Dict with:
                - success: True if downloaded successfully
                - path: Path to extracted directory
                - error: Error message if failed
        """
        import tempfile
        import zipfile
        import shutil

        zip_url = f'https://github.com/{owner}/{repo}/archive/refs/heads/{branch}.zip'

        headers = {}
        if self._token:
            headers['Authorization'] = f'Bearer {self._token}'
        headers['User-Agent'] = USER_AGENT

        try:
            request = urllib.request.Request(zip_url, headers=headers)
            response = urllib.request.urlopen(request, timeout=60)

            # Create output directory
            if output_dir is None:
                output_dir = tempfile.mkdtemp(prefix='github_archive_')
            else:
                os.makedirs(output_dir, exist_ok=True)

            zip_path = os.path.join(output_dir, 'repo.zip')

            # Download ZIP
            chunk_size = 8192
            with open(zip_path, 'wb') as f:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)

            # Extract
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)

            os.remove(zip_path)

            # Find the extracted folder
            extracted_folder = None
            for item in os.listdir(output_dir):
                item_path = os.path.join(output_dir, item)
                if os.path.isdir(item_path):
                    extracted_folder = item_path
                    break

            if not extracted_folder:
                return {
                    'success': False,
                    'path': None,
                    'error': 'Could not find extracted repository folder'
                }

            logger.info(f"Downloaded and extracted {owner}/{repo}@{branch} to {extracted_folder}")
            return {
                'success': True,
                'path': extracted_folder,
                'error': None
            }

        except urllib.error.HTTPError as e:
            error_msg = {
                404: f'Repository or branch not found: {owner}/{repo} ({branch})',
                401: 'Authentication failed. Check your GitHub token.',
                403: 'Access denied. Token may lack required permissions.'
            }.get(e.code, f'Failed to download: HTTP {e.code}')

            return {
                'success': False,
                'path': None,
                'error': error_msg
            }
        except Exception as e:
            return {
                'success': False,
                'path': None,
                'error': str(e)
            }

    def wait_for_fork(
        self,
        owner: str,
        repo: str,
        max_attempts: int = 30,
        delay: int = 2
    ) -> bool:
        """
        Wait for a fork to be ready.
        
        Args:
            owner: Fork owner
            repo: Fork repository name
            max_attempts: Maximum check attempts
            delay: Delay between attempts in seconds
            
        Returns:
            True if fork is ready, False otherwise
        """
        import time
        
        for attempt in range(max_attempts):
            repo_check = self.repo_exists(owner, repo)
            
            if repo_check.get('exists') and repo_check.get('accessible'):
                # Also check branches
                dir_result = self.fetch_directory(owner, repo)
                if dir_result['success']:
                    return True
            
            if attempt < max_attempts - 1:
                time.sleep(delay)
        
        return False


# ============================================================================
# Convenience Functions (for backwards compatibility)
# ============================================================================

def get_github_service(token: Optional[str] = None) -> GitHubService:
    """
    Get a GitHubService instance.
    
    Args:
        token: Optional token (uses stored/env token if not provided)
        
    Returns:
        GitHubService instance
    """
    return GitHubService(token=token)
