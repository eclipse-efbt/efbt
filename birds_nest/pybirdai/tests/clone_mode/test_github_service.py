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
Unit tests for GitHubService.

These tests cover URL parsing, validation, token management, and API operations.
Uses mocking to avoid actual GitHub API calls.

Usage:
    python -m pytest pybirdai/tests/clone_mode/test_github_service.py -v
"""

import os
import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add the birds_nest directory to the path
BIRDS_NEST_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(BIRDS_NEST_DIR))

from pybirdai.services.github_service import GitHubService, GITHUB_URL_PATTERN


class TestURLParsing:
    """Tests for GitHub URL parsing functionality."""

    def test_parse_valid_url(self):
        """Test parsing a standard GitHub URL."""
        owner, repo = GitHubService.parse_url("https://github.com/owner/repo")
        assert owner == "owner"
        assert repo == "repo"

    def test_parse_url_with_git_suffix(self):
        """Test parsing a URL with .git suffix."""
        owner, repo = GitHubService.parse_url("https://github.com/owner/repo.git")
        assert owner == "owner"
        assert repo == "repo"

    def test_parse_url_with_trailing_slash(self):
        """Test parsing a URL with trailing slash."""
        owner, repo = GitHubService.parse_url("https://github.com/owner/repo/")
        assert owner == "owner"
        assert repo == "repo"

    def test_parse_http_url(self):
        """Test parsing an HTTP (not HTTPS) URL."""
        owner, repo = GitHubService.parse_url("http://github.com/owner/repo")
        assert owner == "owner"
        assert repo == "repo"

    def test_parse_url_with_hyphen_underscore(self):
        """Test parsing URLs with hyphens and underscores."""
        owner, repo = GitHubService.parse_url("https://github.com/my-org/my_repo-name")
        assert owner == "my-org"
        assert repo == "my_repo-name"

    def test_parse_invalid_url_none(self):
        """Test parsing None returns None tuple."""
        owner, repo = GitHubService.parse_url(None)
        assert owner is None
        assert repo is None

    def test_parse_invalid_url_empty(self):
        """Test parsing empty string returns None tuple."""
        owner, repo = GitHubService.parse_url("")
        assert owner is None
        assert repo is None

    def test_parse_invalid_url_not_github(self):
        """Test parsing non-GitHub URL returns None tuple."""
        owner, repo = GitHubService.parse_url("https://gitlab.com/owner/repo")
        assert owner is None
        assert repo is None

    def test_parse_invalid_url_missing_repo(self):
        """Test parsing URL without repo returns None tuple."""
        owner, repo = GitHubService.parse_url("https://github.com/owner")
        assert owner is None
        assert repo is None


class TestURLValidation:
    """Tests for GitHub URL validation functionality."""

    def test_validate_valid_url(self):
        """Test validating a valid URL."""
        result = GitHubService.validate_url("https://github.com/owner/repo")
        assert result['valid'] is True
        assert result['owner'] == "owner"
        assert result['repo'] == "repo"
        assert result['error'] is None

    def test_validate_invalid_url(self):
        """Test validating an invalid URL."""
        result = GitHubService.validate_url("not-a-url")
        assert result['valid'] is False
        assert result['owner'] is None
        assert result['repo'] is None
        assert result['error'] is not None


class TestURLBuilding:
    """Tests for URL building functionality."""

    def test_build_url(self):
        """Test building a GitHub URL from owner and repo."""
        url = GitHubService.build_url("owner", "repo")
        assert url == "https://github.com/owner/repo"

    def test_get_raw_url(self):
        """Test building a raw content URL."""
        url = GitHubService.get_raw_url("owner", "repo", "main", "path/to/file.txt")
        assert url == "https://raw.githubusercontent.com/owner/repo/main/path/to/file.txt"


class TestTokenManagement:
    """Tests for token management functionality."""

    def setup_method(self):
        """Clear token before each test."""
        GitHubService.clear_token()
        # Clear environment variable if set
        self.original_env = os.environ.get("GITHUB_TOKEN")
        if "GITHUB_TOKEN" in os.environ:
            del os.environ["GITHUB_TOKEN"]

    def teardown_method(self):
        """Restore original state after each test."""
        GitHubService.clear_token()
        if self.original_env:
            os.environ["GITHUB_TOKEN"] = self.original_env

    def test_set_and_get_token(self):
        """Test setting and getting a token."""
        GitHubService.set_token("ghp_test_token_12345")
        assert GitHubService.get_token() == "ghp_test_token_12345"

    def test_clear_token(self):
        """Test clearing a token."""
        GitHubService.set_token("ghp_test_token")
        GitHubService.clear_token()
        assert GitHubService.get_token() is None

    def test_get_token_from_env(self):
        """Test getting token from environment variable."""
        os.environ["GITHUB_TOKEN"] = "ghp_env_token"
        assert GitHubService.get_token() == "ghp_env_token"

    def test_set_token_overrides_env(self):
        """Test that set token overrides environment variable."""
        os.environ["GITHUB_TOKEN"] = "ghp_env_token"
        GitHubService.set_token("ghp_set_token")
        assert GitHubService.get_token() == "ghp_set_token"

    def test_validate_token_format_valid_ghp(self):
        """Test validating ghp_ prefix token."""
        assert GitHubService.validate_token_format("ghp_abc123def456") is True

    def test_validate_token_format_valid_github_pat(self):
        """Test validating github_pat_ prefix token."""
        assert GitHubService.validate_token_format("github_pat_abc123") is True

    def test_validate_token_format_invalid(self):
        """Test validating invalid token format."""
        assert GitHubService.validate_token_format("invalid_token") is False

    def test_validate_token_format_empty(self):
        """Test validating empty token."""
        assert GitHubService.validate_token_format("") is False

    def test_validate_token_format_none(self):
        """Test validating None token."""
        assert GitHubService.validate_token_format(None) is False


class TestServiceInstance:
    """Tests for GitHubService instance functionality."""

    def setup_method(self):
        """Clear token before each test."""
        GitHubService.clear_token()

    def test_instance_with_explicit_token(self):
        """Test creating instance with explicit token."""
        service = GitHubService(token="ghp_explicit")
        assert service.token == "ghp_explicit"
        assert service.has_token is True

    def test_instance_with_stored_token(self):
        """Test creating instance using stored token."""
        GitHubService.set_token("ghp_stored")
        service = GitHubService()
        assert service.token == "ghp_stored"

    def test_instance_without_token(self):
        """Test creating instance without any token."""
        service = GitHubService()
        assert service.token is None
        assert service.has_token is False


class TestHeaders:
    """Tests for HTTP header generation."""

    def test_headers_without_token(self):
        """Test headers without authentication."""
        service = GitHubService()
        headers = service._get_headers()

        assert 'Accept' in headers
        assert headers['Accept'] == 'application/vnd.github+json'
        assert 'X-GitHub-Api-Version' in headers
        assert 'User-Agent' in headers
        assert 'Authorization' not in headers

    def test_headers_with_token(self):
        """Test headers with authentication."""
        service = GitHubService(token="ghp_test_token")
        headers = service._get_headers()

        assert 'Authorization' in headers
        assert headers['Authorization'] == 'Bearer ghp_test_token'

    def test_headers_with_content_type(self):
        """Test headers with content type included."""
        service = GitHubService(token="ghp_test")
        headers = service._get_headers(include_content_type=True)

        assert 'Content-Type' in headers
        assert headers['Content-Type'] == 'application/json'


class TestAPIRequests:
    """Tests for GitHub API request functionality (mocked)."""

    @patch('urllib.request.urlopen')
    def test_make_request_success(self, mock_urlopen):
        """Test successful API request."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b'{"login": "testuser"}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        service = GitHubService(token="ghp_test")
        data, status, error = service._make_request('/user')

        assert status == 200
        assert data == {"login": "testuser"}
        assert error is None

    @patch('urllib.request.urlopen')
    def test_make_request_404(self, mock_urlopen):
        """Test 404 response handling."""
        import urllib.error

        mock_error = urllib.error.HTTPError(
            url="https://api.github.com/repos/owner/repo",
            code=404,
            msg="Not Found",
            hdrs={},
            fp=MagicMock(read=lambda: b'{"message": "Not Found"}')
        )
        mock_urlopen.side_effect = mock_error

        service = GitHubService(token="ghp_test")
        data, status, error = service._make_request('/repos/owner/repo')

        assert status == 404
        assert error == "Not Found"

    @patch('urllib.request.urlopen')
    def test_make_request_network_error(self, mock_urlopen):
        """Test network error handling."""
        import urllib.error

        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        service = GitHubService(token="ghp_test")
        data, status, error = service._make_request('/user')

        assert status == 0
        assert data is None
        assert "Network error" in error


class TestRepoOperations:
    """Tests for repository operations (mocked)."""

    @patch.object(GitHubService, '_make_request')
    def test_repo_exists_public(self, mock_request):
        """Test checking if a public repo exists."""
        mock_request.return_value = (
            {
                'private': False,
                'permissions': {'push': True},
                'default_branch': 'main',
                'full_name': 'owner/repo',
                'html_url': 'https://github.com/owner/repo'
            },
            200,
            None
        )

        service = GitHubService(token="ghp_test")
        result = service.repo_exists("owner", "repo")

        assert result['exists'] is True
        assert result['accessible'] is True
        assert result['private'] is False
        assert result['default_branch'] == 'main'

    @patch.object(GitHubService, '_make_request')
    def test_repo_exists_not_found(self, mock_request):
        """Test checking if a non-existent repo exists."""
        mock_request.return_value = (None, 404, "Repository not found")

        service = GitHubService(token="ghp_test")
        result = service.repo_exists("owner", "nonexistent")

        assert result['exists'] is False
        assert result['accessible'] is False

    @patch.object(GitHubService, '_make_request')
    def test_repo_exists_unauthorized(self, mock_request):
        """Test checking repo with unauthorized access."""
        mock_request.return_value = (None, 401, "Requires authentication")

        service = GitHubService()
        result = service.repo_exists("owner", "private-repo")

        assert result['exists'] is None  # Unknown - might exist
        assert result['accessible'] is False
        assert result['error'] == "Authentication required"


class TestValidateForOperation:
    """Tests for operation validation."""

    @patch.object(GitHubService, 'repo_exists')
    def test_validate_for_save_with_push_access(self, mock_repo_exists):
        """Test validation for save operation with push access."""
        mock_repo_exists.return_value = {
            'exists': True,
            'accessible': True,
            'permissions': {'push': True},
            'default_branch': 'main'
        }

        service = GitHubService(token="ghp_test")
        result = service.validate_for_operation("https://github.com/owner/repo", "save")

        assert result['valid'] is True
        assert result['has_push_access'] is True
        assert result['action_required'] == 'none'

    @patch.object(GitHubService, 'repo_exists')
    def test_validate_for_save_without_push_access(self, mock_repo_exists):
        """Test validation for save operation without push access."""
        mock_repo_exists.return_value = {
            'exists': True,
            'accessible': True,
            'permissions': {'push': False},
            'default_branch': 'main'
        }

        service = GitHubService(token="ghp_test")
        result = service.validate_for_operation("https://github.com/owner/repo", "save")

        assert result['valid'] is False
        assert result['has_push_access'] is False
        assert "push access" in result['error']

    @patch.object(GitHubService, 'repo_exists')
    def test_validate_for_load_existing_repo(self, mock_repo_exists):
        """Test validation for load operation on existing repo."""
        mock_repo_exists.return_value = {
            'exists': True,
            'accessible': True,
            'default_branch': 'main'
        }

        service = GitHubService(token="ghp_test")
        result = service.validate_for_operation("https://github.com/owner/repo", "load")

        assert result['valid'] is True
        assert result['action_required'] == 'none'

    @patch.object(GitHubService, 'repo_exists')
    def test_validate_for_load_nonexistent_repo(self, mock_repo_exists):
        """Test validation for load operation on non-existent repo."""
        mock_repo_exists.return_value = {
            'exists': False,
            'accessible': False
        }

        service = GitHubService(token="ghp_test")
        result = service.validate_for_operation("https://github.com/owner/repo", "load")

        assert result['valid'] is False
        assert "not found" in result['error'].lower()

    def test_validate_for_operation_invalid_url(self):
        """Test validation with invalid URL."""
        service = GitHubService(token="ghp_test")
        result = service.validate_for_operation("not-a-valid-url", "save")

        assert result['valid'] is False
        assert result['error'] is not None


class TestBranchOperations:
    """Tests for branch operations (mocked)."""

    @patch.object(GitHubService, '_make_request')
    def test_get_branch_success(self, mock_request):
        """Test getting branch info."""
        mock_request.return_value = (
            {
                'ref': 'refs/heads/main',
                'object': {'sha': 'abc123def456'}
            },
            200,
            None
        )

        service = GitHubService(token="ghp_test")
        result = service.get_branch("owner", "repo", "main")

        assert result['success'] is True
        assert result['sha'] == 'abc123def456'
        assert result['ref'] == 'refs/heads/main'

    @patch.object(GitHubService, '_make_request')
    def test_get_branch_not_found(self, mock_request):
        """Test getting non-existent branch."""
        mock_request.return_value = (None, 404, "Not found")

        service = GitHubService(token="ghp_test")
        result = service.get_branch("owner", "repo", "nonexistent")

        assert result['success'] is False
        assert result['sha'] is None


class TestFileOperations:
    """Tests for file operations (mocked)."""

    @patch.object(GitHubService, '_make_request')
    def test_fetch_file_success(self, mock_request):
        """Test fetching a file."""
        import base64
        content = "Hello, World!"
        encoded = base64.b64encode(content.encode()).decode()

        mock_request.return_value = (
            {
                'type': 'file',
                'content': encoded,
                'sha': 'file_sha_123'
            },
            200,
            None
        )

        service = GitHubService(token="ghp_test")
        result = service.fetch_file("owner", "repo", "README.md")

        assert result['success'] is True
        assert result['content'] == content
        assert result['sha'] == 'file_sha_123'

    @patch.object(GitHubService, '_make_request')
    def test_fetch_file_not_found(self, mock_request):
        """Test fetching non-existent file."""
        mock_request.return_value = (None, 404, "Not found")

        service = GitHubService(token="ghp_test")
        result = service.fetch_file("owner", "repo", "nonexistent.txt")

        assert result['success'] is False
        assert result['content'] is None

    @patch.object(GitHubService, '_make_request')
    def test_fetch_directory_success(self, mock_request):
        """Test fetching directory contents."""
        mock_request.return_value = (
            [
                {'name': 'file1.txt', 'type': 'file'},
                {'name': 'dir1', 'type': 'dir'}
            ],
            200,
            None
        )

        service = GitHubService(token="ghp_test")
        result = service.fetch_directory("owner", "repo", "src")

        assert result['success'] is True
        assert len(result['contents']) == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
