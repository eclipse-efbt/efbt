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
Unit tests for GitHubFileFetcher.

These tests cover file fetching operations using mocking to avoid actual GitHub API calls.

Usage:
    python -m pytest pybirdai/tests/clone_mode/test_github_file_fetcher.py -v
"""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add the birds_nest directory to the path
BIRDS_NEST_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(BIRDS_NEST_DIR))

from pybirdai.utils.github_file_fetcher import GitHubFileFetcher


class TestGitHubFileFetcherInit:
    """Tests for GitHubFileFetcher initialization."""

    def test_init_with_valid_url(self):
        """Test initialization with valid GitHub URL."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")
        assert fetcher.owner == "owner"
        assert fetcher.repo == "repo"
        assert fetcher.api_base == "https://api.github.com/repos/owner/repo"
        assert fetcher.token is None

    def test_init_with_token(self):
        """Test initialization with authentication token."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo", token="ghp_test")
        assert fetcher.token == "ghp_test"

    def test_init_with_git_suffix(self):
        """Test initialization with .git suffix in URL."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo.git")
        assert fetcher.owner == "owner"
        assert fetcher.repo == "repo"

    def test_init_with_invalid_url(self):
        """Test initialization with invalid URL raises ValueError."""
        with pytest.raises(ValueError) as excinfo:
            GitHubFileFetcher("https://not-github.com/owner/repo")
        assert "Invalid GitHub URL" in str(excinfo.value)

    def test_init_with_empty_url(self):
        """Test initialization with empty URL raises ValueError."""
        with pytest.raises(ValueError):
            GitHubFileFetcher("")


class TestHeaders:
    """Tests for header generation."""

    def test_headers_without_token(self):
        """Test headers without authentication token."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")
        headers = fetcher._get_headers()

        assert 'Accept' in headers
        assert 'Authorization' not in headers

    def test_headers_with_token(self):
        """Test headers with authentication token."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo", token="ghp_test")
        headers = fetcher._get_headers()

        assert headers['Authorization'] == 'token ghp_test'


class TestRawURL:
    """Tests for raw URL construction."""

    def test_construct_raw_url(self):
        """Test raw URL construction."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")
        raw_url = fetcher._construct_raw_url("path/to/file.txt", branch="main")

        assert raw_url == "https://raw.githubusercontent.com/owner/repo/main/path/to/file.txt"

    def test_construct_raw_url_different_branch(self):
        """Test raw URL with different branch."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")
        raw_url = fetcher._construct_raw_url("file.txt", branch="develop")

        assert raw_url == "https://raw.githubusercontent.com/owner/repo/develop/file.txt"


class TestErrorHandling:
    """Tests for error handling."""

    def test_handle_404_error(self):
        """Test 404 error is recognized as 'not found'."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        error = Exception("404 Not Found")
        result = fetcher._handle_request_error(error, "test context")

        assert result is True  # True means it was a 404

    def test_handle_other_error(self):
        """Test non-404 error returns False."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        error = Exception("500 Internal Server Error")
        result = fetcher._handle_request_error(error, "test context")

        assert result is False


class TestDirectoryOperations:
    """Tests for directory operations."""

    def test_ensure_directory_exists(self):
        """Test directory creation."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "subdir", "nested")
            fetcher._ensure_directory_exists(test_path)

            assert os.path.exists(test_path)
            assert os.path.isdir(test_path)


class TestFetchFiles:
    """Tests for file fetching operations (mocked)."""

    @patch('requests.get')
    def test_fetch_files_success(self, mock_get):
        """Test successful file listing."""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {'name': 'file1.txt', 'type': 'file'},
            {'name': 'dir1', 'type': 'dir'}
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        fetcher = GitHubFileFetcher("https://github.com/owner/repo")
        files = fetcher.fetch_files("some/path")

        assert len(files) == 2
        assert files[0]['name'] == 'file1.txt'

    @patch('requests.get')
    def test_fetch_files_error_returns_empty(self, mock_get):
        """Test error handling returns empty list."""
        import requests
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")

        fetcher = GitHubFileFetcher("https://github.com/owner/repo")
        files = fetcher.fetch_files("some/path")

        assert files == []


class TestDownloadFile:
    """Tests for file download operations (mocked)."""

    @patch('requests.get')
    def test_download_file_success(self, mock_get):
        """Test successful file download."""
        mock_response = MagicMock()
        mock_response.content = b"File content here"
        mock_response.text = "File content here"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "test.txt")
            file_info = {
                'name': 'test.txt',
                'type': 'file',
                'download_url': 'https://raw.githubusercontent.com/owner/repo/main/test.txt'
            }

            result = fetcher.download_file(file_info, local_path)

            assert result == local_path
            assert os.path.exists(local_path)
            with open(local_path, 'rb') as f:
                assert f.read() == b"File content here"

    def test_download_file_non_file_type(self):
        """Test download skips non-file types."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        file_info = {'name': 'dir', 'type': 'dir'}
        result = fetcher.download_file(file_info)

        assert result is None

    def test_download_file_no_download_url(self):
        """Test download returns None when no download URL."""
        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        file_info = {'name': 'file.txt', 'type': 'file'}  # No download_url
        result = fetcher.download_file(file_info)

        assert result is None

    @patch('requests.get')
    def test_download_file_return_content(self, mock_get):
        """Test downloading file and returning content (not saving)."""
        mock_response = MagicMock()
        mock_response.text = "File content returned"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        file_info = {
            'name': 'test.txt',
            'type': 'file',
            'download_url': 'https://raw.githubusercontent.com/owner/repo/main/test.txt'
        }

        # Don't provide local_path - should return content
        result = fetcher.download_file(file_info)

        assert result == "File content returned"


class TestDownloadFromRawURL:
    """Tests for raw URL download operations."""

    @patch('requests.get')
    def test_download_from_raw_url_success(self, mock_get):
        """Test successful download from raw URL."""
        mock_response = MagicMock()
        mock_response.content = b"Raw content"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "file.txt")
            raw_url = "https://raw.githubusercontent.com/owner/repo/main/file.txt"

            result = fetcher._download_from_raw_url(raw_url, local_path)

            assert result is True
            assert os.path.exists(local_path)

    @patch('requests.get')
    def test_download_from_raw_url_failure(self, mock_get):
        """Test failed download from raw URL."""
        import requests
        mock_get.side_effect = requests.exceptions.RequestException("Failed")

        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "file.txt")

            result = fetcher._download_from_raw_url("https://example.com", local_path)

            assert result is False


class TestFetchDirectoryRecursively:
    """Tests for recursive directory fetching (mocked)."""

    @patch.object(GitHubFileFetcher, 'fetch_files')
    @patch.object(GitHubFileFetcher, 'download_file')
    def test_fetch_directory_recursively(self, mock_download, mock_fetch):
        """Test recursive directory fetching."""
        # First call returns files and a dir, second call returns more files
        mock_fetch.side_effect = [
            [
                {'name': 'file1.txt', 'type': 'file'},
                {'name': 'subdir', 'type': 'dir'}
            ],
            [
                {'name': 'file2.txt', 'type': 'file'}
            ]
        ]
        mock_download.return_value = "/path/to/file"

        fetcher = GitHubFileFetcher("https://github.com/owner/repo")

        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher.fetch_directory_recursively("src", tmpdir)

            # Should have called fetch_files twice (root and subdir)
            assert mock_fetch.call_count == 2
            # Should have called download_file twice (2 files)
            assert mock_download.call_count == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
