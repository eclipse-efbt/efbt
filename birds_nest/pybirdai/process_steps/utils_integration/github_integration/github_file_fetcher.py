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

import requests
import os
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class GitHubFileFetcherProcessStep:
    """
    Process step for fetching files from GitHub repositories.
    Refactored from utils.github_file_fetcher to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the GitHub file fetcher process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        self.files = {}
        
    def execute(self, base_url: str, target_directory: str = None, **kwargs) -> Dict[str, Any]:
        """
        Execute the GitHub file fetching process.
        
        Args:
            base_url (str): GitHub repository URL
            target_directory (str): Local directory to save files
            **kwargs: Additional parameters for fetching
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            fetcher = GitHubFileFetcher(base_url)
            
            if target_directory:
                fetcher._ensure_directory_exists(target_directory)
            
            result = {
                'success': True,
                'fetcher': fetcher,
                'message': f'GitHub file fetcher initialized for {base_url}'
            }
            
            if self.context:
                self.context.github_fetcher = fetcher
                
            return result
            
        except Exception as e:
            logger.error(f"Failed to initialize GitHub file fetcher: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'GitHub file fetcher initialization failed'
            }


class GitHubFileFetcher:
    """
    GitHub file fetcher implementation - moved from utils but keeping interface.
    """
    
    def __init__(self, base_url):
        """
        Initialize GitHub file fetcher with repository URL.

        Args:
            base_url (str): GitHub repository URL (e.g., https://github.com/owner/repo)
        """
        logger.info(f"Initializing GitHubFileFetcher with URL: {base_url}")

        self.base_url = base_url
        # Parse the GitHub URL to extract owner and repository name
        parts = base_url.replace('https://github.com/', '').split('/')
        self.owner = parts[0]
        self.repo = parts[1]
        # Construct the GitHub API base URL
        self.api_base = f"https://api.github.com/repos/{self.owner}/{self.repo}"
        # Dictionary to cache file information
        self.files = {}

        logger.info(f"Configured for repository: {self.owner}/{self.repo}")

    def _handle_request_error(self, error, context):
        """
        Common error handling for HTTP requests.

        Args:
            error (Exception): The exception that occurred
            context (str): Context description for logging

        Returns:
            bool: True if error was a 404 (not found), False for other errors
        """
        if "404" in str(error) or "Not Found" in str(error):
            logger.warning(f"URL not found: {context}")
            print(f"URL not found: {context}")
            return True
        logger.error(f"Request exception for {context}: {error}")
        return False

    def _ensure_directory_exists(self, path):
        """
        Ensure a directory exists, creating it if necessary.

        Args:
            path (str): Directory path to create
        """
        os.makedirs(path, exist_ok=True)

    def _construct_raw_url(self, file_path, branch="main"):
        """
        Construct a raw GitHub URL for direct file download.

        Args:
            file_path (str): Path to the file in the repository
            branch (str): Git branch name

        Returns:
            str: Raw GitHub URL
        """
        return f"https://raw.githubusercontent.com/{self.owner}/{self.repo}/{branch}/{file_path}"

    def _download_from_raw_url(self, raw_url, local_path):
        """
        Download a file from a raw GitHub URL.

        Args:
            raw_url (str): Raw GitHub URL
            local_path (str): Local path to save the file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = requests.get(raw_url)
            response.raise_for_status()

            with open(local_path, 'wb') as f:
                f.write(response.content)

            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download from {raw_url}: {e}")
            return False

    def fetch_files(self, path="", branch="main"):
        """
        Fetch file list from a specific directory in the repository.

        Args:
            path (str): Directory path within the repository
            branch (str): Git branch name

        Returns:
            list: List of file information dictionaries
        """
        url = f"{self.api_base}/contents/{path}?ref={branch}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if not self._handle_request_error(e, f"fetching files from {path}"):
                raise
            return []

    def download_file(self, file_info, local_path, branch="main"):
        """
        Download a file using file information from GitHub API.

        Args:
            file_info (dict): File information from GitHub API
            local_path (str): Local path to save the file
            branch (str): Git branch name

        Returns:
            bool: True if successful, False otherwise
        """
        if file_info.get('type') != 'file':
            logger.warning(f"Skipping non-file item: {file_info.get('name', 'Unknown')}")
            return False

        file_path = file_info.get('path', '')
        raw_url = self._construct_raw_url(file_path, branch)
        
        # Ensure the parent directory exists
        parent_dir = os.path.dirname(local_path)
        if parent_dir:
            self._ensure_directory_exists(parent_dir)

        return self._download_from_raw_url(raw_url, local_path)