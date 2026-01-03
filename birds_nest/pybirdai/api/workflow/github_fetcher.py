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

"""
GitHub file fetcher for downloading files from GitHub repositories.
"""

import os
import logging
import requests
import shutil

from pybirdai.utils.github_file_fetcher import GitHubFileFetcher

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


class ConfigurableGitHubFileFetcher(GitHubFileFetcher):
    """Enhanced GitHub file fetcher that supports configurable repositories and specific file types."""

    def __init__(self, repository_url: str, token: str = None):
        """Initialize with a configurable repository URL and optional token."""
        # Normalize the URL - remove .git suffix if present
        normalized_url = repository_url.rstrip('/')
        if normalized_url.endswith('.git'):
            normalized_url = normalized_url[:-4]

        super().__init__(repository_url)
        self.token = token

    def _get_authenticated_headers(self):
        """Get headers with authentication if token is provided."""
        headers = {}
        if self.token:
            headers['Authorization'] = f'token {self.token}'
        return headers

    def _ensure_directory_exists(self, path):
        """Ensure a directory exists, creating it if necessary."""
        os.makedirs(path, exist_ok=True)
        logger.debug(f"Ensured directory exists: {path}")

    def _clear_directory(self, path):
        """Clear all files from a directory if it exists."""
        if os.path.exists(path):
            for filename in os.listdir(path):
                file_path = os.path.join(path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        logger.debug(f"Deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        logger.debug(f"Deleted directory: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}: {e}")
            logger.info(f"Cleared directory: {path}")
        else:
            logger.debug(f"Directory does not exist, skipping clear: {path}")

    def fetch_files(self, folder_path="", branch="main"):
        """Override to add authentication for private repositories."""
        # Construct GitHub API URL for contents
        api_url = f"{self.api_base}/contents/{folder_path}"
        if branch != "main":
            api_url += f"?ref={branch}"

        logger.info(f"Fetching files from API: {api_url}")

        try:
            headers = self._get_authenticated_headers()
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            files_data = response.json()

            # Ensure we return a list (single files are returned as dict)
            result = files_data if isinstance(files_data, list) else [files_data]
            logger.info(f"Successfully fetched {len(result)} items from {folder_path}")
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching files from {api_url}: {e}")
            print(f"Error fetching files: {e}")
            return []

    def download_file(self, file_info, local_path=None):
        """Override to add authentication for private repositories."""
        # Validate that this is actually a file
        if file_info.get('type') != 'file':
            logger.warning(f"Attempted to download non-file item: {file_info.get('name', 'Unknown')}")
            return None

        # Get the download URL from file info
        download_url = file_info.get('download_url')
        if not download_url:
            logger.warning(f"No download URL found for file: {file_info.get('name', 'Unknown')}")
            return None

        file_name = file_info.get('name', 'Unknown')
        logger.info(f"Downloading file: {file_name} from {download_url}")

        try:
            # Download the file content with authentication headers
            headers = self._get_authenticated_headers()
            response = requests.get(download_url, headers=headers)
            response.raise_for_status()

            if local_path:
                # Ensure directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                # Save to local file system
                logger.debug(f"Saving file to: {local_path}")
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Successfully saved file: {local_path}")
                return local_path
            else:
                # Return file content as text
                logger.debug(f"Returning file content for: {file_name}")
                return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading file {file_name}: {e}")
            print(f"Error downloading file: {e}")
            return None

    def explore_repository_structure(self, max_depth=2):
        """
        Explore the repository structure to find CSV files and directories.

        Args:
            max_depth (int): Maximum depth to explore

        Returns:
            dict: Directory structure with CSV file locations
        """
        structure = {}

        def explore_directory(path="", depth=0):
            if depth > max_depth:
                return

            try:
                files = self.fetch_files(path)
                csv_files = []
                subdirs = []

                for item in files:
                    if item.get('type') == 'file' and item.get('name', '').endswith('.csv'):
                        csv_files.append(item)
                    elif item.get('type') == 'dir':
                        subdirs.append(item.get('name'))

                if csv_files:
                    structure[path or 'root'] = {
                        'csv_files': csv_files,
                        'count': len(csv_files)
                    }
                    logger.info(f"Found {len(csv_files)} CSV files in /{path}")

                # Explore subdirectories
                for subdir in subdirs:
                    subpath = f"{path}/{subdir}" if path else subdir
                    explore_directory(subpath, depth + 1)

            except Exception as e:
                logger.debug(f"Could not explore path /{path}: {e}")

        logger.info("Exploring repository structure for CSV files...")
        explore_directory()
        return structure

    def fetch_derivation_files(self, target_directory: str = f"resources{os.sep}derivation_files{os.sep}"):
        logger.info("Fetching derivation files from export/database_export_ldm to bird/ and admin/ subdirectories")
        self._ensure_directory_exists(target_directory)
        export_path = "birds_nest/resources/derivation_files/"
        try:
            logger.info(f"Fetching derivation files from {export_path}")
            logger.debug(f"Fetching files from {export_path}")
            files = self.fetch_files(export_path)
            for file_info in files:
                file_name = file_info.get('name')
                local_path = os.path.join(target_directory, file_name)
                result = self.download_file(file_info, local_path)
            logger.info(f"Found {len(files)} items in {export_path}")
            logger.debug(f"Raw files response: {len(files)} items")
        except Exception as e:
            logger.error(f"Error fetching derivation files from {export_path}: {e}")
            files = []

    def fetch_technical_exports(self, target_directory: str, force_refresh: bool = False):
        """
        Fetch technical export files from the export/database_export_ldm directory.
        Files starting with 'bird_' go to bird/ subdirectory.
        Files starting with 'admin_' go to admin/ subdirectory.
        All other CSV files are considered technical export files.

        Args:
            target_directory (str): Local directory to save files
            force_refresh (bool): Force re-download even if files exist
        """
        logger.info(f"Fetching technical export files from export/database_export_ldm to {target_directory}")
        logger.debug(f"Fetching technical export files to {target_directory}")

        # Clear directories if force refresh is requested
        if force_refresh:
            logger.debug("Force refresh enabled - clearing existing files")
            bird_dir = os.path.join(target_directory, '..', 'bird')
            admin_dir = os.path.join(target_directory, '..', 'admin')

            self._clear_directory(target_directory)
            self._clear_directory(bird_dir)
            self._clear_directory(admin_dir)
            logger.info("Cleared existing technical export files due to force refresh")

        # Ensure target directory exists
        self._ensure_directory_exists(target_directory)

        # Look specifically in the export/database_export_ldm directory
        export_path = "export/database_export_ldm"
        files_downloaded = 0

        try:
            logger.debug(f"Fetching files from {export_path}")
            files = self.fetch_files(export_path)
            logger.debug(f"Raw files response: {len(files)} items")

            csv_files = [f for f in files if f.get('type') == 'file' and f.get('name', '').endswith('.csv')]
            logger.debug(f"Found {len(csv_files)} CSV files")

            if not csv_files:
                logger.warning(f"No CSV files found in {export_path}")
                logger.debug(f"No CSV files found. All files: {[f.get('name', 'Unknown') for f in files]}")
                return 0

            logger.info(f"Found {len(csv_files)} CSV files in {export_path}")
            logger.debug(f"CSV files found: {[f.get('name', 'Unknown') for f in csv_files]}")

            # Categorize files based on naming patterns
            bird_files = []
            admin_files = []
            technical_export_files = []

            for file_info in csv_files:
                file_name = file_info.get('name', '')
                logger.debug(f"Categorizing file: {file_name}")
                if file_name.lower().startswith('bird_'):
                    bird_files.append(file_info)
                    logger.debug(f"-> bird_ file")
                elif file_name.lower().startswith('auth_'):
                    admin_files.append(file_info)
                    logger.debug(f"-> admin_ file")
                else:
                    technical_export_files.append(file_info)
                    logger.debug(f"-> technical export file")

            logger.info(f"Categorized files: {len(bird_files)} bird_ files, {len(admin_files)} admin_ files, {len(technical_export_files)} technical export files")
            logger.debug(f"Categories: bird_={len(bird_files)}, admin_={len(admin_files)}, technical_export={len(technical_export_files)}")

            # Download bird_ files to bird/ subdirectory
            if bird_files:
                bird_dir = os.path.join(target_directory, '..', 'bird')
                logger.debug(f"Bird directory: {bird_dir}")
                self._ensure_directory_exists(bird_dir)

                for file_info in bird_files:
                    file_name = file_info.get('name')
                    local_path = os.path.join(bird_dir, file_name)
                    logger.debug(f"Downloading bird file {file_name} to {local_path}")

                    if os.path.exists(local_path) and not force_refresh:
                        logger.debug(f"Skipping existing bird file: {file_name}")
                        continue

                    result = self.download_file(file_info, local_path)
                    logger.debug(f"Download result for {file_name}: {result}")
                    if result:
                        files_downloaded += 1
                        logger.info(f"Downloaded bird file: {file_name} to bird/")

            # Download admin_ files to admin/ subdirectory
            if admin_files:
                admin_dir = os.path.join(target_directory, '..', 'admin')
                logger.debug(f"Admin directory: {admin_dir}")
                self._ensure_directory_exists(admin_dir)

                for file_info in admin_files:
                    file_name = file_info.get('name')
                    local_path = os.path.join(admin_dir, file_name)
                    logger.debug(f"Downloading admin file {file_name} to {local_path}")

                    if os.path.exists(local_path) and not force_refresh:
                        logger.debug(f"Skipping existing admin file: {file_name}")
                        continue

                    result = self.download_file(file_info, local_path)
                    logger.debug(f"Download result for {file_name}: {result}")
                    if result:
                        files_downloaded += 1
                        logger.info(f"Downloaded admin file: {file_name} to admin/")

            # Download technical export files to main technical_export directory
            for file_info in technical_export_files:
                file_name = file_info.get('name')
                local_path = os.path.join(target_directory, file_name)
                logger.debug(f"Downloading technical export file {file_name} to {local_path}")

                if os.path.exists(local_path) and not force_refresh:
                    logger.debug(f"Skipping existing technical export file: {file_name}")
                    continue

                result = self.download_file(file_info, local_path)
                logger.debug(f"Download result for {file_name}: {result}")
                if result:
                    files_downloaded += 1
                    logger.info(f"Downloaded technical export file: {file_name}")

        except Exception as e:
            logger.error(f"Error accessing {export_path}: {e}")
            logger.debug(f"Exception occurred: {e}")
            import traceback
            traceback.print_exc()
            return 0

        if files_downloaded == 0:
            logger.warning("No new files downloaded from export/database_export_ldm")
            logger.debug("No files were downloaded")
        else:
            logger.info(f"Successfully downloaded {files_downloaded} files from export/database_export_ldm")
            logger.debug(f"Successfully downloaded {files_downloaded} files")

        return files_downloaded

    def fetch_configuration_files(self, target_base_directory: str, force_refresh: bool = False):
        """
        Fetch configuration files (joins configuration, extra variables) from the repository.

        Args:
            target_base_directory (str): Base directory containing subdirectories for different file types
            force_refresh (bool): Force re-download even if files exist
        """
        logger.info(f"Fetching configuration files to {target_base_directory}")

        # Clear configuration directories if force refresh is requested
        if force_refresh:
            logger.info("Force refresh enabled - clearing existing configuration files")
            config_subdirs = ["ldm", "il", "joins_configuration", "extra_variables"]
            for subdir in config_subdirs:
                config_dir = os.path.join(target_base_directory, subdir)
                self._clear_directory(config_dir)
            logger.info("Cleared existing configuration files due to force refresh")

        files_downloaded = 0

        # Configuration file mappings: remote_path -> local_subdirectory
        config_mappings = {
            "birds_nest/resources/ldm": "ldm",
            "birds_nest/resources/il": "il",
            "birds_nest/resources/joins_configuration": "joins_configuration",
            "birds_nest/resources/extra_variables": "extra_variables",
            "resources/ldm": "ldm",
            "resources/il": "il",
            "resources/joins_configuration": "joins_configuration",
            "resources/extra_variables": "extra_variables",
            "ldm": "ldm",
            "il": "il",
            "joins_configuration": "joins_configuration",
            "extra_variables": "extra_variables",
            "config": "joins_configuration",  # Alternative name
            "configuration": "joins_configuration"  # Alternative name
        }

        for remote_path, local_subdir in config_mappings.items():
            try:
                files = self.fetch_files(remote_path)
                csv_files = [f for f in files if f.get('type') == 'file' and f.get('name', '').endswith('.csv')]
                if csv_files:
                    target_dir = os.path.join(target_base_directory, local_subdir)
                    self._ensure_directory_exists(target_dir)

                    logger.info(f"Found {len(csv_files)} CSV files in {remote_path}")
                    for file_info in csv_files:
                        file_name = file_info.get('name')
                        local_path = os.path.join(target_dir, file_name)

                        # Skip if file exists and not forcing refresh
                        if os.path.exists(local_path) and not force_refresh:
                            logger.debug(f"Skipping existing file: {file_name}")
                            continue

                        if self.download_file(file_info, local_path):
                            files_downloaded += 1
                            logger.info(f"Downloaded config file: {file_name} to {local_subdir}/")
            except Exception as e:
                logger.debug(f"Path {remote_path} not found or inaccessible: {e}")
                continue

        logger.info(f"Successfully downloaded {files_downloaded} configuration files")
        return files_downloaded

    def fetch_ldm_files(self, target_directory: str, force_refresh: bool = False):
        """
        Fetch LDM export files from the repository.

        Args:
            target_directory (str): Local directory to save LDM files
            force_refresh (bool): Force re-download even if files exist
        """
        logger.info(f"Fetching LDM files to {target_directory}")

        # Ensure target directory exists
        self._ensure_directory_exists(target_directory)

        # Try common paths for LDM files
        possible_paths = [
            "birds_nest/resources/ldm",
            "resources/ldm",
            "ldm",
            "export/database_export_ldm"
        ]

        files_downloaded = 0
        for path in possible_paths:
            try:
                files = self.fetch_files(path)
                if files:
                    logger.info(f"Found {len(files)} files in {path}")
                    for file_info in files:
                        if file_info.get('type') == 'file':
                            file_name = file_info.get('name')
                            local_path = os.path.join(target_directory, file_name)

                            # Skip if file exists and not forcing refresh
                            if os.path.exists(local_path) and not force_refresh:
                                logger.debug(f"Skipping existing file: {file_name}")
                                continue

                            if self.download_file(file_info, local_path):
                                files_downloaded += 1
                                logger.info(f"Downloaded LDM file: {file_name}")
                    break  # Stop after finding first valid path
            except Exception as e:
                logger.debug(f"Path {path} not found or inaccessible: {e}")
                continue

        if files_downloaded == 0:
            logger.warning("No LDM files found in repository")
        else:
            logger.info(f"Successfully downloaded {files_downloaded} LDM files")

        return files_downloaded

    def _fetch_directory_recursive(self, remote_path: str, local_base_dir: str, force_refresh: bool = False):
        """
        Recursively fetch all files from a remote directory and its subdirectories.

        Args:
            remote_path (str): Remote directory path to fetch from
            local_base_dir (str): Local base directory to save files to
            force_refresh (bool): Force re-download even if files exist

        Returns:
            int: Number of files downloaded
        """
        files_downloaded = 0

        try:
            items = self.fetch_files(remote_path)

            for item in items:
                item_name = item.get('name', '')
                item_type = item.get('type', '')

                if item_type == 'file':
                    # Download file
                    local_path = os.path.join(local_base_dir, item_name)

                    # Skip if file exists and not forcing refresh
                    if os.path.exists(local_path) and not force_refresh:
                        logger.debug(f"Skipping existing file: {item_name}")
                        continue

                    # Ensure parent directory exists
                    self._ensure_directory_exists(os.path.dirname(local_path))

                    result = self.download_file(item, local_path)
                    if result:
                        files_downloaded += 1
                        logger.debug(f"Downloaded file: {item_name}")

                elif item_type == 'dir':
                    # Recursively fetch subdirectory
                    sub_remote_path = f"{remote_path}/{item_name}"
                    sub_local_dir = os.path.join(local_base_dir, item_name)
                    self._ensure_directory_exists(sub_local_dir)
                    files_downloaded += self._fetch_directory_recursive(
                        sub_remote_path, sub_local_dir, force_refresh
                    )

        except Exception as e:
            logger.error(f"Error fetching directory {remote_path}: {e}")

        return files_downloaded

    def fetch_generated_python_files(self, target_directory: str, force_refresh: bool = False):
        """
        Fetch generated Python files from the repository's filter_code directory.
        Now supports the new unified structure with subdirectories:
        - lib/
        - datasets/{FRAMEWORK}/filter/, datasets/{FRAMEWORK}/joins/
        - templates/{FRAMEWORK}/filter/, templates/{FRAMEWORK}/joins/

        Args:
            target_directory (str): Local directory to save generated Python files
            force_refresh (bool): Force re-download even if files exist

        Returns:
            int: Number of Python files downloaded
        """
        logger.info(f"Fetching generated Python files to {target_directory}")

        # Ensure target directory exists
        self._ensure_directory_exists(target_directory)

        # Clear directory if force refresh is requested
        if force_refresh:
            logger.info("Force refresh enabled - clearing existing generated Python files")
            self._clear_directory(target_directory)
            logger.info("Cleared existing generated Python files due to force refresh")

        # Look for Python files in the filter_code directory (recursively)
        filter_code_path = "birds_nest/pybirdai/process_steps/filter_code"

        logger.info(f"Fetching Python files recursively from {filter_code_path}")
        files_downloaded = self._fetch_directory_recursive(
            filter_code_path, target_directory, force_refresh
        )

        if files_downloaded == 0:
            logger.warning("No new Python files downloaded from filter_code directory")
        else:
            logger.info(f"Successfully downloaded {files_downloaded} generated Python files")

        return files_downloaded

    def fetch_filter_code(
            self,
            remote_dir="birds_nest/pybirdai/process_steps/filter_code",
            local_target_dir=f"pybirdai{os.sep}process_steps{os.sep}filter_code",
            force_refresh: bool = False):
        """
        Fetches filter code files from the specified remote directory.
        Now supports the new unified structure with subdirectories:
        - lib/
        - datasets/{FRAMEWORK}/filter/, datasets/{FRAMEWORK}/joins/
        - templates/{FRAMEWORK}/filter/, templates/{FRAMEWORK}/joins/

        Args:
            remote_dir (str): Remote directory path to fetch files from
            local_target_dir (str): Local directory path to save files to
            force_refresh (bool): Force re-download even if files exist

        Returns:
            int: Number of files successfully downloaded
        """
        logger.info(f"Fetching filter code files recursively from {remote_dir} to {local_target_dir}")

        # Ensure the local directory exists
        self._ensure_directory_exists(local_target_dir)

        # Use recursive fetch to handle subdirectories
        files_downloaded = self._fetch_directory_recursive(
            remote_dir, local_target_dir, force_refresh
        )

        if files_downloaded == 0:
            logger.warning("No filter code files downloaded")
        else:
            logger.info(f"Successfully downloaded {files_downloaded} filter code files")

        return files_downloaded

    def fetch_test_fixtures(self, base_url: str = ""):
        """
        Fetch test fixtures and templates from the repository.

        Args:
            base_url (str): Base URL (currently unused but kept for compatibility)

        Returns:
            int: Number of test fixture files successfully downloaded
        """
        logger.info("Fetching test fixtures and templates from repository")

        files_downloaded = 0

        try:
            # Get commit information for the test fixtures directory
            self.get_commit_info("tests/fixtures/templates/")
            logger.info("Retrieved commit info for test fixtures")

            # Ensure the local directory exists
            local_target_dir = f"pybirdai{os.sep}tests{os.sep}fixtures{os.sep}templates{os.sep}"
            self._ensure_directory_exists(local_target_dir)

            # Process all cached file information
            path_downloaded = set()
            initial_count = len(path_downloaded)

            for folder_data in self.files.values():
                self.fetch_test_fixture(folder_data, path_downloaded)

            # Calculate files downloaded by checking the difference in path_downloaded set
            files_downloaded = len(path_downloaded) - initial_count

        except Exception as e:
            logger.error(f"Error fetching test fixtures: {e}")
            return 0

        if files_downloaded == 0:
            logger.warning("No test fixture files downloaded")
        else:
            logger.info(f"Successfully downloaded {files_downloaded} test fixture files")

        return files_downloaded
