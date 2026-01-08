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
Mirror service for non-destructive file updates during workflow execution.

This service is NON-DESTRUCTIVE - it preserves existing generated content.
Use during workflow execution (Step 1) to update content files without
erasing user-generated Python code, filter code, or test results.

For initial setup (destructive), use SetupRepoService instead.
"""

import requests
import zipfile
import os
import shutil
import logging
import time

from .helpers import (
    MIRROR_MAPPING,
    PROTECTED_DIRS,
    MERGE_DIRS,
    UPDATE_DIRS,
    WHITELIST_FILES,
    TMP_PLACEHOLDER_FILES,
)

logger = logging.getLogger(__name__)


class MirrorRepoService:
    """
    Service class for non-destructive file mirroring during workflow execution.

    Unlike SetupRepoService, this service:
    - NEVER deletes protected directories (generated code, filter code)
    - Merges new files into merge directories (tests, derivation files)
    - Updates configuration files in place
    - Preserves all user-generated content

    Use this service when starting a workflow (Step 1) to refresh content
    without losing generated artifacts.
    """

    def __init__(self, token: str = None):
        """
        Initialize the service and set up authentication.

        Args:
            token (str): Optional GitHub token for private repository access
        """
        self.token = token
        self.headers = self._get_authenticated_headers()

    def _get_authenticated_headers(self):
        """Get headers with authentication if token is provided."""
        headers = {}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers

    def _ensure_directory_exists(self, path):
        """Ensure a directory exists, creating it if necessary."""
        os.makedirs(path, exist_ok=True)
        logger.debug(f"Ensured directory exists: {path}")

    def _is_protected(self, target_folder):
        """
        Check if a target folder is protected from deletion.

        Args:
            target_folder (str): The folder path to check

        Returns:
            bool: True if the folder should be protected, False otherwise
        """
        normalized_target = target_folder.replace('/', os.sep).replace('\\', os.sep)
        for protected in PROTECTED_DIRS:
            normalized_protected = protected.replace('/', os.sep).replace('\\', os.sep)
            if normalized_target == normalized_protected or normalized_target.startswith(normalized_protected + os.sep):
                return True
        return False

    def _should_merge(self, target_folder):
        """
        Check if a target folder should use merge strategy (add new, keep existing).

        Args:
            target_folder (str): The folder path to check

        Returns:
            bool: True if the folder should be merged, False otherwise
        """
        normalized_target = target_folder.replace('/', os.sep).replace('\\', os.sep)
        for merge_dir in MERGE_DIRS:
            normalized_merge = merge_dir.replace('/', os.sep).replace('\\', os.sep)
            if normalized_target == normalized_merge or normalized_target.startswith(normalized_merge + os.sep):
                return True
        return False

    def _copy_file_if_newer(self, source_path, target_path):
        """
        Copy a file only if source is newer than target or target doesn't exist.

        Args:
            source_path (str): Source file path
            target_path (str): Target file path

        Returns:
            bool: True if file was copied, False otherwise
        """
        if not os.path.exists(target_path):
            # Target doesn't exist, copy
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.copy2(source_path, target_path)
            logger.debug(f"Copied new file: {target_path}")
            return True

        # Compare modification times
        source_mtime = os.path.getmtime(source_path)
        target_mtime = os.path.getmtime(target_path)

        if source_mtime > target_mtime:
            shutil.copy2(source_path, target_path)
            logger.debug(f"Updated file (source newer): {target_path}")
            return True

        logger.debug(f"Skipped file (target is current): {target_path}")
        return False

    def _merge_directory(self, source_path, target_path):
        """
        Merge a source directory into target without deleting existing files.

        Args:
            source_path (str): Source directory path
            target_path (str): Target directory path

        Returns:
            int: Number of files copied/updated
        """
        files_updated = 0
        self._ensure_directory_exists(target_path)

        for root, dirs, files in os.walk(source_path):
            # Calculate relative path from source
            rel_path = os.path.relpath(root, source_path)
            if rel_path == '.':
                target_dir = target_path
            else:
                target_dir = os.path.join(target_path, rel_path)

            self._ensure_directory_exists(target_dir)

            for file in files:
                source_file = os.path.join(root, file)
                target_file = os.path.join(target_dir, file)

                if self._copy_file_if_newer(source_file, target_file):
                    files_updated += 1

        return files_updated

    def _update_directory(self, source_path, target_path):
        """
        Update a directory by copying files from source.
        Does NOT delete existing files not in source (safe update).

        Args:
            source_path (str): Source directory path
            target_path (str): Target directory path

        Returns:
            int: Number of files copied/updated
        """
        files_updated = 0
        self._ensure_directory_exists(target_path)

        for root, dirs, files in os.walk(source_path):
            # Calculate relative path from source
            rel_path = os.path.relpath(root, source_path)
            if rel_path == '.':
                target_dir = target_path
            else:
                target_dir = os.path.join(target_path, rel_path)

            self._ensure_directory_exists(target_dir)

            for file in files:
                source_file = os.path.join(root, file)
                target_file = os.path.join(target_dir, file)

                # Always overwrite for update directories
                shutil.copy2(source_file, target_file)
                files_updated += 1
                logger.debug(f"Updated file: {target_file}")

        return files_updated

    def clone_repo(self, base_url: str = None, destination_path: str = "FreeBIRD", branch: str = "main"):
        """
        Download and extract a repository from GitHub as a ZIP file.

        Args:
            base_url (str): The base URL of the GitHub repository. If None, reads from configuration.
            destination_path (str): Local directory to extract the repository to
            branch (str): The branch to clone
        """
        # Get URL from configuration if not provided
        if base_url is None:
            from pybirdai.services.pipeline_repo_service import get_configured_pipeline_url
            base_url = get_configured_pipeline_url('main')
            if not base_url:
                raise ValueError("No pipeline URL configured. Please configure it in the dashboard settings.")

        start_time = time.time()
        logger.info(f"Starting repository clone from {base_url} to {destination_path}")

        # Construct the ZIP download URL for the main branch
        repo_url = f"{base_url}/archive/refs/heads/{branch}.zip"
        logger.info(f"Downloading repository from {repo_url}")

        # Download the repository ZIP file with authentication if available
        response = requests.get(repo_url, headers=self.headers)
        os.makedirs(destination_path, exist_ok=True)

        if response.status_code == 200:
            logger.info("Repository downloaded successfully, extracting files")
            # Save the ZIP file temporarily
            with open(f"{destination_path}{os.sep}repo.zip", "wb") as f:
                f.write(response.content)

            # Extract the ZIP file contents
            with zipfile.ZipFile(f"{destination_path}{os.sep}repo.zip", "r") as zip_ref:
                zip_ref.extractall(destination_path)

            # Clean up the temporary ZIP file
            os.remove(f"{destination_path}{os.sep}repo.zip")
            logger.info("Repository extraction completed")
        else:
            # Handle download failure
            logger.error(f"Failed to clone repository: {response.status_code}")
            if response.status_code == 401:
                logger.error("Authentication failed - check your GitHub token")
            elif response.status_code == 404:
                logger.error("Repository not found - check the URL")
            print(f"Failed to clone repository: {response.status_code}")

        end_time = time.time()
        logger.info(f"Clone repo completed in {end_time - start_time:.2f} seconds")

    def mirror_files(self, destination_path: str = "FreeBIRD"):
        """
        Mirror files from the extracted repository WITHOUT destroying existing content.

        Strategy:
        1. Skip PROTECTED directories entirely (generated code, filter code)
        2. Merge new files into MERGE directories (tests, derivation files)
        3. Update files in UPDATE directories (ldm, technical_export, config)
        4. Preserve all user-generated content

        Args:
            destination_path (str): Path where the repository was extracted

        Returns:
            dict: Summary of files updated
        """
        start_time = time.time()
        logger.info(f"Starting NON-DESTRUCTIVE file mirror from {destination_path}")
        logger.info("Protected directories will NOT be modified")

        results = {
            'protected_skipped': [],
            'directories_merged': [],
            'directories_updated': [],
            'files_updated': 0,
            'errors': []
        }

        # Find the extracted folder
        extracted_folder = None
        for item in os.listdir(destination_path):
            item_path = os.path.join(destination_path, item)
            if os.path.isdir(item_path):
                extracted_folder = item_path
                break

        if not extracted_folder:
            logger.error("Could not find extracted repository folder")
            results['errors'].append("Could not find extracted repository folder")
            return results

        logger.info(f"Found extracted folder: {extracted_folder}")

        # Process each mapping in MIRROR_MAPPING (not REPO_MAPPING!)
        for source_folder, target_mappings in MIRROR_MAPPING.items():
            source_path = os.path.join(extracted_folder, source_folder)
            logger.debug(f"Processing source folder: {source_path}")

            # Enhanced logging for filter_code debugging
            is_filter_code = 'filter_code' in source_folder
            if is_filter_code:
                logger.info(f"[FILTER_CODE] Processing mapping: {source_folder}")
                logger.info(f"[FILTER_CODE] Full source path: {source_path}")
                logger.info(f"[FILTER_CODE] Source exists: {os.path.exists(source_path)}")
                if os.path.exists(source_path):
                    # List contents to debug structure
                    try:
                        contents = os.listdir(source_path)
                        logger.info(f"[FILTER_CODE] Source contents: {contents}")
                        # Check for subdirectories
                        for item in contents:
                            item_path = os.path.join(source_path, item)
                            if os.path.isdir(item_path):
                                sub_contents = os.listdir(item_path)
                                logger.info(f"[FILTER_CODE] Subdir '{item}' contents: {sub_contents}")
                    except Exception as e:
                        logger.error(f"[FILTER_CODE] Error listing contents: {e}")

            # Skip if source folder doesn't exist
            if not os.path.exists(source_path):
                logger.debug(f"Source path does not exist, skipping: {source_path}")
                if is_filter_code:
                    logger.warning(f"[FILTER_CODE] Source path NOT FOUND: {source_path}")
                continue

            # Special handling for database export files that need filtering
            if f"export{os.sep}database_export_ldm" == source_folder:
                for file_name in os.listdir(source_path):
                    for target_folder, filter_func in target_mappings.items():
                        if filter_func(file_name):
                            # Check if target is protected
                            if self._is_protected(target_folder):
                                logger.info(f"Skipping protected directory: {target_folder}")
                                if target_folder not in results['protected_skipped']:
                                    results['protected_skipped'].append(target_folder)
                                break

                            source_file = os.path.join(source_path, file_name)
                            self._ensure_directory_exists(target_folder)
                            target_file = os.path.join(target_folder, file_name)

                            if os.path.isfile(source_file):
                                shutil.copy2(source_file, target_file)
                                results['files_updated'] += 1
                                logger.debug(f"Updated file: {file_name}")
                            break
                continue

            # Get the target folder
            target_folder = list(target_mappings.keys())[0]

            # Check if target is protected
            is_protected = self._is_protected(target_folder)
            if is_filter_code:
                logger.info(f"[FILTER_CODE] Target folder: {target_folder}")
                logger.info(f"[FILTER_CODE] Is protected: {is_protected}")

            if is_protected:
                logger.info(f"Skipping protected directory: {target_folder}")
                results['protected_skipped'].append(target_folder)
                continue

            source_folder_path = os.path.join(extracted_folder, source_folder)

            # Choose strategy based on target type
            should_merge = self._should_merge(target_folder)
            if is_filter_code:
                logger.info(f"[FILTER_CODE] Should merge: {should_merge}")

            if should_merge:
                logger.info(f"Merging directory: {source_folder_path} -> {target_folder}")
                files_count = self._merge_directory(source_folder_path, target_folder)
                results['directories_merged'].append(target_folder)
                results['files_updated'] += files_count
                if is_filter_code:
                    logger.info(f"[FILTER_CODE] Merged {files_count} files")
            else:
                logger.info(f"Updating directory: {source_folder_path} -> {target_folder}")
                files_count = self._update_directory(source_folder_path, target_folder)
                results['directories_updated'].append(target_folder)
                results['files_updated'] += files_count
                if is_filter_code:
                    logger.info(f"[FILTER_CODE] Updated {files_count} files")

        # Recreate tmp placeholder files (these are safe to recreate)
        self._recreate_tmp_placeholders()

        end_time = time.time()
        logger.info(f"File mirror completed in {end_time - start_time:.2f} seconds")
        logger.info(f"Summary: {results['files_updated']} files updated, "
                   f"{len(results['protected_skipped'])} protected directories skipped")

        return results

    def mirror_test_suite_files(self, destination_path: str = "FreeBIRD"):
        """
        Mirror test suite files WITHOUT destroying existing test results.

        Args:
            destination_path (str): Path where the repository was extracted

        Returns:
            dict: Summary of files updated
        """
        start_time = time.time()
        logger.info(f"Starting NON-DESTRUCTIVE test suite mirror from {destination_path}")

        results = {
            'files_copied': 0,
            'dirs_copied': 0,
            'skipped': 0,
            'errors': []
        }

        def should_skip_file(file_path, file_name):
            """Check if a file should be skipped (e.g., test results)."""
            # Skip test result files
            if 'test_results' in file_path:
                if file_name.endswith('.json') or file_name.endswith('.txt'):
                    return True
            return False

        # Find the extracted folder
        extracted_folder = None
        repo_name = None
        for item in os.listdir(destination_path):
            item_path = os.path.join(destination_path, item)
            if os.path.isdir(item_path):
                extracted_folder = item_path
                repo_name = item.rsplit('-', 1)[0] if '-' in item else item
                logger.info(f"Found extracted folder: {extracted_folder}, repo name: {repo_name}")
                break

        if not extracted_folder:
            logger.error("Could not find extracted repository folder for test suite files")
            results['errors'].append("Could not find extracted repository folder")
            return results

        # Create target base directory: tests/{repo_name}/
        target_base = os.path.join("tests", repo_name)
        logger.info(f"Test suite will be mirrored to: {target_base}")

        for item in os.listdir(extracted_folder):
            source_item_path = os.path.join(extracted_folder, item)

            # Skip hidden files and directories
            if item.startswith('.'):
                continue

            # Skip README and other documentation files
            if item.lower() in ['readme.md', 'readme.txt', 'license', 'license.txt', 'license.md']:
                continue

            try:
                if os.path.isdir(source_item_path):
                    target_dir = os.path.join(target_base, item)

                    # Use merge strategy instead of replace
                    files_count = self._merge_directory(source_item_path, target_dir)
                    results['files_copied'] += files_count
                    results['dirs_copied'] += 1
                    logger.info(f"Merged directory: {item} -> {target_dir} ({files_count} files)")

                elif os.path.isfile(source_item_path):
                    target_file = os.path.join(target_base, item)

                    if should_skip_file(target_file, item):
                        results['skipped'] += 1
                        continue

                    if self._copy_file_if_newer(source_item_path, target_file):
                        results['files_copied'] += 1

            except Exception as e:
                logger.error(f"Error mirroring {item}: {e}")
                results['errors'].append(str(e))

        end_time = time.time()
        logger.info(f"Test suite mirror completed in {end_time - start_time:.2f} seconds")
        logger.info(f"Summary: {results['dirs_copied']} directories, {results['files_copied']} files mirrored")

        return results

    def _recreate_tmp_placeholders(self):
        """
        Recreate empty tmp placeholder files after GitHub fetch.

        These files are used to ensure certain directories exist in git
        and are tracked by version control.
        """
        created_count = 0
        for file_path in TMP_PLACEHOLDER_FILES:
            parent_dir = os.path.dirname(file_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            if not os.path.exists(file_path):
                with open(file_path, 'w') as f:
                    pass
                created_count += 1
                logger.info(f"Created placeholder file: {file_path}")

        logger.info(f"Placeholder recreation complete: {created_count} files created")

    def remove_fetched_files(self, destination_path: str = "FreeBIRD"):
        """
        Clean up by removing the downloaded repository files after mirroring is complete.

        Args:
            destination_path (str): Path of the downloaded repository to remove
        """
        start_time = time.time()
        logger.info(f"Removing fetched files from {destination_path}")
        if os.path.exists(destination_path):
            shutil.rmtree(destination_path)
        end_time = time.time()
        logger.info(f"Fetched files removed in {end_time - start_time:.2f} seconds")
