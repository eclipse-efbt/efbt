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
import zipfile
import os
import shutil
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Base directory path for the birds_nest project
BASE = f"birds_nest{os.sep}"

# Enhanced mapping configuration that defines how source folders from the repository
# should be copied to target folders, with optional file filtering functions
REPO_MAPPING = {
    # Database export files with specific filtering rules
    f"export{os.sep}database_export_ldm": {
        f"resources{os.sep}admin": (lambda file: file.startswith("auth_")),  # Only auth-related files
        f"resources{os.sep}bird": (lambda file: file.startswith("bird_")),   # Only bird-related files
        f"resources{os.sep}technical_export": (lambda file: True)            # All files
    },
    # Join configuration files
    "joins_configuration": {
        f"resources{os.sep}joins_configuration": (lambda file: True),        # All files
    },
    # Initial correction files
    f"birds_nest{os.sep}resources{os.sep}extra_variables": {
        f"resources{os.sep}extra_variables": (lambda file: True),            # All files
    },
    # Derivation files from birds_nest resources
    f"birds_nest{os.sep}resources{os.sep}derivation_files": {
        f"resources{os.sep}derivation_files": (lambda file: True),           # All files
    },
    # LDM (Logical Data Model) files from birds_nest resources
    f"birds_nest{os.sep}resources{os.sep}ldm": {
        f"resources{os.sep}ldm": (lambda file: True),                        # All files
    },
    # Test files from birds_nest
    f"birds_nest{os.sep}tests": {
        "tests": (lambda file: True),                                        # All files
    },
    # Additional mapping for IL files
    f"birds_nest{os.sep}resources{os.sep}il": {
        f"resources{os.sep}il": (lambda file: True),                         # All files
    },
    # Filter code files
    f"birds_nest{os.sep}pybirdai{os.sep}process_steps{os.sep}filter_code": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),  # Only Python files
    },
    # Generated Python files (alternative locations)
    f"birds_nest{os.sep}results{os.sep}generated_python_filters": {
        f"results{os.sep}generated_python_filters": (lambda file: True),
    },
    f"birds_nest{os.sep}results{os.sep}generated_python_joins": {
        f"results{os.sep}generated_python_joins": (lambda file: True),
    }
}


class CloneRepoServiceProcessStep:
    """
    Process step for cloning repositories and organizing files.
    Refactored from utils.clone_repo_service to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the clone repo service process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, repository_url: str, token: str = None, **kwargs) -> Dict[str, Any]:
        """
        Execute the repository cloning process.
        
        Args:
            repository_url (str): GitHub repository URL to clone
            token (str): Optional GitHub token for authentication
            **kwargs: Additional parameters
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            service = CloneRepoService(token)
            result = service.clone_and_setup_repository(repository_url)
            
            if self.context:
                self.context.clone_service = service
                
            return {
                'success': True,
                'service': service,
                'result': result,
                'message': f'Repository cloned successfully from {repository_url}'
            }
            
        except Exception as e:
            logger.error(f"Failed to clone repository: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Repository cloning failed'
            }


class CloneRepoService:
    """
    Enhanced service class for cloning a repository and setting up files according to the mapping configuration.
    Handles downloading, extracting, organizing files, cleanup operations, and supports authentication.
    """

    def __init__(self, token: str = None):
        """
        Initialize the service by cleaning up existing target directories and setting up authentication.

        Args:
            token (str): Optional GitHub token for private repository access
        """
        logger.info("Initializing CloneRepoService...")
        self.token = token
        
        # Set up authentication headers if token is provided
        self.headers = {}
        if self.token:
            self.headers['Authorization'] = f'token {self.token}'
            logger.info("Authentication token configured for private repository access")
        
        # Clean up all target directories before starting
        self._cleanup_target_directories()
        
        logger.info("CloneRepoService initialization completed")

    def _cleanup_target_directories(self):
        """
        Clean up all target directories mentioned in REPO_MAPPING to ensure a fresh start.
        """
        logger.info("Cleaning up existing target directories...")
        
        target_dirs = set()
        
        # Collect all unique target directories from the mapping
        for source_path, target_mapping in REPO_MAPPING.items():
            for target_dir, filter_func in target_mapping.items():
                target_dirs.add(target_dir)
        
        # Remove each target directory if it exists
        for target_dir in target_dirs:
            if os.path.exists(target_dir):
                try:
                    shutil.rmtree(target_dir)
                    logger.info(f"Removed existing directory: {target_dir}")
                except Exception as e:
                    logger.warning(f"Failed to remove {target_dir}: {e}")
        
        logger.info("Target directory cleanup completed")

    def clone_and_setup_repository(self, repository_url: str) -> dict:
        """
        Main method to clone a repository and set up the file structure.

        Args:
            repository_url (str): The GitHub repository URL to clone

        Returns:
            dict: Dictionary containing operation results and statistics
        """
        start_time = time.time()
        logger.info(f"Starting repository clone and setup for: {repository_url}")
        
        try:
            # Step 1: Download the repository
            zip_path = self._download_repository(repository_url)
            
            # Step 2: Extract the repository
            extract_path = self._extract_repository(zip_path)
            
            # Step 3: Set up the file structure according to mapping
            setup_results = self._setup_file_structure(extract_path)
            
            # Step 4: Clean up temporary files
            self._cleanup_temporary_files(zip_path, extract_path)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            result = {
                'success': True,
                'execution_time': execution_time,
                'files_copied': setup_results['total_files_copied'],
                'directories_created': setup_results['directories_created'],
                'files_filtered': setup_results['files_filtered'],
                'mapping_results': setup_results['mapping_results'],
                'repository_url': repository_url
            }
            
            logger.info(f"Repository setup completed successfully in {execution_time:.2f} seconds")
            logger.info(f"Files copied: {result['files_copied']}, Directories created: {result['directories_created']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Repository setup failed: {e}")
            raise

    def _download_repository(self, repository_url: str) -> str:
        """
        Download the repository as a ZIP file.

        Args:
            repository_url (str): The GitHub repository URL

        Returns:
            str: Path to the downloaded ZIP file
        """
        # Convert GitHub URL to download URL
        if repository_url.endswith('.git'):
            repository_url = repository_url[:-4]
        
        zip_url = f"{repository_url}/archive/refs/heads/main.zip"
        zip_filename = "repository.zip"
        
        logger.info(f"Downloading repository from: {zip_url}")
        
        try:
            response = requests.get(zip_url, headers=self.headers)
            response.raise_for_status()
            
            with open(zip_filename, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Repository downloaded successfully: {zip_filename}")
            return zip_filename
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download repository: {e}")
            raise

    def _extract_repository(self, zip_path: str) -> str:
        """
        Extract the downloaded ZIP file.

        Args:
            zip_path (str): Path to the ZIP file

        Returns:
            str: Path to the extracted directory
        """
        extract_path = "temp_repo_extract"
        
        logger.info(f"Extracting repository to: {extract_path}")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            
            # Find the actual repository directory (usually repo-name-main)
            extracted_items = os.listdir(extract_path)
            if len(extracted_items) == 1 and os.path.isdir(os.path.join(extract_path, extracted_items[0])):
                actual_repo_path = os.path.join(extract_path, extracted_items[0])
                logger.info(f"Repository extracted to: {actual_repo_path}")
                return actual_repo_path
            else:
                logger.info(f"Repository extracted to: {extract_path}")
                return extract_path
                
        except Exception as e:
            logger.error(f"Failed to extract repository: {e}")
            raise

    def _setup_file_structure(self, extract_path: str) -> dict:
        """
        Set up the file structure according to REPO_MAPPING.

        Args:
            extract_path (str): Path to the extracted repository

        Returns:
            dict: Dictionary containing setup results and statistics
        """
        logger.info("Setting up file structure according to mapping configuration...")
        
        total_files_copied = 0
        directories_created = 0
        files_filtered = 0
        mapping_results = {}
        
        for source_path, target_mapping in REPO_MAPPING.items():
            full_source_path = os.path.join(extract_path, source_path)
            
            if not os.path.exists(full_source_path):
                logger.warning(f"Source path not found: {full_source_path}")
                mapping_results[source_path] = {'status': 'source_not_found', 'files_copied': 0}
                continue
            
            logger.info(f"Processing source path: {source_path}")
            
            for target_dir, filter_func in target_mapping.items():
                # Create target directory
                os.makedirs(target_dir, exist_ok=True)
                directories_created += 1
                
                files_copied_for_mapping = 0
                
                # Copy files with filtering
                if os.path.isdir(full_source_path):
                    for item in os.listdir(full_source_path):
                        item_path = os.path.join(full_source_path, item)
                        
                        if os.path.isfile(item_path):
                            if filter_func(item):
                                target_file_path = os.path.join(target_dir, item)
                                shutil.copy2(item_path, target_file_path)
                                files_copied_for_mapping += 1
                                total_files_copied += 1
                                logger.debug(f"Copied: {item} -> {target_file_path}")
                            else:
                                files_filtered += 1
                                logger.debug(f"Filtered out: {item}")
                
                mapping_results[f"{source_path} -> {target_dir}"] = {
                    'status': 'completed',
                    'files_copied': files_copied_for_mapping
                }
                
                logger.info(f"Completed mapping: {source_path} -> {target_dir} ({files_copied_for_mapping} files)")
        
        return {
            'total_files_copied': total_files_copied,
            'directories_created': directories_created,
            'files_filtered': files_filtered,
            'mapping_results': mapping_results
        }

    def _cleanup_temporary_files(self, zip_path: str, extract_path: str):
        """
        Clean up temporary files and directories.

        Args:
            zip_path (str): Path to the ZIP file to remove
            extract_path (str): Path to the extracted directory to remove
        """
        logger.info("Cleaning up temporary files...")
        
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
                logger.info(f"Removed temporary ZIP file: {zip_path}")
            
            # Extract path might be nested, so we need to remove the parent directory
            parent_extract_path = os.path.dirname(extract_path) if extract_path != "temp_repo_extract" else extract_path
            
            if os.path.exists(parent_extract_path):
                shutil.rmtree(parent_extract_path)
                logger.info(f"Removed temporary extract directory: {parent_extract_path}")
                
        except Exception as e:
            logger.warning(f"Failed to clean up some temporary files: {e}")