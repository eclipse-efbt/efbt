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
# Standard library imports for HTTP requests, file operations, and system utilities
import requests
import zipfile
import os
import shutil
import logging
import time

# Set up logging configuration to write to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clone_repo_setup.log'),
        logging.StreamHandler()
    ]
)
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
        self.token = token
        self.headers = self._get_authenticated_headers()

        # Delete all target directories from REPO_MAPPING to enable clean setup
        for source_folder, target_mappings in REPO_MAPPING.items():
            for target_folder, filter_func in target_mappings.items():
                if os.path.exists(target_folder):
                    logger.info(f"Removing existing target directory: {target_folder}")
                    shutil.rmtree(target_folder)

        # Create all target directories beforehand
        self._create_all_target_directories()

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

    def _create_all_target_directories(self):
        """Create all target directories from REPO_MAPPING beforehand."""
        logger.info("Creating all target directories beforehand")
        for source_folder, target_mappings in REPO_MAPPING.items():
            for target_folder, filter_func in target_mappings.items():
                self._ensure_directory_exists(target_folder)
                logger.info(f"Created target directory: {target_folder}")

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

    def clone_repo(self, base_url:str="https://github.com/regcommunity/FreeBIRD", destination_path: str = "FreeBIRD", branch: str = "main"):
        """
        Download and extract a repository from GitHub as a ZIP file.

        Args:
            base_url (str): The base URL of the GitHub repository
            destination_path (str): Local directory to extract the repository to
            branch (str): The branch to clone
        """
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

    def setup_files(self, destination_path: str = "FreeBIRD"):
        """
        Organize and copy files from the extracted repository according to REPO_MAPPING configuration.

        Args:
            destination_path (str): Path where the repository was extracted
        """
        start_time = time.time()
        logger.info(f"Starting file setup from {destination_path}")

        # Find the extracted folder (typically FreeBIRD-main)
        extracted_folder = None
        for item in os.listdir(destination_path):
            item_path = os.path.join(destination_path, item)
            if os.path.isdir(item_path) and item.startswith("FreeBIRD"):
                extracted_folder = item_path
                break

        # Validate that we found the extracted repository folder
        if not extracted_folder:
            logger.error("Could not find extracted repository folder")
            print("Could not find extracted repository folder")
            return

        logger.info(f"Found extracted folder: {extracted_folder}")


        # Process each mapping in REPO_MAPPING
        for source_folder, target_mappings in REPO_MAPPING.items():
            source_path = os.path.join(extracted_folder, source_folder)
            logger.debug(f"Processing source folder: {source_path}")

            # Skip if source folder doesn't exist
            if not os.path.exists(source_path):
                logger.warning(f"Source path does not exist: {source_path}")
                continue

            # Special handling for database export files that need filtering
            if f"export{os.sep}database_export_ldm" == source_folder:
                # Process each file in the source directory
                for file_name in os.listdir(source_path):
                    # Check which target folder this file should go to based on filter functions
                    for target_folder, filter_func in target_mappings.items():
                        if filter_func(file_name):
                            source_file = os.path.join(source_path, file_name)
                            target_file = os.path.join(target_folder, file_name)
                            logger.debug(f"Copying {source_file} to {target_file}")

                            # Handle both files and directories
                            if os.path.isfile(source_file):
                                shutil.copy2(source_file, target_file)
                                logger.debug(f"File copied: {file_name}")
                            elif os.path.isdir(source_file):
                                # Remove existing directory if it exists
                                if os.path.exists(target_file):
                                    shutil.rmtree(target_file)
                                shutil.copytree(source_file, target_file)
                                logger.debug(f"Directory copied: {file_name}")
                            break  # File matched a filter, move to next file
                continue  # Move to next source folder

            # Standard handling for other folders (copy entire directory with filtering)
            target_folder = list(REPO_MAPPING[source_folder].keys())[0]
            if os.path.exists(target_folder):
                shutil.rmtree(target_folder)
            source_folder = f"{extracted_folder}{os.sep}{source_folder}"
            shutil.copytree(source_folder, target_folder)

        end_time = time.time()
        logger.info(f"File setup completed in {end_time - start_time:.2f} seconds")

    def remove_fetched_files(self, destination_path: str = "FreeBIRD"):
        """
        Clean up by removing the downloaded repository files after setup is complete.

        Args:
            destination_path (str): Path of the downloaded repository to remove
        """
        start_time = time.time()
        logger.info(f"Removing fetched files from {destination_path}")
        shutil.rmtree(destination_path)
        end_time = time.time()
        logger.info(f"Fetched files removed in {end_time - start_time:.2f} seconds")

def main():
    """
    Main function that orchestrates the complete repository cloning and setup process.
    Executes clone, setup, and cleanup operations in sequence.
    """
    start_time = time.time()
    logger.info("Starting CloneRepoService execution")

    # Create service instance and execute the complete workflow
    service = CloneRepoService()
    service.clone_repo()        # Download and extract repository
    service.setup_files()       # Organize files according to mapping
    service.remove_fetched_files()  # Clean up downloaded files

    end_time = time.time()
    logger.info(f"CloneRepoService execution completed in {end_time - start_time:.2f} seconds")

# Entry point for script execution
if __name__ == "__main__":
    main()
