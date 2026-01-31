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

# Backup directory for preserving whitelisted files during updates
BACKUP_DIR = "resources_backup"

# Whitelist patterns for files that should be preserved during updates
# These are user configuration files that should not be overwritten
WHITELIST_FILES = [
    # User derivation configuration
    os.path.join("resources", "derivation_files", "derivation_config.csv"),
    # DPM metrics configuration
    os.path.join("resources", "dpm_metrics_configuration", "configuration_dpm_measure_domain.csv"),
    # Automatic tracking wrapper (generated/customized file)
    os.path.join("pybirdai", "process_steps", "filter_code", "automatic_tracking_wrapper.py"),
    # Tests package init file
    os.path.join("tests", "__init__.py"),
]

# Patterns to exclude from backup (tmp files/folders should be deleted)
EXCLUDE_PATTERNS = [
    "tmp",  # Any file or folder named 'tmp'
]

# Placeholder files that should be recreated after GitHub fetch
# These are empty files used to ensure directories exist in git
TMP_PLACEHOLDER_FILES = [
    os.path.join("pybirdai", "process_steps", "filter_code", "tmp"),
    os.path.join("resources", "derivation_files", "generated_from_logical_transformation_rules", "tmp"),
    os.path.join("resources", "derivation_files", "generated_from_member_links", "tmp"),
    os.path.join("resources", "derivation_files", "manually_generated", "tmp"),
    os.path.join("resources", "derivation_files", "tmp"),
    os.path.join("resources", "extra_variables", "tmp"),
    os.path.join("resources", "il", "tmp"),
    os.path.join("artefacts", "smcubes_artefacts", "tmp"),
    os.path.join("artefacts", "joins_configuration", "tmp"),
    os.path.join("artefacts", "filter_code", "production", "tmp"),
    os.path.join("artefacts", "filter_code", "staging", "tmp"),
    os.path.join("artefacts", "derivation_files", "manually_generated", "tmp"),
]

# Enhanced mapping configuration that defines how source folders from the repository
# should be copied to target folders, with optional file filtering functions
REPO_MAPPING = {
    # Database export files with specific filtering rules (artefacts structure)
    f"artefacts{os.sep}smcubes_artefacts": {
        f"resources{os.sep}admin": (lambda file: file.startswith("auth_")),  # Only auth-related files
        f"resources{os.sep}bird": (lambda file: file.startswith("bird_")),   # Only bird-related files
        f"resources{os.sep}technical_export": (lambda file: True),           # Legacy location for imports
        f"artefacts{os.sep}smcubes_artefacts": (lambda file: True)           # All files
    },
    # Legacy export path for backward compatibility
    f"export{os.sep}database_export_ldm": {
        f"resources{os.sep}admin": (lambda file: file.startswith("auth_")),  # Only auth-related files
        f"resources{os.sep}bird": (lambda file: file.startswith("bird_")),   # Only bird-related files
        f"resources{os.sep}technical_export": (lambda file: True),           # Legacy location for imports
        f"artefacts{os.sep}smcubes_artefacts": (lambda file: True)           # All files
    },
    # Join configuration files (artefacts structure)
    f"artefacts{os.sep}joins_configuration": {
        f"artefacts{os.sep}joins_configuration": (lambda file: True),        # All files
    },
    # Legacy joins configuration path for backward compatibility
    "joins_configuration": {
        f"artefacts{os.sep}joins_configuration": (lambda file: True),        # All files
    },
    # Filter code from artefacts (new enhanced export structure)
    f"artefacts{os.sep}filter_code{os.sep}production": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),  # Production filter code
    },
    f"artefacts{os.sep}filter_code{os.sep}staging": {
        f"results{os.sep}generated_python_joins": (lambda file: True),  # Staging filter code
    },
    # Derivation files from artefacts (new enhanced export structure)
    f"artefacts{os.sep}derivation_files{os.sep}manually_generated": {
        f"resources{os.sep}derivation_files{os.sep}manually_generated": (lambda file: True),
    },
    f"artefacts{os.sep}derivation_files": {
        f"resources{os.sep}derivation_files": (lambda file: file.endswith('.csv')),  # Only CSV config files
    },
    # Alternative: artefacts inside birds_nest folder
    f"birds_nest{os.sep}artefacts{os.sep}smcubes_artefacts": {
        f"resources{os.sep}admin": (lambda file: file.startswith("auth_")),
        f"resources{os.sep}bird": (lambda file: file.startswith("bird_")),
        f"resources{os.sep}technical_export": (lambda file: True),
        f"artefacts{os.sep}smcubes_artefacts": (lambda file: True)
    },
    f"birds_nest{os.sep}artefacts{os.sep}joins_configuration": {
        f"artefacts{os.sep}joins_configuration": (lambda file: True),
    },
    f"birds_nest{os.sep}artefacts{os.sep}filter_code{os.sep}production": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),
    },
    f"birds_nest{os.sep}artefacts{os.sep}filter_code{os.sep}staging": {
        f"results{os.sep}generated_python_joins": (lambda file: True),
    },
    f"birds_nest{os.sep}artefacts{os.sep}derivation_files{os.sep}manually_generated": {
        f"resources{os.sep}derivation_files{os.sep}manually_generated": (lambda file: True),
    },
    f"birds_nest{os.sep}artefacts{os.sep}derivation_files": {
        f"resources{os.sep}derivation_files": (lambda file: file.endswith('.csv')),
    },
    # Initial correction files
    f"birds_nest{os.sep}resources{os.sep}extra_variables": {
        f"resources{os.sep}extra_variables": (lambda file: True),            # All files
    },
    # Derivation files from birds_nest resources (legacy location)
    # Note: derivation_config.csv is preserved via WHITELIST_FILES backup/restore
    f"birds_nest{os.sep}resources{os.sep}derivation_files": {
        f"resources{os.sep}derivation_files": (lambda file: True),
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
    # Filter code files (legacy location)
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
        Initialize the service and set up authentication.

        Args:
            token (str): Optional GitHub token for private repository access
        """
        self.token = token
        self.headers = self._get_authenticated_headers()

        # Create all target directories beforehand (if they don't exist)
        # Note: We don't delete existing directories to preserve files from previous fetches
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
            if source_folder == f"birds_nest{os.sep}pybirdai{os.sep}process_steps{os.sep}filter_code":
                continue
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

    def _should_exclude(self, path):
        """
        Check if a path should be excluded from backup (e.g., tmp files/folders).

        Args:
            path (str): The path to check

        Returns:
            bool: True if the path should be excluded, False otherwise
        """
        path_parts = path.replace(os.sep, '/').split('/')
        for pattern in EXCLUDE_PATTERNS:
            if pattern in path_parts:
                return True
        return False

    def _backup_whitelisted_files(self):
        """
        Backup whitelisted files from resources folder to a backup directory.

        Creates a backup of all files specified in WHITELIST_FILES that exist,
        preserving their directory structure.

        Returns:
            dict: Mapping of relative paths to their backup locations
        """
        backed_up_files = {}

        # Clean up any existing backup directory
        if os.path.exists(BACKUP_DIR):
            shutil.rmtree(BACKUP_DIR)
        os.makedirs(BACKUP_DIR, exist_ok=True)

        logger.info(f"Creating backup directory: {BACKUP_DIR}")

        for file_path in WHITELIST_FILES:
            if os.path.exists(file_path):
                # Skip if file matches exclude patterns
                if self._should_exclude(file_path):
                    logger.debug(f"Skipping excluded file: {file_path}")
                    continue

                # Create backup path preserving directory structure
                backup_path = os.path.join(BACKUP_DIR, file_path)
                backup_dir = os.path.dirname(backup_path)
                os.makedirs(backup_dir, exist_ok=True)

                # Copy file to backup
                shutil.copy2(file_path, backup_path)
                backed_up_files[file_path] = backup_path
                logger.info(f"Backed up whitelisted file: {file_path} -> {backup_path}")
            else:
                logger.debug(f"Whitelisted file does not exist, skipping: {file_path}")

        logger.info(f"Backup complete: {len(backed_up_files)} files backed up")
        return backed_up_files

    def _restore_whitelisted_files(self, backed_up_files):
        """
        Restore whitelisted files from backup directory to their original locations.

        Args:
            backed_up_files (dict): Mapping of original paths to backup paths
        """
        restored_count = 0

        for original_path, backup_path in backed_up_files.items():
            if os.path.exists(backup_path):
                # Ensure target directory exists
                target_dir = os.path.dirname(original_path)
                os.makedirs(target_dir, exist_ok=True)

                # Restore file from backup
                shutil.copy2(backup_path, original_path)
                restored_count += 1
                logger.info(f"Restored whitelisted file: {backup_path} -> {original_path}")
            else:
                logger.warning(f"Backup file not found, cannot restore: {backup_path}")

        logger.info(f"Restore complete: {restored_count} files restored")

    def _cleanup_backup(self):
        """Remove the backup directory after successful restore."""
        if os.path.exists(BACKUP_DIR):
            shutil.rmtree(BACKUP_DIR)
            logger.info(f"Cleaned up backup directory: {BACKUP_DIR}")

    def _recreate_tmp_placeholders(self):
        """
        Recreate empty tmp placeholder files after GitHub fetch.

        These files are used to ensure certain directories exist in git
        and are tracked by version control.
        """
        created_count = 0
        for file_path in TMP_PLACEHOLDER_FILES:
            # Ensure parent directory exists
            parent_dir = os.path.dirname(file_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            # Create empty file if it doesn't exist
            if not os.path.exists(file_path):
                with open(file_path, 'w') as f:
                    pass  # Create empty file
                created_count += 1
                logger.info(f"Created placeholder file: {file_path}")
            else:
                logger.debug(f"Placeholder file already exists: {file_path}")

        logger.info(f"Placeholder recreation complete: {created_count} files created")

    def _relocate_manual_derivation_file(self):
        """
        Relocate the manual derivation file to its correct location.

        GitHub repo has derived_field_configuration.py at the root of derivation_files/,
        but it should be at manually_generated/manual_derivations.py
        """
        old_path = os.path.join("resources", "derivation_files", "derived_field_configuration.py")
        new_dir = os.path.join("resources", "derivation_files", "manually_generated")
        new_path = os.path.join(new_dir, "manual_derivations.py")

        if os.path.exists(old_path):
            # Ensure target directory exists
            os.makedirs(new_dir, exist_ok=True)

            # Move and rename the file
            shutil.move(old_path, new_path)
            logger.info(f"Relocated manual derivation file: {old_path} -> {new_path}")
        else:
            logger.debug(f"Manual derivation file not found at old location: {old_path}")

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
        logger.info(f"Using branch: {branch}")

        # Construct the ZIP download URL for the specified branch
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

        Uses a backup/restore pattern to preserve whitelisted user configuration files:
        1. Backup whitelisted files to a backup directory
        2. Delete and copy new files from repository
        3. Restore whitelisted files from backup
        4. Clean up backup directory

        Args:
            destination_path (str): Path where the repository was extracted
        """
        start_time = time.time()
        logger.info(f"Starting file setup from {destination_path}")

        # Step 1: Backup whitelisted files before any deletions
        backed_up_files = self._backup_whitelisted_files()

        # Find the extracted folder (typically RepoName-branchname)
        # The zip extraction creates a folder like 'RepoName-branchname'
        extracted_folder = None
        logger.info(f"Looking for extracted folder in: {destination_path}")
        for item in os.listdir(destination_path):
            item_path = os.path.join(destination_path, item)
            if os.path.isdir(item_path):
                logger.info(f"Found directory: {item}")
                # Accept any directory that's not a hidden folder
                if not item.startswith('.'):
                    extracted_folder = item_path
                    break

        # Validate that we found the extracted repository folder
        if not extracted_folder:
            logger.error(f"Could not find extracted repository folder in {destination_path}")
            logger.error(f"Contents: {os.listdir(destination_path)}")
            print("Could not find extracted repository folder")
            # Restore backed up files before returning
            self._restore_whitelisted_files(backed_up_files)
            self._cleanup_backup()
            return

        logger.info(f"Found extracted folder: {extracted_folder}")

        # Log contents of extracted folder for debugging
        try:
            extracted_contents = os.listdir(extracted_folder)
            logger.info(f"Extracted folder contents: {extracted_contents}")

            # Check for artefacts directory specifically
            artefacts_path = os.path.join(extracted_folder, 'artefacts')
            if os.path.exists(artefacts_path):
                artefacts_contents = os.listdir(artefacts_path)
                logger.info(f"Artefacts directory found with contents: {artefacts_contents}")
            else:
                logger.warning(f"No artefacts directory found at {artefacts_path}")
        except Exception as e:
            logger.warning(f"Could not list extracted folder contents: {e}")

        # Ensure all target directories exist before copying
        required_dirs = [
            "artefacts",
            os.path.join("artefacts", "smcubes_artefacts"),
            os.path.join("artefacts", "joins_configuration"),
            os.path.join("artefacts", "filter_code"),
            os.path.join("artefacts", "filter_code", "production"),
            os.path.join("artefacts", "filter_code", "staging"),
            os.path.join("artefacts", "derivation_files"),
            os.path.join("artefacts", "derivation_files", "manually_generated"),
            "resources",
            os.path.join("resources", "admin"),
            os.path.join("resources", "bird"),
            os.path.join("resources", "technical_export"),  # Legacy location for import functions
            os.path.join("resources", "derivation_files"),
            os.path.join("resources", "derivation_files", "manually_generated"),
            os.path.join("resources", "extra_variables"),
            os.path.join("resources", "ldm"),
            os.path.join("resources", "il"),
            os.path.join("pybirdai", "process_steps", "filter_code"),
            os.path.join("results", "generated_python_joins"),
            os.path.join("results", "generated_python_filters"),
        ]
        for dir_path in required_dirs:
            os.makedirs(dir_path, exist_ok=True)
            logger.debug(f"Ensured directory exists: {dir_path}")

        # Step 2: Process each mapping in REPO_MAPPING
        for source_folder, target_mappings in REPO_MAPPING.items():
            source_path = os.path.join(extracted_folder, source_folder)
            logger.debug(f"Processing source folder: {source_path}")

            # Skip if source folder doesn't exist
            if not os.path.exists(source_path):
                logger.warning(f"Source path does not exist: {source_path}")
                continue

            # Special handling for database export files that need filtering
            # Handles artefacts/smcubes_artefacts, birds_nest/artefacts/smcubes_artefacts, and legacy export/database_export_ldm paths
            smcubes_paths = [
                f"artefacts{os.sep}smcubes_artefacts",
                f"birds_nest{os.sep}artefacts{os.sep}smcubes_artefacts",
                f"export{os.sep}database_export_ldm"
            ]
            if source_folder in smcubes_paths:
                logger.info(f"Processing database export files from: {source_path}")
                # Ensure all target directories exist
                for target_folder in target_mappings.keys():
                    os.makedirs(target_folder, exist_ok=True)
                    logger.debug(f"Ensured target directory exists: {target_folder}")

                # Process each file in the source directory
                files_copied = 0
                for file_name in os.listdir(source_path):
                    # Check which target folder this file should go to based on filter functions
                    for target_folder, filter_func in target_mappings.items():
                        if filter_func(file_name):
                            source_file = os.path.join(source_path, file_name)
                            target_file = os.path.join(target_folder, file_name)

                            # Handle both files and directories
                            if os.path.isfile(source_file):
                                shutil.copy2(source_file, target_file)
                                files_copied += 1
                                logger.debug(f"File copied: {file_name} -> {target_folder}")
                            elif os.path.isdir(source_file):
                                # Remove existing directory if it exists
                                if os.path.exists(target_file):
                                    shutil.rmtree(target_file)
                                shutil.copytree(source_file, target_file)
                                files_copied += 1
                                logger.debug(f"Directory copied: {file_name} -> {target_folder}")
                            break  # File matched a filter, move to next file
                logger.info(f"Copied {files_copied} items from {source_folder}")
                continue  # Move to next source folder

            # Standard handling for other folders
            logger.info(f"Processing folder: {source_path}")

            # Check if any mapping has a filter function that's not "all files"
            has_filter = False
            for target_folder, filter_func in target_mappings.items():
                # Check if filter is selective (not just lambda file: True)
                try:
                    # If filter returns False for empty string, it's selective
                    if not filter_func("test_file.py"):
                        has_filter = True
                        break
                except Exception:
                    pass

            if has_filter:
                # Handle mappings with selective filters - copy individual files
                for target_folder, filter_func in target_mappings.items():
                    os.makedirs(target_folder, exist_ok=True)
                    files_copied = 0
                    for file_name in os.listdir(source_path):
                        source_file = os.path.join(source_path, file_name)
                        if os.path.isfile(source_file) and filter_func(file_name):
                            target_file = os.path.join(target_folder, file_name)
                            shutil.copy2(source_file, target_file)
                            files_copied += 1
                            logger.debug(f"File copied: {file_name} -> {target_folder}")
                    logger.info(f"Copied {files_copied} filtered files from {source_folder} to {target_folder}")
            else:
                # Copy entire directory
                target_folder = list(target_mappings.keys())[0]

                # Ensure parent directory exists
                parent_dir = os.path.dirname(target_folder)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)

                # Remove existing target folder and copy fresh from source
                if os.path.exists(target_folder):
                    shutil.rmtree(target_folder)
                    logger.info(f"Removed existing directory: {target_folder}")

                shutil.copytree(source_path, target_folder)
                logger.info(f"Copied directory: {source_path} -> {target_folder}")

        # Step 3: Restore whitelisted files from backup
        self._restore_whitelisted_files(backed_up_files)

        # Step 4: Clean up backup directory
        self._cleanup_backup()

        # Step 5: Recreate tmp placeholder files
        self._recreate_tmp_placeholders()

        # Step 6: Relocate manual derivation file to new location
        self._relocate_manual_derivation_file()

        end_time = time.time()
        logger.info(f"File setup completed in {end_time - start_time:.2f} seconds")

    def setup_test_suite_files(self, destination_path: str = "FreeBIRD"):
        """
        Organize and copy test suite files from the extracted repository.

        This method automatically discovers the repository structure and copies
        all directories and files to tests/{repo_name}/ to avoid conflicts.

        Args:
            destination_path (str): Path where the repository was extracted
        """
        start_time = time.time()
        logger.info(f"Starting test suite file setup from {destination_path}")

        # Find the extracted folder (the ZIP extraction creates a folder with repo name + branch)
        extracted_folder = None
        repo_name = None
        for item in os.listdir(destination_path):
            item_path = os.path.join(destination_path, item)
            if os.path.isdir(item_path):
                # Accept any directory - should be the extracted repository
                extracted_folder = item_path
                # Extract repo name (e.g., "bird-default-test-suite" from "bird-default-test-suite-main")
                repo_name = item.rsplit('-', 1)[0] if '-' in item else item
                logger.info(f"Found extracted folder: {extracted_folder}, repo name: {repo_name}")
                break

        # Validate that we found the extracted repository folder
        if not extracted_folder:
            logger.error("Could not find extracted repository folder for test suite files")
            return

        # Create target base directory: tests/{repo_name}/
        target_base = os.path.join("tests", repo_name)
        logger.info(f"Test suite will be copied to: {target_base}")

        # Walk through the extracted folder and copy everything to tests/{repo_name}/
        files_copied = 0
        dirs_copied = 0

        for item in os.listdir(extracted_folder):
            source_item_path = os.path.join(extracted_folder, item)

            # Skip hidden files and directories (like .git, .gitignore, etc.)
            if item.startswith('.'):
                logger.debug(f"Skipping hidden item: {item}")
                continue

            # Skip README and other documentation files at root level
            if item.lower() in ['readme.md', 'readme.txt', 'license', 'license.txt', 'license.md']:
                logger.debug(f"Skipping documentation file: {item}")
                continue

            try:
                if os.path.isdir(source_item_path):
                    # Copy directory (e.g., 'tests' directory) to tests/{repo_name}/tests
                    target_dir = os.path.join(target_base, item)

                    # Remove existing target directory if it exists
                    if os.path.exists(target_dir):
                        logger.info(f"Removing existing directory: {target_dir}")
                        shutil.rmtree(target_dir)

                    # Ensure parent directory exists
                    os.makedirs(os.path.dirname(target_dir), exist_ok=True)

                    # Copy the entire directory tree
                    shutil.copytree(source_item_path, target_dir)
                    logger.info(f"Successfully copied directory: {item} -> {target_dir}")
                    dirs_copied += 1

                elif os.path.isfile(source_item_path):
                    # Copy files to tests/{repo_name}/
                    target_file = os.path.join(target_base, item)

                    # Ensure parent directory exists
                    os.makedirs(os.path.dirname(target_file), exist_ok=True)

                    shutil.copy2(source_item_path, target_file)
                    logger.info(f"Successfully copied file: {item} -> {target_file}")
                    files_copied += 1

            except Exception as e:
                logger.error(f"Error copying {item}: {e}")

        end_time = time.time()
        logger.info(f"Test suite file setup completed in {end_time - start_time:.2f} seconds")
        logger.info(f"Summary: Copied {dirs_copied} directories and {files_copied} files to {target_base}")

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
