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
import re
import io
import subprocess
from urllib.parse import quote, urlparse

from django.conf import settings
from pybirdai.utils.safe_zip import safe_extract

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

REPO_DOWNLOAD_TIMEOUT = (10, 300)
GIT_CLONE_TIMEOUT = 600
GIT_LFS_TIMEOUT = 600
GITHUB_OWNER_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$")
GITHUB_REPO_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
GITHUB_REF_PATTERN = re.compile(r"^[A-Za-z0-9_./-]+$")

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
# Note: artefacts directories should NOT have placeholder files - they get content from the repo
TMP_PLACEHOLDER_FILES = [
    os.path.join("pybirdai", "process_steps", "filter_code", "tmp"),
    os.path.join("resources", "derivation_files", "generated_from_logical_transformation_rules", "tmp"),
    os.path.join("resources", "derivation_files", "generated_from_member_links", "tmp"),
    os.path.join("resources", "derivation_files", "manually_generated", "tmp"),
    os.path.join("resources", "derivation_files", "tmp"),
    os.path.join("resources", "extra_variables", "tmp"),
    os.path.join("resources", "il", "tmp"),
]


def _build_github_archive_url(base_url: str, branch: str) -> str:
    """Build a GitHub archive URL after validating the repository and branch."""
    normalized_url = (base_url or "").rstrip("/")
    parsed = urlparse(normalized_url)
    if parsed.scheme != "https" or parsed.netloc.lower() != "github.com":
        raise ValueError("Only https://github.com repositories can be cloned")

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        raise ValueError("GitHub repository URL must include owner and repository")

    owner = parts[0]
    repo = parts[1][:-4] if parts[1].endswith(".git") else parts[1]
    if not GITHUB_OWNER_PATTERN.fullmatch(owner) or not GITHUB_REPO_PATTERN.fullmatch(repo):
        raise ValueError("Invalid GitHub owner or repository name")

    branch = (branch or "").strip()
    if (
        not branch
        or not GITHUB_REF_PATTERN.fullmatch(branch)
        or branch.startswith("/")
        or branch.endswith("/")
        or ".." in branch
        or "//" in branch
    ):
        raise ValueError("Invalid GitHub branch name")

    return (
        f"https://github.com/{quote(owner, safe='')}/{quote(repo, safe='')}"
        f"/archive/refs/heads/{quote(branch, safe='/._-')}.zip"
    )


def _extract_github_repo_parts(base_url: str):
    """Return owner and repo after validating a GitHub repository URL."""
    normalized_url = (base_url or "").rstrip("/")
    parsed = urlparse(normalized_url)
    if parsed.scheme != "https" or parsed.netloc.lower() != "github.com":
        raise ValueError("Only https://github.com repositories can be cloned")

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        raise ValueError("GitHub repository URL must include owner and repository")

    owner = parts[0]
    repo = parts[1][:-4] if parts[1].endswith(".git") else parts[1]
    if not GITHUB_OWNER_PATTERN.fullmatch(owner) or not GITHUB_REPO_PATTERN.fullmatch(repo):
        raise ValueError("Invalid GitHub owner or repository name")
    return owner, repo


def _looks_like_git_lfs_pointer(path: str) -> bool:
    """Return True when a file is a Git LFS pointer instead of materialized content."""
    try:
        if os.path.getsize(path) > 1024:
            return False
        with open(path, "rb") as file:
            return file.read(256).startswith(b"version https://git-lfs.github.com/spec/v1\n")
    except OSError:
        return False

# Enhanced mapping configuration that defines how source folders from the repository
# should be copied to target folders, with optional file filtering functions
# Primary artefacts paths - these are the authoritative source
REPO_MAPPING = {
    # Database export files from artefacts/smcubes_artefacts
    f"artefacts{os.sep}smcubes_artefacts": {
        f"resources{os.sep}admin": (lambda file: file.startswith("auth_")),  # Only auth-related files
        f"resources{os.sep}bird": (lambda file: file.startswith("bird_")),   # Only bird-related files
        f"artefacts{os.sep}smcubes_artefacts": (lambda file: True)           # All files
    },
    # Join configuration files from artefacts/joins_configuration
    f"artefacts{os.sep}joins_configuration": {
        f"artefacts{os.sep}joins_configuration": (lambda file: True),
    },
    # Filter code from artefacts/filter_code/production
    f"artefacts{os.sep}filter_code{os.sep}production": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),
        f"artefacts{os.sep}filter_code{os.sep}production": (lambda file: True),  # Also keep local copy
    },
    # Staging filter code from artefacts/filter_code/staging
    f"artefacts{os.sep}filter_code{os.sep}staging": {
        f"results{os.sep}generated_python_joins": (lambda file: True),
        f"artefacts{os.sep}filter_code{os.sep}staging": (lambda file: True),  # Also keep local copy
    },
    # Derivation files from artefacts/derivation_files
    f"artefacts{os.sep}derivation_files{os.sep}manually_generated": {
        f"resources{os.sep}derivation_files{os.sep}manually_generated": (lambda file: True),
        f"artefacts{os.sep}derivation_files{os.sep}manually_generated": (lambda file: True),  # Also keep local copy
    },
    f"artefacts{os.sep}derivation_files": {
        f"resources{os.sep}derivation_files": (lambda file: file.endswith('.csv')),
        f"artefacts{os.sep}derivation_files": (lambda file: file.endswith('.csv')),  # Also keep local copy
    },
    # Extra variables from birds_nest/resources (no artefacts equivalent)
    f"birds_nest{os.sep}resources{os.sep}extra_variables": {
        f"resources{os.sep}extra_variables": (lambda file: True),
    },
    # LDM files from birds_nest/resources (no artefacts equivalent)
    f"birds_nest{os.sep}resources{os.sep}ldm": {
        f"resources{os.sep}ldm": (lambda file: True),
    },
    # Test files from birds_nest
    f"birds_nest{os.sep}tests": {
        "tests": (lambda file: True),
    },
    # IL files from birds_nest/resources
    f"birds_nest{os.sep}resources{os.sep}il": {
        f"resources{os.sep}il": (lambda file: True),
    },
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
        self.base_dir = str(settings.BASE_DIR)

        # Create all target directories beforehand (if they don't exist)
        # Note: We don't delete existing directories to preserve files from previous fetches
        self._create_all_target_directories()

    def _abs_path(self, path: str) -> str:
        """Resolve repo-relative paths against Django's BASE_DIR."""
        if os.path.isabs(path):
            return path
        return os.path.join(self.base_dir, path)

    def _get_authenticated_headers(self):
        """Get headers with authentication if token is provided."""
        headers = {}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers

    def _ensure_directory_exists(self, path):
        """Ensure a directory exists, creating it if necessary."""
        resolved_path = self._abs_path(path)
        os.makedirs(resolved_path, exist_ok=True)
        logger.debug(f"Ensured directory exists: {resolved_path}")
        return resolved_path

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
        resolved_path = self._abs_path(path)
        if os.path.exists(resolved_path):
            for filename in os.listdir(resolved_path):
                file_path = os.path.join(resolved_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        logger.debug(f"Deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        logger.debug(f"Deleted directory: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}: {e}")
            logger.info(f"Cleared directory: {resolved_path}")
        else:
            logger.debug(f"Directory does not exist, skipping clear: {resolved_path}")

    def _clear_matching_files(self, path, filter_func):
        """Remove files in a directory that match the same filter used for repository copies."""
        resolved_path = self._abs_path(path)
        if not os.path.isdir(resolved_path):
            return 0

        deleted_count = 0
        for file_name in os.listdir(resolved_path):
            file_path = os.path.join(resolved_path, file_name)
            if os.path.isfile(file_path) and filter_func(file_name):
                os.remove(file_path)
                deleted_count += 1
                logger.debug(f"Deleted stale mirrored file: {file_path}")
        return deleted_count

    def _mapping_has_selective_filter(self, target_mappings):
        """Return True when any mapping filter copies only some files."""
        for filter_func in target_mappings.values():
            try:
                if not filter_func("test_file.py"):
                    return True
            except Exception:
                return True
        return False

    def _clear_targets_for_missing_source(self, target_mappings):
        """Remove stale local files when a mapped source folder is absent from the new repo."""
        has_filter = self._mapping_has_selective_filter(target_mappings)
        for target_folder, filter_func in target_mappings.items():
            if target_folder == "tests":
                continue

            if has_filter:
                deleted_count = self._clear_matching_files(target_folder, filter_func)
                if deleted_count:
                    logger.info(
                        f"Removed {deleted_count} stale file(s) from missing source target {target_folder}"
                    )
                continue

            resolved_target_folder = self._abs_path(target_folder)
            if os.path.exists(resolved_target_folder):
                shutil.rmtree(resolved_target_folder)
                logger.info(f"Removed stale target directory for missing source: {resolved_target_folder}")

    def clear_downloaded_test_suite_files(self):
        """Remove previously downloaded test suites while preserving the tests package marker."""
        tests_root = self._abs_path("tests")
        os.makedirs(tests_root, exist_ok=True)

        removed_count = 0
        for item in os.listdir(tests_root):
            if item == "__init__.py":
                continue

            item_path = os.path.join(tests_root, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    continue
                removed_count += 1
                logger.info(f"Removed previously downloaded test suite item: {item_path}")
            except Exception as e:
                logger.error(
                    "Failed to remove previously downloaded test suite item %s: %s",
                    item_path,
                    e,
                )

        logger.info(
            "Cleared %s previously downloaded test suite item(s) from %s",
            removed_count,
            tests_root,
        )

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
        backup_root = self._abs_path(BACKUP_DIR)

        # Clean up any existing backup directory
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)
        os.makedirs(backup_root, exist_ok=True)

        logger.info(f"Creating backup directory: {backup_root}")

        for file_path in WHITELIST_FILES:
            resolved_file_path = self._abs_path(file_path)
            if os.path.exists(resolved_file_path):
                # Skip if file matches exclude patterns
                if self._should_exclude(file_path):
                    logger.debug(f"Skipping excluded file: {file_path}")
                    continue

                # Create backup path preserving directory structure
                backup_path = os.path.join(backup_root, file_path)
                backup_dir = os.path.dirname(backup_path)
                os.makedirs(backup_dir, exist_ok=True)

                # Copy file to backup
                shutil.copy2(resolved_file_path, backup_path)
                backed_up_files[resolved_file_path] = backup_path
                logger.info(f"Backed up whitelisted file: {resolved_file_path} -> {backup_path}")
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
        backup_root = self._abs_path(BACKUP_DIR)
        if os.path.exists(backup_root):
            shutil.rmtree(backup_root)
            logger.info(f"Cleaned up backup directory: {backup_root}")

    def _recreate_tmp_placeholders(self):
        """
        Recreate empty tmp placeholder files after GitHub fetch.

        These files are used to ensure certain directories exist in git
        and are tracked by version control.
        """
        created_count = 0
        for file_path in TMP_PLACEHOLDER_FILES:
            resolved_file_path = self._abs_path(file_path)
            # Ensure parent directory exists
            parent_dir = os.path.dirname(resolved_file_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            # Create empty file if it doesn't exist
            if not os.path.exists(resolved_file_path):
                with open(resolved_file_path, 'w') as f:
                    pass  # Create empty file
                created_count += 1
                logger.info(f"Created placeholder file: {resolved_file_path}")
            else:
                logger.debug(f"Placeholder file already exists: {resolved_file_path}")

        logger.info(f"Placeholder recreation complete: {created_count} files created")

    def _relocate_manual_derivation_file(self):
        """
        Relocate the manual derivation file to its correct location.

        GitHub repo has derived_field_configuration.py at the root of derivation_files/,
        but it should be at manually_generated/manual_derivations.py
        """
        old_path = self._abs_path(os.path.join("resources", "derivation_files", "derived_field_configuration.py"))
        new_dir = self._abs_path(os.path.join("resources", "derivation_files", "manually_generated"))
        new_path = os.path.join(new_dir, "manual_derivations.py")

        if os.path.exists(old_path):
            # Ensure target directory exists
            os.makedirs(new_dir, exist_ok=True)

            # Move and rename the file
            shutil.move(old_path, new_path)
            logger.info(f"Relocated manual derivation file: {old_path} -> {new_path}")
        else:
            logger.debug(f"Manual derivation file not found at old location: {old_path}")

    def _git_environment(self):
        """Build a git environment, passing tokens through config rather than the URL."""
        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        if self.token:
            env["GIT_CONFIG_COUNT"] = "1"
            env["GIT_CONFIG_KEY_0"] = "http.https://github.com/.extraheader"
            env["GIT_CONFIG_VALUE_0"] = f"AUTHORIZATION: bearer {self.token}"
        return env

    def _run_without_token(self, description, operation):
        """Retry a repository operation without the optional token."""
        if not self.token:
            return False

        original_token = self.token
        original_headers = self.headers
        self.token = None
        self.headers = self._get_authenticated_headers()
        try:
            logger.info(f"Retrying {description} without GitHub token")
            return operation()
        finally:
            self.token = original_token
            self.headers = original_headers

    def _clone_repo_with_git(self, base_url: str, destination_dir: str, branch: str) -> bool:
        """Clone a GitHub repository using git so Git LFS files are materialized."""
        owner, repo = _extract_github_repo_parts(base_url)
        clone_url = f"https://github.com/{owner}/{repo}.git"
        branch_label = (
            re.sub(r"[^A-Za-z0-9_.-]+", "-", branch or "main").strip("-") or "main"
        )
        clone_target = os.path.join(destination_dir, f"{repo}-{branch_label}")

        os.makedirs(destination_dir, exist_ok=True)
        logger.info("Cloning repository with git to support Git LFS content")
        clone_result = subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                branch,
                "--single-branch",
                clone_url,
                clone_target,
            ],
            env=self._git_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=GIT_CLONE_TIMEOUT,
            check=False,
        )
        if clone_result.returncode != 0:
            logger.warning(
                "git clone failed with exit code %s: %s",
                clone_result.returncode,
                clone_result.stderr.strip(),
            )
            return False

        if shutil.which("git-lfs"):
            lfs_result = subprocess.run(
                ["git", "lfs", "pull"],
                cwd=clone_target,
                env=self._git_environment(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=GIT_LFS_TIMEOUT,
                check=False,
            )
            if lfs_result.returncode != 0:
                logger.warning(
                    "git lfs pull failed with exit code %s: %s",
                    lfs_result.returncode,
                    lfs_result.stderr.strip(),
                )
        else:
            logger.warning("git-lfs is not installed; Git LFS files may remain as pointer files")

        logger.info("Repository cloned successfully with git")
        return True

    def _download_repo_archive(self, repo_url: str, destination_dir: str) -> bool:
        """Download and extract a GitHub archive as a fallback when git is unavailable."""
        response = requests.get(
            repo_url,
            headers=self.headers,
            timeout=REPO_DOWNLOAD_TIMEOUT,
        )
        os.makedirs(destination_dir, exist_ok=True)

        if response.status_code == 200:
            logger.info("Repository downloaded successfully, extracting files")
            with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_ref:
                safe_extract(zip_ref, destination_dir)
            logger.info("Repository extraction completed")
            return True

        logger.error(f"Failed to clone repository: {response.status_code}")
        if response.status_code == 401:
            logger.error("Authentication failed - check your GitHub token")
        elif response.status_code == 404:
            logger.error("Repository not found - check the URL")
        print(f"Failed to clone repository: {response.status_code}")
        return False

    def _find_git_lfs_pointer_files(self, root_path: str):
        pointer_files = []
        for current_root, dirs, files in os.walk(root_path):
            if ".git" in dirs:
                dirs.remove(".git")
            for file_name in files:
                path = os.path.join(current_root, file_name)
                if _looks_like_git_lfs_pointer(path):
                    pointer_files.append(path)
        return pointer_files

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
        destination_dir = self._abs_path(destination_path)
        repo_url = _build_github_archive_url(base_url, branch)

        # Clean up any existing destination directory to avoid mixing old and new files
        if os.path.exists(destination_dir):
            logger.info(f"Removing existing destination directory: {destination_dir}")
            shutil.rmtree(destination_dir)

        success = False
        if shutil.which("git"):
            success = self._clone_repo_with_git(base_url, destination_dir, branch)
            if not success:
                success = self._run_without_token(
                    "git clone",
                    lambda: self._clone_repo_with_git(base_url, destination_dir, branch),
                )
        else:
            logger.warning("git is not installed; falling back to GitHub archive download")

        if not success:
            logger.info(f"Downloading repository from {repo_url}")
            success = self._download_repo_archive(repo_url, destination_dir)
            if not success:
                success = self._run_without_token(
                    "GitHub archive download",
                    lambda: self._download_repo_archive(repo_url, destination_dir),
                )

        end_time = time.time()
        logger.info(f"Clone repo completed in {end_time - start_time:.2f} seconds")
        return success

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
        destination_dir = self._abs_path(destination_path)

        # Step 1: Backup whitelisted files before any deletions
        backed_up_files = self._backup_whitelisted_files()

        # Find the extracted folder (typically RepoName-branchname)
        # The zip extraction creates a folder like 'RepoName-branchname'
        extracted_folder = None
        logger.info(f"Looking for extracted folder in: {destination_dir}")
        for item in os.listdir(destination_dir):
            item_path = os.path.join(destination_dir, item)
            if os.path.isdir(item_path):
                logger.info(f"Found directory: {item}")
                # Accept any directory that's not a hidden folder
                if not item.startswith('.'):
                    extracted_folder = item_path
                    break

        # Validate that we found the extracted repository folder
        if not extracted_folder:
            logger.error(f"Could not find extracted repository folder in {destination_dir}")
            logger.error(f"Contents: {os.listdir(destination_dir)}")
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

        pointer_files = self._find_git_lfs_pointer_files(extracted_folder)
        if pointer_files:
            sample_files = ", ".join(
                os.path.relpath(path, extracted_folder) for path in pointer_files[:5]
            )
            self._restore_whitelisted_files(backed_up_files)
            self._cleanup_backup()
            raise RuntimeError(
                "Repository checkout contains Git LFS pointer files instead of real content. "
                "Install git-lfs and retrieve artifacts again. "
                f"Pointer files include: {sample_files}"
            )

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
            resolved_dir_path = self._abs_path(dir_path)
            os.makedirs(resolved_dir_path, exist_ok=True)
            logger.debug(f"Ensured directory exists: {resolved_dir_path}")

        # Step 2: Process each mapping in REPO_MAPPING
        for source_folder, target_mappings in REPO_MAPPING.items():
            source_path = os.path.join(extracted_folder, source_folder)
            logger.debug(f"Processing source folder: {source_path}")

            # Skip if source folder doesn't exist
            if not os.path.exists(source_path):
                logger.warning(f"Source path does not exist: {source_path}")
                self._clear_targets_for_missing_source(target_mappings)
                continue

            # Special handling for database export files that need filtering
            smcubes_paths = [
                f"artefacts{os.sep}smcubes_artefacts",
            ]
            if source_folder in smcubes_paths:
                logger.info(f"Processing database export files from: {source_path}")
                # Clear and recreate target directories to remove old files
                for target_folder in target_mappings.keys():
                    resolved_target_folder = self._abs_path(target_folder)
                    if os.path.exists(resolved_target_folder):
                        logger.info(f"Clearing target directory: {resolved_target_folder}")
                        shutil.rmtree(resolved_target_folder)
                    os.makedirs(resolved_target_folder, exist_ok=True)
                    logger.debug(f"Recreated target directory: {resolved_target_folder}")

                # Process each file in the source directory
                files_copied = 0
                for file_name in os.listdir(source_path):
                    # Check which target folder this file should go to based on filter functions
                    for target_folder, filter_func in target_mappings.items():
                        if filter_func(file_name):
                            source_file = os.path.join(source_path, file_name)
                            target_file = os.path.join(self._abs_path(target_folder), file_name)

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
            has_filter = self._mapping_has_selective_filter(target_mappings)

            if has_filter:
                # Handle mappings with selective filters - copy individual files (don't clear directory
                # as it may contain subdirectories that were already processed)
                for target_folder, filter_func in target_mappings.items():
                    resolved_target_folder = self._abs_path(target_folder)
                    os.makedirs(resolved_target_folder, exist_ok=True)
                    if target_folder.startswith(f"artefacts{os.sep}"):
                        deleted_count = self._clear_matching_files(target_folder, filter_func)
                        if deleted_count:
                            logger.info(f"Removed {deleted_count} stale filtered file(s) from {target_folder}")
                    files_copied = 0
                    for file_name in os.listdir(source_path):
                        source_file = os.path.join(source_path, file_name)
                        if os.path.isfile(source_file) and filter_func(file_name):
                            target_file = os.path.join(resolved_target_folder, file_name)
                            shutil.copy2(source_file, target_file)
                            files_copied += 1
                            logger.debug(f"File copied: {file_name} -> {target_folder}")
                    logger.info(f"Copied {files_copied} filtered files from {source_folder} to {target_folder}")
            else:
                # Copy entire directory to every target declared by the mapping.
                for target_folder in target_mappings.keys():
                    resolved_target_folder = self._abs_path(target_folder)

                    # Ensure parent directory exists
                    parent_dir = os.path.dirname(resolved_target_folder)
                    if parent_dir:
                        os.makedirs(parent_dir, exist_ok=True)

                    # Remove existing target folder and copy fresh from source
                    if os.path.exists(resolved_target_folder):
                        shutil.rmtree(resolved_target_folder)
                        logger.info(f"Removed existing directory: {resolved_target_folder}")

                    shutil.copytree(source_path, resolved_target_folder)
                    logger.info(f"Copied directory: {source_path} -> {resolved_target_folder}")

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
        destination_dir = self._abs_path(destination_path)

        # Find the extracted folder (the ZIP extraction creates a folder with repo name + branch)
        extracted_folder = None
        repo_name = None
        for item in os.listdir(destination_dir):
            item_path = os.path.join(destination_dir, item)
            if os.path.isdir(item_path):
                # Accept any directory - should be the extracted repository
                extracted_folder = item_path
                # Extract repo name (e.g., "bird-default-test-suite-eil-67" from "bird-default-test-suite-eil-67-main")
                repo_name = item.rsplit('-', 1)[0] if '-' in item else item
                logger.info(f"Found extracted folder: {extracted_folder}, repo name: {repo_name}")
                break

        # Validate that we found the extracted repository folder
        if not extracted_folder:
            logger.error("Could not find extracted repository folder for test suite files")
            return

        # Create target base directory: tests/{repo_name}/
        target_base = self._abs_path(os.path.join("tests", repo_name))
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
        resolved_destination_path = self._abs_path(destination_path)
        logger.info(f"Removing fetched files from {resolved_destination_path}")
        shutil.rmtree(resolved_destination_path)
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
