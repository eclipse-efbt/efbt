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
Shared utilities and constants for workflow API services.
"""

import os
import logging

logger = logging.getLogger(__name__)


def create_pipeline_filter(pipeline: str = None):
    """
    Create a filter function for filter_code files based on pipeline.

    This returns a function that filters files to only include those from the
    appropriate type directory (datasets for ancrdt, templates for dpm/main).

    Args:
        pipeline: Pipeline name ('main', 'ancrdt', or 'dpm'). If None, accepts all files.

    Returns:
        A filter function that takes a file path and returns True if it should be included.
    """
    if pipeline is None:
        return lambda file: True

    from pybirdai.services.pipeline_repo_service import PipelineRepoService
    code_type = PipelineRepoService.get_code_type_for_pipeline(pipeline)

    def filter_func(file_path: str) -> bool:
        """
        Filter files based on pipeline code type.

        Allows:
        - Files in the correct type directory (datasets/ for ancrdt, templates/ for dpm/main)
        - Files in the lib/ directory (shared utilities)
        - Root level files (legacy support)
        """
        # Normalize path separators
        normalized = file_path.replace('\\', '/').replace(os.sep, '/')

        # Always allow lib files (shared utilities)
        if '/lib/' in normalized or normalized.startswith('lib/'):
            return True

        # Check if file is in a type directory
        if '/datasets/' in normalized or normalized.startswith('datasets/'):
            return code_type == 'datasets'
        if '/templates/' in normalized or normalized.startswith('templates/'):
            return code_type == 'templates'

        # Root level files - allow for legacy support (but log warning)
        logger.debug(f"Allowing root-level filter_code file: {file_path}")
        return True

    return filter_func

# Default GitHub branch
DEFAULT_GITHUB_BRANCH = "main"

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
    # Automatic tracking wrapper (generated/customized file) - now in lib subdirectory
    os.path.join("pybirdai", "process_steps", "filter_code", "lib", "automatic_tracking_wrapper.py"),
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
    # Filter code lib directory
    os.path.join("pybirdai", "process_steps", "filter_code", "lib", "tmp"),
    # Filter code datasets structure (ANCRDT)
    os.path.join("pybirdai", "process_steps", "filter_code", "datasets", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "datasets", "ANCRDT", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "datasets", "ANCRDT", "filter", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "datasets", "ANCRDT", "joins", "tmp"),
    # Filter code templates structure (FINREP, COREP)
    os.path.join("pybirdai", "process_steps", "filter_code", "templates", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "templates", "FINREP", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "templates", "FINREP", "filter", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "templates", "FINREP", "joins", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "templates", "COREP", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "templates", "COREP", "filter", "tmp"),
    os.path.join("pybirdai", "process_steps", "filter_code", "templates", "COREP", "joins", "tmp"),
    # Resource directories
    os.path.join("resources", "derivation_files", "generated_from_logical_transformation_rules", "tmp"),
    os.path.join("resources", "derivation_files", "generated_from_member_links", "tmp"),
    os.path.join("resources", "derivation_files", "manually_generated", "tmp"),
    os.path.join("resources", "derivation_files", "tmp"),
    os.path.join("resources", "extra_variables", "tmp"),
    os.path.join("resources", "il", "tmp"),
]

# Enhanced mapping configuration that defines how source folders from the repository
# should be copied to target folders, with optional file filtering functions
# Used by SETUP (destructive) service - replaces entire directories
REPO_MAPPING = {
    # Database export files with specific filtering rules
    f"export{os.sep}database_export_ldm": {
        f"resources{os.sep}admin": (lambda file: file.startswith("auth_")),  # Only auth-related files
        f"resources{os.sep}bird": (lambda file: file.startswith("bird_")),   # Only bird-related files
        f"resources{os.sep}technical_export": (lambda file: True)            # All files
    },
    # Join configuration files (from export/ structure)
    f"export{os.sep}joins_configuration": {
        f"resources{os.sep}joins_configuration": (lambda file: True),        # All files
    },
    # Join configuration files (legacy location at repo root)
    "joins_configuration": {
        f"resources{os.sep}joins_configuration": (lambda file: True),        # All files
    },
    # Initial correction files
    f"birds_nest{os.sep}resources{os.sep}extra_variables": {
        f"resources{os.sep}extra_variables": (lambda file: True),            # All files
    },
    # Derivation files from birds_nest resources
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
    # Filter code files - new export structure (export/filter_code/{type}/{FRAMEWORK}/...)
    # Maps to production location preserving subdirectory structure
    f"export{os.sep}filter_code": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),
    },
    # Filter code files - legacy location (birds_nest/pybirdai/process_steps/filter_code)
    f"birds_nest{os.sep}pybirdai{os.sep}process_steps{os.sep}filter_code": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),
    },
    # Generated Python files - new unified structure
    f"birds_nest{os.sep}results{os.sep}generated_python": {
        f"results{os.sep}generated_python": (lambda file: True),
    },
    # Generated Python files - legacy locations (for backward compatibility)
    f"birds_nest{os.sep}results{os.sep}generated_python_filters": {
        f"results{os.sep}generated_python_filters": (lambda file: True),
    },
    f"birds_nest{os.sep}results{os.sep}generated_python_joins": {
        f"results{os.sep}generated_python_joins": (lambda file: True),
    }
}

# Directories that should NEVER be deleted during mirror operations (workflow execution)
# These contain user-generated content that must be preserved
PROTECTED_DIRS = [
    # New unified structure
    f"results{os.sep}generated_python",
    # Legacy locations (for backward compatibility)
    f"results{os.sep}generated_python_filters",
    f"results{os.sep}generated_python_joins",
    f"results{os.sep}generated_python_code",
    f"pybirdai{os.sep}process_steps{os.sep}join_code",
]

# Directories that should be merged (add new files without deleting existing)
MERGE_DIRS = [
    f"resources{os.sep}derivation_files",
    "tests",
    f"pybirdai{os.sep}process_steps{os.sep}filter_code",
]

# Directories that can be updated in place (overwrite files from GitHub)
UPDATE_DIRS = [
    f"resources{os.sep}ldm",
    f"resources{os.sep}technical_export",
    f"resources{os.sep}joins_configuration",
    f"resources{os.sep}extra_variables",
]

# Mapping for MIRROR service - only updates content files, not generated code
MIRROR_MAPPING = {
    # Database export files - update configuration, not generated content
    f"export{os.sep}database_export_ldm": {
        f"resources{os.sep}admin": (lambda file: file.startswith("auth_")),
        f"resources{os.sep}bird": (lambda file: file.startswith("bird_")),
        f"resources{os.sep}technical_export": (lambda file: True)
    },
    # Join configuration files - always update
    "joins_configuration": {
        f"resources{os.sep}joins_configuration": (lambda file: True),
    },
    # LDM files - update
    f"birds_nest{os.sep}resources{os.sep}ldm": {
        f"resources{os.sep}ldm": (lambda file: True),
    },
    # IL files - update
    f"birds_nest{os.sep}resources{os.sep}il": {
        f"resources{os.sep}il": (lambda file: True),
    },
    # Filter code files - add/update without deleting existing files
    f"export{os.sep}filter_code": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),
    },
    # Filter code files - legacy location
    f"birds_nest{os.sep}pybirdai{os.sep}process_steps{os.sep}filter_code": {
        f"pybirdai{os.sep}process_steps{os.sep}filter_code": (lambda file: True),
    },
}
