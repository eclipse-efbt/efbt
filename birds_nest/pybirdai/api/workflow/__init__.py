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
Workflow API module for PyBIRD AI.

This module provides services for:
- GitHub file fetching and repository management
- Automode setup and configuration
- Non-destructive file mirroring for workflow execution
- Destructive setup for initial installation
- GitHub integration (PRs, forks, branches)

Usage:
    # For initial automode setup (destructive - clears existing directories)
    from pybirdai.api.workflow import SetupRepoService, AutomodeConfigurationService

    service = AutomodeConfigurationService()
    service.execute_automode_setup(config)  # Uses SetupRepoService internally

    # For workflow execution Step 1 (non-destructive - preserves generated code)
    from pybirdai.api.workflow import MirrorRepoService, AutomodeConfigurationService

    service = AutomodeConfigurationService()
    service._fetch_from_github(url, token, use_mirror=True)  # Uses MirrorRepoService

    # Or directly:
    mirror = MirrorRepoService(token)
    mirror.clone_repo(url, "repo_name", "main")
    mirror.mirror_files("repo_name")  # Non-destructive!
    mirror.remove_fetched_files("repo_name")
"""

# Helper constants and utilities
from .helpers import (
    DEFAULT_GITHUB_BRANCH,
    BASE,
    BACKUP_DIR,
    WHITELIST_FILES,
    EXCLUDE_PATTERNS,
    TMP_PLACEHOLDER_FILES,
    REPO_MAPPING,
    PROTECTED_DIRS,
    MERGE_DIRS,
    UPDATE_DIRS,
    MIRROR_MAPPING,
)

# GitHub file fetcher
from .github_fetcher import ConfigurableGitHubFileFetcher

# Setup service (destructive - for initial setup only)
from .setup_service import SetupRepoService, CloneRepoService

# Mirror service (non-destructive - for workflow execution)
from .mirror_service import MirrorRepoService

# Automode configuration service
from .automode_service import AutomodeConfigurationService

# GitHub integration service
from .github_integration import GitHubIntegrationService

__all__ = [
    # Constants
    'DEFAULT_GITHUB_BRANCH',
    'BASE',
    'BACKUP_DIR',
    'WHITELIST_FILES',
    'EXCLUDE_PATTERNS',
    'TMP_PLACEHOLDER_FILES',
    'REPO_MAPPING',
    'PROTECTED_DIRS',
    'MERGE_DIRS',
    'UPDATE_DIRS',
    'MIRROR_MAPPING',
    # Classes
    'ConfigurableGitHubFileFetcher',
    'SetupRepoService',
    'CloneRepoService',  # Alias for backward compatibility
    'MirrorRepoService',
    'AutomodeConfigurationService',
    'GitHubIntegrationService',
]
