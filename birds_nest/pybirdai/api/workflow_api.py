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
#    Benjamin Arfa - refactored into workflow/ submodule

"""
Backward compatibility module for workflow_api.

This module has been refactored into pybirdai/api/workflow/ with separate services:
- ConfigurableGitHubFileFetcher - GitHub file fetching
- SetupRepoService (destructive) - for initial automode setup
- MirrorRepoService (non-destructive) - for workflow execution
- AutomodeConfigurationService - automode configuration and setup
- GitHubIntegrationService - GitHub PRs, forks, branches

This file re-exports all classes for backward compatibility.
New code should import from pybirdai.api.workflow instead.

Example usage (new style):
    from pybirdai.api.workflow import AutomodeConfigurationService, MirrorRepoService

    # For workflow execution (Step 1) - NON-DESTRUCTIVE
    service = AutomodeConfigurationService()
    service._fetch_from_github(url, token, use_mirror=True)

    # For automode setup - DESTRUCTIVE
    service.execute_automode_setup(config)  # Uses SetupRepoService internally
"""

# Re-export all classes from new locations for backward compatibility
from .workflow import (
    # Constants
    DEFAULT_GITHUB_BRANCH,
    # Classes
    ConfigurableGitHubFileFetcher,
    SetupRepoService,
    CloneRepoService,
    MirrorRepoService,
    AutomodeConfigurationService,
    GitHubIntegrationService,
)

__all__ = [
    'DEFAULT_GITHUB_BRANCH',
    'ConfigurableGitHubFileFetcher',
    'SetupRepoService',
    'CloneRepoService',
    'MirrorRepoService',
    'AutomodeConfigurationService',
    'GitHubIntegrationService',
]
