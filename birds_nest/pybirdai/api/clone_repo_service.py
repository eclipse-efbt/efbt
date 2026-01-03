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
Backward compatibility module for clone_repo_service.

This module has been refactored into pybirdai/api/workflow/ with separate services:
- SetupRepoService (destructive) - for initial automode setup
- MirrorRepoService (non-destructive) - for workflow execution

This file re-exports CloneRepoService for backward compatibility.
New code should import from pybirdai.api.workflow instead.
"""

# Re-export from new location for backward compatibility
from .workflow.setup_service import (
    SetupRepoService,
    CloneRepoService,  # Alias for backward compatibility
    main,
)

from .workflow.helpers import (
    REPO_MAPPING,
    BACKUP_DIR,
    WHITELIST_FILES,
    EXCLUDE_PATTERNS,
    TMP_PLACEHOLDER_FILES,
    BASE,
)

# Also export the new mirror service for easy access
from .workflow.mirror_service import MirrorRepoService

__all__ = [
    'CloneRepoService',
    'SetupRepoService',
    'MirrorRepoService',
    'REPO_MAPPING',
    'BACKUP_DIR',
    'WHITELIST_FILES',
    'EXCLUDE_PATTERNS',
    'TMP_PLACEHOLDER_FILES',
    'BASE',
    'main',
]
