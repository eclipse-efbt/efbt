# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
PyBIRD AI Services Module.

This module contains unified service classes for external integrations.
"""

from .github_service import GitHubService
from .pipeline_repo_service import PipelineRepoService, get_pipeline_service, detect_pipeline
from .backup_service import BackupService, get_backup_service, create_pipeline_backup

__all__ = [
    'GitHubService',
    'PipelineRepoService',
    'get_pipeline_service',
    'detect_pipeline',
    'BackupService',
    'get_backup_service',
    'create_pipeline_backup',
]
