# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Pipeline Repository Service for PyBIRD AI.

This module provides pipeline-specific repository and path management:
- 3 pipelines: main (BIRD), ancrdt (AnaCredit), dpm (COREP only for now)
- Each pipeline has isolated joins_configuration and database_export directories
- Supports pipeline detection from frameworks
- Per-session pipeline GitHub URLs for artifact fetching
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any

from django.conf import settings

logger = logging.getLogger(__name__)

# Session storage for per-pipeline URLs (per-session, in-memory)
_session_pipeline_urls: Dict[str, Dict[str, str]] = {}


# ============================================================================
# Constants
# ============================================================================

PIPELINES = ['main', 'ancrdt', 'dpm']

# Framework to pipeline mapping
# Note: DPM is simplified to COREP only for now
FRAMEWORK_PIPELINE_MAP = {
    # DPM frameworks (COREP only for now)
    'COREP': 'dpm',
    # AnaCredit frameworks
    'ANCRDT': 'ancrdt',
    'ANACREDIT': 'ancrdt',
    # Main/BIRD frameworks
    'BIRD': 'main',
}

# Default repository naming pattern
REPO_NAMING_PATTERN = "pybird-{pipeline}"

# Default pipeline URLs - empty by default, user must configure via UI
DEFAULT_PIPELINE_URLS = {
    'main': '',
    'dpm': '',
    'ancrdt': '',
}


class PipelineRepoService:
    """
    Service for managing pipeline-specific repositories and paths.

    This service provides:
    - Pipeline identification and validation
    - Path resolution for joins_configuration and database_export
    - Repository URL generation for pipeline repos
    - Framework to pipeline mapping

    Usage:
        service = PipelineRepoService()

        # Get paths for a specific pipeline
        joins_path = service.get_joins_path('dpm')
        export_path = service.get_export_path('dpm')

        # Detect pipeline from frameworks
        pipeline = service.detect_pipeline_from_frameworks(['FINREP', 'COREP'])
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the pipeline repo service.

        Args:
            config_path: Optional path to pipeline_repos.json config file.
                         If not provided, uses default location.
        """
        self._config = self._load_config(config_path)
        self._base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        self._resources_dir = os.path.join(self._base_dir, 'resources')

    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load pipeline configuration from JSON file."""
        if config_path is None:
            config_path = os.path.join(
                getattr(settings, 'BASE_DIR', os.getcwd()),
                'pybirdai', 'config', 'pipeline_repos.json'
            )

        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load pipeline config: {e}")

        # Return default config
        return {
            'pipelines': {
                'main': {
                    'name': 'Main BIRD Pipeline',
                    'repo_name': 'pybird-main',
                    'frameworks': ['BIRD']
                },
                'ancrdt': {
                    'name': 'AnaCredit Pipeline',
                    'repo_name': 'pybird-ancrdt',
                    'frameworks': ['ANCRDT', 'ANACREDIT']
                },
                'dpm': {
                    'name': 'DPM Pipeline',
                    'repo_name': 'pybird-dpm',
                    'frameworks': ['FINREP', 'COREP', 'AE', 'FP', 'LCR', 'NSFR', 'MREL', 'REM', 'CON']
                }
            }
        }

    # ========================================================================
    # Pipeline Identification
    # ========================================================================

    @staticmethod
    def get_valid_pipelines() -> List[str]:
        """Get list of valid pipeline names."""
        return PIPELINES.copy()

    @staticmethod
    def is_valid_pipeline(pipeline: str) -> bool:
        """Check if a pipeline name is valid."""
        return pipeline in PIPELINES

    def detect_pipeline_from_frameworks(
        self,
        frameworks: List[str]
    ) -> str:
        """
        Detect pipeline type from a list of frameworks.

        Args:
            frameworks: List of framework codes (e.g., ['FINREP', 'COREP'])

        Returns:
            Pipeline name ('main', 'ancrdt', or 'dpm')
        """
        if not frameworks:
            return 'main'

        # Check each framework
        for framework in frameworks:
            framework_upper = framework.upper()
            if framework_upper in FRAMEWORK_PIPELINE_MAP:
                return FRAMEWORK_PIPELINE_MAP[framework_upper]

        # Default to main
        return 'main'

    # ========================================================================
    # Path Resolution
    # ========================================================================

    def get_joins_path(self, pipeline: str) -> str:
        """
        Get the joins_configuration path for a pipeline.

        Args:
            pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

        Returns:
            Absolute path to joins_configuration directory
        """
        return os.path.join(self._resources_dir, 'joins_configuration', pipeline)

    def get_export_path(self, pipeline: str) -> str:
        """
        Get the database_export path for a pipeline.

        Args:
            pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

        Returns:
            Absolute path to database_export directory
        """
        return os.path.join(self._resources_dir, 'database_export', pipeline)

    def get_backup_path(self, pipeline: str) -> str:
        """
        Get the backup directory path for a pipeline.

        Args:
            pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

        Returns:
            Absolute path to backup directory
        """
        return os.path.join(self._resources_dir, 'backups', pipeline)

    def get_all_paths(self, pipeline: str) -> Dict[str, str]:
        """
        Get all relevant paths for a pipeline.

        Args:
            pipeline: Pipeline name

        Returns:
            Dict with 'joins', 'export', and 'backup' paths
        """
        return {
            'joins': self.get_joins_path(pipeline),
            'export': self.get_export_path(pipeline),
            'backup': self.get_backup_path(pipeline)
        }

    # ========================================================================
    # Repository Management
    # ========================================================================

    def get_repo_name(self, pipeline: str) -> str:
        """
        Get the repository name for a pipeline.

        Args:
            pipeline: Pipeline name

        Returns:
            Repository name (e.g., 'pybird-dpm')
        """
        pipeline_config = self._config.get('pipelines', {}).get(pipeline, {})
        return pipeline_config.get('repo_name', REPO_NAMING_PATTERN.format(pipeline=pipeline))

    def get_repo_url(self, pipeline: str, owner: str) -> str:
        """
        Get the GitHub repository URL for a pipeline.

        Args:
            pipeline: Pipeline name
            owner: GitHub username or organization

        Returns:
            Full GitHub URL
        """
        repo_name = self.get_repo_name(pipeline)
        return f"https://github.com/{owner}/{repo_name}"

    @staticmethod
    def get_code_type_for_pipeline(pipeline: str) -> str:
        """
        Get the code type (datasets or templates) for a pipeline.

        This determines which subdirectory structure to use for filter code:
        - ancrdt pipeline -> 'datasets' (ANCRDT uses datasets structure)
        - dpm/main pipeline -> 'templates' (COREP/FINREP/BIRD use templates structure)

        Args:
            pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

        Returns:
            Code type: 'datasets' or 'templates'
        """
        if pipeline == 'ancrdt':
            return 'datasets'
        # dpm and main pipelines use templates
        return 'templates'

    def get_pipeline_info(self, pipeline: str) -> Dict[str, Any]:
        """
        Get full information about a pipeline.

        Args:
            pipeline: Pipeline name

        Returns:
            Dict with pipeline information
        """
        if not self.is_valid_pipeline(pipeline):
            return {'error': f'Invalid pipeline: {pipeline}'}

        pipeline_config = self._config.get('pipelines', {}).get(pipeline, {})

        return {
            'name': pipeline_config.get('name', f'{pipeline.title()} Pipeline'),
            'repo_name': self.get_repo_name(pipeline),
            'frameworks': pipeline_config.get('frameworks', []),
            'paths': self.get_all_paths(pipeline)
        }

    def list_pipelines(self) -> List[Dict[str, Any]]:
        """
        List all available pipelines with their information.

        Returns:
            List of pipeline information dicts
        """
        return [self.get_pipeline_info(p) for p in PIPELINES]


# ============================================================================
# Session-based Pipeline URL Management
# ============================================================================

def set_pipeline_url(session_id: str, pipeline: str, url: str) -> None:
    """
    Set the GitHub URL for a pipeline in the current session.

    Args:
        session_id: Django session ID
        pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')
        url: GitHub repository URL
    """
    global _session_pipeline_urls
    if session_id not in _session_pipeline_urls:
        _session_pipeline_urls[session_id] = {}
    _session_pipeline_urls[session_id][pipeline] = url
    logger.info(f"Set pipeline URL for {pipeline}: {url[:50]}...")


def get_pipeline_url(session_id: str, pipeline: str) -> str:
    """
    Get the GitHub URL for a pipeline from the current session.

    Args:
        session_id: Django session ID
        pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

    Returns:
        GitHub repository URL, or default if not set
    """
    global _session_pipeline_urls
    if session_id in _session_pipeline_urls:
        if pipeline in _session_pipeline_urls[session_id]:
            return _session_pipeline_urls[session_id][pipeline]
    return DEFAULT_PIPELINE_URLS.get(pipeline, '')


def get_all_pipeline_urls(session_id: str) -> Dict[str, str]:
    """
    Get all pipeline URLs for the current session.

    Args:
        session_id: Django session ID

    Returns:
        Dict mapping pipeline names to URLs
    """
    global _session_pipeline_urls
    result = DEFAULT_PIPELINE_URLS.copy()
    if session_id in _session_pipeline_urls:
        result.update(_session_pipeline_urls[session_id])
    return result


def clear_session_urls(session_id: str) -> None:
    """
    Clear all pipeline URLs for a session.

    Args:
        session_id: Django session ID
    """
    global _session_pipeline_urls
    if session_id in _session_pipeline_urls:
        del _session_pipeline_urls[session_id]
        logger.info(f"Cleared pipeline URLs for session: {session_id[:8]}...")


def set_pipeline_urls_from_config(session_id: str, config: Dict[str, Any]) -> None:
    """
    Set pipeline URLs from a configuration dict.

    Config keys expected:
    - pipeline_url_main: URL for main/BIRD pipeline
    - pipeline_url_dpm: URL for DPM/COREP pipeline
    - pipeline_url_ancrdt: URL for AnaCredit pipeline

    Args:
        session_id: Django session ID
        config: Configuration dictionary
    """
    for pipeline in PIPELINES:
        key = f'pipeline_url_{pipeline}'
        if key in config and config[key]:
            set_pipeline_url(session_id, pipeline, config[key])


# ============================================================================
# Convenience Functions
# ============================================================================

def get_pipeline_service() -> PipelineRepoService:
    """Get a PipelineRepoService instance."""
    return PipelineRepoService()


def detect_pipeline(frameworks: List[str]) -> str:
    """
    Convenience function to detect pipeline from frameworks.

    Args:
        frameworks: List of framework codes

    Returns:
        Pipeline name
    """
    return PipelineRepoService.detect_pipeline_from_frameworks(
        PipelineRepoService(), frameworks
    )


def get_configured_pipeline_url(pipeline: str) -> str:
    """
    Get the configured pipeline URL from all available sources.

    Priority order:
    1. Environment variables (PIPELINE_URL_MAIN, PIPELINE_URL_ANCRDT, PIPELINE_URL_DPM)
    2. Config file (automode_config.json)
    3. Database (AutomodeConfiguration model)

    NO hardcoded defaults - if nothing is configured, returns empty string.

    Args:
        pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

    Returns:
        Configured GitHub URL, or empty string if not configured
    """
    # 1. Check environment variables (highest priority)
    env_var_map = {
        'main': 'PIPELINE_URL_MAIN',
        'ancrdt': 'PIPELINE_URL_ANCRDT',
        'dpm': 'PIPELINE_URL_DPM',
    }
    env_var = env_var_map.get(pipeline)
    if env_var:
        env_url = os.environ.get(env_var, '')
        # Also check alternative ANCRDT env var
        if not env_url and pipeline == 'ancrdt':
            env_url = os.environ.get('ANCRDT_PIPELINE_URL', '')
        if env_url:
            logger.debug(f"Using {pipeline} URL from environment: {env_url[:50]}...")
            return env_url

    # 2. Check config file (automode_config.json)
    config_paths = ['./automode_config.json', 'automode_config.json']
    for config_path in config_paths:
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                    config_key = f'pipeline_url_{pipeline}'
                    file_url = file_config.get(config_key, '')
                    if file_url:
                        logger.debug(f"Using {pipeline} URL from config file: {file_url[:50]}...")
                        return file_url
            except Exception as e:
                logger.warning(f"Could not read config file {config_path}: {e}")

    # 3. Check database configuration
    try:
        from pybirdai.models.workflow_model import AutomodeConfiguration
        config = AutomodeConfiguration.get_active_configuration()
        if config:
            db_url = getattr(config, f'pipeline_url_{pipeline}', '')
            if db_url:
                logger.debug(f"Using {pipeline} URL from database: {db_url[:50]}...")
                return db_url
    except Exception as e:
        logger.warning(f"Could not read database configuration: {e}")

    # No configuration found - return empty string
    logger.debug(f"No configured URL found for pipeline: {pipeline}")
    return ''


def get_configured_test_suite_url(pipeline: str) -> str:
    """
    Get the configured test suite URL from all available sources.

    Priority order:
    1. Environment variables (TEST_SUITE_URL_MAIN, TEST_SUITE_URL_ANCRDT, TEST_SUITE_URL_DPM)
    2. Config file (automode_config.json)
    3. Database (AutomodeConfiguration model)

    NO hardcoded defaults - if nothing is configured, returns empty string.

    Args:
        pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

    Returns:
        Configured test suite URL, or empty string if not configured
    """
    # 1. Check environment variables
    env_var = f'TEST_SUITE_URL_{pipeline.upper()}'
    env_url = os.environ.get(env_var, '')
    if env_url:
        logger.debug(f"Using {pipeline} test suite URL from environment: {env_url[:50]}...")
        return env_url

    # 2. Check config file
    config_paths = ['./automode_config.json', 'automode_config.json']
    for config_path in config_paths:
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                    config_key = f'test_suite_url_{pipeline}'
                    file_url = file_config.get(config_key, '')
                    if file_url:
                        logger.debug(f"Using {pipeline} test suite URL from config file: {file_url[:50]}...")
                        return file_url
            except Exception as e:
                logger.warning(f"Could not read config file {config_path}: {e}")

    # 3. Check database configuration
    try:
        from pybirdai.models.workflow_model import AutomodeConfiguration
        config = AutomodeConfiguration.get_active_configuration()
        if config:
            db_url = getattr(config, f'test_suite_url_{pipeline}', '')
            if db_url:
                logger.debug(f"Using {pipeline} test suite URL from database: {db_url[:50]}...")
                return db_url
    except Exception as e:
        logger.warning(f"Could not read database configuration: {e}")

    return ''
