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
DPM (Data Point Model) Pipeline for COREP/EBA Workflow.

This script runs the DPM pipeline with framework-specific imports.
It fetches data from the configured pipeline_url_dpm (default: FreeBIRD_COREP)
before running the workflow steps.

Usage:
    # Using environment variable for GitHub token (private repos)
    export GITHUB_TOKEN=ghp_xxx
    uv run pybirdai/standalone/run_dpm_pipeline.py

    # Or using command line argument
    uv run pybirdai/standalone/run_dpm_pipeline.py --token ghp_xxx

    # Skip framework fetch (use existing files)
    uv run pybirdai/standalone/run_dpm_pipeline.py --skip-fetch

    # Skip setup (use existing database)
    uv run pybirdai/standalone/run_dpm_pipeline.py --skip-setup
"""
import django
import os
import sys
import argparse
from django.apps import AppConfig
from django.conf import settings
import logging
import ast

# Parse arguments before Django setup
parser = argparse.ArgumentParser(description='Run DPM/COREP Pipeline')
parser.add_argument('--token', default=None,
                    help='GitHub token for private repositories (or set GITHUB_TOKEN env var, or use .pybird_github_token file)')
parser.add_argument('--skip-fetch', action='store_true',
                    help='Skip fetching framework files (use existing)')
parser.add_argument('--skip-setup', action='store_true',
                    help='Skip database setup (use existing database)')
args = parser.parse_args()

# Resolve GitHub token from multiple sources: CLI arg → .pybird_github_token file → env var
def _get_standalone_github_token():
    """Get GitHub token for standalone scripts.

    Priority: CLI --token → .pybird_github_token file → GITHUB_TOKEN env var
    """
    # 1. CLI argument takes highest priority
    if args.token:
        return args.token

    # 2. Try to load from .pybird_github_token file (same as web interface)
    try:
        from pybirdai.views.workflow.github import _get_github_token
        token = _get_github_token(request=None)
        if token:
            return token
    except ImportError:
        pass  # Django not set up yet, will try after setup

    # 3. Fall back to environment variable
    return os.getenv('GITHUB_TOKEN')

# Create a logger
logger = logging.getLogger(__name__)

class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise


def fetch_framework_files(github_token=None):
    """
    Fetch COREP/DPM framework files from the configured pipeline URL.

    Uses pipeline_url_dpm from AutomodeConfiguration (default: FreeBIRD_COREP).
    """
    logger.info("="*80)
    logger.info("FETCHING COREP/DPM FRAMEWORK FILES FROM GITHUB")
    logger.info("="*80)

    from pybirdai.api.workflow_api import AutomodeConfigurationService

    service = AutomodeConfigurationService()
    result = service.fetch_files_for_framework('COREP', github_token=github_token)

    if result.get('errors'):
        for error in result['errors']:
            logger.error(error)
        raise RuntimeError("Failed to fetch framework files")

    logger.info(f"Fetched {result.get('technical_export', 0)} files from {result.get('github_url')}")
    logger.info("Framework file fetch completed successfully")
    return result


if __name__ == "__main__":
    DjangoSetup.configure_django()

    # Resolve GitHub token (CLI → .pybird_github_token file → env var)
    github_token = _get_standalone_github_token()
    if github_token:
        logger.info("GitHub token loaded successfully")
    else:
        logger.info("No GitHub token found (public repos only)")

    # Fetch COREP framework files from GitHub (unless skipped)
    if not args.skip_fetch:
        fetch_framework_files(github_token=github_token)
    else:
        logger.info("Skipping framework file fetch (--skip-fetch)")

    # Only run setup if not skipping
    if not args.skip_setup:
        os.system("uv run pybirdai/standalone/standalone_fetch_artifacts_eil.py")
        os.system("uv run pybirdai/standalone/standalone_setup_migrate_database.py")

        from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
        app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
        app_config.run_delete_bird_metadata_database()
    else:
        logger.info("Skipping database setup (--skip-setup)")

    from pybirdai.entry_points.import_dpm_data import RunImportDPMData

    # Phase A: Extract metadata only (no explosion - fast)
    app_config = RunImportDPMData('pybirdai', 'birds_nest')
    app_config.run_import_phase_a(frameworks=['COREP'])

    # Phase B: Process only selected tables then import
    selected_tables = ['C_07.00.a']  # Limited set for CI testing
    app_config.run_import_phase_b(selected_tables=selected_tables, enable_table_duplication=False)
