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
Entry point for application setup operations.

This module provides AppConfig-based entry points for all database setup operations.
All business logic is contained in pybirdai.process_steps.database_setup.

Usage:
    from pybirdai.entry_points.database_setup import RunApplicationSetup

    app_config = RunApplicationSetup('pybirdai', 'birds_nest')
    app_config.run_automode_setup()
    app_config.run_post_setup()
    app_config.run_migrations()
"""

import os
import logging
from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)


class RunApplicationSetup(AppConfig):
    """AppConfig for application setup operations."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    def __init__(self, app_name, app_module, *args, **kwargs):
        self.app_name = app_name
        self.app_module = app_module
        self.token = kwargs.pop('token', '')

    def ready(self):
        """Run the complete automode database setup."""
        return self.run_automode_setup()

    def run_automode_setup(self):
        """Execute Step 1: Environment cleanup and derivation file generation."""
        from pybirdai.process_steps.database_setup.automode_orchestrator import run_automode_setup
        return run_automode_setup(self.app_name, self.app_module, self.token)

    def run_post_setup(self):
        """Execute Step 2: Generate models, merge derivations, update admin."""
        from pybirdai.process_steps.database_setup.automode_orchestrator import run_post_setup
        return run_post_setup(self.app_name, self.app_module, self.token)

    def run_migrations(self):
        """Execute Step 3: Run Django migrations."""
        from pybirdai.process_steps.database_setup.automode_orchestrator import run_migrations
        return run_migrations(self.app_name, self.app_module, self.token)

    def generate_derivation_files(self, transformation_rules_csv=None, output_dir=None):
        """Generate Python derivation files from transformation rules."""
        from pybirdai.process_steps.database_setup.derivation_pipeline import run_generate_derivation_files
        kwargs = {}
        if transformation_rules_csv:
            kwargs['transformation_rules_csv'] = transformation_rules_csv
        if output_dir:
            kwargs['output_dir'] = output_dir
        return run_generate_derivation_files(**kwargs)

    def merge_derived_fields(self, bird_data_model_path=None):
        """Merge derived fields into the bird data model."""
        from pybirdai.process_steps.database_setup.derivation_pipeline import run_merge_derived_fields
        kwargs = {}
        if bird_data_model_path:
            kwargs['bird_data_model_path'] = bird_data_model_path
        return run_merge_derived_fields(**kwargs)

    def export_rules_to_config(self, transformation_rules_csv=None, config_csv=None, enabled_by_default=False):
        """Export available derivation rules to config CSV."""
        from pybirdai.process_steps.database_setup.derivation_pipeline import export_available_rules_to_config
        kwargs = {'enabled_by_default': enabled_by_default}
        if transformation_rules_csv:
            kwargs['transformation_rules_csv'] = transformation_rules_csv
        if config_csv:
            kwargs['config_csv'] = config_csv
        return export_available_rules_to_config(**kwargs)


# =============================================================================
# Derived Fields Merger
# =============================================================================
from pybirdai.process_steps.database_setup.derived_fields_merger import (
    merge_all_derived_fields_into_model,
    merge_derived_fields_into_original_model,
    extract_classes_with_lineage_properties,
    generate_ast_output,
    load_enabled_fields_from_config,
    collect_all_derivation_files,
    extract_derived_classes_from_files,
    check_if_file_already_modified,
)

# =============================================================================
# Derivation Pipeline
# =============================================================================
from pybirdai.process_steps.database_setup.derivation_pipeline import (
    run_download_transformation_rules,
    run_generate_derivation_files,
    run_list_available_rules,
    run_merge_derived_fields,
    run_full_derivation_pipeline,
    export_available_rules_to_config,
)

# =============================================================================
# Migration Generator
# =============================================================================
from pybirdai.process_steps.database_setup.migration_generator import (
    AdvancedMigrationGenerator,
    ModelParser,
    FieldInfo,
    ModelInfo,
    generate_migration_from_file,
    generate_migration_from_files,
    generate_migration_from_directory,
)

# =============================================================================
# Artifact Fetcher
# =============================================================================
from pybirdai.process_steps.database_setup.artifact_fetcher import (
    ArtifactFetcher,
    PreconfiguredDatabaseFetcher,
    Artifact,
    WorkflowRun,
)

# =============================================================================
# Public API
# =============================================================================
__all__ = [
    # Main AppConfig entry point
    "RunApplicationSetup",
    # Derived fields merger
    "merge_all_derived_fields_into_model",
    "merge_derived_fields_into_original_model",
    "extract_classes_with_lineage_properties",
    "generate_ast_output",
    "load_enabled_fields_from_config",
    "collect_all_derivation_files",
    "extract_derived_classes_from_files",
    "check_if_file_already_modified",
    # Derivation pipeline
    "run_download_transformation_rules",
    "run_generate_derivation_files",
    "run_list_available_rules",
    "run_merge_derived_fields",
    "run_full_derivation_pipeline",
    "export_available_rules_to_config",
    # Migration generator
    "AdvancedMigrationGenerator",
    "ModelParser",
    "FieldInfo",
    "ModelInfo",
    "generate_migration_from_file",
    "generate_migration_from_files",
    "generate_migration_from_directory",
    # Artifact fetcher
    "ArtifactFetcher",
    "PreconfiguredDatabaseFetcher",
    "Artifact",
    "WorkflowRun",
]


if __name__ == '__main__':
    import django
    django.setup()
    RunApplicationSetup('pybirdai', 'birds_nest').ready()
