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
Database setup module containing all business logic for:
- Automode database setup orchestration
- Derived fields merging
- Derivation pipeline (generation from ECB rules)
- Migration generation
- Artifact fetching from GitHub
"""

from .automode_orchestrator import (
    run_automode_setup,
    run_post_setup,
    run_migrations,
)
from .derived_fields_merger import (
    merge_all_derived_fields_into_model,
    merge_derived_fields_into_original_model,
    extract_classes_with_lineage_properties,
    generate_ast_output,
    load_enabled_fields_from_config,
    collect_all_derivation_files,
    extract_derived_classes_from_files,
    check_if_file_already_modified,
)
from .derivation_pipeline import (
    run_download_transformation_rules,
    run_generate_derivation_files,
    run_list_available_rules,
    run_merge_derived_fields,
    run_full_derivation_pipeline,
    export_available_rules_to_config,
)
from .migration_generator import (
    AdvancedMigrationGenerator,
    ModelParser,
    FieldInfo,
    ModelInfo,
    generate_migration_from_file,
    generate_migration_from_files,
    generate_migration_from_directory,
)
from .artifact_fetcher import (
    ArtifactFetcher,
    PreconfiguredDatabaseFetcher,
    Artifact,
    WorkflowRun,
)

__all__ = [
    # Automode orchestrator functions
    "run_automode_setup",
    "run_post_setup",
    "run_migrations",
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
