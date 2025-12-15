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
Member Link Derivation Generator Package.

This package provides tools for generating Python derivation code from
BIRD/ANCRDT member link CSV files. It implements a modular pipeline that:

1. Parses member_link.csv files from ECB BIRD exports
2. Filters entries by target cube and variable
3. Resolves conflicts using specificity-based priority
4. Generates Python @property methods with @lineage decorators

Usage:
    from pybirdai.process_steps.derivation_generation.member_link_derivation import (
        run_derivation_generation,
        preview_derivation,
    )

    # Generate derivation code
    output_path = run_derivation_generation(
        csv_path="results/ancrdt_csv/member_link.csv",
        target_cube="ANCRDT_INSTRMNT_C",
        target_variable="TYP_INSTRMNT",
        output_variable="TYP_INSTRMNT_ANCRDT"
    )

    # Preview mappings without generating code
    mappings = preview_derivation(
        csv_path="results/ancrdt_csv/member_link.csv",
        target_cube="ANCRDT_INSTRMNT_C",
        target_variable="TYP_INSTRMNT"
    )

CLI Usage:
    python -m pybirdai.process_steps.derivation_generation.member_link_derivation.orchestrator \\
        --csv-path results/ancrdt_csv/member_link.csv \\
        --target-cube ANCRDT_INSTRMNT_C \\
        --target-variable TYP_INSTRMNT \\
        --output-variable TYP_INSTRMNT_ANCRDT \\
        --verbose
"""

from .generator import DerivationCodeGenerator, generate_standalone_derivation
from .dataclasses import DerivationConfig, DerivationMapping, MemberLinkEntry
from .model_introspector import ModelIntrospector, create_introspector
from .orchestrator import (
    preview_derivation,
    print_mapping_summary,
    run_derivation_generation,
    run_derivation_generation_for_all_variables,
)
from .parser import (
    MemberLinkParser,
    filter_active_only,
    filter_by_cube,
    filter_by_source_variable,
    filter_by_target_variable,
    get_unique_source_tables,
    get_unique_source_variables,
)
from .resolver import SpecificityResolver

__all__ = [
    # Main entry points
    "run_derivation_generation",
    "run_derivation_generation_for_all_variables",
    "preview_derivation",
    "print_mapping_summary",
    # Dataclasses
    "MemberLinkEntry",
    "DerivationMapping",
    "DerivationConfig",
    # Parser
    "MemberLinkParser",
    "filter_by_cube",
    "filter_by_target_variable",
    "filter_by_source_variable",
    "filter_active_only",
    "get_unique_source_variables",
    "get_unique_source_tables",
    # Resolver
    "SpecificityResolver",
    # Generator
    "DerivationCodeGenerator",
    "generate_standalone_derivation",
    # Model introspector
    "ModelIntrospector",
    "create_introspector",
]
