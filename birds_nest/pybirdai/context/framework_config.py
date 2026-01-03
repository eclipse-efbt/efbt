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
Centralized framework configuration for pipeline and code type mapping.

This module provides a single source of truth for:
- Framework to pipeline mapping (which pipeline processes which frameworks)
- Pipeline to code type mapping (datasets vs templates)

Usage:
    from pybirdai.context.framework_config import (
        get_pipeline_for_framework,
        get_code_type_for_pipeline,
        get_code_type_for_framework,
    )

    pipeline = get_pipeline_for_framework('FINREP')  # Returns 'dpm'
    code_type = get_code_type_for_pipeline('ancrdt')  # Returns 'datasets'
"""

from typing import Optional

# =============================================================================
# Framework to Pipeline Mapping
# =============================================================================
# Defines which pipeline processes each framework.
# Pipelines: 'main' (BIRD), 'ancrdt' (AnaCredit), 'dpm' (DPM/EBA templates)

FRAMEWORK_TO_PIPELINE = {
    'FINREP_REF': 'main',
    'FINREP': 'main',

    # ANCRDT pipeline - AnaCredit dataset processing
    'ANCRDT': 'ancrdt',
    'ANACREDIT': 'ancrdt',

    # DPM pipeline - EBA Data Point Model templates (reporting frameworks)
    'COREP': 'dpm',
    'AE': 'dpm',
    'FP': 'dpm',
    'SBP': 'dpm',
    'REM': 'dpm',
    'RES': 'dpm',
    'GSII': 'dpm',
    'MREL': 'dpm',
    'PAY': 'dpm',
    'COVID19': 'dpm',
    'IF': 'dpm',
    'IMPRAC': 'dpm',
    'ESG': 'dpm',
    'IPU': 'dpm',
    'PILLAR3': 'dpm',
    'IRRBB': 'dpm',
    'DORA': 'dpm',
    'FC': 'dpm',
    'MICA': 'dpm',
}

# =============================================================================
# Pipeline to Code Type Mapping
# =============================================================================
# Defines the code structure type for each pipeline.
# - 'datasets': Used for ANCRDT (filter_code/datasets/{FRAMEWORK}/)
# - 'templates': Used for DPM/EBA templates (filter_code/templates/{FRAMEWORK}/)

PIPELINE_TO_CODE_TYPE = {
    'main': 'templates',      # BIRD uses templates structure
    'ancrdt': 'datasets',     # ANCRDT uses datasets structure
    'dpm': 'templates',       # DPM uses templates structure
}

# Default values when framework/pipeline is unknown
DEFAULT_PIPELINE = 'dpm'
DEFAULT_CODE_TYPE = 'templates'


# =============================================================================
# Helper Functions
# =============================================================================

def normalize_framework_name(framework: str) -> str:
    """
    Normalize a framework name by removing common prefixes/suffixes.

    Handles variants like:
    - EBA_COREP -> COREP
    - COREP_REF -> COREP
    - EBA_FINREP_REF -> FINREP

    Args:
        framework: Framework name with possible prefixes/suffixes

    Returns:
        Normalized framework name (uppercase)
    """
    if not framework:
        return ""

    normalized = framework.upper()
    # Remove common prefixes
    if normalized.startswith('EBA_'):
        normalized = normalized[4:]
    # Remove common suffixes
    if normalized.endswith('_REF'):
        normalized = normalized[:-4]
    return normalized


def get_pipeline_for_framework(framework: str) -> str:
    """
    Get the pipeline name for a given framework.

    Args:
        framework: Framework name (e.g., 'FINREP', 'ANCRDT', 'COREP', 'EBA_COREP', 'COREP_REF')

    Returns:
        Pipeline name: 'main', 'ancrdt', or 'dpm'
    """
    if not framework:
        return DEFAULT_PIPELINE

    framework_normalized = normalize_framework_name(framework)
    return FRAMEWORK_TO_PIPELINE.get(framework_normalized, DEFAULT_PIPELINE)


def get_pipeline_for_frameworks(frameworks: list) -> str:
    """
    Get the pipeline name for a list of frameworks.

    If multiple pipelines would be needed, returns the first detected pipeline.
    For mixed framework lists, priority: ancrdt > dpm > main

    Args:
        frameworks: List of framework names

    Returns:
        Pipeline name: 'main', 'ancrdt', or 'dpm'
    """
    if not frameworks:
        return DEFAULT_PIPELINE

    pipelines = set()
    for fw in frameworks:
        pipelines.add(get_pipeline_for_framework(fw))

    # Priority order: ancrdt (most specific), then dpm, then main
    if 'ancrdt' in pipelines:
        return 'ancrdt'
    if 'dpm' in pipelines:
        return 'dpm'
    if 'main' in pipelines:
        return 'main'

    return DEFAULT_PIPELINE


def get_code_type_for_pipeline(pipeline: str) -> str:
    """
    Get the code type (datasets or templates) for a pipeline.

    Args:
        pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')

    Returns:
        Code type: 'datasets' or 'templates'
    """
    if not pipeline:
        return DEFAULT_CODE_TYPE

    return PIPELINE_TO_CODE_TYPE.get(pipeline.lower(), DEFAULT_CODE_TYPE)


def get_code_type_for_framework(framework: str) -> str:
    """
    Get the code type (datasets or templates) for a framework.

    Convenience function that combines get_pipeline_for_framework
    and get_code_type_for_pipeline.

    Args:
        framework: Framework name (e.g., 'FINREP', 'ANCRDT')

    Returns:
        Code type: 'datasets' or 'templates'
    """
    pipeline = get_pipeline_for_framework(framework)
    return get_code_type_for_pipeline(pipeline)


def detect_pipeline_from_frameworks(frameworks: list) -> Optional[str]:
    """
    Detect pipeline from a list of framework names.

    This is an alias for get_pipeline_for_frameworks for backward compatibility.

    Args:
        frameworks: List of framework names

    Returns:
        Pipeline name or None if frameworks list is empty
    """
    if not frameworks:
        return None
    return get_pipeline_for_frameworks(frameworks)
