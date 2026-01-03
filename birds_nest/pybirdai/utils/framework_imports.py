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
Framework-aware import utilities for dynamically loading framework-specific modules.

This module provides utilities to dynamically import framework-specific report_cells
and other generated modules. It supports the framework isolation pattern where
different frameworks (FINREP, COREP, AE, etc.) have their own generated code files.

Example usage:
    from pybirdai.utils.framework_imports import import_report_cells, get_cell_class

    # Import a specific framework's report cells
    report_cells = import_report_cells('FINREP')

    # Get a specific cell class
    Cell = get_cell_class('F_01_01_REF_FINREP_3_0_12345_REF', framework='FINREP')
"""

import importlib
import logging
import os
from typing import Optional, Any, List

logger = logging.getLogger(__name__)

# Supported frameworks
SUPPORTED_FRAMEWORKS = [
    'FINREP', 'COREP', 'AE', 'FP', 'SBP', 'REM', 'RES', 'PAY',
    'COVID19', 'IF', 'GSII', 'MREL', 'IMPRAC', 'ESG',
    'IPU', 'PILLAR3', 'IRRBB', 'DORA', 'FC', 'MICA', 'ANCRDT'
]

# Module path templates - new unified structure
# filter_code/{type}/{framework}/filter|joins/
FILTER_CODE_MODULE = 'pybirdai.process_steps.filter_code'
LIB_MODULE = 'pybirdai.process_steps.filter_code.lib'

# Templates type (FINREP, COREP, etc.)
TEMPLATES_BASE_MODULE = 'pybirdai.process_steps.filter_code.templates'
# Datasets type (ANCRDT)
DATASETS_BASE_MODULE = 'pybirdai.process_steps.filter_code.datasets'

# Generated results - new unified structure
GENERATED_PYTHON_MODULE = 'results.generated_python'
# Legacy locations (for backward compatibility)
GENERATED_FILTERS_MODULE = 'results.generated_python_filters'

# Mapping of frameworks to their type
FRAMEWORK_TYPE_MAP = {
    'ANCRDT': 'datasets',
    # All other frameworks are templates
}

def get_framework_type(framework: str) -> str:
    """Get the type (datasets or templates) for a framework."""
    return FRAMEWORK_TYPE_MAP.get(framework.upper(), 'templates')


def get_framework_from_cell_id(cell_id: str) -> Optional[str]:
    """
    Extract the framework from a cell/datapoint ID.

    Cell IDs typically follow the pattern:
    - F_01_01_REF_FINREP_3_0_12345_REF (FINREP)
    - C_07_00_a_COREP_4_0_54321_REF (COREP)
    - DPM_FINREP_F_01_01_cell_123 (DPM FINREP)

    Args:
        cell_id: The cell or datapoint identifier

    Returns:
        The detected framework name, or None if not detected
    """
    cell_id_upper = cell_id.upper()

    # Check for DPM prefix first
    if cell_id_upper.startswith('DPM_'):
        # Extract framework after DPM_
        for fw in SUPPORTED_FRAMEWORKS:
            if f'DPM_{fw}' in cell_id_upper or f'DPM_{fw}_' in cell_id_upper:
                return fw

    # Check for standard framework patterns
    for fw in SUPPORTED_FRAMEWORKS:
        if f'_{fw}_' in cell_id_upper or cell_id_upper.startswith(f'{fw}_'):
            return fw

    # Special case for REF suffix patterns (e.g., FINREP_REF)
    for fw in SUPPORTED_FRAMEWORKS:
        if f'{fw}_REF' in cell_id_upper:
            return fw

    return None


def import_report_cells(framework: str = 'FINREP', fallback_to_generic: bool = True) -> Any:
    """
    Import the report_cells module for a specific framework.

    Uses the new unified structure:
    - templates/{FRAMEWORK}/filter/{framework}.py for FINREP/COREP
    - datasets/{FRAMEWORK}/filter/{framework}.py for ANCRDT

    Args:
        framework: The framework name (e.g., 'FINREP', 'COREP', 'ANCRDT')
        fallback_to_generic: Whether to fall back to generic report_cells if
                            framework-specific module not found

    Returns:
        The imported module

    Raises:
        ImportError: If no suitable module can be found
    """
    framework_lower = framework.lower().replace('_ref', '')
    framework_upper = framework.upper().replace('_REF', '')
    fw_type = get_framework_type(framework)

    if fw_type == 'datasets':
        base_module = DATASETS_BASE_MODULE
    else:
        base_module = TEMPLATES_BASE_MODULE

    modules_to_try = [
        # New structure: {type}/{FRAMEWORK}/filter/{framework}.py
        f'{base_module}.{framework_upper}.filter.{framework_lower}',
        # Legacy per-framework structure: filter_code/{framework}_report_cells.py
        f'{FILTER_CODE_MODULE}.{framework_lower}_report_cells',
        f'{FILTER_CODE_MODULE}.dpm_{framework_lower}_report_cells',
        # Legacy flat structure at filter_code root (old repos)
        f'{FILTER_CODE_MODULE}.{framework_lower}',
    ]

    if fallback_to_generic:
        # Legacy fallback - old monolithic report_cells.py (deprecated)
        modules_to_try.append(f'{FILTER_CODE_MODULE}.report_cells')

    last_error = None
    for module_name in modules_to_try:
        try:
            module = importlib.import_module(module_name)
            logger.debug(f"Successfully imported {module_name}")
            return module
        except ImportError as e:
            last_error = e
            logger.debug(f"Could not import {module_name}: {e}")
            continue

    if last_error:
        raise ImportError(
            f"Could not import report_cells for framework '{framework}'. "
            f"Tried: {', '.join(modules_to_try)}. Last error: {last_error}"
        )


def get_cell_class(cell_id: str, framework: Optional[str] = None) -> Any:
    """
    Get a cell class by its ID, automatically detecting or using the specified framework.

    Args:
        cell_id: The cell identifier (e.g., 'F_01_01_REF_FINREP_3_0_12345_REF')
        framework: Optional framework override. If not provided, attempts to
                  detect from cell_id

    Returns:
        The cell class

    Raises:
        ImportError: If the module cannot be imported
        AttributeError: If the cell class is not found in the module
    """
    if framework is None:
        framework = get_framework_from_cell_id(cell_id)
        if framework is None:
            framework = 'FINREP'  # Default fallback
            logger.warning(
                f"Could not detect framework from cell_id '{cell_id}', "
                f"defaulting to {framework}"
            )

    # Import the report_cells module
    report_cells = import_report_cells(framework)

    # Get the cell class
    class_name = f'Cell_{cell_id}'
    if not hasattr(report_cells, class_name):
        raise AttributeError(
            f"Cell class '{class_name}' not found in report_cells module for "
            f"framework '{framework}'"
        )

    return getattr(report_cells, class_name)


def import_all_cell_classes(framework: str = 'FINREP') -> dict:
    """
    Import all cell classes from a framework's report_cells module.

    This is useful for dynamic execution where all cells need to be available
    in globals().

    Args:
        framework: The framework name

    Returns:
        A dictionary mapping cell class names to cell classes
    """
    report_cells = import_report_cells(framework)

    cell_classes = {}
    for name in dir(report_cells):
        if name.startswith('Cell_'):
            cell_classes[name] = getattr(report_cells, name)

    logger.debug(f"Imported {len(cell_classes)} cell classes for framework {framework}")
    return cell_classes


def get_available_frameworks() -> List[str]:
    """
    Get a list of frameworks that have generated report_cells modules available.

    Returns:
        List of framework names with available report_cells
    """
    available = []

    for framework in SUPPORTED_FRAMEWORKS:
        try:
            import_report_cells(framework, fallback_to_generic=False)
            available.append(framework)
        except ImportError:
            continue

    return available


def get_module_path_for_framework(framework: str, module_type: str = 'report_cells') -> str:
    """
    Get the expected module path for a framework's generated code.

    Uses the new unified structure:
    - {type}/{FRAMEWORK}/filter/ for report cells and output tables
    - {type}/{FRAMEWORK}/joins/ for logic files

    Args:
        framework: The framework name
        module_type: Type of module ('report_cells', 'filter', 'joins', 'logic', 'lib')

    Returns:
        The expected module path string
    """
    framework_lower = framework.lower().replace('_ref', '')
    framework_upper = framework.upper().replace('_REF', '')
    fw_type = get_framework_type(framework)

    if fw_type == 'datasets':
        base_module = DATASETS_BASE_MODULE
    else:
        base_module = TEMPLATES_BASE_MODULE

    if module_type in ('report_cells', 'filter', 'output_tables', 'report_datasets'):
        # New structure: {type}/{FRAMEWORK}/filter/{framework}.py
        return f'{base_module}.{framework_upper}.filter.{framework_lower}'
    elif module_type in ('joins', 'logic', 'logic_templates', 'logic_datasets'):
        # New structure: {type}/{FRAMEWORK}/joins/{framework}_logic.py
        return f'{base_module}.{framework_upper}.joins.{framework_lower}_logic'
    elif module_type == 'lib':
        return LIB_MODULE
    else:
        raise ValueError(f"Unknown module_type: {module_type}")


def import_output_tables(framework: str = 'FINREP', fallback_to_generic: bool = True) -> Any:
    """
    Import the output_tables/report_datasets module for a specific framework.

    Uses the new unified structure:
    - templates/{FRAMEWORK}/filter/{framework}.py for FINREP/COREP
    - datasets/{FRAMEWORK}/filter/{framework}.py for ANCRDT

    Args:
        framework: The framework name
        fallback_to_generic: Whether to fall back to generic output_tables

    Returns:
        The imported module
    """
    framework_lower = framework.lower().replace('_ref', '')
    framework_upper = framework.upper().replace('_REF', '')
    fw_type = get_framework_type(framework)

    if fw_type == 'datasets':
        base_module = DATASETS_BASE_MODULE
    else:
        base_module = TEMPLATES_BASE_MODULE

    modules_to_try = [
        # New structure: {type}/{FRAMEWORK}/filter/{framework}.py
        f'{base_module}.{framework_upper}.filter.{framework_lower}',
        # Legacy per-framework structure: filter_code/{framework}_output_tables.py
        f'{FILTER_CODE_MODULE}.{framework_lower}_output_tables',
        f'{FILTER_CODE_MODULE}.dpm_{framework_lower}_output_tables',
        # Legacy flat structure at filter_code root (old repos)
        f'{FILTER_CODE_MODULE}.{framework_lower}',
    ]

    if fallback_to_generic:
        # Legacy fallback (deprecated)
        modules_to_try.append(f'{FILTER_CODE_MODULE}.output_tables')

    last_error = None
    for module_name in modules_to_try:
        try:
            module = importlib.import_module(module_name)
            logger.debug(f"Successfully imported {module_name}")
            return module
        except ImportError as e:
            last_error = e
            logger.debug(f"Could not import {module_name}: {e}")
            continue

    if last_error:
        raise ImportError(
            f"Could not import output_tables for framework '{framework}'. "
            f"Tried: {', '.join(modules_to_try)}. Last error: {last_error}"
        )


def import_logic_module(framework: str, module_type: str = 'auto') -> Any:
    """
    Import a logic module for a specific framework.

    Uses the new unified structure:
    - templates/{FRAMEWORK}/joins/ for FINREP/COREP logic
    - datasets/{FRAMEWORK}/joins/ for ANCRDT logic

    Args:
        framework: The framework name (e.g., 'FINREP', 'COREP', 'ANCRDT')
        module_type: 'auto' to detect from framework, 'templates', or 'datasets'

    Returns:
        The imported module
    """
    framework_lower = framework.lower().replace('_ref', '')
    framework_upper = framework.upper().replace('_REF', '')

    # Auto-detect type if not specified
    if module_type == 'auto':
        fw_type = get_framework_type(framework)
    else:
        fw_type = module_type

    if fw_type == 'datasets':
        base_module = DATASETS_BASE_MODULE
    else:
        base_module = TEMPLATES_BASE_MODULE

    # Try the new structure first
    modules_to_try = [
        # New structure: {type}/{FRAMEWORK}/joins/{framework}_logic.py
        f'{base_module}.{framework_upper}.joins.{framework_lower}_logic',
        # Fallback for other naming patterns
        f'{TEMPLATES_BASE_MODULE}.{framework_upper}.joins.{framework_lower}_logic',
        f'{DATASETS_BASE_MODULE}.{framework_upper}.joins.{framework_lower}_logic',
    ]

    last_error = None
    for module_name in modules_to_try:
        try:
            module = importlib.import_module(module_name)
            logger.debug(f"Successfully imported {module_name}")
            return module
        except ImportError as e:
            last_error = e
            logger.debug(f"Could not import {module_name}: {e}")
            continue

    if last_error:
        raise ImportError(
            f"Could not import logic module for framework '{framework}'. "
            f"Tried: {', '.join(modules_to_try)}. Last error: {last_error}"
        )


def import_tracking_wrapper() -> Any:
    """
    Import the automatic_tracking_wrapper module from lib.

    Returns:
        The imported module
    """
    return importlib.import_module(f'{LIB_MODULE}.automatic_tracking_wrapper')
