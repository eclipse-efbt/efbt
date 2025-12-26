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

# Module path templates
FILTER_CODE_MODULE = 'pybirdai.process_steps.filter_code'
GENERATED_FILTERS_MODULE = 'results.generated_python_filters'


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

    Tries to import in this order:
    1. Framework-specific module (e.g., finrep_report_cells)
    2. DPM framework-specific module (e.g., dpm_finrep_report_cells)
    3. Generic report_cells (if fallback_to_generic is True)

    Args:
        framework: The framework name (e.g., 'FINREP', 'COREP')
        fallback_to_generic: Whether to fall back to generic report_cells if
                            framework-specific module not found

    Returns:
        The imported module

    Raises:
        ImportError: If no suitable module can be found
    """
    framework_lower = framework.lower().replace('_ref', '')
    modules_to_try = [
        f'{FILTER_CODE_MODULE}.{framework_lower}_report_cells',
        f'{FILTER_CODE_MODULE}.dpm_{framework_lower}_report_cells',
    ]

    if fallback_to_generic:
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

    Args:
        framework: The framework name
        module_type: Type of module ('report_cells', 'output_tables', 'logic')

    Returns:
        The expected module path string
    """
    framework_lower = framework.lower().replace('_ref', '')

    if module_type == 'report_cells':
        return f'{FILTER_CODE_MODULE}.{framework_lower}_report_cells'
    elif module_type == 'output_tables':
        return f'{FILTER_CODE_MODULE}.{framework_lower}_output_tables'
    elif module_type == 'logic':
        return f'{FILTER_CODE_MODULE}.{framework_lower}_logic'
    else:
        raise ValueError(f"Unknown module_type: {module_type}")


def import_output_tables(framework: str = 'FINREP', fallback_to_generic: bool = True) -> Any:
    """
    Import the output_tables module for a specific framework.

    Similar to import_report_cells but for output_tables modules.

    Args:
        framework: The framework name
        fallback_to_generic: Whether to fall back to generic output_tables

    Returns:
        The imported module
    """
    framework_lower = framework.lower().replace('_ref', '')
    modules_to_try = [
        f'{FILTER_CODE_MODULE}.{framework_lower}_output_tables',
        f'{FILTER_CODE_MODULE}.dpm_{framework_lower}_output_tables',
    ]

    if fallback_to_generic:
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
