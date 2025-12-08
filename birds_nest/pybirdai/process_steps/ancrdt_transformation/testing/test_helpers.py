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
Test helper functions for ANCRDT comprehensive test suite.

This module provides reusable assertion functions that eliminate code duplication
across ANCRDT test files. It includes utilities for validating row fields,
comparing multiple rows, and asserting expected row counts.

Also includes configuration loading utilities for accessing test expectations
from configuration_file_tests.json.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional


def assert_row_field(
    row,
    field_name: str,
    expected_value: Any,
    row_index: int = 0
):
    """
    Validate a single field on a row object or dictionary.

    This helper supports both row objects with callable field methods
    and plain dictionaries (used for aggregated results).

    Args:
        row: Row object with callable field methods, or dictionary
        field_name (str): Name of the field to validate (e.g., 'INSTRMNT_ID')
        expected_value: Expected value for the field
        row_index (int): Row index for error messages. Defaults to 0.

    Raises:
        AssertionError: If the field value doesn't match expected value

    Example:
        >>> assert_row_field(rows[0], 'INSTRMNT_ID', 'INST_001', row_index=0)
        >>> assert_row_field({'INSTRMNT_TYP_PRDCT': '51', 'count': 2}, 'count', 2)
    """
    # Handle both row objects and dictionaries
    if isinstance(row, dict):
        # Row is a dictionary (aggregated result)
        actual = row.get(field_name)
    else:
        # Row is an object with callable field methods
        actual = getattr(row, field_name)()

    # Provide detailed error message
    assert actual == expected_value, (
        f'Row {row_index} {field_name} validation failed: '
        f'expected {repr(expected_value)}, got {repr(actual)}'
    )


def assert_row_fields(
    row,
    field_expectations: Dict[str, Any],
    row_index: int = 0
):
    """
    Validate multiple fields on a row using a dictionary of expectations.

    This is the primary helper function that reduces code duplication.
    Instead of writing multiple individual assertions, you can pass a
    dictionary of field names and expected values.

    Args:
        row: Row object with callable field methods
        field_expectations (dict): Dictionary mapping field names to expected values.
            Example: {'INSTRMNT_ID': 'INST_001', 'INSTRMNT_TYP_PRDCT': '80'}
        row_index (int): Row index for error messages. Defaults to 0.

    Raises:
        AssertionError: If any field value doesn't match expected value

    Example:
        >>> expected = {'INSTRMNT_ID': 'INST_001', 'INSTRMNT_TYP_PRDCT': '80', 'PRPS': '7'}
        >>> assert_row_fields(rows[0], expected, row_index=0)
    """
    for field_name, expected_value in field_expectations.items():
        assert_row_field(row, field_name, expected_value, row_index)


def validate_result_rows(
    result: Dict[str, Any],
    expected_rows_data: List[Dict[str, Any]]
):
    """
    Validate complete result set with expected row data.

    This is the top-level helper that validates both row count and
    individual field values for multiple rows. Supports both row objects
    (non-aggregated results) and dictionaries (aggregated results).

    Args:
        result (dict): Execution result dictionary containing:
            - 'rows': List of row objects or dictionaries
            - other metadata
        expected_rows_data (list): List of dictionaries, each containing
            field expectations for one row.
            Example: [
                {'INSTRMNT_ID': 'INST_001', 'INSTRMNT_TYP_PRDCT': '80'},
                {'INSTRMNT_ID': 'INST_002', 'INSTRMNT_TYP_PRDCT': '51'}
            ]
            For aggregated results: [
                {'INSTRMNT_TYP_PRDCT': '51', 'count': 2},
                {'INSTRMNT_TYP_PRDCT': '80', 'count': 4}
            ]

    Raises:
        AssertionError: If row count doesn't match or any field validation fails

    Example:
        >>> expected = [
        ...     {'INSTRMNT_ID': 'INST_001', 'INSTRMNT_TYP_PRDCT': '80', 'PRPS': '7'},
        ...     {'INSTRMNT_ID': 'INST_002', 'INSTRMNT_TYP_PRDCT': '51', 'PRPS': '8'}
        ... ]
        >>> validate_result_rows(result, expected)
    """
    rows = result['rows']

    # Validate row count
    expected_count = len(expected_rows_data)
    actual_count = len(rows)

    assert actual_count == expected_count, (
        f'Expected {expected_count} rows but got {actual_count}'
    )

    # Validate each row
    for i, expected_data in enumerate(expected_rows_data):
        assert_row_fields(rows[i], expected_data, row_index=i)


def assert_row_count(
    result: Dict[str, Any],
    expected_rows: int
):
    """
    Assert that result contains expected number of rows.

    Simple helper for tests that only care about row count.

    Args:
        result (dict): Execution result dictionary
        expected_rows (int): Expected number of rows

    Raises:
        AssertionError: If row count doesn't match

    Example:
        >>> assert_row_count(result, 0)  # Empty result test
    """
    rows = result['rows']
    actual_count = len(rows)

    assert actual_count == expected_rows, (
        f'Expected {expected_rows} rows but got {actual_count}'
    )


# ============================================================================
# Configuration Loading Functions
# ============================================================================

def get_test_suite_config_path() -> Path:
    """
    Get the path to the configuration_file_tests.json.

    Returns:
        Path: Path to configuration file

    Example:
        >>> config_path = get_test_suite_config_path()
        >>> print(config_path.exists())
        True
    """
    # Navigate from testing module to test suite directory
    # Path: testing/ -> ancrdt_transformation/ -> process_steps/ -> pybirdai/ -> birds_nest/ -> tests/
    return Path(__file__).parent.parent.parent.parent.parent / 'tests' / 'ancrdt-test-suite' / 'configuration_file_tests.json'


def load_test_config() -> Dict[str, Any]:
    """
    Load the test configuration from configuration_file_tests.json.

    Returns:
        dict: Complete configuration dictionary

    Raises:
        FileNotFoundError: If configuration file doesn't exist
        json.JSONDecodeError: If configuration file is invalid JSON

    Example:
        >>> config = load_test_config()
        >>> print(config['test_suite_name'])
        'ANCRDT Comprehensive Test Suite'
    """
    config_path = get_test_suite_config_path()

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found at: {config_path}"
        )

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_test_by_scenario(scenario: str) -> Optional[Dict[str, Any]]:
    """
    Get test configuration by scenario name.

    Args:
        scenario (str): Scenario name (e.g., '01_happy_path_single_instrument')

    Returns:
        dict or None: Test configuration if found, None otherwise

    Example:
        >>> test = get_test_by_scenario('01_happy_path_single_instrument')
        >>> print(test['expected_rows'])
        1
    """
    config = load_test_config()

    for test in config['tests']:
        if test['scenario'] == scenario:
            return test

    return None


def get_validation_data(scenario: str) -> List[Dict[str, Any]]:
    """
    Get validation data (expected field values) for a scenario.

    Args:
        scenario (str): Scenario name

    Returns:
        list: List of validation dictionaries, one per expected row

    Raises:
        ValueError: If scenario not found in configuration

    Example:
        >>> validations = get_validation_data('01_happy_path_single_instrument')
        >>> print(validations[0]['INSTRMNT_ID'])
        'INST_001'
    """
    test = get_test_by_scenario(scenario)

    if not test:
        raise ValueError(f"Test scenario not found: {scenario}")

    # Convert validation_rules from {row_0: {...}, row_1: {...}}
    # to [{...}, {...}] format
    validation_rules = test.get('validation_rules', {})

    # Sort by row number to ensure correct order
    sorted_rows = sorted(validation_rules.keys(), key=lambda x: int(x.split('_')[1]))

    return [validation_rules[row_key] for row_key in sorted_rows]


def get_expected_row_count(scenario: str) -> int:
    """
    Get expected row count for a scenario.

    Args:
        scenario (str): Scenario name

    Returns:
        int: Expected number of rows

    Raises:
        ValueError: If scenario not found in configuration

    Example:
        >>> count = get_expected_row_count('01_happy_path_single_instrument')
        >>> print(count)
        1
    """
    test = get_test_by_scenario(scenario)

    if not test:
        raise ValueError(f"Test scenario not found: {scenario}")

    return test['expected_rows']
