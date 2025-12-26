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
Test suite discovery utilities.

This module provides functions to dynamically discover test suites
by scanning the tests/ directory and matching by test_type field.
"""

import os
import json
import logging

logger = logging.getLogger(__name__)


def discover_test_suites_by_type(test_type, tests_dir='tests'):
    """
    Discover test suites by scanning tests/ directory for matching test_type.

    Args:
        test_type (str): The test type to match (e.g., 'ancrdt', 'finrep')
        tests_dir (str): Base directory to scan for test suites

    Returns:
        list: List of dicts with 'name' and 'config_path' for each matching suite

    Example:
        >>> suites = discover_test_suites_by_type('ancrdt')
        >>> for suite in suites:
        ...     print(f"Found: {suite['name']} at {suite['config_path']}")
    """
    suites = []

    if not os.path.exists(tests_dir):
        logger.warning(f"Tests directory does not exist: {tests_dir}")
        return suites

    for entry in os.listdir(tests_dir):
        suite_path = os.path.join(tests_dir, entry)

        # Skip if not a directory
        if not os.path.isdir(suite_path):
            continue

        # Look for configuration file
        config_file = os.path.join(suite_path, 'configuration_file_tests.json')

        if not os.path.exists(config_file):
            continue

        try:
            with open(config_file, 'r') as f:
                config = json.load(f)

            # Check if this suite matches the requested test_type
            suite_test_type = config.get('test_type', '').lower()

            if suite_test_type == test_type.lower():
                suites.append({
                    'name': entry,
                    'config_path': config_file
                })
                logger.info(f"Discovered {test_type} test suite: {entry}")

        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to read config from {config_file}: {e}")
            continue

    if not suites:
        logger.warning(f"No test suites found with test_type='{test_type}'")

    return suites


def get_ancrdt_test_suite():
    """
    Get the first available ANCRDT test suite.

    Returns:
        tuple: (config_path, suite_name) or (None, None) if not found

    Example:
        >>> config_path, suite_name = get_ancrdt_test_suite()
        >>> if config_path:
        ...     print(f"Found ANCRDT suite: {suite_name}")
    """
    suites = discover_test_suites_by_type('ancrdt')

    if suites:
        return suites[0]['config_path'], suites[0]['name']

    return None, None


def get_finrep_test_suites():
    """
    Get all available FINREP test suites.

    Returns:
        list: List of dicts with 'name' and 'config_path' for each FINREP suite
    """
    return discover_test_suites_by_type('finrep')


# Map framework IDs to test_type values used in configuration_file_tests.json
FRAMEWORK_TO_TEST_TYPE = {
    'ANCRDT': 'ancrdt',
    'FINREP': 'finrep',
    'COREP': 'corep',
    'BIRD_EIL': 'bird_eil',
    'BIRD_ELDM': 'bird_eldm',
}


def get_test_suite_for_framework(framework_id, tests_dir='tests'):
    """
    Get test suite for any framework by framework ID using auto-discovery.

    This function scans the tests/ directory and matches suites by the
    test_type field in configuration_file_tests.json.

    Args:
        framework_id (str): Framework identifier (ANCRDT, FINREP, COREP, etc.)
        tests_dir (str): Base directory to scan for test suites

    Returns:
        tuple: (config_path, suite_name) or (None, None) if not found

    Example:
        >>> config_path, suite_name = get_test_suite_for_framework('ANCRDT')
        >>> if config_path:
        ...     print(f"Found suite: {suite_name} at {config_path}")
    """
    test_type = FRAMEWORK_TO_TEST_TYPE.get(framework_id.upper())

    if not test_type:
        logger.warning(f"Unknown framework: {framework_id}")
        return None, None

    suites = discover_test_suites_by_type(test_type, tests_dir)

    if suites:
        return suites[0]['config_path'], suites[0]['name']

    return None, None


def discover_all_test_suites(tests_dir='tests'):
    """
    Discover all test suites for all known frameworks.

    Returns:
        dict: Dictionary mapping framework_id to discovered suite info,
              or None if no suite found for that framework.

    Example:
        >>> all_suites = discover_all_test_suites()
        >>> for fw, info in all_suites.items():
        ...     if info:
        ...         print(f"{fw}: {info['config_path']}")
    """
    results = {}

    for framework_id in FRAMEWORK_TO_TEST_TYPE.keys():
        config_path, suite_name = get_test_suite_for_framework(framework_id, tests_dir)
        if config_path:
            results[framework_id] = {
                'name': suite_name,
                'config_path': config_path
            }
        else:
            results[framework_id] = None

    return results
