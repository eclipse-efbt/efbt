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
Shared constants for ANCRDT transformation processes.

This module provides constants that are used across multiple ANCRDT
transformation modules, ensuring consistency and reducing duplication.
"""

# Domain to Python type mapping
# Maps BIRD domain types to Python type annotations
DOMAIN_TYPE_MAP = {
    'String': 'str',
    'Integer': 'int',
    'Date': 'datetime',
    'Float': 'float',
    'Boolean': 'bool',
}

# Default type for unmapped domains
DEFAULT_DOMAIN_TYPE = 'str'

# ANCRDT table name prefix
ANCRDT_TABLE_PREFIX = 'ANCRDT_'

# Framework identifier for ANCRDT cubes
ANCRDT_FRAMEWORK_ID = 'ANCRDT'

# Test data identifier for fixtures
TEST_ID_DEFAULT = '1'

# File extensions
GENERATED_FILE_SUFFIX = '.generated'
BACKUP_FILE_SUFFIX = '.backup'

# Table suffixes used in class generation
TABLE_CLASS_SUFFIX = '_Table'
UNION_TABLE_SUFFIX = '_UnionTable'
FILTERED_SUFFIX = '_filtered'
AGGREGATED_SUFFIX = '_filtered_and_aggregated'

# Common field names to skip in processing
SKIP_FIELDS = ['NEVS']  # NEVS field is typically skipped in cube structure iteration


def get_python_type(domain):
    """
    Get Python type annotation for a given domain.

    Args:
        domain (str): Domain type from BIRD metadata

    Returns:
        str: Python type annotation (e.g., 'str', 'int', 'datetime')

    Example:
        >>> get_python_type('String')
        'str'
        >>> get_python_type('Unknown')
        'str'
    """
    return DOMAIN_TYPE_MAP.get(domain, DEFAULT_DOMAIN_TYPE)
