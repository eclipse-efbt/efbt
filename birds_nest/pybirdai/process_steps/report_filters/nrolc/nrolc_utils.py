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
Utility functions for Non-Reference Output Layer Creator (NROLC).
Pure functions with no dependencies on Django models or classes.
"""


def extract_framework_from_table_id(table_id):
    """
    Extract framework identifier from table ID.
    Table IDs have format: FRAMEWORK_VERSION_TABLECODE
    e.g., EBA_FINREP_3.0.0_F01.01 -> EBA_FINREP

    Args:
        table_id: Table identifier string

    Returns:
        str: Framework identifier (e.g., 'EBA_FINREP')
    """
    parts = table_id.split('_')[:2]
    return '_'.join(parts)


def extract_version_from_table_id(table_id):
    """
    Extract version from table ID.
    e.g., EBA_FINREP_3.0.0_F01.01 -> 3.0.0

    Args:
        table_id: Table identifier string

    Returns:
        str: Version string or None if not found
    """
    parts = table_id.split('_')
    for part in parts:
        if '.' in part and any(c.isdigit() for c in part):
            return part
    return None


def generate_combination_id(cube_id, timestamp, counter):
    """
    Generate unique combination ID.

    Args:
        cube_id: Cube identifier
        timestamp: Timestamp string (format: YYYYMMDDHHMMSS)
        counter: Integer counter

    Returns:
        str: Unique combination ID (e.g., 'CUBE_ID_COMB_20250131_0001')
    """
    return f"{cube_id}_COMB_{timestamp}_{counter:04d}"


def generate_subdomain_id(variable_id, cube_structure_id):
    """
    Generate unique subdomain ID for output layers.

    Args:
        variable_id: Variable identifier
        cube_structure_id: Cube structure identifier

    Returns:
        str: Unique subdomain ID (e.g., 'VAR_ID_OUTPUT_SD_CUBE_STRUCTURE_ID')
    """
    return f"{variable_id}_OUTPUT_SD_{cube_structure_id}"


def generate_csi_code(cube_structure_id, variable_id):
    """
    Generate cube structure item code.

    Args:
        cube_structure_id: Cube structure identifier
        variable_id: Variable identifier

    Returns:
        str: CSI code (e.g., 'CUBE_STRUCTURE_ID__VARIABLE_ID')
    """
    return f"{cube_structure_id}__{variable_id}"
