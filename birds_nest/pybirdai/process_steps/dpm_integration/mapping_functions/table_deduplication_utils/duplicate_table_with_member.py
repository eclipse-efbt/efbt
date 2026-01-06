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

"""Duplicate a table row by appending Z-axis member to ID."""


def duplicate_table_with_member(table_row, member_id, member_name):
    """
    Duplicate a table row by appending the Z-axis member ID to its ID and CODE,
    and updating the NAME with human-readable member information.

    Args:
        table_row: Series representing a single table
        member_id: The Z-axis member ID to append (e.g., "EBA_CU_USD")
        member_name: The Z-axis member name for display (e.g., "United States Dollar")

    Returns:
        Series with modified TABLE_ID, CODE, and NAME
    """
    new_row = table_row.copy()
    original_id = str(new_row['TABLE_ID'])
    new_row['TABLE_ID'] = f"{original_id}__{member_id}"

    # Update CODE field to reflect the member ID
    if 'CODE' in new_row:
        original_code = str(new_row['CODE'])
        new_row['CODE'] = f"{original_code}__{member_id}"

    # Update NAME field with human-readable format
    if 'NAME' in new_row:
        original_name = str(new_row['NAME'])
        new_row['NAME'] = f"{original_name} - Z axis : {member_name}"

    return new_row
