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

import csv
import io
from collections import defaultdict
from pybirdai.models.bird_meta_data_model import (
    MAPPING_DEFINITION,
    MEMBER_MAPPING_ITEM,
    VARIABLE_MAPPING_ITEM,
)


class ExportMappingData:
    """
    Exports an existing mapping definition to business-friendly CSV format.

    This class converts database mapping records into a simple columnar format
    where each row represents one complete mapping rule with source and target
    variables and members.
    """

    @staticmethod
    def handle(mapping_id):
        """
        Export an existing mapping definition to CSV format.

        Args:
            mapping_id: The ID of the MAPPING_DEFINITION to export

        Returns:
            str: CSV content as string

        Raises:
            ValueError: If mapping_id does not exist
        """
        try:
            mapping_def = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)
        except MAPPING_DEFINITION.DoesNotExist:
            raise ValueError(f"Mapping definition with ID '{mapping_id}' not found")

        # Get variable mapping items
        variable_items = []
        if mapping_def.variable_mapping_id:
            variable_items = list(
                VARIABLE_MAPPING_ITEM.objects.filter(
                    variable_mapping_id=mapping_def.variable_mapping_id
                ).select_related('variable_id')
            )

        # Get member mapping items (grouped by row)
        member_items_by_row = defaultdict(list)
        if mapping_def.member_mapping_id:
            member_items = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping_def.member_mapping_id
            ).select_related('variable_id', 'member_id', 'member_hierarchy').order_by('member_mapping_row')

            for item in member_items:
                row_key = item.member_mapping_row or '1'
                member_items_by_row[row_key].append(item)

        # Organize data by rows
        rows_data = ExportMappingData._organize_data_by_rows(
            variable_items, member_items_by_row
        )

        # Determine column headers based on max counts
        headers = ExportMappingData._determine_headers(rows_data)

        # Generate CSV
        return ExportMappingData._generate_csv(headers, rows_data)

    @staticmethod
    def _organize_data_by_rows(variable_items, member_items_by_row):
        """
        Organize variable and member items into rows for CSV export.

        Args:
            variable_items: List of VARIABLE_MAPPING_ITEM objects
            member_items_by_row: Dict of member items grouped by row number

        Returns:
            list: List of dicts, each representing one row of data
        """
        rows_data = []

        # Separate source and target variables
        source_vars = [item for item in variable_items if item.is_source == 'true']
        target_vars = [item for item in variable_items if item.is_source != 'true']

        # If we have member mapping rows, create one CSV row per member mapping row
        if member_items_by_row:
            for row_num, member_items in sorted(member_items_by_row.items()):
                row_data = {
                    'source_variables': [],
                    'target_variables': [],
                    'source_members': [],
                    'target_members': [],
                    'member_hierarchy': None,
                    'valid_from': None,
                    'valid_to': None
                }

                # Add variables (same for all rows in this case)
                row_data['source_variables'] = [
                    item.variable_id.variable_id if item.variable_id else ''
                    for item in source_vars
                ]
                row_data['target_variables'] = [
                    item.variable_id.variable_id if item.variable_id else ''
                    for item in target_vars
                ]

                # Add members for this specific row
                for item in member_items:
                    member_id_str = item.member_id.member_id if item.member_id else ''

                    if item.is_source == 'true':
                        row_data['source_members'].append(member_id_str)
                    else:
                        row_data['target_members'].append(member_id_str)

                    # Store member hierarchy and dates (use first non-null values)
                    if item.member_hierarchy and not row_data['member_hierarchy']:
                        row_data['member_hierarchy'] = item.member_hierarchy.member_hierarchy_id
                    if item.valid_from and not row_data['valid_from']:
                        row_data['valid_from'] = item.valid_from.strftime('%Y-%m-%d') if item.valid_from else ''
                    if item.valid_to and not row_data['valid_to']:
                        row_data['valid_to'] = item.valid_to.strftime('%Y-%m-%d') if item.valid_to else ''

                rows_data.append(row_data)
        else:
            # No member mappings, just export variables
            row_data = {
                'source_variables': [
                    item.variable_id.variable_id if item.variable_id else ''
                    for item in source_vars
                ],
                'target_variables': [
                    item.variable_id.variable_id if item.variable_id else ''
                    for item in target_vars
                ],
                'source_members': [],
                'target_members': [],
                'member_hierarchy': None,
                'valid_from': None,
                'valid_to': None
            }
            rows_data.append(row_data)

        return rows_data

    @staticmethod
    def _determine_headers(rows_data):
        """
        Determine CSV column headers based on max counts of variables/members.

        Args:
            rows_data: List of row data dicts

        Returns:
            list: List of column header strings
        """
        max_source_vars = max((len(row['source_variables']) for row in rows_data), default=0)
        max_target_vars = max((len(row['target_variables']) for row in rows_data), default=0)
        max_source_members = max((len(row['source_members']) for row in rows_data), default=0)
        max_target_members = max((len(row['target_members']) for row in rows_data), default=0)

        headers = []

        # Add source variable columns
        for i in range(1, max_source_vars + 1):
            headers.append(f'SOURCE_VARIABLE_{i}')

        # Add target variable columns
        for i in range(1, max_target_vars + 1):
            headers.append(f'TARGET_VARIABLE_{i}')

        # Add source member columns
        for i in range(1, max_source_members + 1):
            headers.append(f'SOURCE_MEMBER_{i}')

        # Add target member columns
        for i in range(1, max_target_members + 1):
            headers.append(f'TARGET_MEMBER_{i}')

        # Add optional columns
        headers.extend(['MEMBER_HIERARCHY', 'VALID_FROM', 'VALID_TO'])

        return headers

    @staticmethod
    def _generate_csv(headers, rows_data):
        """
        Generate CSV string from headers and row data.

        Args:
            headers: List of column headers
            rows_data: List of row data dicts

        Returns:
            str: CSV content as string
        """
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)

        # Write headers
        writer.writeheader()

        # Write data rows
        for row in rows_data:
            csv_row = {}

            # Add source variables
            for i, var_id in enumerate(row['source_variables'], 1):
                csv_row[f'SOURCE_VARIABLE_{i}'] = var_id

            # Add target variables
            for i, var_id in enumerate(row['target_variables'], 1):
                csv_row[f'TARGET_VARIABLE_{i}'] = var_id

            # Add source members
            for i, member_id in enumerate(row['source_members'], 1):
                csv_row[f'SOURCE_MEMBER_{i}'] = member_id

            # Add target members
            for i, member_id in enumerate(row['target_members'], 1):
                csv_row[f'TARGET_MEMBER_{i}'] = member_id

            # Add optional fields
            csv_row['MEMBER_HIERARCHY'] = row['member_hierarchy'] or ''
            csv_row['VALID_FROM'] = row['valid_from'] or ''
            csv_row['VALID_TO'] = row['valid_to'] or ''

            writer.writerow(csv_row)

        csv_content = output.getvalue()
        output.close()

        return csv_content
