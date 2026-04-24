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
import re


class ParseMappingCSV:
    """
    Parses a mapping CSV file into structured data format.

    This class reads a business-friendly CSV file with columns like
    SOURCE_VARIABLE_1, TARGET_VARIABLE_1, etc. and converts it into
    a structured format suitable for database import.
    """

    @staticmethod
    def parse(csv_file):
        """
        Parse a mapping CSV file into structured data.

        Args:
            csv_file: File object or file-like object

        Returns:
            dict: Parsed mapping data with structure:
                {
                    'rows': [
                        {
                            'row_number': 1,
                            'source_variables': ['var1', 'var2'],
                            'target_variables': ['var3'],
                            'source_members': ['mem1'],
                            'target_members': ['mem2'],
                            'member_hierarchy': 'hierarchy1',
                            'valid_from': '2024-01-01',
                            'valid_to': ''
                        },
                        ...
                    ],
                    'column_info': {
                        'source_variable_count': 2,
                        'target_variable_count': 1,
                        'source_member_count': 1,
                        'target_member_count': 1
                    }
                }

        Raises:
            ValueError: If CSV format is invalid or cannot be parsed
        """
        try:
            # Read CSV file
            if hasattr(csv_file, 'read'):
                # It's a file object
                content = csv_file.read()
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
                lines = content.splitlines()
            else:
                raise ValueError("CSV input must be an uploaded file or file-like object")

            reader = csv.DictReader(lines)
            headers = reader.fieldnames

            if not headers:
                raise ValueError("CSV file is empty or has no headers")

            # Analyze headers to identify column types
            column_info = ParseMappingCSV._analyze_headers(headers)

            # Parse rows
            rows_data = []
            for row_num, row in enumerate(reader, start=1):
                parsed_row = ParseMappingCSV._parse_row(row, column_info, row_num)
                rows_data.append(parsed_row)

            if not rows_data:
                raise ValueError("CSV file contains no data rows")

            return {
                'rows': rows_data,
                'column_info': column_info
            }

        except csv.Error as e:
            raise ValueError(f"CSV parsing error: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error parsing CSV file: {str(e)}")

    @staticmethod
    def _analyze_headers(headers):
        """
        Analyze CSV headers to identify column types and counts.

        Args:
            headers: List of column header strings

        Returns:
            dict: Column information including counts and mapping
        """
        column_info = {
            'source_variables': [],
            'target_variables': [],
            'source_members': [],
            'target_members': [],
            'has_hierarchy': False,
            'has_valid_from': False,
            'has_valid_to': False,
            'source_variable_count': 0,
            'target_variable_count': 0,
            'source_member_count': 0,
            'target_member_count': 0
        }

        # Regular expressions to match column patterns
        source_var_pattern = re.compile(r'^SOURCE_VARIABLE_(\d+)$', re.IGNORECASE)
        target_var_pattern = re.compile(r'^TARGET_VARIABLE_(\d+)$', re.IGNORECASE)
        source_mem_pattern = re.compile(r'^SOURCE_MEMBER_(\d+)$', re.IGNORECASE)
        target_mem_pattern = re.compile(r'^TARGET_MEMBER_(\d+)$', re.IGNORECASE)

        for header in headers:
            header_upper = header.upper().strip()

            # Check for source variables
            match = source_var_pattern.match(header_upper)
            if match:
                col_num = int(match.group(1))
                column_info['source_variables'].append((col_num, header))
                column_info['source_variable_count'] = max(
                    column_info['source_variable_count'], col_num
                )
                continue

            # Check for target variables
            match = target_var_pattern.match(header_upper)
            if match:
                col_num = int(match.group(1))
                column_info['target_variables'].append((col_num, header))
                column_info['target_variable_count'] = max(
                    column_info['target_variable_count'], col_num
                )
                continue

            # Check for source members
            match = source_mem_pattern.match(header_upper)
            if match:
                col_num = int(match.group(1))
                column_info['source_members'].append((col_num, header))
                column_info['source_member_count'] = max(
                    column_info['source_member_count'], col_num
                )
                continue

            # Check for target members
            match = target_mem_pattern.match(header_upper)
            if match:
                col_num = int(match.group(1))
                column_info['target_members'].append((col_num, header))
                column_info['target_member_count'] = max(
                    column_info['target_member_count'], col_num
                )
                continue

            # Check for optional columns
            if header_upper == 'MEMBER_HIERARCHY':
                column_info['has_hierarchy'] = True
            elif header_upper == 'VALID_FROM':
                column_info['has_valid_from'] = True
            elif header_upper == 'VALID_TO':
                column_info['has_valid_to'] = True

        # Sort columns by their number
        column_info['source_variables'].sort()
        column_info['target_variables'].sort()
        column_info['source_members'].sort()
        column_info['target_members'].sort()

        return column_info

    @staticmethod
    def _parse_row(row, column_info, row_number):
        """
        Parse a single CSV row into structured format.

        Args:
            row: Dict representing one CSV row
            column_info: Column information from header analysis
            row_number: Row number for tracking

        Returns:
            dict: Parsed row data
        """
        parsed_row = {
            'row_number': row_number,
            'source_variables': [],
            'target_variables': [],
            'source_members': [],
            'target_members': [],
            'member_hierarchy': None,
            'valid_from': None,
            'valid_to': None
        }

        # Extract source variables
        for _, col_name in column_info['source_variables']:
            value = row.get(col_name, '').strip()
            if value:
                parsed_row['source_variables'].append(value)

        # Extract target variables
        for _, col_name in column_info['target_variables']:
            value = row.get(col_name, '').strip()
            if value:
                parsed_row['target_variables'].append(value)

        # Extract source members
        for _, col_name in column_info['source_members']:
            value = row.get(col_name, '').strip()
            if value:
                parsed_row['source_members'].append(value)

        # Extract target members
        for _, col_name in column_info['target_members']:
            value = row.get(col_name, '').strip()
            if value:
                parsed_row['target_members'].append(value)

        # Extract optional fields
        if column_info['has_hierarchy']:
            hierarchy = row.get('MEMBER_HIERARCHY', '').strip()
            if hierarchy:
                parsed_row['member_hierarchy'] = hierarchy

        if column_info['has_valid_from']:
            valid_from = row.get('VALID_FROM', '').strip()
            if valid_from:
                parsed_row['valid_from'] = valid_from

        if column_info['has_valid_to']:
            valid_to = row.get('VALID_TO', '').strip()
            if valid_to:
                parsed_row['valid_to'] = valid_to

        return parsed_row
