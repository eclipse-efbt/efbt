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

from datetime import datetime
from pybirdai.models.bird_meta_data_model import VARIABLE, MEMBER, MEMBER_HIERARCHY


class ValidateMappingCSV:
    """
    Validates parsed mapping CSV data against database constraints.

    This class checks that all referenced variables, members, and hierarchies
    exist in the database, and that date formats are valid.
    """

    @staticmethod
    def validate(parsed_data):
        """
        Validate parsed mapping data against database constraints.

        Args:
            parsed_data: Parsed mapping data structure from ParseMappingCSV

        Returns:
            dict: Validation report with structure:
                {
                    'is_valid': bool,
                    'errors': [list of error messages],
                    'warnings': [list of warning messages],
                    'details': {
                        'missing_variables': [list of variable IDs],
                        'missing_members': [list of member IDs],
                        'missing_hierarchies': [list of hierarchy IDs],
                        'invalid_dates': [list of row numbers with invalid dates]
                    }
                }
        """
        errors = []
        warnings = []
        details = {
            'missing_variables': set(),
            'missing_members': set(),
            'missing_hierarchies': set(),
            'invalid_dates': []
        }

        rows = parsed_data.get('rows', [])

        if not rows:
            errors.append("No data rows found in CSV file")
            return {
                'is_valid': False,
                'errors': errors,
                'warnings': warnings,
                'details': details
            }

        # Collect all unique variable IDs, member IDs, and hierarchy IDs
        all_variable_ids = set()
        all_member_ids = set()
        all_hierarchy_ids = set()

        for row in rows:
            all_variable_ids.update(row.get('source_variables', []))
            all_variable_ids.update(row.get('target_variables', []))
            all_member_ids.update(row.get('source_members', []))
            all_member_ids.update(row.get('target_members', []))

            hierarchy = row.get('member_hierarchy')
            if hierarchy:
                all_hierarchy_ids.add(hierarchy)

        # Validate variables exist in database
        if all_variable_ids:
            existing_variables = set(
                VARIABLE.objects.filter(
                    variable_id__in=all_variable_ids
                ).values_list('variable_id', flat=True)
            )
            missing_variables = all_variable_ids - existing_variables

            if missing_variables:
                details['missing_variables'] = sorted(missing_variables)
                errors.append(
                    f"The following {len(missing_variables)} variable(s) do not exist in the database: "
                    f"{', '.join(sorted(missing_variables)[:10])}"
                    f"{'...' if len(missing_variables) > 10 else ''}"
                )

        # Validate members exist in database
        if all_member_ids:
            existing_members = set(
                MEMBER.objects.filter(
                    member_id__in=all_member_ids
                ).values_list('member_id', flat=True)
            )
            missing_members = all_member_ids - existing_members

            if missing_members:
                details['missing_members'] = sorted(missing_members)
                errors.append(
                    f"The following {len(missing_members)} member(s) do not exist in the database: "
                    f"{', '.join(sorted(missing_members)[:10])}"
                    f"{'...' if len(missing_members) > 10 else ''}"
                )

        # Validate hierarchies exist in database
        if all_hierarchy_ids:
            existing_hierarchies = set(
                MEMBER_HIERARCHY.objects.filter(
                    member_hierarchy_id__in=all_hierarchy_ids
                ).values_list('member_hierarchy_id', flat=True)
            )
            missing_hierarchies = all_hierarchy_ids - existing_hierarchies

            if missing_hierarchies:
                details['missing_hierarchies'] = sorted(missing_hierarchies)
                warnings.append(
                    f"The following {len(missing_hierarchies)} member hierarchy(ies) do not exist in the database: "
                    f"{', '.join(sorted(missing_hierarchies)[:10])}"
                    f"{'...' if len(missing_hierarchies) > 10 else ''}"
                )

        # Validate date formats
        for row in rows:
            row_num = row.get('row_number')

            valid_from = row.get('valid_from')
            if valid_from and not ValidateMappingCSV._is_valid_date(valid_from):
                details['invalid_dates'].append(row_num)
                errors.append(
                    f"Row {row_num}: Invalid VALID_FROM date format '{valid_from}'. "
                    f"Expected format: YYYY-MM-DD"
                )

            valid_to = row.get('valid_to')
            if valid_to and not ValidateMappingCSV._is_valid_date(valid_to):
                details['invalid_dates'].append(row_num)
                errors.append(
                    f"Row {row_num}: Invalid VALID_TO date format '{valid_to}'. "
                    f"Expected format: YYYY-MM-DD"
                )

        # Check for empty mappings (no variables or members)
        for row in rows:
            row_num = row.get('row_number')
            has_variables = (
                len(row.get('source_variables', [])) > 0 or
                len(row.get('target_variables', [])) > 0
            )
            has_members = (
                len(row.get('source_members', [])) > 0 or
                len(row.get('target_members', [])) > 0
            )

            if not has_variables and not has_members:
                warnings.append(
                    f"Row {row_num}: No variables or members found. Row will be skipped."
                )

        # Convert sets to lists for JSON serialization
        details['missing_variables'] = list(details['missing_variables'])
        details['missing_members'] = list(details['missing_members'])
        details['missing_hierarchies'] = list(details['missing_hierarchies'])

        is_valid = len(errors) == 0

        return {
            'is_valid': is_valid,
            'errors': errors,
            'warnings': warnings,
            'details': details
        }

    @staticmethod
    def _is_valid_date(date_string):
        """
        Check if a string is a valid date in YYYY-MM-DD format.

        Args:
            date_string: String to validate

        Returns:
            bool: True if valid date format, False otherwise
        """
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False
