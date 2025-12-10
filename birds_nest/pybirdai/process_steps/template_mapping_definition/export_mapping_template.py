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


class ExportMappingTemplate:
    """
    Exports an empty mapping template CSV with example data.

    This class generates a CSV template that demonstrates the business-friendly
    format for mapping definitions, including source and target variables/members.
    """

    @staticmethod
    def handle():
        """
        Generate a CSV template with 2-3 example rows.

        Returns:
            str: CSV content as string with headers and example rows
        """
        # Define the column headers
        headers = [
            'SOURCE_VARIABLE_1',
            'SOURCE_VARIABLE_2',
            'TARGET_VARIABLE_1',
            'SOURCE_MEMBER_1',
            'SOURCE_MEMBER_2',
            'TARGET_MEMBER_1',
            'MEMBER_HIERARCHY',
            'VALID_FROM',
            'VALID_TO'
        ]

        # Define example rows to demonstrate usage
        example_rows = [
            {
                'SOURCE_VARIABLE_1': 'v_assets_total',
                'SOURCE_VARIABLE_2': 'v_currency_eur',
                'TARGET_VARIABLE_1': 'v_reporting_assets',
                'SOURCE_MEMBER_1': 'm_domestic',
                'SOURCE_MEMBER_2': 'm_foreign',
                'TARGET_MEMBER_1': 'm_consolidated',
                'MEMBER_HIERARCHY': 'h_geographical',
                'VALID_FROM': '2024-01-01',
                'VALID_TO': ''
            },
            {
                'SOURCE_VARIABLE_1': 'v_liabilities',
                'SOURCE_VARIABLE_2': 'v_currency_usd',
                'TARGET_VARIABLE_1': 'v_reporting_liabilities',
                'SOURCE_MEMBER_1': '',
                'SOURCE_MEMBER_2': '',
                'TARGET_MEMBER_1': 'm_standalone',
                'MEMBER_HIERARCHY': '',
                'VALID_FROM': '2024-01-01',
                'VALID_TO': '2024-12-31'
            },
            {
                'SOURCE_VARIABLE_1': 'v_equity',
                'SOURCE_VARIABLE_2': '',
                'TARGET_VARIABLE_1': 'v_reporting_equity',
                'SOURCE_MEMBER_1': 'm_tier1',
                'SOURCE_MEMBER_2': 'm_tier2',
                'TARGET_MEMBER_1': 'm_total_equity',
                'MEMBER_HIERARCHY': 'h_capital_structure',
                'VALID_FROM': '2024-06-01',
                'VALID_TO': ''
            }
        ]

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)

        # Write headers
        writer.writeheader()

        # Write example rows
        for row in example_rows:
            writer.writerow(row)

        # Get CSV content
        csv_content = output.getvalue()
        output.close()

        return csv_content
