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
Generate test code for ANCRDT table transformations.

This module provides functionality to generate pytest test code for ANCRDT tables
using simple string templates instead of AST manipulation.
"""

import os
import logging

logger = logging.getLogger(__name__)


class ANCRDTTestCodeGenerator:
    """
    A class for generating test code for ANCRDT table transformations.

    This class uses simple string templates to generate clean, readable test code
    that validates ANCRDT table transformations using configuration-driven expectations.
    """

    @staticmethod
    def generate_ancrdt_test(table_name, scenario, expected_rows, validation_rules, output_path,
                           aggregate_by=None, aggregate_func=None, aggregate_column=None):
        """
        Generate a complete ANCRDT test file.

        Args:
            table_name (str): Name of the ANCRDT table (e.g., 'ANCRDT_INSTRMNT_C_1').
            scenario (str): Scenario name for the test (e.g., '01_happy_path_single_instrument').
            expected_rows (int): Expected number of rows.
            validation_rules (dict): Field validation rules (empty for zero-row tests).
            output_path (str): Path where the test file should be written.
            aggregate_by (str or list, optional): Column(s) to group by for aggregation.
            aggregate_func (str, optional): Aggregation function (count, sum, mean).
            aggregate_column (str, optional): Column to aggregate (for sum/mean).

        Returns:
            str: Path to the generated test file.
        """
        logger.info(f"Generating ANCRDT test for table: {table_name}, scenario: {scenario}")

        # Determine if aggregation is requested
        has_aggregation = aggregate_by and aggregate_func

        # Determine test type based on whether we have validation rules
        has_validations = validation_rules and len(validation_rules) > 0

        # Build aggregation parameter string for test execution
        agg_params = ""
        if has_aggregation:
            # Convert aggregate_by to proper format
            if isinstance(aggregate_by, list):
                agg_by_str = repr(aggregate_by)
            else:
                agg_by_str = f"'{aggregate_by}'"

            agg_params = f",\n        aggregate_by={agg_by_str},\n        aggregate_func='{aggregate_func}'"
            if aggregate_column:
                agg_params += f",\n        aggregate_column='{aggregate_column}'"

        # Generate unified test code using single template
        # Validation code differs based on whether we have validation rules
        if has_validations:
            validation_setup = f"validation_data = test_helpers.get_validation_data('{scenario}')"
            validation_call = "test_helpers.validate_result_rows(result, validation_data)"
        else:
            validation_setup = f"expected_count = test_helpers.get_expected_row_count('{scenario}')"
            validation_call = "test_helpers.assert_row_count(result, expected_count)"

        test_code = f'''# Django setup using existing utility
from pybirdai.process_steps.ancrdt_transformation.utils.django_setup import DjangoSetup
DjangoSetup.configure_django()

from pybirdai.process_steps.ancrdt_transformation.execute_ancrdt_table import ExecuteANCRDTTable
from pybirdai.process_steps.ancrdt_transformation.testing import test_helpers


def test_execute_ancrdt_table():
    """Test {table_name} - {scenario.replace('_', ' ')}."""
    # Load configuration
    config = test_helpers.load_test_config()
    table_name = config.get('table_name', '{table_name}')

    # Load expected results from configuration
    {validation_setup}

    # Execute table transformation{' with aggregation' if has_aggregation else ''}
    result = ExecuteANCRDTTable.execute_table(
        table_name=table_name{agg_params}
    )

    # Validate results
    {validation_call}
'''

        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # Write test file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(test_code)

        logger.info(f"Generated test file: {output_path}")
        return output_path


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    table_name = "ANCRDT_INSTRMNT_C_1"
    scenario = "01_happy_path_single_instrument"
    expected_rows = 1
    validation_rules = {
        'row_0': {
            'INSTRMNT_ID': 'INST_001',
            'INSTRMNT_TYP_PRDCT': '80',
            'PRPS': '7'
        }
    }

    output_path = f"test_table_{table_name}__{scenario}.py"

    ANCRDTTestCodeGenerator.generate_ancrdt_test(
        table_name,
        scenario,
        expected_rows,
        validation_rules,
        output_path
    )
