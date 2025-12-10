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
Test runner for ANCRDT table transformations.

This module orchestrates ANCRDT table test execution, including:
- Loading test configurations
- Generating test code
- Executing tests with pytest
- Processing and displaying results
"""

import os
import json
import subprocess
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

# Reuse database cleanup from existing utilities
from pybirdai.utils.datapoint_test_run.database_cleanup_service import DatabaseCleanupService
from pybirdai.utils.datapoint_test_run.parser_for_tests import PytestOutputParser
from pybirdai.process_steps.ancrdt_transformation.test_generator_ancrdt import ANCRDTTestCodeGenerator

logger = logging.getLogger(__name__)


class ANCRDTTestRunner:
    """
    Test runner for ANCRDT table transformations.

    Orchestrates the complete test workflow for ANCRDT tables:
    1. Load configuration
    2. Clean database
    3. Load fixtures
    4. Generate test code (if needed)
    5. Run tests with pytest
    6. Parse and display results
    """

    def __init__(self, suite_name='ancrdt-test-suite', use_uv=False):
        """
        Initialize the ANCRDT test runner.

        Args:
            suite_name (str): Name of the test suite directory.
            use_uv (bool): Whether to use UV as backend for Python execution.
        """
        self.suite_name = suite_name
        self.use_uv = use_uv
        self.tests_dir = "tests"
        self.suite_dir = os.path.join(self.tests_dir, suite_name)

    def load_config(self, config_path):
        """
        Load ANCRDT test configuration from JSON file.

        Args:
            config_path (str): Path to configuration file.

        Returns:
            dict: Configuration dictionary.

        Raises:
            FileNotFoundError: If config file doesn't exist.
            json.JSONDecodeError: If config file is not valid JSON.
        """
        # If relative path, resolve relative to suite directory
        if not os.path.isabs(config_path) and not config_path.startswith(self.suite_dir):
            config_path = os.path.join(self.suite_dir, config_path)

        logger.info(f"Loading ANCRDT test configuration from: {config_path}")

        with open(config_path, 'r') as f:
            config = json.load(f)

        # Validate config structure
        if 'test_type' not in config or config['test_type'] != 'ancrdt':
            logger.warning(
                f"Configuration file does not specify test_type='ancrdt'. "
                f"Assuming ANCRDT tests based on structure."
            )

        if 'tests' not in config:
            raise ValueError("Configuration must contain 'tests' array")

        logger.info(f"Loaded {len(config['tests'])} ANCRDT test configurations")
        return config

    def cleanup_database(self, connection, cursor):
        """
        Clean up database before running tests.

        Args:
            connection: SQLite database connection.
            cursor: Database cursor.
        """
        logger.info("Cleaning up database before tests...")

        try:
            # Use existing cleanup service with Django ORM
            cleanup_service = DatabaseCleanupService()
            results = cleanup_service.cleanup_bird_data_tables(force=True)

            total_deleted = sum(results.values())
            logger.info(f"Database cleanup completed - deleted {total_deleted} records from {len(results)} tables")
        except Exception as e:
            logger.error(f"Database cleanup failed: {e}")
            raise

    def load_sql_fixture(self, cursor, sql_file_path):
        """
        Load SQL fixture file into database.

        Args:
            cursor: Database cursor.
            sql_file_path (str): Path to SQL file.

        Raises:
            FileNotFoundError: If SQL file doesn't exist.
            RuntimeError: If any SQL statement fails to execute.
        """
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"SQL fixture not found: {sql_file_path}")

        with open(sql_file_path, 'r') as f:
            sql_content = f.read()

        # Split by semicolons and execute each statement
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]

        failed_statements = []

        for i, statement in enumerate(statements, 1):
            try:
                cursor.execute(statement)
            except Exception as e:
                logger.error(f"Failed to execute SQL statement {i}/{len(statements)}: {e}")
                logger.error(f"Statement: {statement[:200]}...")
                failed_statements.append((i, statement[:100], str(e)))

        # Fail fast if any statements failed
        if failed_statements:
            cursor.connection.rollback()
            error_summary = '\n'.join([
                f"  Statement {i}: {stmt}... => {error}"
                for i, stmt, error in failed_statements
            ])
            raise RuntimeError(
                f"Failed to load SQL fixture: {len(failed_statements)}/{len(statements)} statements failed\n"
                f"File: {sql_file_path}\n"
                f"Errors:\n{error_summary}"
            )

        cursor.connection.commit()

        # Close Django database connections to ensure they see the new data
        from django.db import connections
        for conn in connections.all():
            conn.close()

    def generate_test_if_needed(self, table_name, scenario, expected_rows, validation_rules,
                                aggregate_by=None, aggregate_func=None, aggregate_column=None):
        """
        Generate test code if it doesn't already exist.

        Args:
            table_name (str): ANCRDT table name.
            scenario (str): Test scenario name.
            expected_rows (int): Expected row count.
            validation_rules (dict): Field validation rules.
            aggregate_by (str or list, optional): Column(s) to group by.
            aggregate_func (str, optional): Aggregation function (count, sum, mean).
            aggregate_column (str, optional): Column to aggregate (for sum/mean).

        Returns:
            str: Path to generated test file.
        """
        # Test file path follows pattern: test_table_{TABLE_NAME}__{scenario}.py
        test_filename = f"test_table_{table_name}__{scenario}.py"
        test_dir = os.path.join(self.suite_dir, "tests", "code")
        test_path = os.path.join(test_dir, test_filename)

        # Only generate if doesn't exist
        if os.path.exists(test_path):
            return test_path

        # Generate test code
        ANCRDTTestCodeGenerator.generate_ancrdt_test(
            table_name=table_name,
            scenario=scenario,
            expected_rows=expected_rows,
            validation_rules=validation_rules,
            output_path=test_path,
            aggregate_by=aggregate_by,
            aggregate_func=aggregate_func,
            aggregate_column=aggregate_column
        )

        return test_path

    def run_pytest(self, test_path, output_txt_path):
        """
        Execute pytest on the generated test file.

        Args:
            test_path (str): Path to test file.
            output_txt_path (str): Path to save pytest output.

        Returns:
            bool: True if test execution succeeded, False otherwise.
        """
        # Build pytest command
        if self.use_uv:
            cmd = ["uv", "run", "pytest", "-v", test_path]
        else:
            cmd = ["python", "-m", "pytest", "-v", test_path]

        try:
            # Set up environment with PYTHONPATH to ensure pybirdai imports work
            # when pytest runs as a subprocess
            env = os.environ.copy()
            current_dir = os.getcwd()
            if 'PYTHONPATH' in env:
                env['PYTHONPATH'] = f"{current_dir}{os.pathsep}{env['PYTHONPATH']}"
            else:
                env['PYTHONPATH'] = current_dir

            # Run pytest and capture output
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                env=env
            )

            # Save output to file
            os.makedirs(os.path.dirname(output_txt_path), exist_ok=True)
            with open(output_txt_path, 'w') as f:
                f.write(result.stdout)
                f.write(result.stderr)

            # Return success if pytest ran (even if tests failed)
            return True

        except Exception as e:
            logger.error(f"Failed to run pytest: {e}")
            return False
        finally:
            # Cleanup after test - always runs even if test fails
            from pybirdai.process_steps.ancrdt_transformation.execute_ancrdt_table import ExecuteANCRDTTable
            ExecuteANCRDTTable.delete_lineage_data()

    def process_test_results(self, txt_output_path, json_output_path, test_config):
        """
        Parse pytest output and generate JSON results.

        Args:
            txt_output_path (str): Path to pytest text output.
            json_output_path (str): Path to save JSON results.
            test_config (dict): Test configuration dict.

        Returns:
            dict: Parsed test results.
        """
        # Parse pytest output - pass all arguments to constructor
        parser = PytestOutputParser(
            output_file_path=txt_output_path,
            dp_value=test_config.get('expected_rows', 0),  # Use row count as "value"
            reg_tid=test_config.get('table_name', ''),
            dp_suffix=test_config.get('scenario', ''),
            scenario_name=test_config.get('scenario', '')
        )

        # parse() method takes no arguments and returns JSON string
        results_json_string = parser.parse()

        # Parse the JSON string to dict
        results = json.loads(results_json_string)

        # Save JSON results
        os.makedirs(os.path.dirname(json_output_path), exist_ok=True)
        with open(json_output_path, 'w') as f:
            json.dump(results, f, indent=2)

        return results

    def display_results(self, results, table_name, scenario, expected_rows):
        """
        Display test results in a user-friendly format.

        Args:
            results (dict): Parsed test results.
            table_name (str): ANCRDT table name.
            scenario (str): Test scenario.
            expected_rows (int): Expected number of rows.
        """
        print("\n" + "=" * 80)
        print(f"TEST RESULTS FOR SCENARIO: {scenario}")
        print(f"Table Name: {table_name}")
        print(f"Scenario: {scenario}")
        print(f"Expected Rows: {expected_rows}")
        print("=" * 80)

        test_results = results.get('test_results', {})
        passed = test_results.get('passed', [])
        failed = test_results.get('failed', [])

        print(f"\nPASSED TESTS ({len(passed)}):")
        for test in passed:
            print(f"  ✓ {test}")

        print(f"\nFAILED TESTS ({len(failed)}):")
        if failed:
            for test in failed:
                print(f"  ✗ {test}")
        else:
            print("  None - All tests passed!")

        print("\n" + "=" * 80 + "\n")

    def process_ancrdt_scenario(self, connection, cursor, test_config):
        """
        Process a single ANCRDT test scenario.

        Args:
            connection: Database connection.
            cursor: Database cursor.
            test_config (dict): Test configuration containing:
                - table_name: ANCRDT table name
                - scenario: Test scenario name
                - expected_rows: Expected row count
                - validation_rules: Field validation rules (optional)
        """
        table_name = test_config.get('table_name')
        scenario = test_config.get('scenario')
        expected_rows = test_config.get('expected_rows', 0)
        validation_rules = test_config.get('validation_rules', {})
        test_id = test_config.get('test_id', 'N/A')
        description = test_config.get('description', '')

        # Extract aggregation parameters
        aggregate_by = test_config.get('aggregate_by')
        aggregate_func = test_config.get('aggregate_func')
        aggregate_column = test_config.get('aggregate_column')

        if not table_name or not scenario:
            logger.error(f"❌ Skipping incomplete test config - missing table_name or scenario")
            logger.error(f"   Config: {test_config}")
            return

        logger.info(f"📋 Test ID: {test_id}")
        logger.info(f"📊 Table: {table_name}")
        logger.info(f"🎯 Scenario: {scenario}")
        if description:
            logger.info(f"📝 Description: {description}")
        logger.info(f"✓ Expected rows: {expected_rows}")

        # 1. Clean database
        self.cleanup_database(connection, cursor)

        # 2. Load SQL fixture
        # Fixtures are consolidated in test suite directory
        fixture_path = os.path.join(
            self.suite_dir,
            "tests",
            "fixtures",
            "templates",
            table_name,
            scenario,
            "sql_inserts.sql"
        )

        if not os.path.exists(fixture_path):
            logger.error(f"❌ Fixture not found at: {fixture_path}")
            return

        try:
            self.load_sql_fixture(cursor, fixture_path)
        except FileNotFoundError:
            logger.error(f"❌ Fixture not found: {fixture_path}")
            return
        except Exception as e:
            logger.error(f"❌ Failed to load fixture: {e}")
            return

        # 3. Generate test code (if needed)
        test_path = self.generate_test_if_needed(
            table_name,
            scenario,
            expected_rows,
            validation_rules,
            aggregate_by,
            aggregate_func,
            aggregate_column
        )

        # 4. Run pytest
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        txt_output_path = os.path.join(
            self.suite_dir,
            "tests",
            "test_results",
            "txt",
            f"{timestamp}__test_results_{table_name.lower()}__{scenario}.txt"
        )

        if not self.run_pytest(test_path, txt_output_path):
            logger.error(f"❌ Pytest execution failed for {table_name} - {scenario}")
            return

        # 5. Process results
        json_output_path = os.path.join(
            self.suite_dir,
            "tests",
            "test_results",
            "json",
            f"{timestamp}__test_results_{table_name.lower()}__{scenario}.json"
        )

        results = self.process_test_results(
            txt_output_path,
            json_output_path,
            test_config
        )

        # 6. Display results
        self.display_results(results, table_name, scenario, expected_rows)

    def run_ancrdt_tests_from_config(self, config_path):
        """
        Run all ANCRDT tests defined in a configuration file.

        Args:
            config_path (str): Path to configuration file.
        """
        # Load configuration
        config = self.load_config(config_path)

        # Extract top-level table_name (optional - can be specified per test)
        top_level_table_name = config.get('table_name')

        # Open database connection
        connection = sqlite3.connect("db.sqlite3")
        cursor = connection.cursor()

        logger.info(f"Running {len(config.get('tests', []))} ANCRDT test(s)")
        if top_level_table_name:
            logger.info(f"Default table: {top_level_table_name}")

        try:
            # Process each test configuration
            for idx, test_config in enumerate(config.get('tests', []), 1):
                # If test doesn't have table_name, use top-level one
                if 'table_name' not in test_config and top_level_table_name:
                    test_config = test_config.copy()  # Don't modify original
                    test_config['table_name'] = top_level_table_name

                self.process_ancrdt_scenario(connection, cursor, test_config)

        finally:
            # Close database connection
            cursor.close()
            connection.close()

        # Generate test URL report (reuse from existing utilities)
        try:
            from pybirdai.utils.datapoint_test_run.generate_test_url import main
            main(suite_name=self.suite_name)
            logger.info(f"Generated test report URL for suite: {self.suite_name}")
        except Exception as e:
            logger.warning(f"Could not generate test URL: {e}")


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    runner = ANCRDTTestRunner(suite_name='ancrdt-test-suite', use_uv=False)
    runner.run_ancrdt_tests_from_config('configuration_file_tests.json')
