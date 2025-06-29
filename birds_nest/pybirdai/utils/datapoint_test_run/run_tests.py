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

import subprocess
import os
import json
import argparse
import sqlite3
import typing
from datetime import datetime
import glob

import logging
from pathlib import Path

import os
# Define constants
# Base directories
TESTS_DIR = "tests"
PYBIRDAI_DIR = "pybirdai"
UTILS_DIR = f"utils{os.sep}datapoint_test_run"
TEST_RESULTS_DIR = os.path.join(TESTS_DIR, "test_results")

# Test results folders
TEST_RESULTS_TXT_FOLDER = os.path.join(TEST_RESULTS_DIR, "txt")
TEST_RESULTS_JSON_FOLDER = os.path.join(TEST_RESULTS_DIR, "json")

# Utility file paths
GENERATOR_FILE_PATH = os.path.join(PYBIRDAI_DIR, UTILS_DIR, "generator_for_tests.py")
PARSER_FILE_PATH = os.path.join(PYBIRDAI_DIR, UTILS_DIR, "parser_for_tests.py")

# SQL file names
SQL_INSERT_FILE_NAME = "sql_inserts.sql"
SQL_DELETE_FILE_NAME = "sql_deletes.sql"

# Define argument defaults as constants
DEFAULT_UV = "True"
DEFAULT_DP_VALUE = 83491250
DEFAULT_REG_TID = "F_05_01_REF_FINREP_3_0"
DEFAULT_DP_SUFFIX = "152589_REF"


def return_logger(__file_name__:str):
    return logging.getLogger(__file_name__)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

class FakeArgs(object):
    def __init__(self):
        pass

class RegulatoryTemplateTestRunner:
    """
    A class to generate and run test code for regulatory templates.

    This class provides functionality to:
    1. Generate test code for regulatory templates
    2. Run tests with pytest
    3. Process and display test results
    4. Handle different test scenarios
    """

    def __init__(self, parser_: bool = True):
        """Initialize the test runner with command line arguments."""
        # Set up command line argument parsing
        self.args = FakeArgs()
        if parser_:
            self.parser = argparse.ArgumentParser(description='Generate and run test code for regulatory templates')
            self.parser.add_argument('--uv', type=str, default=DEFAULT_UV,
                        help=f'run with astral/uv as backend (default: {DEFAULT_UV})')
            self.parser.add_argument('--dp-value', type=int, default=DEFAULT_DP_VALUE,
                        help=f'Datapoint value to test (default: {DEFAULT_DP_VALUE})')
            self.parser.add_argument('--reg-tid', type=str, default=DEFAULT_REG_TID,
                        help=f'Regulatory template ID (default: {DEFAULT_REG_TID})')
            self.parser.add_argument('--dp-suffix', type=str, default=DEFAULT_DP_SUFFIX,
                        help=f'Suffix for datapoint and cell IDs (default: {DEFAULT_DP_SUFFIX})')
            self.parser.add_argument('--config-file', type=str,
                        help='JSON configuration file for multiple tests')
            self.parser.add_argument('--scenario', type=str,
                        help='Specific scenario to run (if not all scenarios)')

            self.args = self.parser.parse_args()

    def get_file_paths(self, reg_tid: str, dp_suffix: str) -> tuple:
        """
        Generate file paths for test results.

        Args:
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix

        Returns:
            Tuple containing paths for text and JSON output files
        """
        cell_class = f"Cell_{reg_tid}_{dp_suffix}"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        result_filename = f"{timestamp}__test_results_{cell_class.lower()}"
        txt_path = os.path.join(TEST_RESULTS_TXT_FOLDER, result_filename)
        json_path = os.path.join(TEST_RESULTS_JSON_FOLDER, result_filename)
        return txt_path, json_path

    def setup_subprocess_commands(self, use_uv: bool, scenario: str, dp_value: str, reg_tid: str, dp_suffix: str) -> tuple:
        """
        Configure subprocess commands for test execution.

        Args:
            use_uv: Whether to use UV as backend
            scenario: Test scenario to run
            dp_value: Datapoint value to test
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix

        Returns:
            Tuple of command lists for test generation, running, and results conversion
        """
        subprocess_list = ["uv", "run"] if use_uv else ["python"]

        test_generation = subprocess_list.copy()
        test_runs = subprocess_list.copy()
        test_results_conversion = subprocess_list.copy()

        test_generation.extend([
            GENERATOR_FILE_PATH,
            "--dp-value", str(dp_value),
            "--reg-tid", reg_tid,
            "--dp-suffix", dp_suffix,
            "--scenario", scenario
        ])

        test_runs.extend(["-m", "pytest", "-v"])
        test_results_conversion.extend([PARSER_FILE_PATH])

        return test_generation, test_runs, test_results_conversion

    def load_sql_fixture(self, connection: sqlite3.Connection, cursor: sqlite3.Cursor, file_path: str, is_delete: bool=False) -> bool:
        """
        Load SQL fixtures from file.

        Args:
            connection: SQLite connection
            cursor: SQLite cursor
            file_path: Path to SQL file
            is_delete: Whether this is a delete operation

        Returns:
            Boolean indicating success
        """
        try:
            with open(file_path, 'r') as sql_file:
                sql_script = sql_file.read()
                cursor.executescript(sql_script)
            connection.commit()
            return True
        except Exception as e:
            action_type = "deleting" if is_delete else "inserting"
            logger.error(f"Error {action_type} SQL fixtures: {str(e)}")
            connection.rollback()
            return False

    def execute_test_process(self, commands: typing.List[str], output_path=None) -> bool:
        """
        Execute a test subprocess with optional output redirection.

        Args:
            commands: List of command arguments
            output_path: Path to redirect output

        Returns:
            Boolean indicating success
        """
        try:
            if output_path:
                with open(output_path, "w") as f:
                    subprocess.run(commands, stdout=f)
            else:
                subprocess.run(commands)
            return True
        except Exception as e:
            logger.error(f"Process execution failed: {str(e)}")
            return False

    def display_test_results(self, json_path: str, scenario: str, reg_tid: str, dp_suffix: str, dp_value: str) -> bool:
        """
        Display formatted test results.

        Args:
            json_path: Path to JSON results file
            scenario: Test scenario name
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            dp_value: Datapoint value

        Returns:
            Boolean indicating success
        """
        try:
            with open(json_path, 'r') as json_file:
                test_data = json.load(json_file)

            print("\n" + "=" * 80)
            print(f"TEST RESULTS FOR SCENARIO: {scenario}")
            print(f"Template ID: {reg_tid}")
            print(f"Datapoint: {dp_suffix}")
            print(f"Value: {dp_value}")
            print("=" * 80)

            passed = test_data.get('test_results', {}).get('passed', [])
            failed = test_data.get('test_results', {}).get('failed', [])

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
            return True
        except Exception as e:
            logger.error(f"Failed to read and print test results: {str(e)}")
            return False

    def process_scenario(self, connection: sqlite3.Connection, cursor: sqlite3.Cursor,
                        scenario_path: str, reg_tid: str, dp_suffix: str, dp_value: str, use_uv: bool):
        """
        Process a single test scenario.

        Args:
            connection: SQLite connection
            cursor: SQLite cursor
            scenario_path: Path to scenario
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            dp_value: Datapoint value
            use_uv: Whether to use UV as backend
        """
        # Set up paths
        test_data_scenario_path = f"tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}"
        test_data_sql_path = f"{test_data_scenario_path}{os.sep}{scenario_path}{os.sep}"
        txt_path_stub, json_path_stub = self.get_file_paths(reg_tid, dp_suffix)

        logger.debug(f"Starting scenario: {scenario_path} from {reg_tid} at datapoint {dp_suffix}")
        logger.debug(f"Loading fixture SQL files for scenario: {scenario_path}")

        # Load SQL fixtures
        delete_path = f"{test_data_sql_path}{SQL_DELETE_FILE_NAME}"
        insert_path = f"{test_data_sql_path}{SQL_INSERT_FILE_NAME}"

        self.load_sql_fixture(connection, cursor, delete_path, is_delete=True)
        self.load_sql_fixture(connection, cursor, insert_path)

        # Prepare commands
        test_generation, test_runs, test_results_conversion = self.setup_subprocess_commands(
            use_uv, scenario_path, dp_value, reg_tid, dp_suffix
        )

        # Ensure directories exist
        os.makedirs(TEST_RESULTS_TXT_FOLDER, exist_ok=True)
        os.makedirs(TEST_RESULTS_JSON_FOLDER, exist_ok=True)

        # Run test generator
        logger.debug("Starting test generator...")
        if not self.execute_test_process(test_generation):
            return
        logger.debug("Test generator completed successfully")

        # Run tests
        logger.debug("Running pytest...")
        txt_output_path = f"{txt_path_stub}__{scenario_path}.txt"
        test_path = os.path.join(TESTS_DIR,
            f"test_cell_{reg_tid}_{dp_suffix}__{scenario_path}.py".lower())
        if not self.execute_test_process(test_runs+[test_path], txt_output_path):
            return
        logger.debug("Pytest completed successfully")

        # Process results
        logger.debug("Processing test results...")
        json_output_path = f"{json_path_stub}__{scenario_path}.json"
        result_args = [
            txt_output_path,
            str(dp_value),
            reg_tid,
            dp_suffix,
            scenario_path
        ]
        if not self.execute_test_process(test_results_conversion + result_args, json_output_path):
            return
        logger.debug("Test results processed successfully")

        # Display results
        self.display_test_results(json_output_path, scenario_path, reg_tid, dp_suffix, dp_value)

        logger.debug(f"Finished scenario: {scenario_path} from {reg_tid} at datapoint {dp_suffix}")

    def load_config_file(self, config_path: str) -> dict:
        """
        Load test configuration from a JSON file.

        Args:
            config_path: Path to config file

        Returns:
            Configuration dictionary or None if failed
        """
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config file: {str(e)}")
            return None

    def run_tests_from_config(self, config_path: str, use_uv: bool=False):
        """
        Run tests based on a configuration file.

        Args:
            config_path: Path to config file
            use_uv: Whether to use UV as backend
        """
        config = self.load_config_file(config_path)
        if not config:
            logger.error("Invalid or missing configuration file.")
            return

        connection = sqlite3.connect("db.sqlite3")
        cursor = connection.cursor()

        # Process each test configuration
        for test_config in config.get('tests', []):
            reg_tid = test_config.get('reg_tid')
            dp_suffix = test_config.get('dp_suffix')
            dp_value = test_config.get('dp_value')
            scenario = test_config.get('scenario')

            if not all([reg_tid, dp_suffix, dp_value]):
                logger.warning(f"Skipping incomplete test configuration: {test_config}")
                continue

            if scenario:
                # Run specific scenario
                self.process_scenario(connection, cursor, scenario, reg_tid, dp_suffix, str(dp_value), use_uv)
            else:
                # Run all scenarios for this template/datapoint
                test_data_scenario_path = f"tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}{os.sep}"
                try:
                    for scenario_path in os.listdir(test_data_scenario_path):
                        if ".py" in scenario_path:
                            continue
                        self.process_scenario(connection, cursor, scenario_path, reg_tid, dp_suffix, str(dp_value), use_uv)
                except Exception as e:
                    logger.error(f"Error processing scenarios: {str(e)}")

        cursor.close()
        connection.close()

    def run_tests(self, reg_tid: str="", dp_suffix: str="", dp_value: str="", use_uv: bool=False, specific_scenario: str=None):
        """
        Main function to run all test scenarios.

        Args:
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            dp_value: Datapoint value
            use_uv: Whether to use UV as backend
            specific_scenario: Specific scenario to run
        """
        connection = sqlite3.connect("db.sqlite3")
        cursor = connection.cursor()

        test_data_scenario_path = f"tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}{os.sep}"

        if specific_scenario:
            self.process_scenario(
                connection,
                cursor,
                specific_scenario,
                reg_tid,
                dp_suffix,
                dp_value,
                use_uv
            )
        else:
            for scenario_path in os.listdir(test_data_scenario_path):
                if ".py" in scenario_path:
                    continue

                self.process_scenario(
                    connection,
                    cursor,
                    scenario_path,
                    reg_tid,
                    dp_suffix,
                    dp_value,
                    use_uv
                )
        cursor.close()
        connection.close()

    def main(self):
        """
        Main entry point for the test runner.
        Determines whether to run from config file or command line arguments.
        """
        # clear old results
        old_txt_files = glob.glob(os.path.join(TEST_RESULTS_TXT_FOLDER, "*.txt"))
        old_json_files = glob.glob(os.path.join(TEST_RESULTS_JSON_FOLDER, "*.json"))

        for file_path in old_txt_files + old_json_files:
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Failed to delete file {file_path}: {str(e)}")

        # Check if running from config file
        if self.args.config_file:
            self.run_tests_from_config(self.args.config_file, eval(self.args.uv))
        else:
            # Run with command line arguments
            self.run_tests(
                self.args.reg_tid,
                self.args.dp_suffix,
                str(self.args.dp_value),
                eval(self.args.uv),
                self.args.scenario
            )
