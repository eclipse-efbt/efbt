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
import os.path
import json
import argparse
import sqlite3
import typing
from datetime import datetime
import glob
import sys
import io

import logging
from pathlib import Path

# Add the Django project root to Python path for imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import database cleanup service
try:
    from ...utils.database_cleanup_service import DatabaseCleanupService
except ImportError:
    # Fallback import path
    try:
        import sys
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
        from utils.database_cleanup_service import DatabaseCleanupService
    except ImportError:
        DatabaseCleanupService = None
        logging.warning("DatabaseCleanupService not available - will fall back to SQL files")

# Import test suite git service
try:
    from ...utils.test_suite_git_service import TestSuiteGitService
except ImportError:
    # Fallback import paths
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
        from utils.test_suite_git_service import TestSuiteGitService
    except ImportError:
        try:
            # Try direct import from pybirdai.utils
            from pybirdai.utils.test_suite_git_service import TestSuiteGitService
        except ImportError:
            try:
                # Add the project root to path and try absolute import
                current_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.join(current_dir, '..', '..', '..')
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                from pybirdai.utils.test_suite_git_service import TestSuiteGitService
            except ImportError:
                TestSuiteGitService = None
                logging.warning("TestSuiteGitService not available - git-based test fetching disabled")


# Define safe directory for configuration files
SAFE_CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Force UTF-8 encoding for stdout on Windows to handle Unicode characters
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

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
            self.parser.add_argument('--fetch-tests', action='store_true',
                        help='Auto-fetch missing test suites via git clone')
            self.parser.add_argument('--test-repo', type=str,
                        help='Git repository URL to fetch test suite from')

            self.args = self.parser.parse_args()

    def get_file_paths(self, reg_tid: str, dp_suffix: str, test_config: dict = None) -> tuple:
        """
        Generate file paths for test results.

        Args:
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            test_config: Test configuration dict (may contain _suite metadata)

        Returns:
            Tuple containing paths for text and JSON output files
        """
        cell_class = f"Cell_{reg_tid}_{dp_suffix}"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        result_filename = f"{timestamp}__test_results_{cell_class.lower()}"

        # Get suite-aware results paths
        txt_results_folder, json_results_folder = self.get_test_results_path(test_config)

        txt_path = os.path.join(txt_results_folder, result_filename)
        json_path = os.path.join(json_results_folder, result_filename)
        return txt_path, json_path

    def get_suite_test_dir(self, test_config: dict = None) -> str:
        """Get the test directory for the current test suite."""
        if test_config and '_suite' in test_config:
            suite_name = test_config['_suite']
            return f"tests{os.sep}{suite_name}{os.sep}tests{os.sep}code"
        else:
            return TESTS_DIR

    def setup_subprocess_commands(self, use_uv: bool, scenario: str, dp_value: str, reg_tid: str, dp_suffix: str, test_config: dict = None) -> tuple:
        """
        Configure subprocess commands for test execution.

        Args:
            use_uv: Whether to use UV as backend
            scenario: Test scenario to run
            dp_value: Datapoint value to test
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            test_config: Test configuration dict (may contain _suite metadata)

        Returns:
            Tuple of command lists for test generation, running, and results conversion
        """
        subprocess_list = ["uv", "run"] if use_uv else ["python"]

        test_generation = subprocess_list.copy()
        test_runs = subprocess_list.copy()
        test_results_conversion = subprocess_list.copy()

        # Add test suite awareness to generator if needed
        generator_args = [
            GENERATOR_FILE_PATH,
            "--dp-value", str(dp_value),
            "--reg-tid", reg_tid,
            "--dp-suffix", dp_suffix,
            "--scenario", scenario
        ]

        # Add suite info to generator if available
        if test_config and '_suite' in test_config:
            generator_args.extend(["--test-suite", test_config['_suite']])

        test_generation.extend(generator_args)

        extension = ["-m","pytest", "-v"] if not use_uv else ["pytest", "-v"]
        test_runs.extend(extension)
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

    def get_test_fixture_path(self, reg_tid: str, dp_suffix: str, test_config: dict = None) -> str:
        """
        Resolve fixture path based on test suite metadata.

        Args:
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            test_config: Test configuration dict (may contain _suite metadata)

        Returns:
            Path to test fixtures directory
        """
        if test_config and '_suite' in test_config:
            suite_name = test_config['_suite']
            logger.debug(f"Using test suite: {suite_name}")

            # Use tests/{suite_name}/ for test suites
            tests_path = f"tests{os.sep}{suite_name}{os.sep}tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}"
            logger.debug(f"Test fixture path: {tests_path}")
            return tests_path
        else:
            # Legacy path for tests without suite metadata
            path = f"tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}"
            logger.debug(f"Legacy fixture path: {path}")
            return path

    def get_test_results_path(self, test_config: dict = None) -> tuple:
        """
        Resolve test results paths based on test suite metadata.

        Args:
            test_config: Test configuration dict (may contain _suite metadata)

        Returns:
            Tuple of (txt_folder_path, json_folder_path)
        """
        if test_config and '_suite' in test_config:
            suite_name = test_config['_suite']

            # Use tests/{suite_name}/ for test suites
            tests_base_dir = f"tests{os.sep}{suite_name}{os.sep}tests{os.sep}test_results"
            txt_folder = f"{tests_base_dir}{os.sep}txt"
            json_folder = f"{tests_base_dir}{os.sep}json"
        else:
            txt_folder = TEST_RESULTS_TXT_FOLDER
            json_folder = TEST_RESULTS_JSON_FOLDER

        return txt_folder, json_folder

    def process_scenario(self, connection: sqlite3.Connection, cursor: sqlite3.Cursor,
                        scenario_path: str, reg_tid: str, dp_suffix: str, dp_value: str, use_uv: bool, test_config: dict = None):
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
            test_config: Test configuration dict (may contain _suite metadata)
        """
        # Set up paths with suite-aware resolution
        test_data_scenario_path = self.get_test_fixture_path(reg_tid, dp_suffix, test_config)
        test_data_sql_path = f"{test_data_scenario_path}{os.sep}{scenario_path}{os.sep}"
        txt_path_stub, json_path_stub = self.get_file_paths(reg_tid, dp_suffix, test_config)

        logger.debug(f"Starting scenario: {scenario_path} from {reg_tid} at datapoint {dp_suffix}")
        logger.debug(f"Loading fixture SQL files for scenario: {scenario_path}")

        # Load SQL fixtures
        insert_path = f"{test_data_sql_path}{SQL_INSERT_FILE_NAME}"

        # Use database cleanup service for data cleanup
        if DatabaseCleanupService is not None:
            try:
                logger.info("Using database cleanup service for data cleanup")
                cleanup_service = DatabaseCleanupService()
                cleanup_service.cleanup_bird_data_tables()
                logger.info("Database cleanup completed successfully")
            except Exception as e:
                logger.error(f"Database cleanup service failed: {e}")
                return
        else:
            logger.error("Database cleanup service not available - cannot proceed with test")
            return

        self.load_sql_fixture(connection, cursor, insert_path)

        # Prepare commands
        test_generation, test_runs, test_results_conversion = self.setup_subprocess_commands(
            use_uv, scenario_path, dp_value, reg_tid, dp_suffix, test_config
        )

        # Get suite-aware results paths
        txt_results_folder, json_results_folder = self.get_test_results_path(test_config)

        # Ensure directories exist
        os.makedirs(txt_results_folder, exist_ok=True)
        os.makedirs(json_results_folder, exist_ok=True)

        # Run test generator
        logger.debug("Starting test generator...")
        if not self.execute_test_process(test_generation):
            return
        logger.debug("Test generator completed successfully")

        # Run tests
        logger.debug("Running pytest...")
        txt_output_path = f"{txt_path_stub}__{scenario_path}.txt"

        # Get suite-aware test directory
        suite_test_dir = self.get_suite_test_dir(test_config)
        test_path = os.path.join(suite_test_dir,
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

    

    def get_safe_config_path(self, user_config_path: str) -> str:
        """
        Validates and constructs a safe absolute path for a config file.
        Returns safe config path if valid; raises ValueError if unsafe.
        """
        # If user_config_path is absolute, join with SAFE_CONFIG_DIR to prevent absolute escapes
        joined_path = os.path.join(SAFE_CONFIG_DIR, os.path.basename(user_config_path))

        # If input contains subdirs, preserve under SAFE_CONFIG_DIR, normalize path
        normalized_path = os.path.normpath(os.path.join(SAFE_CONFIG_DIR, user_config_path))
        abs_normalized_path = os.path.abspath(normalized_path)
        # Ensure containment
        if not abs_normalized_path.startswith(SAFE_CONFIG_DIR):
            raise ValueError(f"Invalid config path: {user_config_path}. Access denied.")
        return abs_normalized_path

    def detect_test_suite_from_path(self, config_path: str) -> str:
        """
        Auto-detect test suite name from configuration file path.

        Args:
            config_path: Path to config file

        Returns:
            Test suite name if detected, None otherwise
        """
        import re

        # Normalize path separators for consistent matching
        normalized_path = config_path.replace('\\', '/')

        # Pattern 1: tests/{suite_name}/configuration_file_tests.json
        match = re.search(r'tests/([^/]+)/.*\.json$', normalized_path)
        if match:
            suite_name = match.group(1)
            # Skip if it's just "tests" directory (legacy structure)
            if suite_name not in ('fixtures', 'test_results', 'code'):
                return suite_name

        # Pattern 2: test_suites/{suite_name}/configuration_file_tests.json
        match = re.search(r'test_suites/([^/]+)/.*\.json$', normalized_path)
        if match:
            suite_name = match.group(1)
            return suite_name

        return None

    def load_config_file(self, config_path: str) -> dict:
        """
        Load test configuration from a JSON file.

        Args:
            config_path: Path to config file

        Returns:
            Configuration dictionary or None if failed
        """
        try:
            safe_path = self.get_safe_config_path(config_path)
            with open(safe_path, 'r') as f:
                config = json.load(f)

            # Auto-detect test suite if not explicitly specified
            detected_suite = self.detect_test_suite_from_path(config_path)
            if detected_suite:
                logger.info(f"Auto-detected test suite: {detected_suite}")
                # Add _suite to each test configuration if not present
                for test_config in config.get('tests', []):
                    if '_suite' not in test_config:
                        test_config['_suite'] = detected_suite

            return config
        except Exception as e:
            logger.error(f"Failed to load config file: {str(e)}")
            return None
        
    def fetch_missing_test_suites(self, config: dict):
        """
        Fetch missing test suites via git clone if enabled.

        Args:
            config: Test configuration dictionary
        """
        if not TestSuiteGitService:
            logger.warning("TestSuiteGitService not available")
            return

        if not (self.args.fetch_tests or self.args.test_repo):
            return

        git_service = TestSuiteGitService()

        # Handle direct repository URL
        if self.args.test_repo:
            logger.info(f"Fetching test suite from repository: {self.args.test_repo}")
            git_service.fetch_test_suite(self.args.test_repo)
            return

        # Auto-fetch missing suites from test configurations
        if self.args.fetch_tests:
            missing_suites = set()

            for test_config in config.get('tests', []):
                suite_name = test_config.get('_suite')
                if suite_name:
                    suite_path = os.path.join("tests", suite_name)
                    if not os.path.exists(suite_path):
                        missing_suites.add(suite_name)
                        logger.info(f"Detected missing test suite: {suite_name}")

                # Check for git URL in test config
                git_url = test_config.get('_git_url')
                if git_url:
                    logger.info(f"Fetching test suite from configuration URL: {git_url}")
                    git_service.fetch_test_suite(git_url, suite_name)

            # Try to fetch from registry
            for suite_name in missing_suites:
                logger.info(f"Attempting to fetch missing suite '{suite_name}' from registry")
                if not git_service.fetch_from_registry(suite_name):
                    logger.warning(f"Could not fetch suite '{suite_name}' - please provide --test-repo or register the repository")

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

        # Fetch missing test suites if requested
        self.fetch_missing_test_suites(config)

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
                self.process_scenario(connection, cursor, scenario, reg_tid, dp_suffix, str(dp_value), use_uv, test_config)
            else:
                # Run all scenarios for this template/datapoint
                test_data_scenario_path = self.get_test_fixture_path(reg_tid, dp_suffix, test_config)
                try:
                    for scenario_path in os.listdir(test_data_scenario_path):
                        if ".py" in scenario_path:
                            continue
                        self.process_scenario(connection, cursor, scenario_path, reg_tid, dp_suffix, str(dp_value), use_uv, test_config)
                except Exception as e:
                    logger.error(f"Error processing scenarios: {str(e)}")

        cursor.close()
        connection.close()
        from pybirdai.utils.datapoint_test_run.generate_test_url import main
        main()

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
                use_uv,
                None  # No test config for direct calls
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
                    use_uv,
                    None  # No test config for direct calls
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


if __name__ == "__main__":
    runner = RegulatoryTemplateTestRunner(parser_=True)
    runner.main()
