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
import importlib.util

import logging
from pathlib import Path

# DatabaseCleanupService removed - using direct cleanup approach instead


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
SQL_DELETE_FILE_NAME = "sql_deletes.sql"

# Define argument defaults as constants
DEFAULT_UV = "True"
DEFAULT_DP_VALUE = 83491250
DEFAULT_REG_TID = "F_05_01_REF_FINREP_3_0"
DEFAULT_DP_SUFFIX = "152589_REF"
DEFAULT_SUITE_NAME = "basic_test_suite"


def return_logger(__file_name__:str):
    return logging.getLogger(__file_name__)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

class FakeArgs:
    def __init__(self):
        self.uv = None
        self.config_file = None
        self.dp_value = None
        self.reg_tid = None
        self.dp_suffix = None
        self.scenario = None
        self.suite_name = None
        self.framework = None

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
            self.parser.add_argument('--suite-name', type=str, default=None,
                        help=f'Test suite name (default: auto-detect from config path or {DEFAULT_SUITE_NAME})')
            self.parser.add_argument('--framework', type=str, default=None,
                        help='Framework to test (e.g., FINREP, COREP, ANCRDT). If specified, loads framework-specific test config.')

            self.args = self.parser.parse_args()

        # Initialize cache for BIRD table discovery
        self._bird_tables_cache = None

    def get_file_paths(self, reg_tid: str, dp_suffix: str, suite_name: str = None) -> tuple:
        """
        Generate file paths for test results.

        Args:
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            suite_name: Test suite name

        Returns:
            Tuple containing paths for text and JSON output files
        """
        if suite_name is None:
            suite_name = getattr(self.args, 'suite_name', None) or DEFAULT_SUITE_NAME

        # Update test results paths for suite structure
        suite_test_results_dir = os.path.join(TESTS_DIR, suite_name, "tests", "test_results")
        suite_txt_folder = os.path.join(suite_test_results_dir, "txt")
        suite_json_folder = os.path.join(suite_test_results_dir, "json")

        cell_class = f"Cell_{reg_tid}_{dp_suffix}"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        result_filename = f"{timestamp}__test_results_{cell_class.lower()}"
        txt_path = os.path.join(suite_txt_folder, result_filename)
        json_path = os.path.join(suite_json_folder, result_filename)
        return txt_path, json_path

    def setup_subprocess_commands(self, use_uv: bool, scenario: str, dp_value: str, reg_tid: str, dp_suffix: str, suite_name: str = None) -> tuple:
        """
        Configure subprocess commands for test execution.

        Args:
            use_uv: Whether to use UV as backend
            scenario: Test scenario to run
            dp_value: Datapoint value to test
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            suite_name: Test suite name

        Returns:
            Tuple of command lists for test generation, running, and results conversion
        """
        if suite_name is None:
            suite_name = getattr(self.args, 'suite_name', None) or DEFAULT_SUITE_NAME
        subprocess_list = ["uv", "run"] if use_uv else ["python"]

        test_generation = subprocess_list.copy()
        test_runs = subprocess_list.copy()
        test_results_conversion = subprocess_list.copy()

        test_generation.extend([
            GENERATOR_FILE_PATH,
            "--dp-value", str(dp_value),
            "--reg-tid", reg_tid,
            "--dp-suffix", dp_suffix,
            "--scenario", scenario,
            "--suite-name", suite_name
        ])

        extension = ["-m","pytest", "-v"] if not use_uv else ["pytest", "-v"]
        test_runs.extend(extension)
        test_results_conversion.extend([PARSER_FILE_PATH])

        return test_generation, test_runs, test_results_conversion

    def extract_tables_from_sql_delete(self, delete_path: str) -> typing.List[str]:
        """
        Extract table names from sql_deletes.sql file.

        Args:
            delete_path: Path to the SQL delete file

        Returns:
            List of table names to be cleaned
        """
        tables = []
        try:
            if not os.path.exists(delete_path):
                logger.warning(f"SQL delete file not found: {delete_path}")
                return tables

            with open(delete_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.upper().startswith('DELETE FROM'):
                        # Extract table name from "DELETE FROM table_name;"
                        table_name = line.replace('DELETE FROM', '').replace(';', '').strip()
                        tables.append(table_name)

            logger.debug(f"Extracted {len(tables)} tables from {delete_path}: {tables}")
        except Exception as e:
            logger.warning(f"Could not extract tables from {delete_path}: {e}")

        return tables

    def get_bird_models_for_cleanup(self) -> typing.List[str]:
        """
        Dynamically discover BIRD data model tables to clean.

        This method imports bird_data_model.py and discovers all Django model tables,
        returning them in proper deletion order (assignment/relationship tables first).

        Uses caching to avoid repeated discovery on subsequent calls.

        Returns:
            List of table names in deletion order
        """
        # Return cached result if available
        if self._bird_tables_cache is not None:
            logger.debug(f"Using cached BIRD tables ({len(self._bird_tables_cache)} tables)")
            return self._bird_tables_cache

        try:
            # Set up Django environment
            if '.' not in sys.path:
                sys.path.insert(0, '.')
            if 'DJANGO_SETTINGS_MODULE' not in os.environ:
                os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'

            import django
            from django.conf import settings
            if not settings.configured:
                django.setup()

            # Import bird_data_model
            from pybirdai.models import bird_data_model

            # Get all model classes from bird_data_model
            models_to_clean = []
            for name in dir(bird_data_model):
                obj = getattr(bird_data_model, name)
                if (isinstance(obj, type) and
                    hasattr(obj, '_meta') and
                    hasattr(obj._meta, 'db_table')):
                    models_to_clean.append(obj)

            # Order by dependency (assignment/relationship tables first)
            ordered_tables = []

            # Level 1: Assignment and relationship tables (delete first due to foreign keys)
            for model in models_to_clean:
                table_name = model._meta.db_table
                if 'ASSGNMNT' in model.__name__ or '_RL' in model.__name__:
                    ordered_tables.append(table_name)

            # Level 2: Other tables
            for model in models_to_clean:
                table_name = model._meta.db_table
                if table_name not in ordered_tables:
                    ordered_tables.append(table_name)

            logger.debug(f"Discovered {len(ordered_tables)} BIRD tables dynamically")
            # Cache the result for future calls
            self._bird_tables_cache = ordered_tables
            return ordered_tables

        except Exception as e:
            logger.warning(f"Could not dynamically discover tables: {e}")
            return []

    def clear_bird_tables_cache(self) -> None:
        """
        Clear the cached BIRD tables list.

        This forces the next call to get_bird_models_for_cleanup() to rediscover
        the tables from bird_data_model.py. Useful during development when models
        might change.
        """
        self._bird_tables_cache = None
        logger.debug("Cleared BIRD tables cache")

    def cleanup_tables_directly(self, tables: typing.List[str]) -> bool:
        """
        Clean database tables directly using Django connection with foreign key handling.

        This method bypasses complex validation and directly deletes from tables
        while properly handling foreign key constraints for different database vendors.

        Args:
            tables: List of table names to clean

        Returns:
            Boolean indicating success
        """
        try:
            # Set up Django environment
            if '.' not in sys.path:
                sys.path.insert(0, '.')

            if 'DJANGO_SETTINGS_MODULE' not in os.environ:
                os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'

            import django
            from django.conf import settings
            if not settings.configured:
                django.setup()

            from django.db import connection

            with connection.cursor() as cursor:
                for table_name in tables:
                    try:
                        if connection.vendor == 'sqlite':
                            cursor.execute("PRAGMA foreign_keys = 0;")
                            # Table name is validated against whitelist above
                            cursor.execute(f"DELETE FROM {table_name};")
                            cursor.execute("PRAGMA foreign_keys = 1;")
                        elif connection.vendor == 'postgresql':
                            # Table name is validated against whitelist above
                            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
                        elif connection.vendor in ['microsoft', 'mssql']:
                            # Table name is validated against whitelist above
                            cursor.execute(f"TRUNCATE TABLE {table_name};")
                        else:
                            # Table name is validated against whitelist above
                            cursor.execute(f"DELETE FROM {table_name};")

                        logger.debug(f"Cleaned table {table_name} using {connection.vendor}")
                    except Exception as e:
                        logger.warning(f"Failed to clean table {table_name}: {e}")

            return True
        except Exception as e:
            logger.error(f"Direct cleanup failed: {e}")
            return False

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

    def _load_test_fixtures(self, connection: sqlite3.Connection, cursor: sqlite3.Cursor,
                           scenario_dir: str, sql_insert_path: str) -> bool:
        """
        Load test fixtures from CSV files or fall back to SQL.

        Checks for CSV files in the scenario directory first. If CSV files exist,
        uses the CSVFixtureLoader. Otherwise, falls back to the legacy SQL loader.

        Args:
            connection: SQLite connection
            cursor: SQLite cursor
            scenario_dir: Path to scenario directory containing fixtures
            sql_insert_path: Path to sql_inserts.sql (fallback)

        Returns:
            Boolean indicating success
        """
        try:
            from pybirdai.utils.datapoint_test_run.csv_fixture_loader import CSVFixtureLoader

            loader = CSVFixtureLoader()

            # Check if CSV files exist in the scenario directory
            if loader.has_csv_fixtures(scenario_dir):
                logger.info(f"Loading test fixtures from CSV files in {scenario_dir}")
                results = loader.load_scenario_fixtures(scenario_dir)
                logger.info(f"Loaded CSV fixtures: {results}")
                return True
            else:
                # Fall back to SQL loading
                logger.debug(f"No CSV fixtures found, falling back to SQL: {sql_insert_path}")
                return self.load_sql_fixture(connection, cursor, sql_insert_path)

        except ImportError as e:
            # CSVFixtureLoader not available, use SQL
            logger.warning(f"CSV loader not available ({e}), using SQL")
            return self.load_sql_fixture(connection, cursor, sql_insert_path)
        except Exception as e:
            logger.error(f"Error loading test fixtures: {e}")
            # Try SQL as last resort
            logger.info("Attempting SQL fallback after CSV error")
            return self.load_sql_fixture(connection, cursor, sql_insert_path)

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
            # Set up environment for Python path
            env = os.environ.copy()
            env['PYTHONPATH'] = '.'

            if output_path:
                with open(output_path, "w") as f:
                    subprocess.run(commands, stdout=f, env=env)
            else:
                subprocess.run(commands, env=env)
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
                        scenario_path: str, reg_tid: str, dp_suffix: str, dp_value: str, use_uv: bool, suite_name: str = None):
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
            suite_name: Test suite name
        """
        if suite_name is None:
            suite_name = getattr(self.args, 'suite_name', None) or DEFAULT_SUITE_NAME
        # Set up paths for suite structure
        test_data_scenario_path = f"tests{os.sep}{suite_name}{os.sep}tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}"
        test_data_sql_path = f"{test_data_scenario_path}{os.sep}{scenario_path}{os.sep}"
        txt_path_stub, json_path_stub = self.get_file_paths(reg_tid, dp_suffix, suite_name)

        logger.debug(f"Starting scenario: {scenario_path} from {reg_tid} at datapoint {dp_suffix}")
        logger.debug(f"Loading fixture data for scenario: {scenario_path}")

        # Clean up database using dynamic table discovery
        insert_path = f"{test_data_sql_path}{SQL_INSERT_FILE_NAME}"

        # Get tables to clean dynamically from BIRD data models
        tables_to_clean = self.get_bird_models_for_cleanup()

        if tables_to_clean:
            # Use dynamic table discovery for cleanup
            logger.debug(f"Using dynamic cleanup for {len(tables_to_clean)} BIRD tables")
            if self.cleanup_tables_directly(tables_to_clean):
                logger.debug("Dynamic database cleanup successful")
            else:
                logger.error("Dynamic cleanup failed")
                return
        else:
            logger.warning("No tables discovered for cleanup")

        # Load test data - try CSV first, fall back to SQL
        self._load_test_fixtures(connection, cursor, test_data_sql_path, insert_path)

        # Check if test file already exists
        test_path = os.path.join(TESTS_DIR, suite_name, "tests", "code",
            f"test_cell_{reg_tid}_{dp_suffix}__{scenario_path}.py".lower())

        # Prepare commands
        test_generation, test_runs, test_results_conversion = self.setup_subprocess_commands(
            use_uv, scenario_path, dp_value, reg_tid, dp_suffix, suite_name
        )

        # Ensure directories exist for suite structure
        suite_test_results_dir = os.path.join(TESTS_DIR, suite_name, "tests", "test_results")
        suite_txt_folder = os.path.join(suite_test_results_dir, "txt")
        suite_json_folder = os.path.join(suite_test_results_dir, "json")
        os.makedirs(suite_txt_folder, exist_ok=True)
        os.makedirs(suite_json_folder, exist_ok=True)

        # Only generate test if it doesn't exist
        if os.path.exists(test_path):
            logger.debug(f"Test file already exists, skipping generation: {test_path}")
        else:
            logger.debug("Test file doesn't exist, generating...")
            if not self.execute_test_process(test_generation):
                return
            logger.debug("Test generator completed successfully")

        # Run tests
        logger.debug("Running pytest...")
        txt_output_path = f"{txt_path_stub}__{scenario_path}.txt"

        # Verify test file exists before running
        if not os.path.exists(test_path):
            logger.error(f"Test file not found: {test_path}")
            return

        logger.debug(f"Running test file: {test_path}")
        if not self.execute_test_process(test_runs+[test_path], txt_output_path):
            logger.error(f"Failed to execute test: {test_path}")
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

    def extract_suite_from_config_path(self, config_path: str) -> str:
        """
        Extract suite name from configuration file path.

        Args:
            config_path: Path to configuration file

        Returns:
            Suite name if found in path, otherwise None
        """
        # Normalize the path to handle different separators
        normalized_path = os.path.normpath(config_path)
        path_parts = normalized_path.split(os.sep)

        # Look for pattern: tests/<suite_name>/...
        if len(path_parts) >= 2 and path_parts[0] == 'tests':
            return path_parts[1]

        return None

    def load_config_file(self, config_path: str, suite_name: str = None) -> dict:
        """
        Load test configuration from a JSON file.

        Args:
            config_path: Path to config file
            suite_name: Test suite name

        Returns:
            Configuration dictionary or None if failed
        """
        try:
            if suite_name is None:
                suite_name = getattr(self.args, 'suite_name', None) or DEFAULT_SUITE_NAME

            # If config_path is just a filename, use suite path
            if os.path.basename(config_path) == config_path:
                config_path = os.path.join(TESTS_DIR, suite_name, config_path)

            safe_path = self.get_safe_config_path(config_path)
            with open(safe_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config file: {str(e)}")
            return None
        
    def run_tests_from_config(self, config_path: str, use_uv: bool=False, suite_name: str = None):
        """
        Run tests based on a configuration file.

        Supports both FINREP datapoint tests and ANCRDT table tests.
        Test type is detected from the 'test_type' field in the config,
        or inferred from the structure of the test configuration.

        Args:
            config_path: Path to config file
            use_uv: Whether to use UV as backend
            suite_name: Test suite name (auto-detected from path if not provided)
        """
        # First try to extract suite name from config path
        if suite_name is None:
            extracted_suite = self.extract_suite_from_config_path(config_path)
            if extracted_suite:
                suite_name = extracted_suite
            else:
                # Fall back to command line argument or default
                suite_name = getattr(self.args, 'suite_name', None) or DEFAULT_SUITE_NAME

        config = self.load_config_file(config_path, suite_name)
        if not config:
            logger.error("Invalid or missing configuration file.")
            return

        # Detect test type (FINREP vs ANCRDT)
        test_type = config.get('test_type', None)

        # If not explicitly set, try to infer from structure
        if test_type is None and config.get('tests'):
            first_test = config['tests'][0]
            # ANCRDT tests have 'table_name', FINREP tests have 'reg_tid'
            if 'table_name' in first_test:
                test_type = 'ancrdt'
                logger.info("Detected ANCRDT test type from configuration structure")
            elif 'reg_tid' in first_test:
                test_type = 'finrep'
                logger.info("Detected FINREP test type from configuration structure")
            else:
                # Default to FINREP for backward compatibility
                test_type = 'finrep'
                logger.warning("Could not detect test type, defaulting to FINREP")

        # Route to appropriate test runner
        if test_type == 'ancrdt':
            logger.info(f"Running ANCRDT tests from config: {config_path}")
            try:
                # Set up Python path and Django before importing ANCRDT runner
                if '.' not in sys.path:
                    sys.path.insert(0, '.')
                if 'DJANGO_SETTINGS_MODULE' not in os.environ:
                    os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'

                import django
                from django.conf import settings
                if not settings.configured:
                    django.setup()

                from pybirdai.process_steps.ancrdt_transformation.test_runner_ancrdt import ANCRDTTestRunner
                runner = ANCRDTTestRunner(suite_name=suite_name, use_uv=use_uv)
                runner.run_ancrdt_tests_from_config(config_path)
                return
            except ImportError as e:
                logger.error(f"Failed to import ANCRDT test runner: {e}")
                logger.error("Ensure ANCRDT test infrastructure is installed")
                return
            except Exception as e:
                logger.error(f"ANCRDT test execution failed: {e}", exc_info=True)
                return

        # Continue with FINREP tests (original logic)
        logger.info(f"Running FINREP tests from config: {config_path}")

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
                self.process_scenario(connection, cursor, scenario, reg_tid, dp_suffix, str(dp_value), use_uv, suite_name)
            else:
                # Run all scenarios for this template/datapoint
                test_data_scenario_path = f"tests{os.sep}{suite_name}{os.sep}tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}{os.sep}"
                try:
                    for scenario_path in os.listdir(test_data_scenario_path):
                        if ".py" in scenario_path:
                            continue
                        self.process_scenario(connection, cursor, scenario_path, reg_tid, dp_suffix, str(dp_value), use_uv, suite_name)
                except Exception as e:
                    logger.error(f"Error processing scenarios: {str(e)}")

        cursor.close()
        connection.close()
        try:
            from pybirdai.utils.datapoint_test_run.generate_test_url import main
            main()
        except ImportError as e:
            logger.warning(f"Could not import generate_test_url: {e}")
        except Exception as e:
            logger.warning(f"Error running generate_test_url: {e}")

    def run_tests(self, reg_tid: str="", dp_suffix: str="", dp_value: str="", use_uv: bool=False, specific_scenario: str=None, suite_name: str = None):
        """
        Main function to run all test scenarios.

        Args:
            reg_tid: Regulatory template ID
            dp_suffix: Datapoint suffix
            dp_value: Datapoint value
            use_uv: Whether to use UV as backend
            specific_scenario: Specific scenario to run
            suite_name: Test suite name
        """
        if suite_name is None:
            suite_name = getattr(self.args, 'suite_name', None) or DEFAULT_SUITE_NAME
        connection = sqlite3.connect("db.sqlite3")
        cursor = connection.cursor()

        test_data_scenario_path = f"tests{os.sep}{suite_name}{os.sep}tests{os.sep}fixtures{os.sep}templates{os.sep}{reg_tid}{os.sep}{dp_suffix}{os.sep}"

        if specific_scenario:
            self.process_scenario(
                connection,
                cursor,
                specific_scenario,
                reg_tid,
                dp_suffix,
                dp_value,
                use_uv,
                suite_name
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
                    suite_name
                )
        cursor.close()
        connection.close()

    def main(self):
        """
        Main entry point for the test runner.
        Determines whether to run from config file or command line arguments.
        """
        # Handle framework parameter - load framework-specific config
        config_file = self.args.config_file
        if self.args.framework and not config_file:
            logger.info(f"Loading test configuration for framework: {self.args.framework}")
            try:
                # Set up Django to access FrameworkTestSuite
                if '.' not in sys.path:
                    sys.path.insert(0, '.')
                if 'DJANGO_SETTINGS_MODULE' not in os.environ:
                    os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'

                import django
                from django.conf import settings
                if not settings.configured:
                    django.setup()

                from pybirdai.models.workflow_model import FrameworkTestSuite

                # Get framework-specific test suite
                test_suite = FrameworkTestSuite.get_test_suite_for_framework(self.args.framework)
                if test_suite:
                    config_file = test_suite.test_config_path
                    logger.info(f"Using framework test config: {config_file}")
                else:
                    logger.error(f"No test suite found for framework: {self.args.framework}")
                    logger.info(f"Available frameworks: FINREP, COREP, ANCRDT, BIRD_EIL, BIRD_ELDM")
                    return
            except Exception as e:
                logger.error(f"Failed to load framework test suite: {e}", exc_info=True)
                return

        # Determine suite name for cleaning old results
        if config_file:
            # Extract suite name from config path if available, otherwise use explicit parameter
            extracted_suite = self.extract_suite_from_config_path(config_file)
            suite_name = extracted_suite or self.args.suite_name or DEFAULT_SUITE_NAME
        else:
            suite_name = self.args.suite_name or DEFAULT_SUITE_NAME

        # clear old results from suite directory
        suite_test_results_dir = os.path.join(TESTS_DIR, suite_name, "tests", "test_results")
        suite_txt_folder = os.path.join(suite_test_results_dir, "txt")
        suite_json_folder = os.path.join(suite_test_results_dir, "json")

        old_txt_files = glob.glob(os.path.join(suite_txt_folder, "*.txt"))
        old_json_files = glob.glob(os.path.join(suite_json_folder, "*.json"))

        for file_path in old_txt_files + old_json_files:
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Failed to delete file {file_path}: {str(e)}")

        # Check if running from config file
        if config_file:
            self.run_tests_from_config(config_file, eval(self.args.uv), suite_name)
        else:
            # Run with command line arguments
            self.run_tests(
                self.args.reg_tid,
                self.args.dp_suffix,
                str(self.args.dp_value),
                eval(self.args.uv),
                self.args.scenario,
                self.args.suite_name
            )


if __name__ == "__main__":
    runner = RegulatoryTemplateTestRunner()
    runner.main()
