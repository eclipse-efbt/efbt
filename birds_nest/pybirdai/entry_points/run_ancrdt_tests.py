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
Entry point for running ANCRDT table transformation tests.

This module provides a command-line interface and Django AppConfig wrapper
for orchestrating ANCRDT table tests from configuration files.
"""

import os
import sys
import argparse
import logging
import django
from django.apps import AppConfig
from django.conf import settings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise



class RunANCRDTTests(AppConfig):
    """
    Django AppConfig for running ANCRDT table transformation tests.

    This class provides functionality to:
    1. Load test configuration from JSON files
    2. Generate test code for ANCRDT tables
    3. Execute tests with pytest
    4. Process and display results
    """

    name = 'pybirdai'
    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_tests(config_file_path, suite_name='ancrdt-test-suite', use_uv=False):
        """
        Run ANCRDT tests from a configuration file.

        Args:
            config_file_path (str): Path to JSON configuration file.
            suite_name (str): Name of the test suite directory.
            use_uv (bool): Whether to use UV as Python backend.

        Returns:
            int: Exit code (0 for success, 1 for failure).

        Example:
            >>> RunANCRDTTests.run_tests('tests/ancrdt-test-suite/configuration_file_tests.json')
        """
        from pybirdai.process_steps.ancrdt_transformation.test_runner_ancrdt import (
            ANCRDTTestRunner
        )

        logger.info(f"Starting ANCRDT test execution")
        logger.info(f"Configuration: {config_file_path}")
        logger.info(f"Suite: {suite_name}")
        logger.info(f"Use UV: {use_uv}")

        try:
            # Create test runner
            runner = ANCRDTTestRunner(suite_name=suite_name, use_uv=use_uv)

            # Run tests from configuration
            runner.run_ancrdt_tests_from_config(config_file_path)

            logger.info("ANCRDT tests completed successfully")
            return 0

        except Exception as e:
            logger.error(f"ANCRDT test execution failed: {e}", exc_info=True)
            return 1

    def ready(self):
        """
        Called when Django app is ready.

        This method is required for Django's AppConfig but is not used
        for standalone execution.
        """
        pass


def main():
    """
    Command-line interface for running ANCRDT tests.

    Usage:
        python pybirdai/entry_points/run_ancrdt_tests.py --config-file tests/ancrdt-test-suite/configuration_file_tests.json
        uv run pybirdai/entry_points/run_ancrdt_tests.py --config-file tests/ancrdt-test-suite/configuration_file_tests.json --uv
    """
    DjangoSetup.configure_django()

    parser = argparse.ArgumentParser(
        description='Run ANCRDT table transformation tests',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with standard Python
  python pybirdai/entry_points/run_ancrdt_tests.py --config-file tests/ancrdt-test-suite/configuration_file_tests.json

  # Run with UV
  uv run pybirdai/entry_points/run_ancrdt_tests.py --config-file tests/ancrdt-test-suite/configuration_file_tests.json --uv

Configuration File Format:
  {
    "test_type": "ancrdt",
    "tests": [
      {
        "table_name": "ANCRDT_INSTRMNT_C_1",
        "scenario": "basic_scenario",
        "expected_rows": 2,
        "validation_rules": {
          "row_0": {"INSTRMNT_ID": "ANCRDT_INST_1", "PRPS": "7"}
        }
      }
    ]
  }
        """
    )

    parser.add_argument(
        '--config-file',
        type=str,
        required=True,
        help='Path to JSON configuration file'
    )

    parser.add_argument(
        '--suite-name',
        type=str,
        default='ancrdt-test-suite',
        help='Test suite directory name (default: ancrdt-test-suite)'
    )

    parser.add_argument(
        '--uv',
        action='store_true',
        help='Use UV as Python backend (default: False)'
    )

    args = parser.parse_args()

    # Set up Django environment
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

    try:
        django.setup()
        logger.info("Django environment configured successfully")
    except Exception as e:
        logger.error(f"Failed to configure Django: {e}")
        sys.exit(1)

    # Run tests
    exit_code = RunANCRDTTests.run_tests(
        config_file_path=args.config_file,
        suite_name=args.suite_name,
        use_uv=args.uv
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
