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
import django
import os
import sys
from django.apps import AppConfig
from django.conf import settings
import logging
from importlib import metadata
import ast

# Create a logger
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

if __name__ == "__main__":
    DjangoSetup.configure_django()
    from pybirdai.utils.datapoint_test_run.run_tests import RegulatoryTemplateTestRunner

    logger.info("Executing run tests substep...")

    # Auto-discover test suites in tests/ directory
    tests_dir = 'tests'
    test_suites = []

    if os.path.exists(tests_dir):
        for entry in os.listdir(tests_dir):
            suite_path = os.path.join(tests_dir, entry)
            # Check if this is a directory and contains a configuration file
            if os.path.isdir(suite_path):
                config_file_path = os.path.join(suite_path, 'configuration_file_tests.json')
                if os.path.exists(config_file_path):
                    test_suites.append({
                        'name': entry,
                        'config_path': config_file_path
                    })
                    logger.info(f"Discovered test suite: {entry}")

    if not test_suites:
        logger.error("No test suites found in tests/ directory")
        sys.exit(1)

    # Run tests for each discovered suite
    for suite in test_suites:
        logger.info(f"Running test suite: {suite['name']}")

        # Create test runner instance for this suite
        test_runner = RegulatoryTemplateTestRunner(False)

        # Configure test runner
        test_runner.args.uv = "True"
        test_runner.args.config_file = suite['config_path']
        test_runner.args.dp_value = None
        test_runner.args.reg_tid = None
        test_runner.args.dp_suffix = None
        test_runner.args.scenario = None
        test_runner.args.suite_name = suite['name']

        # Execute tests
        try:
            test_runner.main()
            logger.info(f"Completed test suite: {suite['name']}")
        except Exception as e:
            logger.error(f"Error running test suite {suite['name']}: {str(e)}")
            raise
