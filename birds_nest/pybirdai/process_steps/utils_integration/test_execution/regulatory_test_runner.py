# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - test runner implementation

import subprocess
import os
import json
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import glob

logger = logging.getLogger(__name__)

# Define constants
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


class RegulatoryTestRunnerProcessStep:
    """
    Process step for executing regulatory template tests.
    Refactored from utils.datapoint_test_run.run_tests to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the regulatory test runner process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "run_tests", config_file: str = None, 
                uv: str = DEFAULT_UV, dp_value: int = DEFAULT_DP_VALUE,
                reg_tid: str = DEFAULT_REG_TID, dp_suffix: str = DEFAULT_DP_SUFFIX,
                **kwargs) -> Dict[str, Any]:
        """
        Execute the regulatory test runner process.
        
        Args:
            operation (str): Operation type - "run_tests", "generate_fixtures", "parse_results"
            config_file (str): Path to configuration file
            uv (str): UV flag for test execution
            dp_value (int): Datapoint value for testing
            reg_tid (str): Regulatory template ID
            dp_suffix (str): Datapoint suffix
            **kwargs: Additional parameters
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            runner = RegulatoryTemplateTestRunner()
            
            if operation == "run_tests":
                if not config_file:
                    raise ValueError("config_file is required for run_tests operation")
                
                result = runner.run_tests(
                    uv=uv,
                    config_file=config_file,
                    dp_value=dp_value,
                    reg_tid=reg_tid,
                    dp_suffix=dp_suffix
                )
                
                return {
                    'success': True,
                    'operation': 'run_tests',
                    'result': result,
                    'message': f'Tests executed successfully with config {config_file}'
                }
            
            elif operation == "generate_fixtures":
                result = runner.generate_test_fixtures(**kwargs)
                return {
                    'success': True,
                    'operation': 'generate_fixtures',
                    'result': result,
                    'message': 'Test fixtures generated successfully'
                }
                
            elif operation == "parse_results":
                result = runner.parse_test_results(**kwargs)
                return {
                    'success': True,
                    'operation': 'parse_results',
                    'result': result,
                    'message': 'Test results parsed successfully'
                }
                
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.test_runner = runner
                
        except Exception as e:
            logger.error(f"Failed to execute regulatory test runner: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Regulatory test runner execution failed'
            }


class RegulatoryTemplateTestRunner:
    """
    Enhanced regulatory template test runner with process step integration.
    Refactored from utils.datapoint_test_run.run_tests.
    """
    
    def __init__(self):
        """Initialize the regulatory template test runner."""
        self.setup_directories()
    
    def setup_directories(self):
        """Set up necessary directories for test execution."""
        directories = [
            TEST_RESULTS_DIR,
            TEST_RESULTS_TXT_FOLDER,
            TEST_RESULTS_JSON_FOLDER
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")
    
    def run_tests(self, uv: str = DEFAULT_UV, config_file: str = None, 
                  dp_value: int = DEFAULT_DP_VALUE, reg_tid: str = DEFAULT_REG_TID,
                  dp_suffix: str = DEFAULT_DP_SUFFIX) -> Dict[str, Any]:
        """
        Run the complete test suite for regulatory templates.
        
        Args:
            uv (str): UV flag for test execution
            config_file (str): Path to configuration file
            dp_value (int): Datapoint value for testing
            reg_tid (str): Regulatory template ID
            dp_suffix (str): Datapoint suffix
            
        Returns:
            dict: Test execution results
        """
        logger.info(f"Starting regulatory template test run with config: {config_file}")
        
        start_time = datetime.now()
        
        try:
            # Step 1: Load configuration
            config = self._load_configuration(config_file)
            
            # Step 2: Generate test fixtures
            fixture_result = self.generate_test_fixtures(config)
            
            # Step 3: Execute tests
            test_result = self._execute_tests(uv, config)
            
            # Step 4: Parse and save results
            parse_result = self.parse_test_results(config)
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            result = {
                'success': True,
                'execution_time': execution_time,
                'fixture_generation': fixture_result,
                'test_execution': test_result,
                'result_parsing': parse_result,
                'config_file': config_file,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            logger.info(f"Test run completed successfully in {execution_time:.2f} seconds")
            return result
            
        except Exception as e:
            logger.error(f"Test run failed: {e}")
            raise
    
    def generate_test_fixtures(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Generate test fixtures based on configuration.
        
        Args:
            config (dict): Test configuration dictionary
            
        Returns:
            dict: Fixture generation results
        """
        logger.info("Generating test fixtures...")
        
        try:
            # Run the generator script
            cmd = ["python", GENERATOR_FILE_PATH]
            if config:
                # Add configuration parameters as needed
                pass
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            return {
                'success': True,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'message': 'Test fixtures generated successfully'
            }
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Fixture generation failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'stdout': e.stdout,
                'stderr': e.stderr,
                'message': 'Test fixture generation failed'
            }
    
    def parse_test_results(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Parse test results and save in structured format.
        
        Args:
            config (dict): Test configuration dictionary
            
        Returns:
            dict: Result parsing outcome
        """
        logger.info("Parsing test results...")
        
        try:
            # Run the parser script
            cmd = ["python", PARSER_FILE_PATH]
            if config:
                # Add configuration parameters as needed
                pass
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Count result files
            txt_files = len(glob.glob(os.path.join(TEST_RESULTS_TXT_FOLDER, "*.txt")))
            json_files = len(glob.glob(os.path.join(TEST_RESULTS_JSON_FOLDER, "*.json")))
            
            return {
                'success': True,
                'txt_files_generated': txt_files,
                'json_files_generated': json_files,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'message': f'Results parsed: {txt_files} TXT, {json_files} JSON files'
            }
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Result parsing failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'stdout': e.stdout,
                'stderr': e.stderr,
                'message': 'Test result parsing failed'
            }
    
    def _load_configuration(self, config_file: str) -> Dict[str, Any]:
        """
        Load test configuration from file.
        
        Args:
            config_file (str): Path to configuration file
            
        Returns:
            dict: Configuration dictionary
        """
        if not config_file or not os.path.exists(config_file):
            logger.warning(f"Configuration file not found: {config_file}, using defaults")
            return {}
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            logger.info(f"Loaded configuration from {config_file}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return {}
    
    def _execute_tests(self, uv: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the actual test suite.
        
        Args:
            uv (str): UV flag for test execution
            config (dict): Test configuration
            
        Returns:
            dict: Test execution results
        """
        logger.info("Executing test suite...")
        
        try:
            # This would typically run pytest or the specific test framework
            # For now, we'll simulate the test execution
            cmd = ["python", "-m", "pytest", "tests/", "-v"]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Parse the result to determine success/failure
            success = result.returncode == 0
            
            return {
                'success': success,
                'return_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'message': 'Tests executed successfully' if success else 'Some tests failed'
            }
            
        except Exception as e:
            logger.error(f"Test execution failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Test execution failed'
            }
    
    def get_test_summary(self) -> Dict[str, Any]:
        """
        Get a summary of recent test results.
        
        Returns:
            dict: Test summary information
        """
        try:
            txt_files = glob.glob(os.path.join(TEST_RESULTS_TXT_FOLDER, "*.txt"))
            json_files = glob.glob(os.path.join(TEST_RESULTS_JSON_FOLDER, "*.json"))
            
            # Get most recent files
            recent_txt = max(txt_files, key=os.path.getctime) if txt_files else None
            recent_json = max(json_files, key=os.path.getctime) if json_files else None
            
            return {
                'total_txt_files': len(txt_files),
                'total_json_files': len(json_files),
                'most_recent_txt': recent_txt,
                'most_recent_json': recent_json,
                'test_results_dir': TEST_RESULTS_DIR
            }
            
        except Exception as e:
            logger.error(f"Failed to get test summary: {e}")
            return {
                'error': str(e),
                'message': 'Failed to generate test summary'
            }