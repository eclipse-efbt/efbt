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

import django
import os
from django.apps import AppConfig
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class RunRegulatoryTestRunner(AppConfig):
    """
    Django AppConfig for running regulatory template test services.
    
    This entry point provides access to regulatory test execution
    functionality through the process step architecture.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    def ready(self):
        """
        Prepare and execute regulatory test runner services.
        
        This method sets up the necessary contexts and provides access to
        regulatory template testing services.
        """
        from pybirdai.process_steps.utils_integration.test_execution.regulatory_test_runner import (
            RegulatoryTestRunnerProcessStep
        )
        from pybirdai.context.context import Context
        
        logger.info("Initializing Regulatory Test Runner entry point")
        
        try:
            # Create context for test runner services
            context = Context()
            
            # Initialize regulatory test runner process step
            test_runner_step = RegulatoryTestRunnerProcessStep(context)
            context.test_runner_step = test_runner_step
            
            # Store context globally for access by other components
            if not hasattr(settings, 'TEST_RUNNER_CONTEXT'):
                settings.TEST_RUNNER_CONTEXT = context
            
            logger.info("Regulatory Test Runner entry point initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Regulatory Test Runner: {e}")
            raise


def run_regulatory_tests(config_file: str, uv: str = "True", 
                        dp_value: int = 83491250, reg_tid: str = "F_05_01_REF_FINREP_3_0",
                        dp_suffix: str = "152589_REF"):
    """
    Entry point function for running regulatory template tests.
    
    Args:
        config_file (str): Path to test configuration file
        uv (str): UV flag for test execution
        dp_value (int): Datapoint value for testing
        reg_tid (str): Regulatory template ID
        dp_suffix (str): Datapoint suffix
        
    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.test_execution.regulatory_test_runner import (
            RegulatoryTestRunnerProcessStep
        )
        
        step = RegulatoryTestRunnerProcessStep()
        result = step.execute(
            operation="run_tests",
            config_file=config_file,
            uv=uv,
            dp_value=dp_value,
            reg_tid=reg_tid,
            dp_suffix=dp_suffix
        )
        
        logger.info(f"Regulatory test execution completed: {result.get('message', 'Success')}")
        return result
        
    except Exception as e:
        logger.error(f"Regulatory test execution failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Regulatory test execution failed'
        }


def generate_test_fixtures(**kwargs):
    """
    Entry point function for generating test fixtures.
    
    Args:
        **kwargs: Additional parameters for fixture generation
        
    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.test_execution.regulatory_test_runner import (
            RegulatoryTestRunnerProcessStep
        )
        
        step = RegulatoryTestRunnerProcessStep()
        result = step.execute(
            operation="generate_fixtures",
            **kwargs
        )
        
        logger.info(f"Test fixture generation completed: {result.get('message', 'Success')}")
        return result
        
    except Exception as e:
        logger.error(f"Test fixture generation failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Test fixture generation failed'
        }


def parse_test_results(**kwargs):
    """
    Entry point function for parsing test results.
    
    Args:
        **kwargs: Additional parameters for result parsing
        
    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.test_execution.regulatory_test_runner import (
            RegulatoryTestRunnerProcessStep
        )
        
        step = RegulatoryTestRunnerProcessStep()
        result = step.execute(
            operation="parse_results",
            **kwargs
        )
        
        logger.info(f"Test result parsing completed: {result.get('message', 'Success')}")
        return result
        
    except Exception as e:
        logger.error(f"Test result parsing failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Test result parsing failed'
        }


# Convenience functions for backward compatibility
def get_regulatory_test_runner():
    """
    Get a regulatory test runner instance.
    
    Returns:
        RegulatoryTemplateTestRunner: Configured test runner instance
    """
    from pybirdai.process_steps.utils_integration.test_execution.regulatory_test_runner import (
        RegulatoryTemplateTestRunner
    )
    
    return RegulatoryTemplateTestRunner()


def get_test_summary():
    """
    Get a summary of recent test results.
    
    Returns:
        dict: Test summary information
    """
    runner = get_regulatory_test_runner()
    return runner.get_test_summary()


def setup_test_directories():
    """
    Set up necessary test directories.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        runner = get_regulatory_test_runner()
        runner.setup_directories()
        return True
    except Exception as e:
        logger.error(f"Failed to set up test directories: {e}")
        return False