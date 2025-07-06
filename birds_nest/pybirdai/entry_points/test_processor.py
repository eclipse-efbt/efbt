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

from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class TestProcessorConfig(AppConfig):
    """
    Django AppConfig for Test Processing operations.
    Provides regulatory test running and validation functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.test_processor'
    verbose_name = 'Test Processor'

    def ready(self):
        """Initialize the test processor when Django starts."""
        logger.info("Test Processor initialized")


def get_regulatory_test_runner():
    """
    Get regulatory test runner process step for test execution.
    
    Returns:
        RegulatoryTestRunnerProcessStep: Configured regulatory test runner
    """
    from pybirdai.process_steps.utils_integration.test_execution.regulatory_test_runner import RegulatoryTestRunnerProcessStep
    return RegulatoryTestRunnerProcessStep()


def run_regulatory_tests(test_config_file=None, test_suite=None, **kwargs):
    """
    Run regulatory tests using the configured test runner.
    
    Args:
        test_config_file (str): Path to test configuration file
        test_suite (str): Specific test suite to run
        **kwargs: Additional test parameters
        
    Returns:
        dict: Test results with success status and details
    """
    logger.info(f"Running regulatory tests with config: {test_config_file}")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="run_tests",
        test_config_file=test_config_file,
        test_suite=test_suite,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Regulatory tests completed: {result.get('message')}")
    else:
        logger.error(f"Regulatory tests failed: {result.get('error')}")
    
    return result


def generate_test_report(test_results, output_format="html", output_path=None, **kwargs):
    """
    Generate test report from test results.
    
    Args:
        test_results (dict): Test results to generate report from
        output_format (str): Output format - "html", "json", "xml"
        output_path (str): Path for output report file
        **kwargs: Additional report parameters
        
    Returns:
        dict: Report generation results
    """
    logger.info(f"Generating test report in {output_format} format")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="generate_report",
        test_results=test_results,
        output_format=output_format,
        output_path=output_path,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Test report generated: {result.get('message')}")
    else:
        logger.error(f"Test report generation failed: {result.get('error')}")
    
    return result


def validate_test_fixtures(fixtures_path=None, **kwargs):
    """
    Validate test fixtures for consistency and completeness.
    
    Args:
        fixtures_path (str): Path to test fixtures directory
        **kwargs: Additional validation parameters
        
    Returns:
        dict: Validation results with success status and details
    """
    logger.info(f"Validating test fixtures at: {fixtures_path}")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="validate_fixtures",
        fixtures_path=fixtures_path,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Test fixtures validation completed: {result.get('message')}")
    else:
        logger.error(f"Test fixtures validation failed: {result.get('error')}")
    
    return result


def generate_test_fixtures(source_data=None, output_path=None, **kwargs):
    """
    Generate test fixtures from source data.
    
    Args:
        source_data: Source data for generating fixtures
        output_path (str): Path to save generated fixtures
        **kwargs: Additional generation parameters
        
    Returns:
        dict: Generation results with success status and details
    """
    logger.info("Generating test fixtures from source data")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="generate_fixtures",
        source_data=source_data,
        output_path=output_path,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Test fixtures generated: {result.get('message')}")
    else:
        logger.error(f"Test fixtures generation failed: {result.get('error')}")
    
    return result


def parse_test_configuration(config_file_path, **kwargs):
    """
    Parse test configuration file.
    
    Args:
        config_file_path (str): Path to test configuration file
        **kwargs: Additional parsing parameters
        
    Returns:
        dict: Parsed configuration with success status
    """
    logger.debug(f"Parsing test configuration: {config_file_path}")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="parse_config",
        config_file_path=config_file_path,
        **kwargs
    )
    
    return result


def execute_datapoint_tests(datapoint_config=None, **kwargs):
    """
    Execute tests for specific datapoints.
    
    Args:
        datapoint_config (dict): Datapoint configuration for testing
        **kwargs: Additional test parameters
        
    Returns:
        dict: Test results with success status and details
    """
    logger.info("Executing datapoint tests")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="test_datapoints",
        datapoint_config=datapoint_config,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Datapoint tests completed: {result.get('message')}")
    else:
        logger.error(f"Datapoint tests failed: {result.get('error')}")
    
    return result


def run_performance_tests(performance_config=None, **kwargs):
    """
    Run performance tests for regulatory processes.
    
    Args:
        performance_config (dict): Performance test configuration
        **kwargs: Additional performance test parameters
        
    Returns:
        dict: Performance test results
    """
    logger.info("Running performance tests")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="performance_tests",
        performance_config=performance_config,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Performance tests completed: {result.get('message')}")
    else:
        logger.error(f"Performance tests failed: {result.get('error')}")
    
    return result


def clean_test_data(cleanup_config=None, **kwargs):
    """
    Clean up test data and temporary files.
    
    Args:
        cleanup_config (dict): Cleanup configuration
        **kwargs: Additional cleanup parameters
        
    Returns:
        dict: Cleanup results
    """
    logger.info("Cleaning test data")
    
    runner = get_regulatory_test_runner()
    result = runner.execute(
        operation="cleanup_tests",
        cleanup_config=cleanup_config,
        **kwargs
    )
    
    if result.get('success'):
        logger.info(f"Test cleanup completed: {result.get('message')}")
    else:
        logger.error(f"Test cleanup failed: {result.get('error')}")
    
    return result


class TestProcessor:
    """
    Main test processor class providing high-level interface.
    Handles regulatory test execution, validation, and reporting.
    """
    
    def __init__(self):
        """Initialize the test processor."""
        self.test_runner = get_regulatory_test_runner()
        logger.info("TestProcessor initialized")
    
    def process_complete_test_workflow(self, test_config_file, generate_report=True, cleanup_after=True):
        """
        Process complete test workflow from configuration to cleanup.
        
        Args:
            test_config_file (str): Path to test configuration file
            generate_report (bool): Whether to generate test report
            cleanup_after (bool): Whether to cleanup after tests
            
        Returns:
            dict: Complete workflow results
        """
        workflow_results = {
            'success': True,
            'steps_completed': [],
            'test_results': None,
            'report_generated': False,
            'cleanup_completed': False,
            'errors': []
        }
        
        try:
            # Step 1: Parse test configuration
            config_result = parse_test_configuration(test_config_file)
            if config_result.get('success'):
                workflow_results['steps_completed'].append('config_parsing')
                test_config = config_result.get('configuration', {})
            else:
                workflow_results['success'] = False
                workflow_results['errors'].append(f"Config parsing failed: {config_result.get('error')}")
                return workflow_results
            
            # Step 2: Validate test fixtures if specified
            if test_config.get('fixtures_path'):
                fixture_result = validate_test_fixtures(test_config['fixtures_path'])
                if fixture_result.get('success'):
                    workflow_results['steps_completed'].append('fixture_validation')
                else:
                    workflow_results['errors'].append(f"Fixture validation failed: {fixture_result.get('error')}")
            
            # Step 3: Run regulatory tests
            test_result = run_regulatory_tests(test_config_file=test_config_file)
            if test_result.get('success'):
                workflow_results['steps_completed'].append('test_execution')
                workflow_results['test_results'] = test_result
            else:
                workflow_results['success'] = False
                workflow_results['errors'].append(f"Test execution failed: {test_result.get('error')}")
                return workflow_results
            
            # Step 4: Generate report if requested
            if generate_report and workflow_results['test_results']:
                report_result = generate_test_report(
                    workflow_results['test_results'],
                    output_format="html",
                    output_path="test_report.html"
                )
                if report_result.get('success'):
                    workflow_results['steps_completed'].append('report_generation')
                    workflow_results['report_generated'] = True
                    workflow_results['report_path'] = report_result.get('output_path')
                else:
                    workflow_results['errors'].append(f"Report generation failed: {report_result.get('error')}")
            
            # Step 5: Cleanup if requested
            if cleanup_after:
                cleanup_result = clean_test_data()
                if cleanup_result.get('success'):
                    workflow_results['steps_completed'].append('cleanup')
                    workflow_results['cleanup_completed'] = True
                else:
                    workflow_results['errors'].append(f"Cleanup failed: {cleanup_result.get('error')}")
            
        except Exception as e:
            workflow_results['success'] = False
            workflow_results['errors'].append(f"Workflow error: {str(e)}")
            logger.error(f"Test workflow error: {e}")
        
        return workflow_results
    
    def run_datapoint_validation_suite(self, datapoint_configs, **kwargs):
        """
        Run validation suite for multiple datapoints.
        
        Args:
            datapoint_configs (list): List of datapoint configurations
            **kwargs: Additional validation parameters
            
        Returns:
            dict: Validation suite results
        """
        suite_results = {
            'success': True,
            'datapoints_tested': 0,
            'datapoints_passed': 0,
            'datapoints_failed': 0,
            'individual_results': [],
            'errors': []
        }
        
        for config in datapoint_configs:
            try:
                result = execute_datapoint_tests(datapoint_config=config, **kwargs)
                suite_results['individual_results'].append(result)
                suite_results['datapoints_tested'] += 1
                
                if result.get('success'):
                    suite_results['datapoints_passed'] += 1
                else:
                    suite_results['datapoints_failed'] += 1
                    suite_results['errors'].append(f"Datapoint {config.get('name', 'unknown')} failed: {result.get('error')}")
                    
            except Exception as e:
                suite_results['datapoints_failed'] += 1
                suite_results['errors'].append(f"Error testing datapoint {config.get('name', 'unknown')}: {str(e)}")
                logger.error(f"Datapoint validation error: {e}")
        
        # Determine overall success
        suite_results['success'] = suite_results['datapoints_failed'] == 0
        
        return suite_results
    
    def benchmark_test_performance(self, test_configs, iterations=1, **kwargs):
        """
        Benchmark test performance across multiple iterations.
        
        Args:
            test_configs (list): List of test configurations to benchmark
            iterations (int): Number of iterations to run
            **kwargs: Additional benchmark parameters
            
        Returns:
            dict: Benchmark results with performance metrics
        """
        benchmark_results = {
            'success': True,
            'iterations_completed': 0,
            'total_iterations': iterations,
            'performance_metrics': {},
            'errors': []
        }
        
        for iteration in range(iterations):
            try:
                iteration_results = {}
                
                for config in test_configs:
                    test_name = config.get('name', f'test_{len(iteration_results)}')
                    perf_result = run_performance_tests(performance_config=config, **kwargs)
                    
                    if perf_result.get('success'):
                        iteration_results[test_name] = perf_result.get('performance_data', {})
                    else:
                        benchmark_results['errors'].append(f"Performance test failed for {test_name}: {perf_result.get('error')}")
                
                # Aggregate performance metrics
                if iteration_results:
                    if iteration == 0:
                        benchmark_results['performance_metrics'] = {name: [data] for name, data in iteration_results.items()}
                    else:
                        for name, data in iteration_results.items():
                            if name in benchmark_results['performance_metrics']:
                                benchmark_results['performance_metrics'][name].append(data)
                
                benchmark_results['iterations_completed'] += 1
                
            except Exception as e:
                benchmark_results['errors'].append(f"Iteration {iteration} error: {str(e)}")
                logger.error(f"Benchmark iteration error: {e}")
        
        # Calculate average performance metrics
        if benchmark_results['performance_metrics']:
            benchmark_results['average_metrics'] = self._calculate_average_metrics(
                benchmark_results['performance_metrics']
            )
        
        benchmark_results['success'] = benchmark_results['iterations_completed'] > 0
        
        return benchmark_results
    
    def _calculate_average_metrics(self, metrics_data):
        """Calculate average performance metrics across iterations."""
        averages = {}
        
        for test_name, iterations_data in metrics_data.items():
            if iterations_data:
                # Assume each iteration_data is a dict with numeric values
                avg_data = {}
                for key in iterations_data[0].keys():
                    values = [data.get(key, 0) for data in iterations_data if isinstance(data.get(key), (int, float))]
                    if values:
                        avg_data[key] = sum(values) / len(values)
                
                averages[test_name] = avg_data
        
        return averages


# Convenience function for backwards compatibility
def run_test_operations():
    """Get a configured test processor instance."""
    return TestProcessor()


# Export main functions for easy access
__all__ = [
    'TestProcessorConfig',
    'get_regulatory_test_runner',
    'run_regulatory_tests',
    'generate_test_report',
    'validate_test_fixtures',
    'generate_test_fixtures',
    'parse_test_configuration',
    'execute_datapoint_tests',
    'run_performance_tests',
    'clean_test_data',
    'TestProcessor',
    'run_test_operations'
]