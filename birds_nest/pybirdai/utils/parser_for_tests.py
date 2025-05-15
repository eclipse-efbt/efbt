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

import json
import sys
from logger_factory import return_logger, Path
logger = return_logger(str(Path(__file__).resolve()).rsplit("/",1)[-1])
from datetime import datetime

FAILURE_STRING = "=================================== FAILURES ==================================="


class PytestOutputParser:
    """
    A class to parse pytest output files and extract test results in structured format.
    """

    def __init__(self, output_file_path, dp_value, reg_tid, dp_suffix, scenario_name):
        """
        Initialize the PytestOutputParser with test information.

        Args:
            output_file_path (str): Path to the pytest output file
            dp_value (str): Datapoint value
            reg_tid (str): Regulatory template ID
            dp_suffix (str): Datapoint suffix
            scenario_name (str): Scenario name
        """
        self.output_file_path = output_file_path
        self.dp_value = dp_value
        self.reg_tid = reg_tid
        self.dp_suffix = dp_suffix
        self.scenario_name = scenario_name
        self.result = self._initialize_result_structure()
        self.output_content = None

    def _initialize_result_structure(self):
        """
        Initialize the result dictionary structure.

        Returns:
            dict: The initialized result structure
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "test_information": {
                "datapoint_value": self.dp_value,
                "regulatory_template_id": self.reg_tid,
                "datapoint_suffix": self.dp_suffix,
                "scenario_name": self.scenario_name
            },
            'platform_info': {
                "os": "",
                "packages": "",
                "python": ""
            },
            'paths': {
                "cachedir": "",
                "rootdir": "",
                "configfile": ""
            },
            'test_results': {
                "passed": [],
                "failed": [],
                "details": {
                    "failures": [],
                    "captured_stdout": [],
                    "captured_stderr": []
                }
            }
        }

    def read_output_file(self):
        """
        Read the pytest output file content.

        Raises:
            Exception: If the file cannot be read
        """
        logger.debug("Reading pytest output file")
        try:
            with open(self.output_file_path, 'r') as f:
                self.output_content = f.read()
        except Exception as e:
            logger.error(f"Error reading output file: {str(e)}")
            raise

    def _parse_platform_info(self, line):
        """
        Parse platform information from a line.

        Args:
            line (str): The line containing platform information

        Returns:
            bool: True if platform info was parsed, False otherwise
        """
        if line.startswith('platform'):
            try:
                platform_parts = line.split('--')
                self.result['platform_info']['os'] = platform_parts[0].replace('platform', '').strip()
                self.result['platform_info']['python'] = platform_parts[2].strip()
                self.result['platform_info']['packages'] = list(map(str.strip,
                    platform_parts[1].split(',')))
            except Exception as e:
                logger.error(f"Error parsing platform info: {str(e)}")
            return True
        return False

    def _parse_path_info(self, line):
        """
        Parse path information from a line.

        Args:
            line (str): The line containing path information

        Returns:
            bool: True if path info was parsed, False otherwise
        """
        path_prefixes = {
            'cachedir:': 'cachedir',
            'rootdir:': 'rootdir',
            'configfile:': 'configfile'
        }

        for prefix, key in path_prefixes.items():
            if line.startswith(prefix):
                self.result['paths'][key] = line.replace(prefix, '').strip()
                return True
        return False

    def _parse_test_result(self, line):
        """
        Parse test result information from a line.

        Args:
            line (str): The line containing test result information

        Returns:
            bool: True if test result was parsed, False otherwise
        """
        if '::' in line:
            test_name = line.split('::')[1].split()[0]
            status_mapping = {k: self.result['test_results'][k.lower()] for k in ["PASSED", "FAILED"]}

            for status, test_list in status_mapping.items():
                if status in line and test_name not in test_list:
                    test_list.append(test_name)
                    logger.debug(f"Test {test_name} {status.lower()}")
                    return True
        return False

    def _parse_failure_details(self, lines, start_index):
        """
        Parse failure details from lines starting at a given index.

        Args:
            lines (list): List of all lines in the output
            start_index (int): Index to start parsing from

        Returns:
            dict: Dictionary of failure details by test name
        """
        logger.debug("Processing test failures")
        failures = {k: [] for k in self.result['test_results']['failed']}
        i = start_index + 1
        key_to_assign = ""

        while i < len(lines):
            line = lines[i].strip()
            for test_name in self.result['test_results']['failed']:
                if f"def {test_name}" in line:
                    key_to_assign = test_name

            if line.startswith('E '):
                failures[key_to_assign].append(line.replace('E ', ''))
            elif '====' in line:  # End of failures section
                break
            i += 1

        return failures

    def parse(self):
        """
        Parse the pytest output content.

        Returns:
            str: JSON string of the parsed result
        """
        if not self.output_content:
            self.read_output_file()

        lines = self.output_content.split('\n')

        logger.debug("Processing test output lines")
        for index, line in enumerate(lines):
            line = line.strip()

            if not line:
                continue

            # Parse platform and path information
            if self._parse_platform_info(line):
                continue

            if self._parse_path_info(line):
                continue

            # Parse test results
            self._parse_test_result(line)

            # Parse failure details
            if line == FAILURE_STRING:
                failures = self._parse_failure_details(lines, index)
                self.result['test_results']['details']['failures'] = failures

        logger.debug("Finished parsing pytest output")
        return json.dumps(self.result, indent=2)


def main():
    """
    Main function to run the pytest output parser from command line.

    Command line arguments:
        1. Path to pytest results file
        2. Datapoint value
        3. Regulatory template ID
        4. Datapoint suffix
        5. Scenario name
    """
    try:
        path_of_results = sys.argv[1]
        dp_value = sys.argv[2]
        reg_tid = sys.argv[3]
        dp_suffix = sys.argv[4]
        scenario_name = sys.argv[5]

        if not path_of_results:
            raise ValueError("No output file path provided")

        parser = PytestOutputParser(path_of_results, dp_value, reg_tid, dp_suffix, scenario_name)
        print(parser.parse())

    except Exception as e:
        logger.error(f"Error running script: {str(e)}")
        raise


if __name__ == '__main__':
    main()
