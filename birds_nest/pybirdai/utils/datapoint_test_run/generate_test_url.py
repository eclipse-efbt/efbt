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
#!/usr/bin/env python3
import base64
import json
import os
import gzip
from datetime import datetime
from urllib.parse import urlencode

def main(suite_name=None):
    """
    Generate a shareable URL for test results.

    Args:
        suite_name (str, optional): Name of the test suite. If not provided,
            defaults to "basic_test_suite" or searches for available suites.
    """
    # Get all JSON files from suite structure
    # Allow caller to specify suite name, otherwise use environment variable or default
    if suite_name is None:
        suite_name = os.environ.get('TEST_SUITE_NAME', 'basic_test_suite')

    PATH = os.path.join("tests", suite_name, "tests", "test_results", "json")

    # Check if path exists, if not, try to find other suites
    if not os.path.exists(PATH):
        tests_dir = "tests"
        if os.path.exists(tests_dir):
            # Find the first suite directory that has test results
            for item in os.listdir(tests_dir):
                suite_path = os.path.join(tests_dir, item, "tests", "test_results", "json")
                if os.path.exists(suite_path):
                    PATH = suite_path
                    print(f"Using test results from suite: {item}")
                    break

        # If still no path found, create the default path structure
        if not os.path.exists(PATH):
            os.makedirs(PATH, exist_ok=True)
            print(f"Created directory: {PATH}")

    json_files = []
    if os.path.exists(PATH):
        current_year = str(datetime.now().year)
        json_files = [f for f in os.listdir(PATH) if f.endswith('.json') and f.startswith(current_year)]
        json_files.sort()

    # Collect all test data
    all_tests = []
    common_platform_info = None
    common_paths = None

    for filename in json_files:
        with open(os.path.join(PATH,filename), 'r') as f:
            data = json.load(f)

        # Extract common data from first file
        if common_platform_info is None:
            common_platform_info = data.get('platform_info', {})
            common_paths = data.get('paths', {})

        # Create compact test entry
        compact_test = {
            't': data['timestamp'],
            'i': {
                'v': data['test_information']['datapoint_value'],
                'r': data['test_information']['regulatory_template_id'],
                'd': data['test_information']['datapoint_suffix'],
                's': data['test_information']['scenario_name']
            },
            'p': data['test_results']['passed'],
            'f': data['test_results']['failed'],
        }

        # Only include failure details if there are failures
        if data['test_results']['details']['failures']:
            compact_test['fd'] = data['test_results']['details']['failures']

        # Only include stdout/stderr if not empty
        if data['test_results']['details']['captured_stdout']:
            compact_test['so'] = data['test_results']['details']['captured_stdout']
        if data['test_results']['details']['captured_stderr']:
            compact_test['se'] = data['test_results']['details']['captured_stderr']

        all_tests.append(compact_test)

    # Create combined data structure
    combined_data = {
        'v': 1,  # Version for future compatibility
        'pi': common_platform_info,
        'pa': common_paths,
        't': all_tests
    }

    # Convert to JSON and compress
    json_str = json.dumps(combined_data, separators=(',', ':'))
    compressed = gzip.compress(json_str.encode())
    encoded = base64.urlsafe_b64encode(compressed).decode()

    # Generate the URL
    base_url = "index.html"
    params = {'d': encoded}
    query_string = urlencode(params)

    print(f"Please see the test report at https://freebird-test-result.github.io?{query_string}")

if __name__ == "__main__":
    main()
