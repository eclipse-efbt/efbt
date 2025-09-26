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
from urllib.parse import urlencode

def find_test_results_directories():
    """Find all test results directories (both legacy and suite-based)"""
    result_paths = []

    # Check legacy location
    legacy_path = os.path.join("tests", "test_results", "json")
    if os.path.exists(legacy_path):
        result_paths.append(legacy_path)

    # Check for test suites in tests/ directory
    tests_dir = "tests"
    if os.path.exists(tests_dir):
        for item in os.listdir(tests_dir):
            suite_path = os.path.join(tests_dir, item, "tests", "test_results", "json")
            if os.path.exists(suite_path):
                result_paths.append(suite_path)

    return result_paths

def main():

    # Find all test results directories
    paths = find_test_results_directories()

    # Get all JSON files from all paths
    all_json_files = []
    for PATH in paths:
        json_files = [os.path.join(PATH, f) for f in os.listdir(PATH) if f.endswith('.json') and f.startswith('2025')]
        all_json_files.extend(json_files)

    all_json_files.sort()

    # Collect all test data
    all_tests = []
    common_platform_info = None
    common_paths = None

    for json_file_path in all_json_files:
        with open(json_file_path, 'r') as f:
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
