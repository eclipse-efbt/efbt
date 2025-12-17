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

import os
import json
import logging
from django.conf import settings

def ensure_results_directory():
    """Ensure the test results directory exists and return its path."""
    results_dir = os.path.join(settings.BASE_DIR, 'tests', 'test_results', 'json')

    # Create a directory for the results if it doesn't exist
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    return results_dir

def process_test_results_files(results_dir):
    """Process all JSON test result files in the specified directory."""
    templates = dict()

    try:
        json_files = [f for f in os.listdir(results_dir) if f.endswith('.json')]
        for filename in json_files:
            data = load_test_data(results_dir, filename)
            if data:
                process_test_data(data, templates)
    except Exception as e:
        logging.error(f"Error reading test results: {e}")

    return templates

def load_test_data(results_dir, filename):
    """Load test data from a JSON file."""
    try:
        with open(os.path.join(results_dir, filename)) as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading test data from {filename}: {e}")
        return None

def process_test_data(data, templates):
    """Process a single test data file and update the templates dictionary."""
    template_id = data["test_information"]["regulatory_template_id"]
    template_obj = get_or_create_template(templates, template_id)

    datapoint_suffix = data["test_information"]["datapoint_suffix"]
    datapoint_obj = get_or_create_datapoint(template_obj, datapoint_suffix)

    scenario_name = data["test_information"]["scenario_name"]
    scenario_obj = get_or_create_scenario(datapoint_obj, scenario_name)

    update_scenario_with_tests(scenario_obj, data["test_results"])

def get_or_create_template(templates, template_id):
    """Get or create a template in the templates dictionary."""
    if template_id not in templates:
        templates[template_id] = {
            'name': template_id,
            'datapoints': []
        }
    return templates[template_id]

def get_or_create_datapoint(template_obj, datapoint_suffix):
    """Get or create a datapoint in the template's datapoints list."""
    for dp in template_obj['datapoints']:
        if dp['name'] == datapoint_suffix:
            return dp

    datapoint_obj = {
        "name": datapoint_suffix,
        "scenarios": []
    }
    template_obj['datapoints'].append(datapoint_obj)
    return datapoint_obj

def get_or_create_scenario(datapoint_obj, scenario_name):
    """Get or create a scenario in the datapoint's scenarios list."""
    for sc in datapoint_obj["scenarios"]:
        if sc["name"] == scenario_name:
            return sc

    scenario_obj = {
        "name": scenario_name,
        "tests": [],
        "passed": None
    }
    datapoint_obj["scenarios"].append(scenario_obj)
    return scenario_obj

def update_scenario_with_tests(scenario_obj, test_results):
    """Update the scenario with test results."""
    tests = []

    # Process passed tests
    for test_name in test_results.get("passed", []):
        tests.append({
            "name": test_name,
            "passed": True
        })

    # Process failed tests
    for test_name in test_results.get("failed", []):
        tests.append({
            "name": test_name,
            "passed": False
        })

    scenario_obj["tests"] = tests
    if tests and all(test["passed"] for test in tests):
        scenario_obj["passed"] = True
    elif tests:
        scenario_obj["passed"] = False
