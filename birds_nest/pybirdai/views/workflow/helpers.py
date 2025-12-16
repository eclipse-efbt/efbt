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
#
# Extracted from workflow_views.py

import zlib
import binascii
import logging
import os
import glob
import json

from django.conf import settings
from django.db import OperationalError

from pybirdai.models.workflow_model import WorkflowTaskExecution

logger = logging.getLogger(__name__)


def _discover_test_suites() -> list:
    """
    Discover test suites in the tests/ directory.
    Looks for directories containing suite_manifest.yaml files.

    Returns:
        List of test suite directory names
    """
    test_suites = []
    tests_dir = "tests"

    try:
        # Scan for directories with suite_manifest.yaml
        for item in os.listdir(tests_dir):
            item_path = os.path.join(tests_dir, item)

            # Check if it's a directory
            if not os.path.isdir(item_path):
                continue

            # Check for suite_manifest.yaml
            manifest_bool = os.path.exists(os.path.join(item_path, "suite_manifest.json")) or os.path.exists(os.path.join(item_path, "suite_manifest.yaml"))
            if manifest_bool:
                test_suites.append(item)
                logger.info(f"Discovered test suite: {item}")
    except FileNotFoundError:
        logger.warning(f"Tests directory '{tests_dir}' not found")

    return test_suites

def encode_file_list(file_list):
    """
    Compress and hex-encode a list of filenames for URL transmission.

    Args:
        file_list: List of filenames (strings)

    Returns:
        Hex-encoded string representing compressed file list
    """
    if not file_list:
        return ""

    # Join filenames with pipe separator
    file_string = "|".join(file_list)

    # Compress using zlib
    compressed = zlib.compress(file_string.encode('utf-8'))

    # Convert to hex string
    hex_string = binascii.hexlify(compressed).decode('ascii')

    return hex_string


def refresh_complete_status(task:int=3,all:bool=True):

    try:

        task_to_complete_mapping = {
            1:5,
            2:2,
            3:2,
            4:1
        }

        def check_one_task(execution,task:int=3):
            steps_completed = sum([_ for _ in execution.execution_data.values() if isinstance(_,bool)])
            if (execution.task_number == task) and (steps_completed == task_to_complete_mapping[task]):
                execution.status = "completed"
            return execution

        if all:
            task_executions = WorkflowTaskExecution.objects.all()
            for task_number,_ in task_to_complete_mapping.items():
                for execution in task_executions:
                    execution = check_one_task(execution,task_number)
                    execution.save()
            return

        task_executions = WorkflowTaskExecution.objects.filter(
            task_number=task,
            operation_type='do'
        ).first()
        if isinstance(task_executions,WorkflowTaskExecution):
            task_executions = [task_executions]
        for execution in task_executions:
            execution = check_one_task(execution,task)
            execution.save()
        return
    except OperationalError:
        return


def load_test_results():
    """Load and parse test results from JSON files across all test suites"""
    test_results = []
    # Use Django's BASE_DIR to construct the full path
    base_dir = getattr(settings, 'BASE_DIR', os.getcwd())

    try:
        # Discover all test suites
        test_suites = _discover_test_suites()

        if not test_suites:
            logger.warning("No test suites found")
            return test_results

        logger.info(f"Discovered {len(test_suites)} test suite(s): {', '.join(test_suites)}")

        # Load test results from each suite
        for suite_name in test_suites:
            json_files_path = os.path.join(base_dir, 'tests', suite_name, 'tests', 'test_results', 'json', '*.json')
            logger.info(f"Looking for test results in suite '{suite_name}': {json_files_path}")

            json_files = glob.glob(json_files_path)
            logger.info(f"Found {len(json_files)} JSON file(s) in suite '{suite_name}'")

            for json_file in json_files:
                try:
                    logger.debug(f"Loading test result file: {json_file}")
                    with open(json_file, 'r', encoding='utf-8') as f:
                        result_data = json.load(f)
                        # Add filename and suite name for reference
                        result_data['filename'] = os.path.basename(json_file)
                        result_data['suite_name'] = suite_name
                        test_results.append(result_data)
                        logger.debug(f"Successfully loaded {json_file}")
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"Error loading test result file {json_file}: {e}")
                    continue

        # Sort by timestamp (newest first)
        test_results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        logger.info(f"Loaded {len(test_results)} test result(s) successfully from {len(test_suites)} suite(s)")

    except Exception as e:
        logger.error(f"Error loading test results: {e}")

    return test_results
