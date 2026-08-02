# coding=UTF-8
# Copyright (c) 2026 Bird Software Solutions Ltd
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
"""
Views for editing the expected results held in configuration_file_tests.json.

Each test suite keeps the value every datapoint should produce in its own
configuration file. Changing test data usually changes those values, so these
views edit them next to the fixture pages rather than leaving it to a text
editor.
"""

import json
import logging

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from pybirdai.utils.datapoint_test_run.expected_results_editor import (
    ExpectedResultsError,
    read_test_configuration,
    save_test_entries,
    scenario_choices,
)
from pybirdai.utils.datapoint_test_run.test_suite_directory import discover_test_suites
from pybirdai.utils.secure_error_handling import SecureErrorHandler

logger = logging.getLogger(__name__)


def _internal_json_error_response(exception: Exception, context: str, request, status: int = 500):
    """Hide implementation details from JSON error responses."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse({"error": error_data["message"]}, status=status)


@require_http_methods(["GET"])
def expected_results_page(request):
    """Render the page for editing a suite's expected results."""
    suites = [suite.as_dict() for suite in discover_test_suites(str(settings.BASE_DIR)) if suite.has_config]
    return render(
        request,
        "pybirdai/expected_results_edit.html",
        {"suites": suites},
    )


@require_http_methods(["GET"])
def list_expected_results(request):
    """
    Return one suite's expected results, with the scenarios that can be tested.

    Query Parameters:
        suite: the suite's directory name under tests/

    Returns:
        JSON holding the configuration and the suite's fixture scenarios
    """
    suite_name = (request.GET.get("suite") or "").strip()
    if not suite_name:
        return JsonResponse({"error": "A test suite is required."}, status=400)

    try:
        base_dir = str(settings.BASE_DIR)
        configuration = read_test_configuration(base_dir, suite_name)
        payload = configuration.as_dict()
        payload["scenarios"] = scenario_choices(base_dir, suite_name)
        return JsonResponse(payload)
    except ExpectedResultsError as exc:
        logger.info("Could not read expected results for %s: %s", suite_name, exc)
        return JsonResponse({"error": "Unable to read expected results for the selected test suite."}, status=400)
    except Exception as e:
        return _internal_json_error_response(e, "reading expected test results", request)


@require_http_methods(["POST"])
def save_expected_results(request):
    """
    Write a suite's expected results back to its configuration file.

    The submitted entries replace the file's ``tests`` array in full, so a save
    can edit, add and remove tests at once. Anything else the document holds is
    left as it was.

    Request Body (JSON):
        suite: the suite's directory name under tests/
        entries: the tests to write, each with ``identity`` and
            ``expected_value``, and the ``index`` it was read from when it is
            an edit of an existing test

    Returns:
        JSON describing what was written
    """
    try:
        try:
            data = json.loads(request.body or "{}")
        except ValueError:
            return JsonResponse({"error": "Invalid request body."}, status=400)

        suite_name = (data.get("suite") or "").strip()
        if not suite_name:
            return JsonResponse({"error": "A test suite is required."}, status=400)

        try:
            result = save_test_entries(str(settings.BASE_DIR), suite_name, data.get("entries", []))
        except ExpectedResultsError as exc:
            logger.info("Rejected expected results for %s: %s", suite_name, exc)
            return JsonResponse({"error": str(exc)}, status=400)

        result["success"] = True
        result["message"] = f"Saved {result['test_count']} expected result(s) to {result['config_project_path']}"
        return JsonResponse(result)

    except Exception as e:
        return _internal_json_error_response(e, "saving expected test results", request)
