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
Views for pushing a local test suite to a new branch of its GitHub repository.

Test data is edited locally - by importing a workbook into a fixture scenario -
and expected results are edited alongside it. These views take both, work out
what differs from the repository the suite came from, and push the difference to
a branch the user names.
"""

import json
import logging
import os

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from pybirdai.utils.datapoint_test_run.test_suite_directory import (
    discover_test_suites,
    find_test_suite,
)
from pybirdai.utils.datapoint_test_run.test_suite_publisher import (
    TestSuitePublisher,
    TestSuitePublishError,
)
from pybirdai.utils.secure_error_handling import SecureErrorHandler
from pybirdai.views.workflow.github import _get_github_token

logger = logging.getLogger(__name__)

AUTOMODE_CONFIG_FILE_NAME = "automode_config.json"
DEFAULT_BASE_BRANCH = "main"

#: A commit message is written into the repository, so it is length-limited and
#: kept to a single line.
MAX_COMMIT_MESSAGE_LENGTH = 500


def _internal_json_error_response(exception: Exception, context: str, request, status: int = 500):
    """Hide implementation details from JSON error responses."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse({"error": error_data["message"]}, status=status)


def _automode_configuration() -> dict:
    """Read the workflow configuration, for the test suite repository defaults."""
    config_path = os.path.join(str(settings.BASE_DIR), AUTOMODE_CONFIG_FILE_NAME)
    try:
        with open(config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
    except (OSError, ValueError) as exc:
        logger.info("Could not read %s for test suite defaults: %s", AUTOMODE_CONFIG_FILE_NAME, exc)
        return {}

    return config if isinstance(config, dict) else {}


def _default_repository_settings() -> dict:
    config = _automode_configuration()
    return {
        "repository_url": str(config.get("test_suite_github_url") or "").strip(),
        "base_branch": str(config.get("test_suite_branch") or "").strip() or DEFAULT_BASE_BRANCH,
    }


def _clean_commit_message(value) -> str:
    """Keep a commit message to one line of a sane length."""
    message = " ".join(str(value or "").split())
    return message[:MAX_COMMIT_MESSAGE_LENGTH]


def _read_publish_request(request):
    """
    Read the parts of a publish request that the preview and the push share.

    Returns:
        A tuple of (suite, publisher, options, error_response). The error
        response is set, and the rest are None, when the request cannot be used.
    """
    try:
        data = json.loads(request.body or "{}")
    except ValueError:
        return None, None, None, JsonResponse({"error": "Invalid request body."}, status=400)

    suite_name = (data.get("suite") or "").strip()
    if not suite_name:
        return None, None, None, JsonResponse({"error": "A test suite is required."}, status=400)

    suite = find_test_suite(str(settings.BASE_DIR), suite_name)
    if suite is None:
        return None, None, None, JsonResponse({"error": "Test suite was not found."}, status=404)

    defaults = _default_repository_settings()
    repository_url = (data.get("repository_url") or "").strip() or defaults["repository_url"]
    if not repository_url:
        return (
            None,
            None,
            None,
            JsonResponse(
                {"error": "A test suite repository URL is required; none is configured for this workflow."},
                status=400,
            ),
        )

    # A token in the request is used for this call only - it is never stored.
    token = (data.get("github_token") or "").strip() or _get_github_token()

    options = {
        "base_branch": (data.get("base_branch") or "").strip() or defaults["base_branch"],
        "include_test_results": bool(data.get("include_test_results", False)),
        "branch_name": (data.get("branch_name") or "").strip(),
        "commit_message": _clean_commit_message(data.get("commit_message")),
    }

    try:
        publisher = TestSuitePublisher(repository_url, token)
    except TestSuitePublishError:
        logger.exception("Failed to initialize test suite publisher during publish request parsing.")
        return (
            None,
            None,
            None,
            JsonResponse({"error": "Unable to initialize test suite publisher."}, status=400),
        )

    return suite, publisher, options, None


@require_http_methods(["GET"])
def publish_test_suite_page(request):
    """Render the page for pushing a test suite to a new GitHub branch."""
    defaults = _default_repository_settings()
    suites = discover_test_suites(str(settings.BASE_DIR))

    return render(
        request,
        "pybirdai/test_suite_publish.html",
        {
            "suites": [suite.as_dict() for suite in suites],
            "default_repository_url": defaults["repository_url"],
            "default_base_branch": defaults["base_branch"],
            "token_available": bool(_get_github_token()),
        },
    )


@require_http_methods(["POST"])
def preview_test_suite_changes(request):
    """
    Report what pushing a suite would change, without changing anything.

    Request Body (JSON):
        suite: the suite's directory name under tests/
        repository_url: the GitHub repository to compare against
        base_branch: the branch to compare against
        include_test_results: compare local test run output too
        github_token: a token for this call only, when none is held already

    Returns:
        JSON listing the files that would be added, changed and removed
    """
    suite, publisher, options, error_response = _read_publish_request(request)
    if error_response is not None:
        return error_response

    try:
        plan = publisher.plan(
            suite.name,
            suite.directory,
            base_branch=options["base_branch"],
            include_test_results=options["include_test_results"],
        )
    except TestSuitePublishError as exc:
        logger.info("Could not compare suite %s with GitHub: %s", suite.name, exc)
        return JsonResponse({"error": "Could not compare the test suite with GitHub."}, status=400)
    except Exception as e:
        return _internal_json_error_response(e, "comparing the test suite with GitHub", request)

    return JsonResponse(plan.as_dict())


@require_http_methods(["POST"])
def publish_test_suite(request):
    """
    Push a suite's local changes to a new branch of its GitHub repository.

    The branch is created by the push and must not already exist, so nothing
    that is already in the repository is moved or overwritten.

    Request Body (JSON):
        suite: the suite's directory name under tests/
        branch_name: the new branch to create
        repository_url: the GitHub repository to push to
        base_branch: the branch the new one starts from
        commit_message: an optional message for the commit
        include_test_results: push local test run output too
        github_token: a token for this call only, when none is held already

    Returns:
        JSON with what was pushed and links to the new branch
    """
    suite, publisher, options, error_response = _read_publish_request(request)
    if error_response is not None:
        return error_response

    if not options["branch_name"]:
        return JsonResponse({"error": "A name for the new branch is required."}, status=400)

    try:
        plan = publisher.plan(
            suite.name,
            suite.directory,
            base_branch=options["base_branch"],
            include_test_results=options["include_test_results"],
        )
        result = publisher.publish(
            plan,
            options["branch_name"],
            commit_message=options["commit_message"],
        )
    except TestSuitePublishError as exc:
        logger.info("Could not push suite %s to GitHub: %s", suite.name, exc)
        return JsonResponse({"error": "Could not push the test suite to GitHub."}, status=400)
    except Exception as e:
        return _internal_json_error_response(e, "pushing the test suite to GitHub", request)

    result["message"] = (
        f"Pushed {result['added_count']} new, {result['modified_count']} changed and "
        f"{result['deleted_count']} removed file(s) to branch '{result['branch']}'"
    )
    return JsonResponse(result)
