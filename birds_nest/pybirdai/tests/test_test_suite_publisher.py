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

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase, override_settings

from pybirdai.utils.datapoint_test_run.test_suite_publisher import (
    TestSuitePublisher,
    TestSuitePublishError,
    collect_suite_files,
    git_blob_sha,
)
from pybirdai.views.test_suite_publish_views import (
    preview_test_suite_changes,
    publish_test_suite,
)

SUITE_NAME = "bird-test-suite"
REPOSITORY_URL = "https://github.com/regcommunity/bird-test-suite"
TOKEN = "ghp_testtoken"

BASE_COMMIT_SHA = "1111111111111111111111111111111111111111"
BASE_TREE_SHA = "2222222222222222222222222222222222222222"


def _write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_suite(base_dir, files=None):
    """Create a local suite directory holding the given repository-relative files."""
    suite_dir = Path(base_dir) / "tests" / SUITE_NAME
    for repo_path, content in (files or {}).items():
        _write(suite_dir / Path(repo_path), content)
    suite_dir.mkdir(parents=True, exist_ok=True)
    return suite_dir


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


class FakeGitHub:
    """A stand-in for the repository, recording what a push sends it."""

    def __init__(self, remote_files=None, branches=("main",), truncated=False):
        self.remote_files = dict(remote_files or {})
        self.branches = set(branches)
        self.truncated = truncated
        self.uploaded_blobs = []
        self.tree_entries = None
        self.commit = None
        self.created_ref = None

    def __call__(self, method, *parts, **kwargs):
        path = "/".join(parts)
        payload = kwargs.get("json") or {}

        if method == "GET" and path.startswith("git/ref/heads/"):
            branch = path[len("git/ref/heads/"):]
            if branch not in self.branches:
                return FakeResponse(404, {"message": "Not Found"})
            return FakeResponse(200, {"object": {"sha": BASE_COMMIT_SHA}})

        if method == "GET" and path == f"git/commits/{BASE_COMMIT_SHA}":
            return FakeResponse(200, {"tree": {"sha": BASE_TREE_SHA}})

        if method == "GET" and path == f"git/trees/{BASE_TREE_SHA}":
            return FakeResponse(
                200,
                {
                    "truncated": self.truncated,
                    "tree": [
                        {"path": repo_path, "type": "blob", "sha": git_blob_sha(content.encode("utf-8"))}
                        for repo_path, content in self.remote_files.items()
                    ],
                },
            )

        if method == "POST" and path == "git/blobs":
            self.uploaded_blobs.append(payload)
            return FakeResponse(201, {"sha": f"blob-{len(self.uploaded_blobs)}"})

        if method == "POST" and path == "git/trees":
            self.tree_entries = payload["tree"]
            return FakeResponse(201, {"sha": "new-tree-sha"})

        if method == "POST" and path == "git/commits":
            self.commit = payload
            return FakeResponse(201, {"sha": "new-commit-sha"})

        if method == "POST" and path == "git/refs":
            self.created_ref = payload
            return FakeResponse(201, payload)

        raise AssertionError(f"Unexpected GitHub call: {method} {path}")


def _publisher(fake_github):
    publisher = TestSuitePublisher(REPOSITORY_URL, TOKEN)
    publisher._request = fake_github
    return publisher


class GitBlobShaTests(unittest.TestCase):
    def test_the_hash_matches_the_one_git_stores(self):
        # git hash-object of a file holding "hello\n"
        self.assertEqual(git_blob_sha(b"hello\n"), "ce013625030ba8dba906f756967f9e9ca394464a")


class CollectSuiteFilesTests(unittest.TestCase):
    def test_the_suite_source_is_collected_with_repository_paths(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(
                base_dir,
                {
                    "configuration_file_tests.json": "{}",
                    "tests/fixtures/templates/F_05_01/DP/scenario_1/prty.csv": "a\n",
                },
            )

            files = collect_suite_files(str(suite_dir))

            self.assertEqual(
                [suite_file.repo_path for suite_file in files],
                ["configuration_file_tests.json", "tests/fixtures/templates/F_05_01/DP/scenario_1/prty.csv"],
            )

    def test_caches_hidden_files_and_test_output_are_left_out(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(
                base_dir,
                {
                    "configuration_file_tests.json": "{}",
                    "tests/code/__pycache__/test_cell.cpython-313.pyc": "cached",
                    "tests/code/test_cell.pyc": "compiled",
                    ".gitignore": "ignored",
                    "tests/test_results/json/20260802__results.json": "{}",
                },
            )

            collected = {suite_file.repo_path for suite_file in collect_suite_files(str(suite_dir))}

            self.assertEqual(collected, {"configuration_file_tests.json"})

    def test_test_output_can_be_asked_for(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(
                base_dir,
                {
                    "configuration_file_tests.json": "{}",
                    "tests/test_results/json/20260802__results.json": "{}",
                },
            )

            collected = {
                suite_file.repo_path for suite_file in collect_suite_files(str(suite_dir), include_test_results=True)
            }

            self.assertIn("tests/test_results/json/20260802__results.json", collected)


class PublishPlanTests(unittest.TestCase):
    def test_new_changed_and_removed_files_are_worked_out(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(
                base_dir,
                {
                    "configuration_file_tests.json": '{"dp_value": 42}',
                    "tests/fixtures/templates/F_05_01/DP/scenario_1/prty.csv": "unchanged\n",
                    "tests/fixtures/templates/F_05_01/DP/scenario_2/prty.csv": "new scenario\n",
                },
            )
            github = FakeGitHub(
                remote_files={
                    "configuration_file_tests.json": '{"dp_value": 11}',
                    "tests/fixtures/templates/F_05_01/DP/scenario_1/prty.csv": "unchanged\n",
                    "tests/fixtures/templates/F_05_01/DP/scenario_3/prty.csv": "deleted locally\n",
                    "README.md": "documentation the fetch never copies\n",
                }
            )

            plan = _publisher(github).plan(SUITE_NAME, str(suite_dir))

            self.assertEqual(plan.added, ["tests/fixtures/templates/F_05_01/DP/scenario_2/prty.csv"])
            self.assertEqual(plan.modified, ["configuration_file_tests.json"])
            self.assertEqual(plan.deleted, ["tests/fixtures/templates/F_05_01/DP/scenario_3/prty.csv"])
            self.assertEqual(plan.unchanged_count, 1)
            self.assertTrue(plan.has_changes)

    def test_a_suite_that_matches_the_branch_has_no_changes(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir, {"configuration_file_tests.json": "{}"})
            github = FakeGitHub(remote_files={"configuration_file_tests.json": "{}"})

            plan = _publisher(github).plan(SUITE_NAME, str(suite_dir))

            self.assertFalse(plan.has_changes)

    def test_a_missing_base_branch_is_reported(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir, {"configuration_file_tests.json": "{}"})
            github = FakeGitHub(branches=("main",))

            with self.assertRaises(TestSuitePublishError):
                _publisher(github).plan(SUITE_NAME, str(suite_dir), base_branch="no-such-branch")

    def test_a_file_list_that_github_truncated_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir, {"configuration_file_tests.json": "{}"})
            github = FakeGitHub(remote_files={"configuration_file_tests.json": "x"}, truncated=True)

            with self.assertRaises(TestSuitePublishError) as raised:
                _publisher(github).plan(SUITE_NAME, str(suite_dir))

            self.assertIn("too large", str(raised.exception))

    def test_an_empty_suite_directory_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)

            with self.assertRaises(TestSuitePublishError):
                _publisher(FakeGitHub()).plan(SUITE_NAME, str(suite_dir))


class PublishTests(unittest.TestCase):
    def _plan_and_publish(self, github, base_dir, branch_name="new-test-data", commit_message=None):
        suite_dir = _make_suite(
            base_dir,
            {
                "configuration_file_tests.json": '{"dp_value": 42}',
                "tests/fixtures/templates/F_05_01/DP/scenario_2/prty.csv": "new scenario\n",
            },
        )
        publisher = _publisher(github)
        plan = publisher.plan(SUITE_NAME, str(suite_dir))
        return publisher.publish(plan, branch_name, commit_message=commit_message)

    def test_the_commit_carries_the_changes_and_the_branch_points_at_it(self):
        with tempfile.TemporaryDirectory() as base_dir:
            github = FakeGitHub(
                remote_files={
                    "configuration_file_tests.json": '{"dp_value": 11}',
                    "tests/fixtures/templates/F_05_01/DP/scenario_3/prty.csv": "deleted locally\n",
                }
            )

            result = self._plan_and_publish(github, base_dir)

            self.assertTrue(result["success"])
            self.assertEqual(result["branch"], "new-test-data")
            self.assertEqual(result["commit_sha"], "new-commit-sha")
            self.assertEqual(github.created_ref, {"ref": "refs/heads/new-test-data", "sha": "new-commit-sha"})
            self.assertEqual(github.commit["parents"], [BASE_COMMIT_SHA])
            self.assertEqual(len(github.uploaded_blobs), 2)

            by_path = {entry["path"]: entry for entry in github.tree_entries}
            self.assertIsNone(by_path["tests/fixtures/templates/F_05_01/DP/scenario_3/prty.csv"]["sha"])
            self.assertIsNotNone(by_path["configuration_file_tests.json"]["sha"])

    def test_the_commit_message_can_be_given(self):
        with tempfile.TemporaryDirectory() as base_dir:
            github = FakeGitHub(remote_files={"configuration_file_tests.json": "{}"})

            self._plan_and_publish(github, base_dir, commit_message="New expected value for F 05.01")

            self.assertEqual(github.commit["message"], "New expected value for F 05.01")

    def test_a_branch_that_already_exists_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            github = FakeGitHub(remote_files={"configuration_file_tests.json": "{}"}, branches=("main", "taken"))

            with self.assertRaises(TestSuitePublishError) as raised:
                self._plan_and_publish(github, base_dir, branch_name="taken")

            self.assertIn("already exists", str(raised.exception))
            self.assertIsNone(github.created_ref)

    def test_a_branch_name_git_could_not_use_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            github = FakeGitHub(remote_files={"configuration_file_tests.json": "{}"})

            with self.assertRaises(TestSuitePublishError):
                self._plan_and_publish(github, base_dir, branch_name="not a branch name")

    def test_pushing_nothing_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir, {"configuration_file_tests.json": "{}"})
            github = FakeGitHub(remote_files={"configuration_file_tests.json": "{}"})
            publisher = _publisher(github)

            plan = publisher.plan(SUITE_NAME, str(suite_dir))
            with self.assertRaises(TestSuitePublishError) as raised:
                publisher.publish(plan, "new-test-data")

            self.assertIn("nothing to push", str(raised.exception))


class PublisherConstructionTests(unittest.TestCase):
    def test_a_missing_token_is_reported(self):
        with self.assertRaises(TestSuitePublishError):
            TestSuitePublisher(REPOSITORY_URL, "")

    def test_a_url_that_is_not_a_github_repository_is_reported(self):
        with self.assertRaises(TestSuitePublishError):
            TestSuitePublisher("https://example.com/not/github", TOKEN)


class PublishViewTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def _post(self, view, body, base_dir, github=None):
        request = self.factory.post("/api/test-data/publish/", data=json.dumps(body), content_type="application/json")
        with override_settings(BASE_DIR=base_dir):
            if github is None:
                return view(request)
            with patch.object(TestSuitePublisher, "_request", github):
                return view(request)

    def test_a_preview_reports_what_would_change(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, {"configuration_file_tests.json": '{"dp_value": 42}'})
            github = FakeGitHub(remote_files={"configuration_file_tests.json": '{"dp_value": 11}'})

            response = self._post(
                preview_test_suite_changes,
                {"suite": SUITE_NAME, "repository_url": REPOSITORY_URL, "github_token": TOKEN},
                base_dir,
                github,
            )

            self.assertEqual(response.status_code, 200)
            payload = json.loads(response.content)
            self.assertEqual(payload["modified"], ["configuration_file_tests.json"])
            self.assertTrue(payload["has_changes"])

    def test_publishing_returns_the_new_branch(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, {"configuration_file_tests.json": '{"dp_value": 42}'})
            github = FakeGitHub(remote_files={"configuration_file_tests.json": '{"dp_value": 11}'})

            response = self._post(
                publish_test_suite,
                {
                    "suite": SUITE_NAME,
                    "repository_url": REPOSITORY_URL,
                    "github_token": TOKEN,
                    "branch_name": "my-updated-test-data",
                },
                base_dir,
                github,
            )

            self.assertEqual(response.status_code, 200)
            payload = json.loads(response.content)
            self.assertEqual(payload["branch"], "my-updated-test-data")
            self.assertEqual(
                payload["branch_url"],
                "https://github.com/regcommunity/bird-test-suite/tree/my-updated-test-data",
            )
            self.assertEqual(github.created_ref["ref"], "refs/heads/my-updated-test-data")

    def test_publishing_without_a_branch_name_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, {"configuration_file_tests.json": "{}"})

            response = self._post(
                publish_test_suite,
                {"suite": SUITE_NAME, "repository_url": REPOSITORY_URL, "github_token": TOKEN},
                base_dir,
            )

            self.assertEqual(response.status_code, 400)
            self.assertIn("branch", json.loads(response.content)["error"])

    def test_an_unknown_suite_is_reported(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, {"configuration_file_tests.json": "{}"})

            response = self._post(
                preview_test_suite_changes,
                {"suite": "no-such-suite", "repository_url": REPOSITORY_URL, "github_token": TOKEN},
                base_dir,
            )

            self.assertEqual(response.status_code, 404)

    def test_a_request_with_no_token_anywhere_is_reported(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, {"configuration_file_tests.json": "{}"})

            with patch("pybirdai.views.test_suite_publish_views._get_github_token", return_value=""):
                response = self._post(
                    preview_test_suite_changes,
                    {"suite": SUITE_NAME, "repository_url": REPOSITORY_URL},
                    base_dir,
                )

            self.assertEqual(response.status_code, 400)
            self.assertIn("token", json.loads(response.content)["error"])


if __name__ == "__main__":
    unittest.main()
