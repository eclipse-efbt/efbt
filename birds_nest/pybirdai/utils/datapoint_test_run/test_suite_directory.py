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
Find the test suites that have been fetched into the project's tests directory.

A suite is one directory under ``tests/``: it holds the suite's own
``configuration_file_tests.json`` and a ``tests/fixtures/templates`` tree. The
directory is named after the repository the suite was fetched from - see
``CloneRepoService.setup_test_suite_files`` - which is what lets local changes
be pushed back to that repository.

Suites are always looked up by name against this listing rather than taken from
a request, so a caller can never reach a directory outside ``tests/``.
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

TESTS_DIRECTORY_NAME = "tests"
TEST_CONFIG_FILE_NAME = "configuration_file_tests.json"
SUITE_MANIFEST_FILE_NAME = "suite_manifest.json"
TEMPLATES_RELATIVE_PATH = os.path.join("tests", "fixtures", "templates")

#: Directory entries under tests/ that are part of the Python package rather
#: than a fetched suite.
NON_SUITE_ENTRIES = {"__pycache__"}


@dataclass(frozen=True)
class TestSuite:
    """One test suite directory below ``tests/``."""

    name: str
    #: Path from the project root, e.g. "tests/bird-default-test-suite-eil-67"
    project_path: str
    #: Absolute path to the same directory
    directory: str
    has_config: bool = False
    display_name: str = ""

    @property
    def config_project_path(self) -> str:
        return os.path.join(self.project_path, TEST_CONFIG_FILE_NAME)

    @property
    def config_path(self) -> str:
        return os.path.join(self.directory, TEST_CONFIG_FILE_NAME)

    @property
    def templates_directory(self) -> str:
        return os.path.join(self.directory, TEMPLATES_RELATIVE_PATH)

    @property
    def label(self) -> str:
        return f"{self.display_name} ({self.name})" if self.display_name else self.name

    def as_dict(self) -> Dict:
        return {
            "name": self.name,
            "label": self.label,
            "display_name": self.display_name,
            "project_path": self.project_path,
            "config_project_path": self.config_project_path,
            "has_config": self.has_config,
        }


def _read_display_name(suite_directory: str) -> str:
    """Read the human-readable suite name from its manifest, if there is one."""
    manifest_path = os.path.join(suite_directory, SUITE_MANIFEST_FILE_NAME)
    try:
        with open(manifest_path, "r", encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
    except (OSError, ValueError):
        return ""

    if not isinstance(manifest, dict):
        return ""

    metadata = manifest.get("metadata")
    if isinstance(metadata, dict) and metadata.get("display_name"):
        return str(metadata["display_name"])

    return str(manifest.get("name") or "")


def discover_test_suites(base_dir: str) -> List[TestSuite]:
    """
    List the test suites present under the project's tests directory.

    Args:
        base_dir: The project root (Django's BASE_DIR)

    Returns:
        Suites sorted by directory name
    """
    tests_dir = os.path.join(base_dir, TESTS_DIRECTORY_NAME)
    if not os.path.isdir(tests_dir):
        return []

    suites: List[TestSuite] = []
    for entry in sorted(os.listdir(tests_dir)):
        if entry.startswith(".") or entry in NON_SUITE_ENTRIES:
            continue

        directory = os.path.join(tests_dir, entry)
        if not os.path.isdir(directory):
            continue

        has_config = os.path.isfile(os.path.join(directory, TEST_CONFIG_FILE_NAME))
        has_templates = os.path.isdir(os.path.join(directory, TEMPLATES_RELATIVE_PATH))
        if not has_config and not has_templates:
            continue

        suites.append(
            TestSuite(
                name=entry,
                project_path=os.path.join(TESTS_DIRECTORY_NAME, entry),
                directory=directory,
                has_config=has_config,
                display_name=_read_display_name(directory),
            )
        )

    return suites


def find_test_suite(base_dir: str, name: str) -> Optional[TestSuite]:
    """Return the discovered suite with this directory name, if it exists.

    Matching against the listing - rather than joining the name onto a path -
    keeps callers inside the tests directory whatever the request contained.
    """
    for suite in discover_test_suites(base_dir):
        if suite.name == name:
            return suite
    return None
