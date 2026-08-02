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
Read and write the expected results in a suite's configuration_file_tests.json.

Every entry in that file's ``tests`` array names a fixture scenario and the
value the datapoint is expected to produce when the scenario is loaded. Editing
test data usually changes that value, so it is edited here rather than by hand.

Two shapes of entry exist, and both are kept as the test runners read them:

* FINREP datapoint tests - ``reg_tid``, ``dp_suffix``, ``scenario`` and the
  expected ``dp_value``
* ANCRDT table tests - ``table_name``, ``scenario`` and the expected
  ``expected_rows``

Keys that neither shape defines are carried through untouched, so a suite can
hold fields this editor knows nothing about without losing them on save.

The identifying fields become directory names when the runner looks for the
scenario's fixtures, so they are restricted to characters that cannot escape the
templates directory.
"""

import json
import logging
import math
import os
import re
import tempfile
from dataclasses import dataclass, field
from typing import Any, Dict, List

from pybirdai.utils.datapoint_test_run.test_suite_directory import (
    TEST_CONFIG_FILE_NAME,
    TestSuite,
    find_test_suite,
)

logger = logging.getLogger(__name__)

FINREP_TEST_TYPE = "finrep"
ANCRDT_TEST_TYPE = "ancrdt"

#: The fields that identify a test, in the order they name directories below
#: ``tests/fixtures/templates``.
IDENTITY_FIELDS = {
    FINREP_TEST_TYPE: ("reg_tid", "dp_suffix", "scenario"),
    ANCRDT_TEST_TYPE: ("table_name", "scenario"),
}

#: The field holding the expected result for each shape of test.
EXPECTED_VALUE_FIELD = {
    FINREP_TEST_TYPE: "dp_value",
    ANCRDT_TEST_TYPE: "expected_rows",
}

#: A FINREP entry may leave the scenario out, which asks the runner to run every
#: scenario under the datapoint against the same expected value.
OPTIONAL_IDENTITY_FIELDS = {"scenario"}

#: Identifiers are used as directory names by the test runners.
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
MAX_IDENTIFIER_LENGTH = 200

#: A guard against a request that would rewrite the file into something no
#: longer reviewable.
MAX_TEST_ENTRIES = 5000


class ExpectedResultsError(ValueError):
    """A request that cannot be applied to the configuration file."""


@dataclass
class TestEntry:
    """One entry of the ``tests`` array, as the editor presents it."""

    index: int
    identity: Dict[str, str]
    expected_value: Any
    expected_field: str
    scenario_project_path: str = ""
    scenario_exists: bool = False
    other_fields: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict:
        return {
            "index": self.index,
            "identity": self.identity,
            "expected_value": self.expected_value,
            "expected_field": self.expected_field,
            "scenario_project_path": self.scenario_project_path,
            "scenario_exists": self.scenario_exists,
            "other_fields": self.other_fields,
        }


@dataclass
class TestConfiguration:
    """A suite's configuration file, split into what is editable and what is not."""

    suite: TestSuite
    test_type: str
    entries: List[TestEntry]
    document: Dict[str, Any]

    def as_dict(self) -> Dict:
        return {
            "suite": self.suite.as_dict(),
            "test_type": self.test_type,
            "identity_fields": list(IDENTITY_FIELDS[self.test_type]),
            "optional_identity_fields": sorted(
                field_name
                for field_name in IDENTITY_FIELDS[self.test_type]
                if field_name in OPTIONAL_IDENTITY_FIELDS
            ),
            "expected_field": EXPECTED_VALUE_FIELD[self.test_type],
            "entries": [entry.as_dict() for entry in self.entries],
        }


def detect_test_type(document: Dict[str, Any]) -> str:
    """
    Work out which shape of test the file holds.

    Mirrors what ``RegulatoryTemplateTestRunner.run_tests_from_config`` does: an
    explicit ``test_type`` wins, otherwise the first entry's fields decide, and
    FINREP is the fallback.
    """
    declared_type = str(document.get("test_type") or "").strip().lower()
    if declared_type in IDENTITY_FIELDS:
        return declared_type

    tests = document.get("tests")
    if isinstance(tests, list) and tests and isinstance(tests[0], dict):
        if "table_name" in tests[0]:
            return ANCRDT_TEST_TYPE

    return FINREP_TEST_TYPE


def _load_document(config_path: str) -> Dict[str, Any]:
    try:
        with open(config_path, "r", encoding="utf-8") as config_file:
            document = json.load(config_file)
    except FileNotFoundError as exc:
        raise ExpectedResultsError("The suite has no configuration_file_tests.json.") from exc
    except ValueError as exc:
        raise ExpectedResultsError("The suite's configuration_file_tests.json is not valid JSON.") from exc

    if not isinstance(document, dict):
        raise ExpectedResultsError("The suite's configuration_file_tests.json is not a JSON object.")

    tests = document.get("tests", [])
    if not isinstance(tests, list):
        raise ExpectedResultsError("The 'tests' entry of configuration_file_tests.json is not a list.")

    return document


def _resolve_suite(base_dir: str, suite_name: str) -> TestSuite:
    suite = find_test_suite(base_dir, suite_name)
    if suite is None:
        raise ExpectedResultsError("Test suite was not found.")
    return suite


def _scenario_directory(suite: TestSuite, test_type: str, identity: Dict[str, str]) -> str:
    """The fixture directory a test entry points at, or "" when it names none."""
    parts = [identity.get(name, "") for name in IDENTITY_FIELDS[test_type]]
    parts = [part for part in parts if part]
    if not parts:
        return ""
    return os.path.join(suite.templates_directory, *parts)


def read_test_configuration(base_dir: str, suite_name: str) -> TestConfiguration:
    """
    Read one suite's test configuration.

    Args:
        base_dir: The project root (Django's BASE_DIR)
        suite_name: Directory name of a suite under ``tests/``

    Returns:
        The configuration, with each entry split into identity, expected value
        and any other fields the entry carries

    Raises:
        ExpectedResultsError: the suite or its configuration file is unusable
    """
    suite = _resolve_suite(base_dir, suite_name)
    document = _load_document(suite.config_path)
    test_type = detect_test_type(document)
    identity_fields = IDENTITY_FIELDS[test_type]
    expected_field = EXPECTED_VALUE_FIELD[test_type]

    entries: List[TestEntry] = []
    for index, raw_entry in enumerate(document.get("tests", [])):
        if not isinstance(raw_entry, dict):
            logger.warning("Skipping entry %s of %s: not an object", index, suite.config_project_path)
            continue

        identity = {name: str(raw_entry.get(name) or "") for name in identity_fields}
        other_fields = {
            key: value
            for key, value in raw_entry.items()
            if key not in identity_fields and key != expected_field
        }

        scenario_directory = _scenario_directory(suite, test_type, identity)
        entries.append(
            TestEntry(
                index=index,
                identity=identity,
                expected_value=raw_entry.get(expected_field),
                expected_field=expected_field,
                scenario_project_path=(
                    os.path.relpath(scenario_directory, base_dir) if scenario_directory else ""
                ),
                scenario_exists=bool(scenario_directory) and os.path.isdir(scenario_directory),
                other_fields=other_fields,
            )
        )

    return TestConfiguration(suite=suite, test_type=test_type, entries=entries, document=document)


def _validate_identifier(value: Any, field_name: str) -> str:
    value = str(value or "").strip()
    if len(value) > MAX_IDENTIFIER_LENGTH or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ExpectedResultsError(
            f"'{field_name}' may only contain letters, digits, '.', '_' and '-'."
        )
    return value


def _parse_expected_value(value: Any, expected_field: str) -> Any:
    """
    Read an expected result as a number.

    Both shapes of test compare against a number, and the runner passes the
    value on as a string, so anything that is not numeric would fail later with
    a much less obvious message.
    """
    if isinstance(value, bool):
        raise ExpectedResultsError(f"'{expected_field}' must be a number.")

    if isinstance(value, (int, float)):
        number = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ExpectedResultsError(f"'{expected_field}' is required.")
        try:
            number = float(text)
        except ValueError as exc:
            raise ExpectedResultsError(f"'{expected_field}' must be a number.") from exc

    if isinstance(number, float):
        if math.isnan(number) or math.isinf(number):
            raise ExpectedResultsError(f"'{expected_field}' must be a number.")
        # Keep whole numbers whole: the file stores 11 rather than 11.0.
        if number.is_integer():
            number = int(number)

    if expected_field == EXPECTED_VALUE_FIELD[ANCRDT_TEST_TYPE]:
        if not isinstance(number, int) or number < 0:
            raise ExpectedResultsError(f"'{expected_field}' must be a whole number of rows.")

    return number


def build_test_entries(
    configuration: TestConfiguration,
    submitted_entries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Turn the entries a request submitted into the ``tests`` array to be written.

    Each submitted entry may carry the index of the entry it came from; the
    fields that entry holds beyond identity and expected value are kept, so a
    save never drops what the editor does not show.

    Raises:
        ExpectedResultsError: an entry is missing a field, or holds a value that
            the test runner could not use
    """
    if not isinstance(submitted_entries, list):
        raise ExpectedResultsError("The submitted entries are not a list.")

    if len(submitted_entries) > MAX_TEST_ENTRIES:
        raise ExpectedResultsError(f"A suite may hold at most {MAX_TEST_ENTRIES} tests.")

    identity_fields = IDENTITY_FIELDS[configuration.test_type]
    expected_field = EXPECTED_VALUE_FIELD[configuration.test_type]
    original_entries = configuration.document.get("tests", [])

    rebuilt: List[Dict[str, Any]] = []
    seen_identities = set()

    for position, submitted in enumerate(submitted_entries, start=1):
        if not isinstance(submitted, dict):
            raise ExpectedResultsError(f"Test {position} is not an object.")

        identity = submitted.get("identity")
        if not isinstance(identity, dict):
            raise ExpectedResultsError(f"Test {position} has no identifying fields.")

        entry: Dict[str, Any] = {}

        source_index = submitted.get("index")
        if isinstance(source_index, int) and 0 <= source_index < len(original_entries):
            original = original_entries[source_index]
            if isinstance(original, dict):
                entry.update(original)

        resolved_identity = {}
        for field_name in identity_fields:
            raw_value = str(identity.get(field_name) or "").strip()
            if not raw_value:
                if field_name in OPTIONAL_IDENTITY_FIELDS:
                    entry.pop(field_name, None)
                    resolved_identity[field_name] = ""
                    continue
                raise ExpectedResultsError(f"Test {position} is missing '{field_name}'.")

            value = _validate_identifier(raw_value, field_name)
            entry[field_name] = value
            resolved_identity[field_name] = value

        identity_key = tuple(resolved_identity[name] for name in identity_fields)
        if identity_key in seen_identities:
            raise ExpectedResultsError(
                "Two tests name the same scenario: " + " / ".join(part for part in identity_key if part)
            )
        seen_identities.add(identity_key)

        entry[expected_field] = _parse_expected_value(submitted.get("expected_value"), expected_field)
        rebuilt.append(entry)

    return rebuilt


def _write_document_atomically(config_path: str, document: Dict[str, Any]) -> None:
    """Replace the configuration file in one step, so a failure leaves it intact."""
    directory = os.path.dirname(config_path)
    handle, temporary_path = tempfile.mkstemp(
        prefix=f".{TEST_CONFIG_FILE_NAME}.",
        suffix=".tmp",
        dir=directory,
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as temporary_file:
            json.dump(document, temporary_file, indent=2)
            temporary_file.write("\n")
        os.replace(temporary_path, config_path)
    except Exception:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)
        raise


def save_test_entries(
    base_dir: str,
    suite_name: str,
    submitted_entries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Write a new set of expected results into a suite's configuration file.

    The whole ``tests`` array is replaced by what was submitted, so entries can
    be edited, added and removed in one save. Everything else in the document is
    left exactly as it was.

    Args:
        base_dir: The project root (Django's BASE_DIR)
        suite_name: Directory name of a suite under ``tests/``
        submitted_entries: The entries to write, in the order to write them

    Returns:
        A summary of what was written

    Raises:
        ExpectedResultsError: the suite, the file, or one of the entries is
            unusable
    """
    configuration = read_test_configuration(base_dir, suite_name)
    rebuilt_entries = build_test_entries(configuration, submitted_entries)

    document = dict(configuration.document)
    document["tests"] = rebuilt_entries

    _write_document_atomically(configuration.suite.config_path, document)

    logger.info(
        "Wrote %s expected result(s) to %s",
        len(rebuilt_entries),
        configuration.suite.config_project_path,
    )
    return {
        "suite": configuration.suite.name,
        "config_project_path": configuration.suite.config_project_path,
        "test_count": len(rebuilt_entries),
        "previous_test_count": len(configuration.entries),
    }


def scenario_choices(base_dir: str, suite_name: str) -> List[Dict[str, str]]:
    """
    The fixture scenarios of one suite, as choices for a new test entry.

    Returns:
        One entry per scenario directory, holding the identifying fields split
        out of its path below ``tests/fixtures/templates``
    """
    from pybirdai.utils.datapoint_test_run.fixture_data_loader import discover_fixture_scenarios

    choices = []
    for scenario in discover_fixture_scenarios(base_dir):
        if scenario.suite != suite_name:
            continue
        choices.append(
            {
                "key": scenario.key,
                "name": scenario.name,
                "relative_path": scenario.relative_path,
                "parts": scenario.relative_path.replace(os.sep, "/").split("/"),
            }
        )
    return choices
