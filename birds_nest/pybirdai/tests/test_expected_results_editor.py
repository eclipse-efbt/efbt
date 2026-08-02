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
import os
import tempfile
import unittest
from pathlib import Path

from django.test import RequestFactory, SimpleTestCase, override_settings

from pybirdai.utils.datapoint_test_run.expected_results_editor import (
    ExpectedResultsError,
    detect_test_type,
    read_test_configuration,
    save_test_entries,
)
from pybirdai.utils.datapoint_test_run.test_suite_directory import (
    discover_test_suites,
    find_test_suite,
)
from pybirdai.views.expected_results_views import (
    list_expected_results,
    save_expected_results,
)

SUITE_NAME = "bird-test-suite"

FINREP_DOCUMENT = {
    "tests": [
        {
            "reg_tid": "F_05_01_REF_FINREP_3_0",
            "dp_suffix": "152589_REF",
            "dp_value": 166982511,
            "scenario": "scenario_1",
        },
        {
            "reg_tid": "F_05_01_REF_FINREP_3_0",
            "dp_suffix": "152457_REF",
            "dp_value": 11,
            "scenario": "scenario_2",
            "description": "a field the editor does not show",
        },
    ]
}


def _make_suite(base_dir, document=None, scenarios=(), suite=SUITE_NAME):
    """Create a test suite directory holding a configuration file and fixtures."""
    suite_dir = Path(base_dir) / "tests" / suite
    suite_dir.mkdir(parents=True, exist_ok=True)

    config = FINREP_DOCUMENT if document is None else document
    (suite_dir / "configuration_file_tests.json").write_text(json.dumps(config, indent=2), encoding="utf-8")

    for scenario in scenarios:
        scenario_dir = suite_dir / "tests" / "fixtures" / "templates" / Path(scenario)
        scenario_dir.mkdir(parents=True, exist_ok=True)
        (scenario_dir / "prty.csv").write_text("PRTY_uniqueID\nID_1\n", encoding="utf-8")

    return suite_dir


def _read_config(suite_dir):
    return json.loads((Path(suite_dir) / "configuration_file_tests.json").read_text(encoding="utf-8"))


class TestSuiteDiscoveryTests(unittest.TestCase):
    def test_only_directories_holding_a_suite_are_listed(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir)
            # A cache directory, a loose file and an empty directory are not suites.
            (Path(base_dir) / "tests" / "__pycache__").mkdir(parents=True)
            (Path(base_dir) / "tests" / "__init__.py").write_text("", encoding="utf-8")
            (Path(base_dir) / "tests" / "empty").mkdir()

            suites = discover_test_suites(base_dir)

            self.assertEqual([suite.name for suite in suites], [SUITE_NAME])
            self.assertTrue(suites[0].has_config)

    def test_a_suite_with_fixtures_but_no_configuration_is_still_listed(self):
        with tempfile.TemporaryDirectory() as base_dir:
            templates = Path(base_dir) / "tests" / "fixtures-only" / "tests" / "fixtures" / "templates"
            templates.mkdir(parents=True)

            suites = discover_test_suites(base_dir)

            self.assertEqual([suite.name for suite in suites], ["fixtures-only"])
            self.assertFalse(suites[0].has_config)

    def test_the_display_name_comes_from_the_suite_manifest(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            (suite_dir / "suite_manifest.json").write_text(
                json.dumps({"metadata": {"display_name": "Default Test Suite"}}), encoding="utf-8"
            )

            suite = find_test_suite(base_dir, SUITE_NAME)

            self.assertEqual(suite.display_name, "Default Test Suite")
            self.assertEqual(suite.label, f"Default Test Suite ({SUITE_NAME})")

    def test_an_unknown_suite_is_not_found(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir)

            self.assertIsNone(find_test_suite(base_dir, "no-such-suite"))
            # A name that would otherwise walk out of the tests directory.
            self.assertIsNone(find_test_suite(base_dir, os.path.join("..", "resources")))


class TestTypeDetectionTests(unittest.TestCase):
    def test_an_entry_naming_a_table_is_an_ancrdt_test(self):
        self.assertEqual(detect_test_type({"tests": [{"table_name": "ancrdt_instrmnt"}]}), "ancrdt")

    def test_an_entry_naming_a_template_is_a_finrep_test(self):
        self.assertEqual(detect_test_type({"tests": [{"reg_tid": "F_05_01"}]}), "finrep")

    def test_a_declared_test_type_wins(self):
        self.assertEqual(detect_test_type({"test_type": "ancrdt", "tests": [{"reg_tid": "F_05_01"}]}), "ancrdt")

    def test_an_empty_file_reads_as_finrep(self):
        self.assertEqual(detect_test_type({"tests": []}), "finrep")


class ReadTestConfigurationTests(unittest.TestCase):
    def test_entries_are_split_into_identity_expected_value_and_the_rest(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, scenarios=[os.path.join("F_05_01_REF_FINREP_3_0", "152589_REF", "scenario_1")])

            configuration = read_test_configuration(base_dir, SUITE_NAME)

            self.assertEqual(configuration.test_type, "finrep")
            first, second = configuration.entries
            self.assertEqual(
                first.identity,
                {"reg_tid": "F_05_01_REF_FINREP_3_0", "dp_suffix": "152589_REF", "scenario": "scenario_1"},
            )
            self.assertEqual(first.expected_value, 166982511)
            self.assertEqual(first.expected_field, "dp_value")
            self.assertEqual(second.other_fields, {"description": "a field the editor does not show"})

    def test_an_entry_is_marked_when_its_fixtures_are_missing(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, scenarios=[os.path.join("F_05_01_REF_FINREP_3_0", "152589_REF", "scenario_1")])

            configuration = read_test_configuration(base_dir, SUITE_NAME)

            self.assertTrue(configuration.entries[0].scenario_exists)
            self.assertFalse(configuration.entries[1].scenario_exists)

    def test_an_ancrdt_configuration_exposes_its_own_fields(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(
                base_dir,
                document={"tests": [{"table_name": "ancrdt_instrmnt", "scenario": "scenario_1", "expected_rows": 4}]},
            )

            configuration = read_test_configuration(base_dir, SUITE_NAME)

            self.assertEqual(configuration.test_type, "ancrdt")
            self.assertEqual(configuration.entries[0].expected_field, "expected_rows")
            self.assertEqual(configuration.as_dict()["identity_fields"], ["table_name", "scenario"])

    def test_a_file_that_is_not_json_is_reported(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            (suite_dir / "configuration_file_tests.json").write_text("{not json", encoding="utf-8")

            with self.assertRaises(ExpectedResultsError):
                read_test_configuration(base_dir, SUITE_NAME)


class SaveTestEntriesTests(unittest.TestCase):
    def test_an_edited_expected_value_is_written(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            configuration = read_test_configuration(base_dir, SUITE_NAME)

            entries = [entry.as_dict() for entry in configuration.entries]
            entries[0]["expected_value"] = "42"

            result = save_test_entries(base_dir, SUITE_NAME, entries)

            written = _read_config(suite_dir)
            self.assertEqual(written["tests"][0]["dp_value"], 42)
            self.assertEqual(result["test_count"], 2)

    def test_fields_the_editor_does_not_show_survive_a_save(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir, document=dict(FINREP_DOCUMENT, test_type="finrep", suite="local"))
            configuration = read_test_configuration(base_dir, SUITE_NAME)

            save_test_entries(base_dir, SUITE_NAME, [entry.as_dict() for entry in configuration.entries])

            written = _read_config(suite_dir)
            self.assertEqual(written["tests"][1]["description"], "a field the editor does not show")
            self.assertEqual(written["test_type"], "finrep")
            self.assertEqual(written["suite"], "local")

    def test_tests_can_be_added_and_removed_in_one_save(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            configuration = read_test_configuration(base_dir, SUITE_NAME)

            entries = [entry.as_dict() for entry in configuration.entries][:1]
            entries.append(
                {
                    "identity": {
                        "reg_tid": "F_05_01_REF_FINREP_3_0",
                        "dp_suffix": "152457_REF",
                        "scenario": "scenario_3",
                    },
                    "expected_value": 7,
                }
            )

            save_test_entries(base_dir, SUITE_NAME, entries)

            written = _read_config(suite_dir)["tests"]
            self.assertEqual(len(written), 2)
            self.assertEqual(written[1]["scenario"], "scenario_3")
            self.assertEqual(written[1]["dp_value"], 7)
            # The removed entry took its extra fields with it.
            self.assertNotIn("description", written[1])

    def test_a_whole_number_is_not_written_as_a_decimal(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            configuration = read_test_configuration(base_dir, SUITE_NAME)

            entries = [entry.as_dict() for entry in configuration.entries]
            entries[0]["expected_value"] = "11.0"
            entries[1]["expected_value"] = "12.5"

            save_test_entries(base_dir, SUITE_NAME, entries)

            written = _read_config(suite_dir)["tests"]
            self.assertIsInstance(written[0]["dp_value"], int)
            self.assertEqual(written[0]["dp_value"], 11)
            self.assertEqual(written[1]["dp_value"], 12.5)

    def test_an_empty_scenario_asks_for_every_scenario(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            configuration = read_test_configuration(base_dir, SUITE_NAME)

            entries = [entry.as_dict() for entry in configuration.entries]
            entries[0]["identity"]["scenario"] = ""

            save_test_entries(base_dir, SUITE_NAME, entries)

            self.assertNotIn("scenario", _read_config(suite_dir)["tests"][0])

    def test_an_identifier_that_would_leave_the_templates_directory_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            configuration = read_test_configuration(base_dir, SUITE_NAME)

            entries = [entry.as_dict() for entry in configuration.entries]
            entries[0]["identity"]["scenario"] = "../../../etc"

            with self.assertRaises(ExpectedResultsError):
                save_test_entries(base_dir, SUITE_NAME, entries)

            # The file is only replaced once the whole save is accepted.
            self.assertEqual(_read_config(suite_dir)["tests"][0]["scenario"], "scenario_1")

    def test_a_missing_identifier_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir)

            with self.assertRaises(ExpectedResultsError):
                save_test_entries(
                    base_dir,
                    SUITE_NAME,
                    [{"identity": {"reg_tid": "", "dp_suffix": "152589_REF", "scenario": "s"}, "expected_value": 1}],
                )

    def test_an_expected_value_that_is_not_a_number_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir)

            with self.assertRaises(ExpectedResultsError):
                save_test_entries(
                    base_dir,
                    SUITE_NAME,
                    [
                        {
                            "identity": {"reg_tid": "F", "dp_suffix": "D", "scenario": "s"},
                            "expected_value": "not a number",
                        }
                    ],
                )

    def test_two_tests_naming_the_same_scenario_are_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir)
            entry = {"identity": {"reg_tid": "F", "dp_suffix": "D", "scenario": "s"}, "expected_value": 1}

            with self.assertRaises(ExpectedResultsError):
                save_test_entries(base_dir, SUITE_NAME, [entry, dict(entry)])

    def test_a_negative_row_count_is_refused_for_an_ancrdt_test(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(
                base_dir,
                document={"tests": [{"table_name": "ancrdt_instrmnt", "scenario": "scenario_1", "expected_rows": 4}]},
            )

            with self.assertRaises(ExpectedResultsError):
                save_test_entries(
                    base_dir,
                    SUITE_NAME,
                    [{"identity": {"table_name": "ancrdt_instrmnt", "scenario": "scenario_1"}, "expected_value": -1}],
                )


class ExpectedResultsViewTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def test_the_listing_returns_the_configuration_and_its_scenarios(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_suite(base_dir, scenarios=[os.path.join("F_05_01_REF_FINREP_3_0", "152589_REF", "scenario_1")])

            with override_settings(BASE_DIR=base_dir):
                response = list_expected_results(
                    self.factory.get("/api/test-data/expected-results/", {"suite": SUITE_NAME})
                )

            self.assertEqual(response.status_code, 200)
            payload = json.loads(response.content)
            self.assertEqual(payload["expected_field"], "dp_value")
            self.assertEqual(len(payload["entries"]), 2)
            self.assertEqual(
                payload["scenarios"][0]["parts"],
                ["F_05_01_REF_FINREP_3_0", "152589_REF", "scenario_1"],
            )

    def test_an_unknown_suite_is_reported(self):
        with tempfile.TemporaryDirectory() as base_dir:
            with override_settings(BASE_DIR=base_dir):
                response = list_expected_results(
                    self.factory.get("/api/test-data/expected-results/", {"suite": "no-such-suite"})
                )

            self.assertEqual(response.status_code, 400)
            self.assertIn("error", json.loads(response.content))

    def test_saving_writes_the_configuration_file(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            body = {
                "suite": SUITE_NAME,
                "entries": [
                    {
                        "index": 0,
                        "identity": {
                            "reg_tid": "F_05_01_REF_FINREP_3_0",
                            "dp_suffix": "152589_REF",
                            "scenario": "scenario_1",
                        },
                        "expected_value": 999,
                    }
                ],
            }

            with override_settings(BASE_DIR=base_dir):
                response = save_expected_results(
                    self.factory.post(
                        "/api/test-data/expected-results/save/",
                        data=json.dumps(body),
                        content_type="application/json",
                    )
                )

            self.assertEqual(response.status_code, 200)
            written = _read_config(suite_dir)["tests"]
            self.assertEqual(len(written), 1)
            self.assertEqual(written[0]["dp_value"], 999)

    def test_a_rejected_entry_is_reported_without_writing(self):
        with tempfile.TemporaryDirectory() as base_dir:
            suite_dir = _make_suite(base_dir)
            body = {
                "suite": SUITE_NAME,
                "entries": [{"identity": {"reg_tid": "F", "dp_suffix": "D", "scenario": "s"}, "expected_value": ""}],
            }

            with override_settings(BASE_DIR=base_dir):
                response = save_expected_results(
                    self.factory.post(
                        "/api/test-data/expected-results/save/",
                        data=json.dumps(body),
                        content_type="application/json",
                    )
                )

            self.assertEqual(response.status_code, 400)
            self.assertEqual(len(_read_config(suite_dir)["tests"]), 2)


if __name__ == "__main__":
    unittest.main()
