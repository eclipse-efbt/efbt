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
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from pybirdai.utils.datapoint_test_run.fixture_data_loader import (
    FixtureScenario,
    discover_fixture_scenarios,
    find_fixture_scenario,
    load_fixture_scenario,
    resolve_scenario_path,
)
from pybirdai.views.fixture_data_views import (
    clear_bird_data,
    list_fixture_scenarios,
    load_fixture_data,
)

LOADER_MODULE = "pybirdai.utils.datapoint_test_run.fixture_data_loader"


def _make_scenario(base_dir, suite, relative_path, csv_files=("prty.csv",), sql=False):
    """Create a fixture scenario directory on disk."""
    scenario_dir = Path(base_dir) / "tests" / suite / "tests" / "fixtures" / "templates" / relative_path
    scenario_dir.mkdir(parents=True, exist_ok=True)
    for csv_file in csv_files:
        (scenario_dir / csv_file).write_text("PRTY_uniqueID\nID_1\n", encoding="utf-8")
    if sql:
        (scenario_dir / "sql_inserts.sql").write_text("INSERT INTO x VALUES (1);", encoding="utf-8")
    return scenario_dir


class FixtureScenarioDiscoveryTests(unittest.TestCase):
    def test_scenarios_are_found_at_any_depth(self):
        with tempfile.TemporaryDirectory() as base_dir:
            # Suites nest fixtures differently: some put the scenario directly
            # under the template, others under a datapoint as well.
            _make_scenario(base_dir, "suite-a", os.path.join("TEMPLATE_1", "DP_1", "scenario_1"))
            _make_scenario(base_dir, "suite-b", os.path.join("TEMPLATE_2", "scenario_2"))

            scenarios = discover_fixture_scenarios(base_dir)

            self.assertEqual(
                [scenario.key for scenario in scenarios],
                [
                    "suite-a/TEMPLATE_1/DP_1/scenario_1",
                    "suite-b/TEMPLATE_2/scenario_2",
                ],
            )
            self.assertEqual(scenarios[0].name, "scenario_1")
            self.assertEqual(scenarios[0].group, "TEMPLATE_1 / DP_1")
            self.assertEqual(scenarios[1].group, "TEMPLATE_2")

    def test_directories_without_fixtures_are_not_scenarios(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_scenario(base_dir, "suite-a", os.path.join("TEMPLATE_1", "scenario_1"))
            empty = Path(base_dir) / "tests" / "suite-a" / "tests" / "fixtures" / "templates" / "TEMPLATE_1" / "notes"
            empty.mkdir(parents=True)
            (empty / "readme.txt").write_text("no fixtures here", encoding="utf-8")

            keys = [scenario.key for scenario in discover_fixture_scenarios(base_dir)]

            self.assertEqual(keys, ["suite-a/TEMPLATE_1/scenario_1"])

    def test_a_sql_only_scenario_is_found(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_scenario(base_dir, "suite-a", "TEMPLATE_1/legacy", csv_files=(), sql=True)

            scenarios = discover_fixture_scenarios(base_dir)

            self.assertEqual(len(scenarios), 1)
            self.assertTrue(scenarios[0].has_sql_fixture)
            self.assertEqual(scenarios[0].csv_file_count, 0)

    def test_a_project_without_tests_has_no_scenarios(self):
        with tempfile.TemporaryDirectory() as base_dir:
            self.assertEqual(discover_fixture_scenarios(base_dir), [])

    def test_lookup_by_key(self):
        with tempfile.TemporaryDirectory() as base_dir:
            _make_scenario(base_dir, "suite-a", os.path.join("TEMPLATE_1", "scenario_1"))

            found = find_fixture_scenario(base_dir, "suite-a/TEMPLATE_1/scenario_1")

            self.assertIsNotNone(found)
            self.assertEqual(found.name, "scenario_1")
            self.assertIsNone(find_fixture_scenario(base_dir, "suite-a/TEMPLATE_1/missing"))


class ScenarioPathResolutionTests(unittest.TestCase):
    def test_a_path_outside_the_project_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            with self.assertRaises(ValueError):
                resolve_scenario_path(base_dir, os.path.join("..", "..", "etc"))

    def test_a_missing_directory_is_refused(self):
        with tempfile.TemporaryDirectory() as base_dir:
            with self.assertRaises(ValueError):
                resolve_scenario_path(base_dir, "tests/does/not/exist")

    def test_a_real_scenario_resolves(self):
        with tempfile.TemporaryDirectory() as base_dir:
            scenario_dir = _make_scenario(base_dir, "suite-a", os.path.join("TEMPLATE_1", "scenario_1"))
            scenario = discover_fixture_scenarios(base_dir)[0]

            resolved = resolve_scenario_path(base_dir, scenario.project_path)

            self.assertEqual(Path(resolved).resolve(), scenario_dir.resolve())


class FixtureLoadingTests(SimpleTestCase):
    def test_csv_fixtures_are_preferred_and_the_database_is_cleaned_first(self):
        with tempfile.TemporaryDirectory() as base_dir:
            scenario_dir = _make_scenario(
                base_dir, "suite-a", os.path.join("TEMPLATE_1", "scenario_1"), sql=True
            )

            with patch(f"{LOADER_MODULE}.clean_bird_data", return_value=7) as clean, patch(
                "pybirdai.utils.datapoint_test_run.csv_fixture_loader.CSVFixtureLoader.load_scenario_fixtures",
                return_value={"PRTY": 3},
            ):
                result = load_fixture_scenario(str(scenario_dir))

            clean.assert_called_once()
            self.assertEqual(result.source, "csv")
            self.assertEqual(result.tables, {"PRTY": 3})
            self.assertEqual(result.row_count, 3)
            self.assertEqual(result.deleted_rows, 7)
            self.assertTrue(result.cleaned)

    def test_loading_can_leave_existing_data_in_place(self):
        with tempfile.TemporaryDirectory() as base_dir:
            scenario_dir = _make_scenario(base_dir, "suite-a", os.path.join("TEMPLATE_1", "scenario_1"))

            with patch(f"{LOADER_MODULE}.clean_bird_data") as clean, patch(
                "pybirdai.utils.datapoint_test_run.csv_fixture_loader.CSVFixtureLoader.load_scenario_fixtures",
                return_value={"PRTY": 1},
            ):
                result = load_fixture_scenario(str(scenario_dir), clean_first=False)

            clean.assert_not_called()
            self.assertFalse(result.cleaned)
            self.assertEqual(result.deleted_rows, 0)

    def test_a_sql_only_scenario_falls_back_to_the_sql_fixture(self):
        with tempfile.TemporaryDirectory() as base_dir:
            scenario_dir = _make_scenario(
                base_dir, "suite-a", os.path.join("TEMPLATE_1", "legacy"), csv_files=(), sql=True
            )

            with patch(f"{LOADER_MODULE}.clean_bird_data", return_value=0), patch(
                f"{LOADER_MODULE}._load_sql_fixture"
            ) as load_sql:
                result = load_fixture_scenario(str(scenario_dir))

            load_sql.assert_called_once_with(str(scenario_dir))
            self.assertEqual(result.source, "sql")

    def test_a_directory_without_fixtures_is_rejected(self):
        with tempfile.TemporaryDirectory() as base_dir:
            empty = Path(base_dir) / "empty"
            empty.mkdir()

            with self.assertRaises(ValueError):
                load_fixture_scenario(str(empty))

    def test_a_missing_directory_is_rejected(self):
        with self.assertRaises(FileNotFoundError):
            load_fixture_scenario("/does/not/exist")


class FixtureDataViewTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def test_listing_reports_scenarios_and_what_is_loaded(self):
        scenarios = [
            FixtureScenario(
                suite="suite-a",
                relative_path=os.path.join("TEMPLATE_1", "scenario_1"),
                project_path=os.path.join("tests", "suite-a", "scenario_1"),
                csv_file_count=4,
            )
        ]

        with patch("pybirdai.views.fixture_data_views.discover_fixture_scenarios", return_value=scenarios), patch(
            "pybirdai.views.fixture_data_views.current_bird_row_counts",
            return_value={"pybirdai_prty": 3, "pybirdai_instrmnt": 2},
        ):
            response = list_fixture_scenarios(self.factory.get("/api/test-data/fixtures/"))

        payload = json.loads(response.content)
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["scenarios"][0]["csv_file_count"], 4)
        self.assertEqual(payload["loaded"], {"table_count": 2, "row_count": 5, "tables": {"pybirdai_prty": 3, "pybirdai_instrmnt": 2}})

    def test_listing_still_works_when_row_counts_cannot_be_read(self):
        with patch("pybirdai.views.fixture_data_views.discover_fixture_scenarios", return_value=[]), patch(
            "pybirdai.views.fixture_data_views.current_bird_row_counts",
            side_effect=RuntimeError("no such table"),
        ):
            response = list_fixture_scenarios(self.factory.get("/api/test-data/fixtures/"))

        payload = json.loads(response.content)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["loaded"]["row_count"], 0)

    def _post(self, body):
        return self.factory.post(
            "/api/test-data/fixtures/load/",
            data=json.dumps(body),
            content_type="application/json",
        )

    def test_loading_by_key_reports_what_was_loaded(self):
        scenario = FixtureScenario(
            suite="suite-a",
            relative_path="TEMPLATE_1/scenario_1",
            project_path="tests/suite-a/scenario_1",
            csv_file_count=2,
        )
        with patch("pybirdai.views.fixture_data_views.find_fixture_scenario", return_value=scenario), patch(
            "pybirdai.views.fixture_data_views.resolve_scenario_path", return_value="/abs/scenario_1"
        ), patch("pybirdai.views.fixture_data_views.load_fixture_scenario") as load:
            load.return_value.as_dict.return_value = {
                "scenario_path": "/abs/scenario_1",
                "source": "csv",
                "tables": {"PRTY": 3},
                "table_count": 1,
                "row_count": 3,
                "deleted_rows": 5,
                "cleaned": True,
            }
            response = load_fixture_data(self._post({"key": scenario.key}))

        payload = json.loads(response.content)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["row_count"], 3)
        # The response points at the project-relative path, not the server's.
        self.assertEqual(payload["scenario_path"], "tests/suite-a/scenario_1")
        self.assertIn("Loaded 3 row(s) into 1 table(s)", payload["message"])
        self.assertEqual(payload["deleted_rows"], 5)

    def test_an_unknown_key_is_a_404(self):
        with patch("pybirdai.views.fixture_data_views.find_fixture_scenario", return_value=None):
            response = load_fixture_data(self._post({"key": "suite-a/missing"}))

        self.assertEqual(response.status_code, 404)

    def test_a_request_without_a_scenario_is_rejected(self):
        response = load_fixture_data(self._post({}))
        self.assertEqual(response.status_code, 400)

    def test_an_invalid_body_is_rejected(self):
        request = self.factory.post(
            "/api/test-data/fixtures/load/",
            data="not json",
            content_type="application/json",
        )
        self.assertEqual(load_fixture_data(request).status_code, 400)

    def test_a_path_outside_the_project_is_rejected(self):
        with patch(
            "pybirdai.views.fixture_data_views.resolve_scenario_path",
            side_effect=ValueError("Invalid fixture scenario path."),
        ):
            response = load_fixture_data(self._post({"scenario_path": "../../etc"}))

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid fixture scenario path.", json.loads(response.content)["error"])

    def test_clearing_reports_the_number_of_rows_removed(self):
        with patch(f"{LOADER_MODULE}.clean_bird_data", return_value=81):
            response = clear_bird_data(self.factory.post("/api/test-data/fixtures/clear/"))

        payload = json.loads(response.content)
        self.assertEqual(payload["deleted_rows"], 81)
        self.assertIn("81", payload["message"])
