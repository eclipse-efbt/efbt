"""
Load the data from one test fixture scenario into the database.

The test runner loads a scenario, runs a datapoint, and moves on to the next
one. This module does the loading part on its own, so a scenario's data can be
put into the database and left there - to look at in the admin site, to run a
transformation against, or to use as the starting point for new test data.

Scenarios are discovered rather than assumed. Suites nest their fixtures at
different depths (``templates/<template>/<scenario>`` in some,
``templates/<template>/<datapoint>/<scenario>`` in others), so any directory
holding fixture files counts as a scenario.
"""

import logging
import os
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from django.core.exceptions import SuspiciousFileOperation
from django.utils._os import safe_join

logger = logging.getLogger(__name__)

TESTS_DIRECTORY_NAME = "tests"
TEMPLATES_RELATIVE_PATH = os.path.join("tests", "fixtures", "templates")
SQL_INSERT_FILE_NAME = "sql_inserts.sql"


@dataclass(frozen=True)
class FixtureScenario:
    """One directory of fixture files that can be loaded on its own."""

    suite: str
    #: Path below the suite's templates directory, e.g. "F_05_01/152589_REF/scenario_1"
    relative_path: str
    #: Path from the project root, which is what the loader is given
    project_path: str
    csv_file_count: int = 0
    has_sql_fixture: bool = False

    @property
    def key(self) -> str:
        return f"{self.suite}/{self.relative_path}"

    @property
    def name(self) -> str:
        return os.path.basename(self.relative_path)

    @property
    def group(self) -> str:
        """The path above the scenario itself, used to group the listing."""
        parent = os.path.dirname(self.relative_path)
        return parent.replace(os.sep, " / ") if parent else ""

    def as_dict(self) -> Dict:
        return {
            "key": self.key,
            "suite": self.suite,
            "name": self.name,
            "group": self.group,
            "relative_path": self.relative_path,
            "project_path": self.project_path,
            "csv_file_count": self.csv_file_count,
            "has_sql_fixture": self.has_sql_fixture,
        }


@dataclass
class FixtureLoadResult:
    """What happened when a scenario was loaded."""

    scenario_path: str
    source: str = ""
    tables: Dict[str, int] = field(default_factory=dict)
    deleted_rows: int = 0
    cleaned: bool = False

    @property
    def row_count(self) -> int:
        return sum(self.tables.values())

    def as_dict(self) -> Dict:
        return {
            "scenario_path": self.scenario_path,
            "source": self.source,
            "tables": self.tables,
            "table_count": len(self.tables),
            "row_count": self.row_count,
            "deleted_rows": self.deleted_rows,
            "cleaned": self.cleaned,
        }


def _directory_has_fixtures(path: str) -> tuple[int, bool]:
    try:
        entries = os.listdir(path)
    except OSError:
        return 0, False

    csv_count = sum(1 for entry in entries if entry.lower().endswith(".csv"))
    has_sql = SQL_INSERT_FILE_NAME in entries
    return csv_count, has_sql


def discover_fixture_scenarios(base_dir: str) -> List[FixtureScenario]:
    """
    Find every loadable fixture scenario under the project's tests directory.

    Args:
        base_dir: The project root (Django's BASE_DIR)

    Returns:
        Scenarios sorted by suite and path
    """
    tests_dir = os.path.join(base_dir, TESTS_DIRECTORY_NAME)
    if not os.path.isdir(tests_dir):
        return []

    scenarios: List[FixtureScenario] = []

    for suite in sorted(os.listdir(tests_dir)):
        templates_dir = os.path.join(tests_dir, suite, TEMPLATES_RELATIVE_PATH)
        if not os.path.isdir(templates_dir):
            continue

        for current_dir, sub_directories, _files in os.walk(templates_dir):
            sub_directories[:] = sorted(
                name for name in sub_directories if not name.startswith((".", "__"))
            )

            if current_dir == templates_dir:
                continue

            csv_count, has_sql = _directory_has_fixtures(current_dir)
            if not csv_count and not has_sql:
                continue

            scenarios.append(
                FixtureScenario(
                    suite=suite,
                    relative_path=os.path.relpath(current_dir, templates_dir),
                    project_path=os.path.relpath(current_dir, base_dir),
                    csv_file_count=csv_count,
                    has_sql_fixture=has_sql,
                )
            )

    return sorted(scenarios, key=lambda scenario: (scenario.suite, scenario.relative_path))


def find_fixture_scenario(base_dir: str, key: str) -> Optional[FixtureScenario]:
    """Return the discovered scenario with this key, if it exists.

    Matching against discovered scenarios - rather than trusting the key as a
    path - keeps the loader pointed at real fixture directories.
    """
    for scenario in discover_fixture_scenarios(base_dir):
        if scenario.key == key:
            return scenario
    return None


def resolve_scenario_path(base_dir: str, project_path: str) -> str:
    """Resolve a project-relative scenario path, refusing to leave the project."""
    try:
        resolved = safe_join(str(base_dir), project_path)
    except SuspiciousFileOperation as exc:
        raise ValueError("Invalid fixture scenario path.") from exc

    if not os.path.isdir(resolved):
        raise ValueError("Fixture scenario directory was not found.")

    return resolved


def _bird_table_names() -> List[str]:
    """Every BIRD data table, subtype tables included.

    ``DatabaseCleanupService`` discovers models by looking for a ``models.Model``
    base class, which only matches the root of each hierarchy. Reading the
    imported module instead picks up the subtype tables as well.
    """
    from pybirdai.utils.datapoint_test_run.test_data_template_utils import get_bird_model_classes

    return [model._meta.db_table for model in get_bird_model_classes().values()]


def _row_counts_by_table() -> Dict[str, int]:
    """Count the rows held by each BIRD table in its own right."""
    from django.db import connection

    counts: Dict[str, int] = {}
    quote_name = connection.ops.quote_name

    with connection.cursor() as cursor:
        for table in _bird_table_names():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {quote_name(table)}")
                count = cursor.fetchone()[0]
            except Exception as exc:
                logger.debug("Could not count rows in %s: %s", table, exc)
                continue
            if count:
                counts[table] = count

    return counts


def clean_bird_data() -> int:
    """Delete the rows in every BIRD data table. Returns the number removed."""
    from django.db import connection

    from pybirdai.utils.datapoint_test_run.database_cleanup_service import DatabaseCleanupService

    rows_before = sum(_row_counts_by_table().values())

    try:
        DatabaseCleanupService().cleanup_bird_data_tables(force=True)
    except Exception as exc:
        logger.warning("Cleanup service failed, falling back to a direct sweep: %s", exc)

    # The service works from the root of each hierarchy and relies on cascades
    # to reach subtype tables, so sweep up whatever is left directly.
    remaining = _row_counts_by_table()
    if remaining:
        quote_name = connection.ops.quote_name
        with connection.constraint_checks_disabled():
            with connection.cursor() as cursor:
                for table in remaining:
                    cursor.execute(f"DELETE FROM {quote_name(table)}")

    return rows_before - sum(_row_counts_by_table().values())


def _load_sql_fixture(scenario_path: str) -> None:
    """Load a legacy sql_inserts.sql fixture through the active connection."""
    from django.db import connection

    sql_path = os.path.join(scenario_path, SQL_INSERT_FILE_NAME)
    with open(sql_path, "r", encoding="utf-8") as sql_file:
        script = sql_file.read()

    if connection.vendor == "sqlite":
        # executescript is the only reliable way to run a multi-statement file,
        # and it is available on the underlying sqlite3 connection.
        raw_connection = connection.connection
        if isinstance(raw_connection, sqlite3.Connection):
            raw_connection.executescript(script)
            raw_connection.commit()
            return

    with connection.cursor() as cursor:
        for statement in filter(None, (part.strip() for part in script.split(";"))):
            cursor.execute(statement)


def load_fixture_scenario(scenario_path: str, clean_first: bool = True) -> FixtureLoadResult:
    """
    Load one scenario's fixture data into the database.

    CSV fixtures are preferred and a ``sql_inserts.sql`` file is the fallback,
    matching what the test runner does, so data loaded here is the same data a
    test would see.

    Args:
        scenario_path: Absolute path to the scenario directory
        clean_first: Empty the BIRD tables first. Loading on top of existing
            data usually collides, because scenarios reuse identifiers.

    Returns:
        A :class:`FixtureLoadResult` describing what was loaded
    """
    from pybirdai.utils.datapoint_test_run.csv_fixture_loader import CSVFixtureLoader

    if not os.path.isdir(scenario_path):
        raise FileNotFoundError(f"Scenario directory not found: {scenario_path}")

    csv_count, has_sql = _directory_has_fixtures(scenario_path)
    if not csv_count and not has_sql:
        raise ValueError("The selected directory holds no CSV or SQL fixtures.")

    result = FixtureLoadResult(scenario_path=scenario_path)

    if clean_first:
        result.deleted_rows = clean_bird_data()
        result.cleaned = True

    if csv_count:
        result.source = "csv"
        result.tables = CSVFixtureLoader().load_scenario_fixtures(scenario_path)
    else:
        result.source = "sql"
        _load_sql_fixture(scenario_path)

    logger.info(
        "Loaded %s fixtures from %s: %s table(s), %s row(s)",
        result.source,
        scenario_path,
        len(result.tables),
        result.row_count,
    )
    return result


def current_bird_row_counts() -> Dict[str, int]:
    """Return the BIRD tables that currently hold data, with their row counts.

    Counting per table, rather than per model, keeps the number comparable with
    what a load reports: a subtype record occupies a row in its own table and a
    row in each table it inherits from.
    """
    return _row_counts_by_table()
