"""
Views for loading test fixture data into the database outside of a test run.

The test runner loads a scenario, asserts a datapoint and moves on. These views
do the loading on its own so the data stays in the database to be inspected or
worked with.
"""

import json
import logging

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from pybirdai.utils.datapoint_test_run.fixture_data_loader import (
    current_bird_row_counts,
    discover_fixture_scenarios,
    find_fixture_scenario,
    load_fixture_scenario,
    resolve_scenario_path,
)
from pybirdai.utils.secure_error_handling import SecureErrorHandler

logger = logging.getLogger(__name__)


def _internal_json_error_response(exception: Exception, context: str, request, status: int = 500):
    """Hide implementation details from JSON error responses."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse({"error": error_data["message"]}, status=status)


@require_http_methods(["GET"])
def fixture_data_page(request):
    """Render the page for choosing a fixture scenario to load."""
    return render(request, "pybirdai/fixture_data_load.html")


@require_http_methods(["GET"])
def list_fixture_scenarios(request):
    """
    List every fixture scenario that can be loaded, and what is loaded now.

    Returns:
        JSON with the available scenarios and the current BIRD row counts
    """
    try:
        scenarios = discover_fixture_scenarios(str(settings.BASE_DIR))

        try:
            loaded_tables = current_bird_row_counts()
        except Exception as exc:
            # A model file that has not been generated yet should not stop the
            # scenario list from being shown.
            logger.warning("Could not read current BIRD row counts: %s", exc)
            loaded_tables = {}

        return JsonResponse(
            {
                "scenarios": [scenario.as_dict() for scenario in scenarios],
                "total": len(scenarios),
                "loaded": {
                    "table_count": len(loaded_tables),
                    "row_count": sum(loaded_tables.values()),
                    "tables": loaded_tables,
                },
            }
        )
    except Exception as e:
        return _internal_json_error_response(e, "listing fixture scenarios", request)


@require_http_methods(["POST"])
def load_fixture_data(request):
    """
    Load one fixture scenario's data into the database.

    Request Body (JSON):
        key: the scenario key from the listing, e.g. "suite/TEMPLATE/DP/scenario"
        scenario_path: alternatively, a project-relative scenario directory
        clean_first: empty the BIRD tables first (default true)

    Returns:
        JSON describing what was loaded
    """
    try:
        try:
            data = json.loads(request.body or "{}")
        except ValueError:
            return JsonResponse({"error": "Invalid request body."}, status=400)

        base_dir = str(settings.BASE_DIR)
        clean_first = bool(data.get("clean_first", True))

        key = (data.get("key") or "").strip()
        scenario_path = (data.get("scenario_path") or "").strip()

        if key:
            scenario = find_fixture_scenario(base_dir, key)
            if scenario is None:
                return JsonResponse({"error": "Fixture scenario was not found."}, status=404)
            project_path = scenario.project_path
        elif scenario_path:
            project_path = scenario_path
        else:
            return JsonResponse({"error": "A fixture scenario is required."}, status=400)

        try:
            resolved_path = resolve_scenario_path(base_dir, project_path)
        except ValueError as exc:
            logger.info("Invalid fixture scenario path %s: %s", project_path, exc)
            return JsonResponse({"error": "Invalid fixture scenario path."}, status=400)

        try:
            result = load_fixture_scenario(resolved_path, clean_first=clean_first)
        except (FileNotFoundError, ValueError) as exc:
            logger.info("Could not load fixture scenario %s: %s", project_path, exc)
            return JsonResponse({"error": str(exc)}, status=400)

        payload = result.as_dict()
        payload["scenario_path"] = project_path
        payload["success"] = True
        payload["message"] = (
            f"Loaded {payload['row_count']} row(s) into {payload['table_count']} table(s)"
        )
        return JsonResponse(payload)

    except Exception as e:
        return _internal_json_error_response(e, "loading fixture data", request)


@require_http_methods(["POST"])
def clear_bird_data(request):
    """Empty every BIRD data table, leaving the model and metadata in place."""
    try:
        from pybirdai.utils.datapoint_test_run.fixture_data_loader import clean_bird_data

        deleted_rows = clean_bird_data()
        return JsonResponse(
            {
                "success": True,
                "deleted_rows": deleted_rows,
                "message": f"Removed {deleted_rows} row(s) from the BIRD data tables",
            }
        )
    except Exception as e:
        return _internal_json_error_response(e, "clearing BIRD data", request)
