# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Views for ANCRDT (Analytical Credit Dataset) table execution.

Provides web endpoints for executing ANCRDT table transformations
similar to the FINREP datapoint execution pattern.
"""

import logging

from django.http import HttpResponse, JsonResponse
from django.utils.html import format_html, format_html_join

from pybirdai.entry_points.run_ancrdt_table import RunANCRDTTable
from pybirdai.utils.secure_error_handling import SecureErrorHandler
from pybirdai.utils.secure_logging import sanitize_log_value


logger = logging.getLogger(__name__)


def _build_filters_html(filters_applied):
    """Render applied filters with auto-escaped values."""
    return format_html(
        "<ul>{}</ul>",
        format_html_join(
            "",
            "<li><strong>{}:</strong> [{}]</li>",
            (
                (dimension, ", ".join(str(value) for value in values))
                for dimension, values in filters_applied.items()
            ),
        ),
    )


def _build_result_html(table_name, row_count, csv_path, row_count_total=None, filters_applied=None):
    """Build the HTML response while escaping user-controlled values."""
    if filters_applied:
        status_html = ""
        if row_count == 0:
            status_html = format_html(
                "<p><strong>Status:</strong> <span style=\"color: orange;\">"
                "No records matched the specified filters"
                "</span></p>"
            )

        return format_html(
            "<h3>ANCRDT Table Execution Results (Filtered)</h3>"
            "<p><strong>Table Name:</strong> {}</p>"
            "<p><strong>Applied Filters:</strong></p>"
            "{}"
            "<p><strong>Total Rows (before filter):</strong> {}</p>"
            "<p><strong>Filtered Rows:</strong> {}</p>"
            "{}"
            "<p><strong>CSV Path:</strong> {}</p>"
            "<p><a href=\"/pybirdai/trails/\">Go To Lineage Viewer</a></p>",
            table_name,
            _build_filters_html(filters_applied),
            row_count_total,
            row_count,
            status_html,
            csv_path,
        )

    return format_html(
        "<h3>ANCRDT Table Execution Results</h3>"
        "<p><strong>Table Name:</strong> {}</p>"
        "<p><strong>Rows Generated:</strong> {}</p>"
        "<p><strong>CSV Path:</strong> {}</p>"
        "<p><a href=\"/pybirdai/trails/\">Go To Lineage Viewer</a></p>",
        table_name,
        row_count,
        csv_path,
    )


def execute_ancrdt_table(request, table_name):
    """
    Execute an ANCRDT table transformation and display results with optional filtering.

    This view mirrors the execute_data_point pattern for FINREP but is adapted
    for ANCRDT table transformations which generate multiple rows instead of
    a single metric value.

    Supports query parameters for filtering by dimension values:
    - Single value: ?INSTRMNT_TYP_PRDCT=51
    - Multiple values: ?PRPS=7,8&INSTRMNT_TYP_PRDCT=51,80
    - Multiple dimensions: ?RPYMNT_RGHTS=1&INSTRMNT_TYP_PRDCT=51&PRPS=7,8&RCRS=1

    Args:
        request: Django HttpRequest object
        table_name (str): Name of the ANCRDT table to execute (e.g., 'ANCRDT_INSTRMNT_C_1')

    Returns:
        HttpResponse: HTML page showing execution results including row count,
                     CSV path, applied filters, and link to lineage viewer

    Example:
        GET /pybirdai/execute-ancrdt-table/ANCRDT_INSTRMNT_C_1/
        GET /pybirdai/execute-ancrdt-table/ANCRDT_INSTRMNT_C_1/?PRPS=7,8&INSTRMNT_TYP_PRDCT=51
    """
    try:
        # Check if JSON format is requested
        format_type = request.GET.get('format', 'html')

        # Parse query parameters for filtering
        filters = {}
        for param_name, param_value in request.GET.items():
            # Skip the 'format' parameter itself
            if param_name == 'format':
                continue
            # Split comma-separated values
            values = [v.strip() for v in param_value.split(',') if v.strip()]
            if values:
                filters[param_name] = values

        # Execute the ANCRDT table transformation with filters
        app_config = RunANCRDTTable('pybirdai', 'birds_nest')
        result = app_config.run_execute_ancrdt_table(table_name, filters=filters if filters else None)

        # Extract results
        row_count = result.get('row_count', 0)
        csv_path = result.get('csv_path', 'N/A')
        row_count_total = result.get('row_count_total', None)
        filters_applied = result.get('filters_applied', None)
        intermediate_tables = result.get('intermediate_tables', [])
        trail_id = result.get('trail_id', None)

        # Return JSON response if requested
        if format_type == 'json':
            json_data = {
                'success': True,
                'table_name': table_name,
                'row_count': row_count,
                'csv_path': csv_path,
            }

            # Add the actual row data - convert objects to dictionaries
            if 'rows' in result:
                rows_data = []
                for row_obj in result['rows']:
                    # Convert row object to dictionary by calling methods
                    row_dict = {}

                    # Get all methods/attributes from the object
                    for attr_name in dir(row_obj):
                        # Skip private/magic methods
                        if attr_name.startswith('_'):
                            continue

                        try:
                            attr = getattr(row_obj, attr_name)

                            # If it's a callable (method), try to call it with no arguments
                            if callable(attr):
                                try:
                                    # Try calling the method (for properties like PRPS(), RCRS(), etc.)
                                    attr_value = attr()

                                    # Only include if it's a simple type
                                    if isinstance(attr_value, (str, int, float, bool, type(None))):
                                        row_dict[attr_name] = attr_value
                                    elif hasattr(attr_value, 'isoformat'):
                                        # Convert datetime objects to ISO format strings
                                        row_dict[attr_name] = attr_value.isoformat()
                                except (TypeError, AttributeError):
                                    # Method requires arguments or can't be called - skip it
                                    pass
                            else:
                                # It's an attribute, only include simple types
                                if isinstance(attr, (str, int, float, bool, type(None))):
                                    row_dict[attr_name] = attr
                                elif hasattr(attr, 'isoformat'):
                                    row_dict[attr_name] = attr.isoformat()
                                # Skip complex objects like 'base', nested objects, etc.

                        except Exception:
                            # Skip attributes/methods that can't be accessed
                            pass

                    rows_data.append(row_dict)
                json_data['rows'] = rows_data

            if filters_applied:
                json_data['filters_applied'] = filters_applied
                json_data['row_count_total'] = row_count_total

            if intermediate_tables:
                json_data['intermediate_tables'] = intermediate_tables

            if trail_id:
                json_data['trail_id'] = trail_id

            # Add aggregation info if present
            if 'aggregation_applied' in result:
                json_data['aggregation_applied'] = result['aggregation_applied']

            return JsonResponse(json_data, json_dumps_params={'indent': 2})

        return HttpResponse(
            _build_result_html(
                table_name,
                row_count,
                csv_path,
                row_count_total=row_count_total,
                filters_applied=filters_applied,
            )
        )

    except AttributeError:
        # Table class not found - likely code generation not run
        format_type = request.GET.get('format', 'html')
        safe_error = 'Requested ANCRDT table is not available.'
        solution = 'Make sure ANCRDT code generation has been run (Step 3 of the ANCRDT pipeline).'
        logger.warning(
            "ANCRDT table class not found for %s",
            sanitize_log_value(table_name),
            exc_info=True,
        )

        if format_type == 'json':
            return JsonResponse(
                {
                    'success': False,
                    'error': safe_error,
                    'solution': solution,
                },
                status=404,
                json_dumps_params={'indent': 2},
            )

        error_html = format_html(
            "<h3>ANCRDT Table Execution Failed</h3>"
            "<p><strong>Error:</strong> {}</p>"
            "<p><strong>Solution:</strong> {}</p>"
            "<p>Run: <code>python pybirdai/standalone/run_ancrdt_pipeline.py</code></p>",
            safe_error,
            solution,
        )
        return HttpResponse(error_html, status=404)

    except Exception as e:
        # General error
        format_type = request.GET.get('format', 'html')
        error_data = SecureErrorHandler.handle_exception(
            e,
            f"executing ANCRDT table {table_name}",
            request,
        )

        if format_type == 'json':
            return JsonResponse(
                {
                    'success': False,
                    'error': error_data['message'],
                    'table_name': table_name,
                },
                status=500,
                json_dumps_params={'indent': 2},
            )

        error_html = format_html(
            "<h3>ANCRDT Table Execution Failed</h3>"
            "<p><strong>Table Name:</strong> {}</p>"
            "<p><strong>Error:</strong> {}</p>"
            "<p>Check the server logs for more details.</p>",
            table_name,
            error_data['message'],
        )
        return HttpResponse(error_html, status=500)
