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

from django.http import HttpResponse, JsonResponse
from pybirdai.entry_points.run_ancrdt_table import RunANCRDTTable


def execute_ancrdt_table(request, table_name):
    """
    Execute an ANCRDT table transformation and display results with optional filtering.

    This view mirrors the execute_data_point pattern for FINREP but is adapted
    for ANCRDT table transformations which generate multiple rows instead of
    a single metric value.

    Supports query parameters for filtering by dimension values:
    - Single value: ?TYP_INSTRMNT=51
    - Multiple values: ?PRPS=7,8&TYP_INSTRMNT=51,80
    - Multiple dimensions: ?RPYMNT_RGHTS=1&TYP_INSTRMNT=51&PRPS=7,8&RCRS=1

    Args:
        request: Django HttpRequest object
        table_name (str): Name of the ANCRDT table to execute (e.g., 'ANCRDT_INSTRMNT_C_1')

    Returns:
        HttpResponse: HTML page showing execution results including row count,
                     CSV path, applied filters, and link to lineage viewer

    Example:
        GET /pybirdai/execute-ancrdt-table/ANCRDT_INSTRMNT_C_1/
        GET /pybirdai/execute-ancrdt-table/ANCRDT_INSTRMNT_C_1/?PRPS=7,8&TYP_INSTRMNT=51
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

        # Build HTML response with filter information
        if filters_applied:
            # Filters were applied
            filter_list_html = "<ul>"
            for dimension, values in filters_applied.items():
                values_str = ", ".join(values)
                filter_list_html += f"<li><strong>{dimension}:</strong> [{values_str}]</li>"
            filter_list_html += "</ul>"

            if row_count == 0:
                # No results after filtering
                html_response = f"""
                    <h3>ANCRDT Table Execution Results (Filtered)</h3>
                    <p><strong>Table Name:</strong> {table_name}</p>
                    <p><strong>Applied Filters:</strong></p>
                    {filter_list_html}
                    <p><strong>Total Rows (before filter):</strong> {row_count_total}</p>
                    <p><strong>Filtered Rows:</strong> {row_count}</p>
                    <p><strong>Status:</strong> <span style="color: orange;">No records matched the specified filters</span></p>
                    <p><strong>CSV Path:</strong> {csv_path}</p>
                    <p><a href="/pybirdai/trails/">Go To Lineage Viewer</a></p>
                """
            else:
                # Results found after filtering
                html_response = f"""
                    <h3>ANCRDT Table Execution Results (Filtered)</h3>
                    <p><strong>Table Name:</strong> {table_name}</p>
                    <p><strong>Applied Filters:</strong></p>
                    {filter_list_html}
                    <p><strong>Total Rows (before filter):</strong> {row_count_total}</p>
                    <p><strong>Filtered Rows:</strong> {row_count}</p>
                    <p><strong>CSV Path:</strong> {csv_path}</p>
                    <p><a href="/pybirdai/trails/">Go To Lineage Viewer</a></p>
                """
        else:
            # No filters applied
            html_response = f"""
                <h3>ANCRDT Table Execution Results</h3>
                <p><strong>Table Name:</strong> {table_name}</p>
                <p><strong>Rows Generated:</strong> {row_count}</p>
                <p><strong>CSV Path:</strong> {csv_path}</p>
                <p><a href="/pybirdai/trails/">Go To Lineage Viewer</a></p>
            """

        return HttpResponse(html_response)

    except AttributeError as e:
        # Table class not found - likely code generation not run
        format_type = request.GET.get('format', 'html')

        if format_type == 'json':
            return JsonResponse({
                'success': False,
                'error': f"Table class '{table_name}_Table' not found",
                'details': str(e),
                'solution': 'Make sure ANCRDT code generation has been run (Step 3 of the ANCRDT pipeline)'
            }, status=404, json_dumps_params={'indent': 2})

        error_html = f"""
            <h3>ANCRDT Table Execution Failed</h3>
            <p><strong>Error:</strong> Table class '{table_name}_Table' not found</p>
            <p><strong>Details:</strong> {str(e)}</p>
            <p><strong>Solution:</strong> Make sure ANCRDT code generation has been run (Step 3 of the ANCRDT pipeline)</p>
            <p>Run: <code>python pybirdai/standalone/run_ancrdt_pipeline.py</code></p>
        """
        return HttpResponse(error_html, status=404)

    except Exception as e:
        # General error
        format_type = request.GET.get('format', 'html')

        if format_type == 'json':
            return JsonResponse({
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__,
                'table_name': table_name
            }, status=500, json_dumps_params={'indent': 2})

        error_html = f"""
            <h3>ANCRDT Table Execution Failed</h3>
            <p><strong>Table Name:</strong> {table_name}</p>
            <p><strong>Error:</strong> {str(e)}</p>
            <p><strong>Type:</strong> {type(e).__name__}</p>
            <p>Check the server logs for more details.</p>
        """
        return HttpResponse(error_html, status=500)
