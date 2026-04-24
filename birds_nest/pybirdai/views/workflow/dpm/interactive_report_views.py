"""
Interactive Report Viewer Views

Provides views and API endpoints for the Interactive Report Viewer feature.
Allows users to view regulatory templates and execute datapoints by clicking cells.
"""

import json
import time
import logging
from typing import Generator

from django.shortcuts import render
from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.http import require_http_methods, require_GET, require_POST
from django.views.decorators.csrf import csrf_exempt

from pybirdai.models import TABLE, TABLE_CELL
from pybirdai.services.cell_execution_service import CellExecutionService
from pybirdai.services.table_rendering_service import TableRenderingService

logger = logging.getLogger(__name__)
SAFE_CELL_ERROR_CODES = {'CELL_NOT_FOUND', 'CELL_SHADED', 'NO_DATAPOINT', 'INVALID_DATAPOINT'}


def _public_cell_error(result):
    """Return a user-facing error message that does not expose internals."""
    if result.success:
        return None

    if result.error_code in SAFE_CELL_ERROR_CODES:
        return result.error

    return 'Cell execution failed. Please try again later.'


# =============================================================================
# Page Views
# =============================================================================

def report_viewer_index(request):
    """
    Template selection page for the Interactive Report Viewer.
    Lists available templates with filtering and search.
    """
    context = {
        'page_title': 'Interactive Report Viewer',
    }
    return render(request, 'pybirdai/workflow/interactive_report/index.html', context)


def report_viewer_detail(request, table_id: str):
    """
    Main viewer page for a specific table/template.
    Renders the table with clickable cells for datapoint execution.
    """
    # Verify table exists
    try:
        table = TABLE.objects.get(table_id=table_id)
    except TABLE.DoesNotExist:
        return render(request, 'pybirdai/workflow/interactive_report/viewer.html', {
            'error': f'Table not found: {table_id}',
            'table_id': table_id,
        })

    context = {
        'page_title': f'Report Viewer - {table.code or table.name or table_id}',
        'table_id': table_id,
        'table_code': table.code or '',
        'table_name': table.name or '',
        'table_description': table.description or '',
    }
    return render(request, 'pybirdai/workflow/interactive_report/viewer.html', context)


# =============================================================================
# API Endpoints
# =============================================================================

@require_GET
def api_get_templates(request):
    """
    List available templates with filtering.

    Query Parameters:
        framework: Filter by framework (FINREP, COREP, etc.)
        version: Filter by version
        search: Search by name or code
        has_executable_cells: Only show templates with executable cells
        page: Page number (default: 1)
        page_size: Page size (default: 50)

    Returns:
        JSON with templates list and pagination info
    """
    framework = request.GET.get('framework', '')
    version = request.GET.get('version', '')
    search = request.GET.get('search', '')
    has_executable_cells = request.GET.get('has_executable_cells', '').lower() == 'true'

    try:
        page = int(request.GET.get('page', 1))
    except ValueError:
        page = 1

    try:
        page_size = int(request.GET.get('page_size', 50))
        page_size = min(page_size, 100)  # Max 100 per page
    except ValueError:
        page_size = 50

    result = TableRenderingService.get_templates_list(
        framework=framework if framework else None,
        version=version if version else None,
        search=search if search else None,
        has_executable_cells=has_executable_cells,
        page=page,
        page_size=page_size
    )

    return JsonResponse(result)


@require_GET
def api_render_table(request, table_id: str):
    """
    Get the full renderable structure for a table.

    Args:
        table_id: The table ID

    Returns:
        JSON with table structure including headers and cells
    """
    result = TableRenderingService.render_table(table_id)
    return JsonResponse(result)


@require_POST
def api_execute_cell(request, cell_id: str):
    """
    Execute the datapoint for a specific cell.

    Args:
        cell_id: The cell ID

    Request Body (JSON):
        force_refresh: bool - Ignore cache (optional)
        include_lineage: bool - Include lineage summary (optional)

    Returns:
        JSON with execution result
    """
    try:
        body = json.loads(request.body) if request.body else {}
    except json.JSONDecodeError:
        body = {}

    include_lineage = body.get('include_lineage', False)

    result = CellExecutionService.execute_cell(cell_id, include_lineage=include_lineage)
    return JsonResponse(result.to_dict())


@require_POST
def api_execute_all(request, table_id: str):
    """
    Execute all executable cells in a table.

    Args:
        table_id: The table ID

    Request Body (JSON):
        skip_cached: bool - Skip cells that have been executed (optional)

    Returns:
        JSON with all results
    """
    try:
        body = json.loads(request.body) if request.body else {}
    except json.JSONDecodeError:
        body = {}

    skip_cached = body.get('skip_cached', False)

    start_time = time.time()

    # Get all executable cells for this table
    cells = TABLE_CELL.objects.filter(table_id=table_id)
    executable_cells = [
        c for c in cells
        if CellExecutionService.is_cell_executable(c)
    ]

    results = []
    errors = 0

    for cell in executable_cells:
        result = CellExecutionService.execute_cell(cell.cell_id)
        results.append(result.to_dict())
        if not result.success:
            errors += 1

    total_duration_ms = int((time.time() - start_time) * 1000)

    return JsonResponse({
        'success': True,
        'results': results,
        'completed': len(results),
        'total': len(executable_cells),
        'errors': errors,
        'duration_ms': total_duration_ms
    })


@require_GET
def api_execute_all_stream(request, table_id: str):
    """
    Execute all executable cells with SSE streaming for progress updates.

    Args:
        table_id: The table ID

    Returns:
        Server-Sent Events stream with progress updates
    """
    def generate_events() -> Generator[str, None, None]:
        """Generate SSE events for cell execution progress."""
        start_time = time.time()

        # Get all executable cells
        cells = TABLE_CELL.objects.filter(table_id=table_id)
        executable_cells = [
            c for c in cells
            if CellExecutionService.is_cell_executable(c)
        ]

        total = len(executable_cells)
        completed = 0
        errors = 0

        for cell in executable_cells:
            result = CellExecutionService.execute_cell(cell.cell_id)
            completed += 1

            if not result.success:
                errors += 1

            # Send progress event
            event_data = {
                'completed': completed,
                'total': total,
                'cell_id': cell.cell_id,
                'result': result.value if result.success else None,
                'formatted_result': result.formatted_value if result.success else None,
                'error': _public_cell_error(result),
                'datapoint_id': result.datapoint_id
            }
            yield f"event: progress\ndata: {json.dumps(event_data)}\n\n"

        # Send complete event
        total_duration_ms = int((time.time() - start_time) * 1000)
        complete_data = {
            'completed': completed,
            'total': total,
            'errors': errors,
            'duration_ms': total_duration_ms
        }
        yield f"event: complete\ndata: {json.dumps(complete_data)}\n\n"

    response = StreamingHttpResponse(
        generate_events(),
        content_type='text/event-stream'
    )
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


@require_GET
def api_get_cell_lineage(request, cell_id: str):
    """
    Get detailed lineage information for a cell.

    Args:
        cell_id: The cell ID

    Returns:
        JSON with lineage information
    """
    result = CellExecutionService.get_cell_lineage(cell_id)
    return JsonResponse(result)
