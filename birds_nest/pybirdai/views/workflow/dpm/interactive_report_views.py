"""
Interactive Report Viewer Views

Provides views and API endpoints for the Interactive Report Viewer feature.
Allows users to view regulatory templates and execute datapoints by clicking cells.
"""

import json
import time
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Generator

from django.shortcuts import render
from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.http import require_http_methods, require_GET, require_POST
from django.db import close_old_connections, connection

from pybirdai.models import TABLE, TABLE_CELL
from pybirdai.services.cell_execution_service import CellExecutionService
from pybirdai.services.table_rendering_service import TableRenderingService

logger = logging.getLogger(__name__)
SAFE_CELL_ERROR_CODES = {'CELL_NOT_FOUND', 'CELL_SHADED', 'NO_DATAPOINT', 'INVALID_DATAPOINT'}
DEFAULT_BATCH_WORKERS = max(1, min(4, (os.cpu_count() or 2)))


def _public_cell_error(result):
    """Return a user-facing error message that does not expose internals."""
    if result.success:
        return None

    if result.error_code in SAFE_CELL_ERROR_CODES:
        return result.error

    return 'Cell execution failed. Please try again later.'


def _parse_parallel_setting(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value = value.lower()
        if value in {'1', 'true', 'yes', 'on'}:
            return True
        if value in {'0', 'false', 'no', 'off'}:
            return False
    return 'auto'


def _get_batch_execution_plan(total, requested_parallel='auto', requested_workers=None):
    if total <= 1 or requested_parallel is False:
        return 1, False

    try:
        worker_count = int(requested_workers) if requested_workers is not None else DEFAULT_BATCH_WORKERS
    except (TypeError, ValueError):
        worker_count = DEFAULT_BATCH_WORKERS
    worker_count = max(1, min(worker_count, total, DEFAULT_BATCH_WORKERS))

    if requested_parallel is True:
        return worker_count, False

    try:
        from pybirdai.context.context import Context
        lineage_enabled = Context.get_current_lineage_setting()
    except Exception:
        lineage_enabled = True

    # SQLite serializes writes aggressively. With lineage enabled every cell writes
    # many lineage rows. In auto mode, use value-only parallel execution so SQLite
    # sees mostly reads; single-cell execution still creates lineage on demand.
    if connection.vendor == 'sqlite' and lineage_enabled:
        return worker_count, True

    return worker_count, False


def _cell_chunks(cells, worker_count):
    chunk_size = max(1, math.ceil(len(cells) / worker_count))
    for start in range(0, len(cells), chunk_size):
        yield start, cells[start:start + chunk_size]


def _execute_cell_chunk(start_index, cells, suppress_lineage=False):
    close_old_connections()
    try:
        from contextlib import nullcontext
        from pybirdai.context.context import lineage_tracking_override
        from pybirdai.process_steps.pybird.execute_datapoint import lineage_file_cleanup_scope
        from pybirdai.process_steps.pybird.orchestration import shared_reference_cache

        results = []
        lineage_context = lineage_tracking_override(False) if suppress_lineage else nullcontext()
        with lineage_context, lineage_file_cleanup_scope(cleaned=True), shared_reference_cache():
            for cell in cells:
                results.append(CellExecutionService.execute_loaded_cell(cell).to_dict())
        return start_index, results
    finally:
        close_old_connections()


def _execute_cells_serial(cells, suppress_lineage=False):
    from contextlib import nullcontext
    from pybirdai.context.context import lineage_tracking_override
    from pybirdai.process_steps.pybird.execute_datapoint import lineage_file_cleanup_scope
    from pybirdai.process_steps.pybird.orchestration import shared_reference_cache

    results = []
    lineage_context = lineage_tracking_override(False) if suppress_lineage else nullcontext()
    with lineage_context, lineage_file_cleanup_scope(), shared_reference_cache():
        for cell in cells:
            results.append(CellExecutionService.execute_loaded_cell(cell).to_dict())
    return results


def _execute_cells_parallel(cells, worker_count, suppress_lineage=False):
    from pybirdai.process_steps.pybird.execute_datapoint import ExecuteDataPoint

    ExecuteDataPoint.delete_lineage_data()
    chunks = list(_cell_chunks(cells, worker_count))
    chunk_results = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(_execute_cell_chunk, start_index, chunk, suppress_lineage)
            for start_index, chunk in chunks
        ]
        for future in futures:
            chunk_results.append(future.result())

    results = []
    for _, result_chunk in sorted(chunk_results, key=lambda item: item[0]):
        results.extend(result_chunk)
    return results


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
        force_lineage: bool - Execute with lineage tracking enabled (optional)

    Returns:
        JSON with execution result
    """
    try:
        body = json.loads(request.body) if request.body else {}
    except json.JSONDecodeError:
        body = {}

    include_lineage = body.get('include_lineage', False)
    force_lineage = body.get('force_lineage', False)

    if force_lineage:
        from pybirdai.context.context import lineage_tracking_override

        with lineage_tracking_override(True):
            result = CellExecutionService.execute_cell(cell_id, include_lineage=include_lineage)
    else:
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
    requested_cell_ids = body.get('cell_ids')
    parallel_setting = _parse_parallel_setting(body.get('parallel', 'auto'))
    requested_workers = body.get('max_workers')

    start_time = time.time()

    # Get all executable cells for this table
    cells_query = TABLE_CELL.objects.filter(table_id=table_id).select_related('table_id')
    if isinstance(requested_cell_ids, list) and requested_cell_ids:
        requested_cell_ids = [str(cell_id) for cell_id in requested_cell_ids]
        cells_by_id = {
            cell.cell_id: cell
            for cell in cells_query.filter(cell_id__in=requested_cell_ids)
        }
        cells = [cells_by_id[cell_id] for cell_id in requested_cell_ids if cell_id in cells_by_id]
    else:
        cells = list(cells_query)

    executable_cells = [c for c in cells if CellExecutionService.is_cell_executable(c)]

    worker_count, suppress_lineage = _get_batch_execution_plan(
        len(executable_cells),
        requested_parallel=parallel_setting,
        requested_workers=requested_workers
    )
    if worker_count > 1:
        results = _execute_cells_parallel(executable_cells, worker_count, suppress_lineage=suppress_lineage)
    else:
        results = _execute_cells_serial(executable_cells, suppress_lineage=suppress_lineage)
    errors = sum(1 for result in results if not result.get('success'))

    total_duration_ms = int((time.time() - start_time) * 1000)

    return JsonResponse({
        'success': True,
        'results': results,
        'completed': len(results),
        'total': len(executable_cells),
        'errors': errors,
        'duration_ms': total_duration_ms,
        'parallel': worker_count > 1,
        'workers': worker_count,
        'lineage': not suppress_lineage
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

        from pybirdai.process_steps.pybird.execute_datapoint import lineage_file_cleanup_scope
        from pybirdai.process_steps.pybird.orchestration import shared_reference_cache
        with lineage_file_cleanup_scope(), shared_reference_cache():
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
