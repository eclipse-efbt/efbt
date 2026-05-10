"""
Cell Execution Service

Provides business logic for executing datapoints from table cells.
Wraps the existing RunExecuteDataPoint with error handling and result formatting.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import time
import logging

from pybirdai.models import TABLE_CELL
from pybirdai.utils.secure_error_handling import SecureErrorHandler
from pybirdai.utils.secure_logging import sanitize_log_value

logger = logging.getLogger(__name__)

GENERIC_EXECUTION_ERROR = SecureErrorHandler.GENERIC_MESSAGES['processing_error']


@dataclass
class CellExecutionResult:
    """Result of a cell execution."""
    success: bool
    value: Optional[str]
    formatted_value: Optional[str]
    error: Optional[str]
    error_code: Optional[str]
    duration_ms: int
    timestamp: datetime
    cell_id: Optional[str] = None
    datapoint_id: Optional[str] = None
    lineage_summary: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            'success': self.success,
            'cell_id': self.cell_id,
            'datapoint_id': self.datapoint_id,
            'execution': {
                'duration_ms': self.duration_ms,
                'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            }
        }

        if self.success:
            result['result'] = {
                'value': self.value,
                'formatted_value': self.formatted_value,
                'data_type': 'numeric' if self._is_numeric(self.value) else 'string'
            }
        else:
            result['error'] = {
                'code': self.error_code or 'EXECUTION_ERROR',
                'message': self.error
            }

        if self.lineage_summary:
            result['lineage_summary'] = self.lineage_summary

        return result

    @staticmethod
    def _is_numeric(value: Optional[str]) -> bool:
        """Check if value is numeric."""
        if value is None:
            return False
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False


class CellExecutionService:
    """Service for executing datapoints from table cells."""

    @staticmethod
    def execute_cell(cell_id: str, include_lineage: bool = False) -> CellExecutionResult:
        """
        Execute the datapoint associated with a table cell.

        Args:
            cell_id: The ID of the TABLE_CELL to execute
            include_lineage: Whether to include lineage summary in result

        Returns:
            CellExecutionResult with execution details
        """
        start_time = time.time()
        timestamp = datetime.now()

        try:
            # Get the cell
            try:
                cell = TABLE_CELL.objects.get(cell_id=cell_id)
            except TABLE_CELL.DoesNotExist:
                return CellExecutionResult(
                    success=False,
                    value=None,
                    formatted_value=None,
                    error=f"Cell not found: {cell_id}",
                    error_code="CELL_NOT_FOUND",
                    duration_ms=0,
                    timestamp=timestamp,
                    cell_id=cell_id,
                    datapoint_id=None
                )

            return CellExecutionService.execute_loaded_cell(
                cell,
                include_lineage=include_lineage,
                start_time=start_time,
                timestamp=timestamp,
                requested_cell_id=cell_id,
            )

        except Exception as e:
            logger.exception(
                "Unexpected error executing cell %s",
                sanitize_log_value(cell_id),
            )
            duration_ms = int((time.time() - start_time) * 1000)
            return CellExecutionResult(
                success=False,
                value=None,
                formatted_value=None,
                error=GENERIC_EXECUTION_ERROR,
                error_code="UNEXPECTED_ERROR",
                duration_ms=duration_ms,
                timestamp=timestamp,
                cell_id=cell_id,
                datapoint_id=None
            )

    @staticmethod
    def execute_loaded_cell(
        cell: TABLE_CELL,
        include_lineage: bool = False,
        start_time: Optional[float] = None,
        timestamp: Optional[datetime] = None,
        requested_cell_id: Optional[str] = None,
    ) -> CellExecutionResult:
        """Execute a datapoint for a TABLE_CELL that has already been loaded."""
        start_time = start_time if start_time is not None else time.time()
        timestamp = timestamp or datetime.now()
        cell_id = requested_cell_id or getattr(cell, 'cell_id', None)

        try:
            if not CellExecutionService.is_cell_executable(cell):
                error_msg = "Cell is shaded/disabled" if getattr(cell, 'is_shaded', False) else "Cell has no associated datapoint"
                error_code = "CELL_SHADED" if getattr(cell, 'is_shaded', False) else "NO_DATAPOINT"
                return CellExecutionResult(
                    success=False,
                    value=None,
                    formatted_value=None,
                    error=error_msg,
                    error_code=error_code,
                    duration_ms=0,
                    timestamp=timestamp,
                    cell_id=cell_id,
                    datapoint_id=None
                )

            datapoint_id = CellExecutionService._build_datapoint_id(cell)
            if not datapoint_id:
                return CellExecutionResult(
                    success=False,
                    value=None,
                    formatted_value=None,
                    error="Could not construct datapoint ID from cell",
                    error_code="INVALID_DATAPOINT",
                    duration_ms=0,
                    timestamp=timestamp,
                    cell_id=cell_id,
                    datapoint_id=None
                )

            try:
                from pybirdai.entry_points.execute_datapoint import RunExecuteDataPoint
                result = RunExecuteDataPoint.run_execute_data_point(datapoint_id)
            except Exception:
                logger.exception(f"Error executing datapoint {datapoint_id}")
                duration_ms = int((time.time() - start_time) * 1000)
                return CellExecutionResult(
                    success=False,
                    value=None,
                    formatted_value=None,
                    error=GENERIC_EXECUTION_ERROR,
                    error_code="EXECUTION_ERROR",
                    duration_ms=duration_ms,
                    timestamp=timestamp,
                    cell_id=cell_id,
                    datapoint_id=datapoint_id
                )

            duration_ms = int((time.time() - start_time) * 1000)
            formatted_value = CellExecutionService._format_value(result)
            lineage_summary = None
            if include_lineage:
                lineage_summary = CellExecutionService._get_lineage_summary(datapoint_id)

            return CellExecutionResult(
                success=True,
                value=result,
                formatted_value=formatted_value,
                error=None,
                error_code=None,
                duration_ms=duration_ms,
                timestamp=timestamp,
                cell_id=cell_id,
                datapoint_id=datapoint_id,
                lineage_summary=lineage_summary
            )

        except Exception:
            logger.exception(
                "Unexpected error executing loaded cell %s",
                sanitize_log_value(cell_id),
            )
            duration_ms = int((time.time() - start_time) * 1000)
            return CellExecutionResult(
                success=False,
                value=None,
                formatted_value=None,
                error=GENERIC_EXECUTION_ERROR,
                error_code="UNEXPECTED_ERROR",
                duration_ms=duration_ms,
                timestamp=timestamp,
                cell_id=cell_id,
                datapoint_id=None
            )

    @staticmethod
    def is_cell_executable(cell: TABLE_CELL) -> bool:
        """
        Check if a cell can be executed.

        Args:
            cell: The TABLE_CELL to check

        Returns:
            True if the cell has an associated datapoint and is not shaded
        """
        return (
            cell.table_cell_combination_id is not None
            and cell.table_cell_combination_id != ''
            and not cell.is_shaded
        )

    @staticmethod
    def _format_value(value: Optional[str]) -> Optional[str]:
        """
        Format a numeric value with thousands separator.

        Args:
            value: The value to format

        Returns:
            Formatted value string
        """
        if value is None:
            return None

        try:
            num = float(value)
            # Check if it's an integer
            if num == int(num):
                return f"{int(num):,}"
            else:
                return f"{num:,.2f}"
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _build_datapoint_id(cell: TABLE_CELL) -> Optional[str]:
        """
        Build the full datapoint ID from a cell's table and combination info.

        The Cell_ classes are named like: Cell_F_04_01_REF_FINREP_3_0_11112_REF
        where:
        - Table code: F_04_01_REF (from TABLE.code field, normalized)
        - Version: FINREP_3_0 (from TABLE.version field, normalized)
        - Combination number: 11112 (extracted from table_cell_combination_id like "EBA_11112")
        - Suffix: _REF (standard suffix for reference implementations)

        Args:
            cell: The TABLE_CELL to build datapoint ID for

        Returns:
            Full datapoint ID string or None if cannot be constructed
        """
        try:
            combination_id = cell.table_cell_combination_id
            if not combination_id:
                return None

            # Extract the numeric combination ID
            # Format can be:
            # - "EBA_11112" (EBA prefix) -> extract "11112"
            # - "67316_REF" (REF suffix) -> extract "67316"
            # - Just numeric like "11112"
            combination_number = combination_id
            if combination_id.endswith('_REF'):
                combination_number = combination_id[:-4]  # Remove "_REF" suffix
            elif combination_id.startswith('EBA_'):
                combination_number = combination_id[4:]  # Remove "EBA_" prefix
            elif '_' in combination_id:
                # Handle other formats - try to find the numeric part
                parts = combination_id.split('_')
                for part in parts:
                    if part.isdigit():
                        combination_number = part
                        break

            # Get the table's code and version fields
            if not cell.table_id:
                return None

            table = cell.table_id
            table_code = table.code  # e.g., "F_04.01_REF"
            table_version = table.version  # e.g., "FINREP 3.0"

            if not table_code or not table_version:
                logger.warning(f"Table {table.table_id} missing code or version")
                return None

            # Normalize the table code (replace dots with underscores)
            # E.g., "F_04.01_REF" -> "F_04_01_REF"
            normalized_code = table_code.replace('.', '_')

            # Normalize the version (replace dots and spaces with underscores)
            # E.g., "FINREP 3.0" -> "FINREP_3_0"
            normalized_version = table_version.replace('.', '_').replace(' ', '_')

            # Build the full datapoint ID
            # Format: {code}_{version}_{combination_number}_REF
            # E.g., "F_04_01_REF_FINREP_3_0_11112_REF"
            datapoint_id = f"{normalized_code}_{normalized_version}_{combination_number}_REF"

            logger.debug(f"Built datapoint ID: {datapoint_id} from code={table_code}, version={table_version}, combination={combination_id}")

            return datapoint_id

        except Exception as e:
            logger.exception(f"Error building datapoint ID for cell {cell.cell_id}")
            return None

    @staticmethod
    def _get_lineage_summary(datapoint_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a summary of lineage data for an executed datapoint.

        Args:
            datapoint_id: The datapoint ID

        Returns:
            Dictionary with lineage summary or None
        """
        try:
            from pybirdai.models import Trail

            # Find the most recent trail for this datapoint
            trail = Trail.objects.filter(
                name__contains=datapoint_id
            ).order_by('-id').first()

            if not trail:
                return None

            return {
                'trail_id': str(trail.id) if trail.id else None,
                'source_tables': [],  # Would need to query related tables
                'rows_processed': 0,  # Would need to count from lineage data
            }
        except Exception:
            return None

    @staticmethod
    def get_cell_lineage(cell_id: str) -> Dict[str, Any]:
        """
        Get detailed lineage information for a cell.

        Args:
            cell_id: The cell ID

        Returns:
            Dictionary with full lineage information
        """
        try:
            cell = TABLE_CELL.objects.select_related('table_id').get(cell_id=cell_id)
        except TABLE_CELL.DoesNotExist:
            return {
                'success': False,
                'error': f"Cell not found: {cell_id}"
            }

        if not cell.table_cell_combination_id:
            return {
                'success': False,
                'error': "Cell has no associated datapoint"
            }

        # Build the full datapoint ID
        datapoint_id = CellExecutionService._build_datapoint_id(cell)
        if not datapoint_id:
            datapoint_id = cell.table_cell_combination_id  # Fallback

        try:
            from pybirdai.models import Trail

            # Find the most recent trail for this datapoint
            trail = Trail.objects.filter(
                name__contains=datapoint_id
            ).order_by('-id').first()

            if not trail:
                return {
                    'success': True,
                    'cell_id': cell_id,
                    'datapoint_id': datapoint_id,
                    'trail_id': None,
                    'message': "No lineage data found. Execute the cell first."
                }

            return {
                'success': True,
                'cell_id': cell_id,
                'datapoint_id': datapoint_id,
                'trail_id': str(trail.id) if trail.id else None,
                'lineage': {
                    'source_tables': [],  # Would populate from actual lineage data
                    'filters_applied': [],
                    'aggregation': None,
                    'calculation_path': []
                },
                'visualization_url': f"/pybirdai/lineage/view/{trail.id}/" if trail.id else None
            }
        except Exception as e:
            error_data = SecureErrorHandler.handle_exception(
                e,
                f'getting lineage for cell {cell_id}',
            )
            return {
                'success': False,
                'error': error_data['message'],
            }
