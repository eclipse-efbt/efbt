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
Execute ANCRDT table transformations for testing and validation.

This module provides execution framework for ANCRDT table transformations,
similar to execute_datapoint.py but for table-based outputs instead of
single datapoint values.
"""

import importlib
import logging
import os
from datetime import datetime
from django.conf import settings

logger = logging.getLogger(__name__)


class ExecuteANCRDTTable:
    """
    Execute ANCRDT table transformations and return structured results.

    This class provides methods to execute ANCRDT table transformations
    dynamically by table name, with support for lineage tracking and
    validation of generated data.
    """

    @staticmethod
    def filter_rows(rows, filters):
        """
        Filter rows based on dimension values.

        Applies post-execution filtering to generated rows based on query parameters.
        Each filter is a dimension name (e.g., 'PRPS', 'INSTRMNT_TYP_PRDCT') with allowed values.

        Args:
            rows (list): List of row objects to filter
            filters (dict): Dictionary of dimension filters, e.g.:
                {
                    'PRPS': ['7', '8'],
                    'INSTRMNT_TYP_PRDCT': ['51', '80'],
                    'RCRS': ['1']
                }

        Returns:
            list: Filtered list of rows that match ALL specified filters

        Example:
            >>> filters = {'PRPS': ['7', '8'], 'INSTRMNT_TYP_PRDCT': ['51']}
            >>> filtered = ExecuteANCRDTTable.filter_rows(rows, filters)
        """
        if not filters:
            return rows

        filtered = []
        for row in rows:
            match = True
            for dimension, allowed_values in filters.items():
                # Get dimension value from row (calls method like row.PRPS())
                if hasattr(row, dimension):
                    try:
                        # Call the method to get the actual value
                        actual_value = getattr(row, dimension)()

                        # Check if value matches allowed values
                        if actual_value not in allowed_values:
                            match = False
                            break
                    except Exception as e:
                        logger.warning(f"Could not get value for dimension '{dimension}': {e}")
                        match = False
                        break
                else:
                    # Dimension not found on row object - skip this row
                    match = False
                    break

            if match:
                filtered.append(row)

        return filtered

    @staticmethod
    def aggregate_rows(rows, aggregate_by, aggregate_func, aggregate_column=None):
        """
        Aggregate rows based on specified dimensions and aggregation function.

        Groups rows by one or more dimensions and applies an aggregation function
        (count, sum, mean) to produce summary rows.

        Args:
            rows (list): List of row objects to aggregate
            aggregate_by (str or list): Column name(s) to group by
            aggregate_func (str): Aggregation function - 'count', 'sum', or 'mean'
            aggregate_column (str, optional): Column to aggregate (required for sum/mean)

        Returns:
            list: Aggregated rows as dictionaries with group dimensions and aggregated value

        Example:
            >>> # Count instruments by type
            >>> agg = ExecuteANCRDTTable.aggregate_rows(rows, 'INSTRMNT_TYP_PRDCT', 'count')
            >>> # Result: [{'INSTRMNT_TYP_PRDCT': '51', 'count': 5}, {'INSTRMNT_TYP_PRDCT': '80', 'count': 3}]

            >>> # Sum by multiple dimensions
            >>> agg = ExecuteANCRDTTable.aggregate_rows(
            ...     rows, ['INSTRMNT_TYP_PRDCT', 'PRPS'], 'sum', 'CMMTMNT_INCPTN'
            ... )
        """
        if not rows:
            return []

        # Normalize aggregate_by to list
        if isinstance(aggregate_by, str):
            aggregate_by = [aggregate_by]

        # Validate aggregate_func
        valid_funcs = ['count', 'sum', 'mean']
        if aggregate_func not in valid_funcs:
            raise ValueError(f"Invalid aggregate_func '{aggregate_func}'. Must be one of: {valid_funcs}")

        # For sum/mean, aggregate_column is required
        if aggregate_func in ['sum', 'mean'] and not aggregate_column:
            raise ValueError(f"aggregate_column is required for aggregate_func='{aggregate_func}'")

        # Group rows by aggregate_by dimensions
        groups = {}
        for row in rows:
            # Build group key from aggregate_by dimensions
            group_key_values = []
            for dimension in aggregate_by:
                if hasattr(row, dimension):
                    try:
                        value = getattr(row, dimension)()
                        group_key_values.append(str(value))
                    except Exception as e:
                        logger.warning(f"Could not get value for dimension '{dimension}': {e}")
                        group_key_values.append(None)
                else:
                    group_key_values.append(None)

            # Use tuple of values as dictionary key
            group_key = tuple(group_key_values)

            # Initialize group if not exists
            if group_key not in groups:
                groups[group_key] = []

            # Add row to group
            groups[group_key].append(row)

        # Apply aggregation function to each group
        aggregated = []
        for group_key, group_rows in groups.items():
            # Build result dictionary with group dimensions
            result = {}
            for i, dimension in enumerate(aggregate_by):
                result[dimension] = group_key[i]

            # Apply aggregation function
            if aggregate_func == 'count':
                result['count'] = len(group_rows)

            elif aggregate_func == 'sum':
                total = 0
                for row in group_rows:
                    if hasattr(row, aggregate_column):
                        try:
                            value = getattr(row, aggregate_column)()
                            # Handle numeric conversion
                            if value is not None:
                                total += float(value)
                        except Exception as e:
                            logger.warning(f"Could not aggregate column '{aggregate_column}': {e}")
                result['sum'] = total

            elif aggregate_func == 'mean':
                total = 0
                count = 0
                for row in group_rows:
                    if hasattr(row, aggregate_column):
                        try:
                            value = getattr(row, aggregate_column)()
                            if value is not None:
                                total += float(value)
                                count += 1
                        except Exception as e:
                            logger.warning(f"Could not aggregate column '{aggregate_column}': {e}")
                result['mean'] = total / count if count > 0 else 0

            aggregated.append(result)

        # Sort aggregated results by aggregate_by dimensions for consistent ordering
        # This ensures tests can validate results deterministically
        if aggregated:
            def smart_sort_key(row_dict):
                """Sort key that handles numeric strings correctly."""
                if isinstance(aggregate_by, list):
                    values = []
                    for dim in aggregate_by:
                        val = row_dict.get(dim, '')
                        # Try to convert to int/float for numeric sorting
                        try:
                            values.append(int(val))
                        except (ValueError, TypeError):
                            try:
                                values.append(float(val))
                            except (ValueError, TypeError):
                                values.append(val)  # Keep as string
                    return tuple(values)
                else:
                    val = row_dict.get(aggregate_by, '')
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        try:
                            return float(val)
                        except (ValueError, TypeError):
                            return val

            aggregated.sort(key=smart_sort_key)

        return aggregated

    @staticmethod
    def execute_table(table_name, filters=None, aggregate_by=None, aggregate_func=None, aggregate_column=None):
        """
        Execute an ANCRDT table transformation by name with optional filtering and aggregation.

        Args:
            table_name (str): Name of the ANCRDT table to execute (e.g., 'ANCRDT_INSTRMNT_C_1')
            filters (dict, optional): Dictionary of dimension filters for post-execution filtering.
                Example: {'PRPS': ['7', '8'], 'INSTRMNT_TYP_PRDCT': ['51', '80']}
            aggregate_by (str or list, optional): Column name(s) to group by for aggregation
            aggregate_func (str, optional): Aggregation function - 'count', 'sum', or 'mean'
            aggregate_column (str, optional): Column to aggregate (required for sum/mean)

        Returns:
            dict: Execution results containing:
                - table_name (str): Name of executed table
                - row_count (int): Number of rows after filtering/aggregation
                - row_count_total (int): Number of rows before filtering (if filters applied)
                - csv_path (str): Path to generated CSV file
                - rows (list): List of row objects or aggregated dictionaries
                - filters_applied (dict): Filters that were applied (if any)
                - aggregation_applied (dict): Aggregation params that were applied (if any)

        Raises:
            AttributeError: If table class not found in ancrdt_output_tables
            ValueError: If invalid aggregation parameters provided
            Exception: If table execution fails
        """
        ExecuteANCRDTTable.delete_lineage_data()
        logger.info(f"Executing ANCRDT table: {table_name}")

        # Set up AORTA lineage tracking
        from pybirdai.process_steps.pybird.orchestration import Orchestration, OrchestrationWithLineage
        from pybirdai.annotations.decorators import set_lineage_orchestration

        # Create orchestration based on configuration
        orchestration = Orchestration()
        execution_name = f"ANCRDTTable_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Only set up lineage if using the lineage-enhanced orchestrator and lineage is enabled
        if isinstance(orchestration, OrchestrationWithLineage) and orchestration.lineage_enabled:
            # Initialize the trail and metadata without dummy objects
            orchestration.trail = None
            orchestration.metadata_trail = None
            orchestration.current_populated_tables = {}
            orchestration.current_rows = {}

            # Create trail directly
            from pybirdai.models import MetaDataTrail, Trail
            orchestration.metadata_trail = MetaDataTrail.objects.create()
            orchestration.trail = Trail.objects.create(
                name=execution_name,
                metadata_trail=orchestration.metadata_trail
            )
            logger.info(f"Created AORTA Trail: {orchestration.trail.name}")

            # Set the global lineage context
            set_lineage_orchestration(orchestration)
        elif isinstance(orchestration, OrchestrationWithLineage):
            logger.info(f"Using lineage orchestrator but lineage tracking is disabled in config")
            # Clear the global lineage context since lineage is disabled
            set_lineage_orchestration(None)
        else:
            logger.info(f"Using original orchestrator - lineage tracking disabled")
            # Clear the global lineage context since we're using original orchestrator
            set_lineage_orchestration(None)

        try:
            # Import the table class dynamically
            # Tables are generated to pybirdai/process_steps/filter_code/ancrdt_output_tables.py
            module = importlib.import_module(
                'pybirdai.process_steps.filter_code.ancrdt_output_tables'
            )

            # Look for the table class (e.g., ANCRDT_INSTRMNT_C_1_Table)
            table_class_name = f"{table_name}_Table"
            if not hasattr(module, table_class_name):
                raise AttributeError(
                    f"Table class '{table_class_name}' not found in ancrdt_output_tables. "
                    f"Make sure ANCRDT code generation has been run (Step 3 of pipeline)."
                )

            table_class = getattr(module, table_class_name)

            # Set calculation context early if lineage is enabled
            if isinstance(orchestration, OrchestrationWithLineage) and orchestration.lineage_enabled:
                calculation_name = table_class.__name__
                orchestration.current_calculation = calculation_name
                logger.debug(f"Set calculation context: {calculation_name}")

                # Add debugging to orchestration
                from pybirdai.api.debug_tracking import add_debug_to_orchestration
                add_debug_to_orchestration(orchestration)

            # Initialize the table
            # The init() method will:
            # 1. Call Orchestration().init(self) to wire up dependencies
            # 2. Call calc_*s() methods to generate row items
            # 3. Auto-save to CSV via CSVConverter
            logger.debug(f"Initializing table class: {table_class_name}")
            table_instance = table_class()
            table_instance.init()

            # Extract generated rows
            # Table class has attribute like ANCRDT_INSTRMNT_C_1s (table name + 's')
            rows_attr_name = f"{table_name}s"
            if not hasattr(table_instance, rows_attr_name):
                raise AttributeError(
                    f"Table instance does not have expected rows attribute '{rows_attr_name}'. "
                    f"Check generated code structure."
                )

            rows = getattr(table_instance, rows_attr_name)
            row_count_total = len(rows)

            # CSV file path (auto-generated by CSVConverter.persist_object_as_csv)
            csv_path = os.path.join(
                settings.BASE_DIR,
                'results',
                'lineage',
                f'{table_name}_longnames.csv'
            )

            logger.info(f"Table execution completed: {row_count_total} rows generated")
            logger.info(f"CSV saved to: {csv_path}")

            # Apply post-execution filtering if filters provided
            if filters:
                logger.info(f"Applying post-execution filters: {filters}")
                filtered_rows = ExecuteANCRDTTable.filter_rows(rows, filters)
                row_count_filtered = len(filtered_rows)
                logger.info(f"Filtering completed: {row_count_filtered} rows match filters (from {row_count_total} total)")
            else:
                filtered_rows = rows
                row_count_filtered = row_count_total

            # Apply post-execution aggregation if aggregation parameters provided
            aggregation_applied = None
            if aggregate_by and aggregate_func:
                logger.info(f"Applying aggregation: GROUP BY {aggregate_by}, {aggregate_func.upper()}" +
                      (f"({aggregate_column})" if aggregate_column else ""))
                aggregated_rows = ExecuteANCRDTTable.aggregate_rows(
                    filtered_rows,
                    aggregate_by,
                    aggregate_func,
                    aggregate_column
                )
                row_count_aggregated = len(aggregated_rows)
                logger.info(f"Aggregation completed: {row_count_aggregated} groups (from {row_count_filtered} rows)")

                # Update rows to aggregated results
                final_rows = aggregated_rows
                final_row_count = row_count_aggregated

                # Store aggregation metadata
                aggregation_applied = {
                    'aggregate_by': aggregate_by,
                    'aggregate_func': aggregate_func,
                    'aggregate_column': aggregate_column,
                    'row_count_before_aggregation': row_count_filtered
                }
            else:
                final_rows = filtered_rows
                final_row_count = row_count_filtered

            # Print lineage summary if enabled
            if isinstance(orchestration, OrchestrationWithLineage) and orchestration.lineage_enabled:
                trail = orchestration.get_lineage_trail()
                if trail:
                    logger.debug(f"AORTA Trail created: {trail.name} (ID: {trail.id})")
                    from pybirdai.models import (
                        DatabaseTable, PopulatedDataBaseTable, DatabaseField, DatabaseRow,
                        CalculationUsedRow, CalculationUsedField
                    )
                    logger.debug(f"  DatabaseTables: {DatabaseTable.objects.count()}")
                    logger.debug(f"  PopulatedTables: {PopulatedDataBaseTable.objects.count()}")
                    logger.debug(f"  DatabaseFields: {DatabaseField.objects.count()}")
                    logger.debug(f"  DatabaseRows: {DatabaseRow.objects.count()}")

                    # Print tracking information
                    used_rows = CalculationUsedRow.objects.filter(trail=trail)
                    used_fields = CalculationUsedField.objects.filter(trail=trail)
                    logger.debug(f"  Tracked Used Rows: {used_rows.count()}")
                    logger.debug(f"  Tracked Used Fields: {used_fields.count()}")

            # Return structured result for validation
            result = {
                'table_name': table_name,
                'row_count': final_row_count,
                'csv_path': csv_path,
                'rows': final_rows
            }

            # Add filtering metadata if filters were applied
            if filters:
                result['row_count_total'] = row_count_total
                result['filters_applied'] = filters

            # Add aggregation metadata if aggregation was applied
            if aggregation_applied:
                result['aggregation_applied'] = aggregation_applied

            # Collect intermediate table information from orchestration
            intermediate_tables = []
            trail_id = None

            if isinstance(orchestration, OrchestrationWithLineage) and orchestration.lineage_enabled:
                trail = orchestration.get_lineage_trail()
                if trail:
                    trail_id = trail.id

                    # Import models if not already available
                    from pybirdai.models import (
                        DatabaseTable, PopulatedDataBaseTable, EvaluatedDerivedTable,
                        DatabaseRow, DerivedTableRow
                    )

                    # Collect information about all populated tables
                    if hasattr(orchestration, 'current_populated_tables'):
                        for table_name_key, populated_table in orchestration.current_populated_tables.items():
                            table_info = {
                                'name': table_name_key,
                                'type': 'database' if isinstance(populated_table, PopulatedDataBaseTable) else 'derived',
                            }

                            # Get row count
                            try:
                                if isinstance(populated_table, PopulatedDataBaseTable):
                                    table_info['row_count'] = DatabaseRow.objects.filter(populated_table=populated_table).count()
                                elif isinstance(populated_table, EvaluatedDerivedTable):
                                    table_info['row_count'] = DerivedTableRow.objects.filter(populated_table=populated_table).count()
                                else:
                                    table_info['row_count'] = 0
                            except Exception as e:
                                logger.warning(f"Could not get row count for {table_name_key}: {e}")
                                table_info['row_count'] = 0

                            # Check for CSV file
                            csv_path = os.path.join(settings.BASE_DIR, 'results', 'lineage', f'{table_name_key}_longnames.csv')
                            if os.path.exists(csv_path):
                                table_info['csv_path'] = csv_path
                                table_info['csv_url'] = f'/media/lineage/{table_name_key}_longnames.csv'

                            intermediate_tables.append(table_info)

            # Add intermediate tables and trail ID to result
            if intermediate_tables:
                result['intermediate_tables'] = intermediate_tables
            if trail_id:
                result['trail_id'] = trail_id

            return result

        except Exception as e:
            logger.error(f"Error executing table {table_name}: {str(e)}")
            raise
        finally:
            # Cleanup
            if 'table_instance' in locals():
                del table_instance

    @staticmethod
    def delete_lineage_data():
        """
        Delete generated lineage data files.

        This method cleans up CSV files and other artifacts from previous
        executions to ensure clean test runs.
        """
        base_dir = settings.BASE_DIR
        lineage_dir = os.path.join(base_dir, 'results', 'lineage')

        # Ensure directory exists
        if not os.path.exists(lineage_dir):
            os.makedirs(lineage_dir, exist_ok=True)
            return

        # Delete all files except __init__.py
        for file in os.listdir(lineage_dir):
            if file != "__init__.py":
                file_path = os.path.join(lineage_dir, file)
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    # File doesn't exist - this is fine, nothing to clean up
                    pass
                except Exception as e:
                    # Real error (permissions, I/O, etc.) - warn user
                    logger.warning(f"Could not delete {file_path}: {e}")
