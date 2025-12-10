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
Entry point for executing ANCRDT table transformations.

This module provides a Django AppConfig wrapper for executing ANCRDT tables,
similar to execute_datapoint.py but for table-based transformations.
"""

import django
import os
from django.apps import AppConfig
from django.conf import settings
import sys
import logging

# Create a logger
logger = logging.getLogger(__name__)

class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise


class RunANCRDTTable(AppConfig):
    """
    Django AppConfig for running ANCRDT table transformations.

    This class sets up the necessary context and runs the execution process
    to generate and validate ANCRDT table transformations.
    """

    name = 'pybirdai'
    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_execute_ancrdt_table(table_name, filters=None,
                                 aggregate_by=None, aggregate_func=None, aggregate_column=None):
        """
        Execute an ANCRDT table transformation by name with optional filtering and aggregation.

        This method sets up the necessary contexts and executes the table
        transformation, returning structured results for validation.

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
            Exception: If table execution fails

        Example:
            >>> # No filtering
            >>> result = RunANCRDTTable.run_execute_ancrdt_table('ANCRDT_INSTRMNT_C_1')
            >>> print(f"Generated {result['row_count']} rows")

            >>> # With filtering
            >>> filters = {'PRPS': ['7', '8'], 'INSTRMNT_TYP_PRDCT': ['51']}
            >>> result = RunANCRDTTable.run_execute_ancrdt_table('ANCRDT_INSTRMNT_C_1', filters=filters)
            >>> print(f"Filtered: {result['row_count']} from {result['row_count_total']}")

            >>> # With aggregation
            >>> result = RunANCRDTTable.run_execute_ancrdt_table(
            ...     'ANCRDT_INSTRMNT_C_1',
            ...     aggregate_by='INSTRMNT_TYP_PRDCT',
            ...     aggregate_func='count'
            ... )
            >>> print(f"Aggregated into {result['row_count']} groups")
        """
        from pybirdai.process_steps.ancrdt_transformation.execute_ancrdt_table import (
            ExecuteANCRDTTable
        )
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.context.context import Context

        # Set up contexts (same pattern as execute_datapoint)
        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        # Execute the ANCRDT table transformation with optional filters and aggregation
        return ExecuteANCRDTTable.execute_table(
            table_name,
            filters=filters,
            aggregate_by=aggregate_by,
            aggregate_func=aggregate_func,
            aggregate_column=aggregate_column
        )

    def ready(self):
        """
        Called when Django app is ready.

        This method is required for Django's AppConfig but is not used
        for standalone execution.
        """
        pass
