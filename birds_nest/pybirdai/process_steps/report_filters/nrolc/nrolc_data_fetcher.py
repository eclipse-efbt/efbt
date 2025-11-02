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
Optimized data fetching for NROLC (Non-Reference Output Layer Creator).
Handles bulk data retrieval with chunking to avoid SQLite limitations.
"""

from pybirdai.models.bird_meta_data_model import TABLE_CELL, CELL_POSITION, ORDINATE_ITEM


class DataFetcher:
    """Handles optimized data fetching with chunking for SQLite limits."""

    @staticmethod
    def chunk_list(lst, chunk_size=900):
        """
        Split a list into chunks of specified size to avoid SQLite's 999 variable limit.

        Args:
            lst: List to chunk
            chunk_size: Maximum size of each chunk (default 900 for SQLite safety)

        Yields:
            Chunks of the list
        """
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    @staticmethod
    def fetch_objects_for_creation(table):
        """
        Fetch all related objects needed for creating output layers.
        Uses chunking to avoid SQLite's 999 variable limit.

        OPTIMIZED: Added member_id__domain_id to prevent N+1 queries.

        Args:
            table: TABLE instance

        Returns:
            tuple: (cells, cell_positions, ordinate_items)
        """
        # Fetch all cells
        cells = list(TABLE_CELL.objects.filter(table_id=table))

        # Fetch all cell positions for these cells with chunking
        cell_positions = []
        for cell_chunk in DataFetcher.chunk_list(cells):
            chunk_positions = list(CELL_POSITION.objects.filter(
                cell_id__in=cell_chunk
            ).select_related('axis_ordinate_id'))
            cell_positions.extend(chunk_positions)

        # Get unique axis ordinates and fetch all their ordinate items with related objects
        axis_ordinate_ids = list(set(cp.axis_ordinate_id_id for cp in cell_positions))
        ordinate_items = []
        for ordinate_chunk in DataFetcher.chunk_list(axis_ordinate_ids):
            chunk_items = list(ORDINATE_ITEM.objects.filter(
                axis_ordinate_id__in=ordinate_chunk
            ).select_related(
                'axis_ordinate_id',
                'variable_id',
                'variable_id__domain_id',
                'member_id',
                'member_id__domain_id'
            ))
            ordinate_items.extend(chunk_items)

        return cells, cell_positions, ordinate_items
