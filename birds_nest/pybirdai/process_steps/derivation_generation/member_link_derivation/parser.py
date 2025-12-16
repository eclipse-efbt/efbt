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
CSV Parser for member link data.

This module handles parsing of member_link.csv files from the BIRD/ANCRDT
data exports. It extracts member link entries and provides filtering
capabilities for specific cubes and variables.
"""

import csv
import os
from typing import Iterator, List, Optional

from .dataclasses import MemberLinkEntry


class MemberLinkParser:
    """Parser for member_link.csv files.

    This class reads member link CSV files and converts them into
    MemberLinkEntry objects with parsed component fields.
    """

    # CSV column names
    COL_CUBE_STRUCTURE_ITEM_LINK_ID = "CUBE_STRUCTURE_ITEM_LINK_ID"
    COL_FOREIGN_MEMBER_ID = "FOREIGN_MEMBER_ID"
    COL_PRIMARY_MEMBER_ID = "PRIMARY_MEMBER_ID"
    COL_VALID_FROM = "VALID_FROM"
    COL_VALID_TO = "VALID_TO"
    COL_IS_LINKED = "IS_LINKED"

    def __init__(self, csv_path: str):
        """Initialize the parser with a CSV file path.

        Args:
            csv_path: Path to the member_link.csv file
        """
        self.csv_path = csv_path
        self._validate_path()

    def _validate_path(self):
        """Validate that the CSV file exists."""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"Member link CSV not found: {self.csv_path}")

    def parse(self) -> List[MemberLinkEntry]:
        """Parse the entire CSV file into MemberLinkEntry objects.

        Returns:
            List of MemberLinkEntry objects
        """
        entries = []
        for entry in self._iter_entries():
            entries.append(entry)
        return entries

    def parse_filtered(
        self,
        target_cube: Optional[str] = None,
        target_variable: Optional[str] = None,
        source_table: Optional[str] = None,
        only_active: bool = True
    ) -> List[MemberLinkEntry]:
        """Parse CSV with filtering applied during iteration.

        This method is more memory-efficient for large files as it
        filters during iteration rather than loading all entries first.

        Args:
            target_cube: Filter for specific target cube (e.g., ANCRDT_INSTRMNT_C)
            target_variable: Filter for specific target variable (e.g., TYP_INSTRMNT)
            source_table: Filter for specific source table (e.g., BIRD_INSTRMNT_EIL)
            only_active: Only include entries where is_linked is True

        Returns:
            List of filtered MemberLinkEntry objects
        """
        entries = []
        for entry in self._iter_entries():
            # Apply filters
            if only_active and not entry.is_linked:
                continue
            if target_cube and entry.target_cube != target_cube:
                continue
            if target_variable and entry.target_variable != target_variable:
                continue
            if source_table and entry.source_table != source_table:
                continue
            entries.append(entry)
        return entries

    def _iter_entries(self) -> Iterator[MemberLinkEntry]:
        """Iterate over CSV rows yielding MemberLinkEntry objects.

        Yields:
            MemberLinkEntry for each row in the CSV
        """
        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield self._row_to_entry(row)

    def _row_to_entry(self, row: dict) -> MemberLinkEntry:
        """Convert a CSV row dict to a MemberLinkEntry.

        Args:
            row: Dictionary from csv.DictReader

        Returns:
            MemberLinkEntry with parsed fields
        """
        is_linked_str = row.get(self.COL_IS_LINKED, "false").lower()
        is_linked = is_linked_str == "true"

        return MemberLinkEntry(
            cube_structure_item_link_id=row.get(self.COL_CUBE_STRUCTURE_ITEM_LINK_ID, ""),
            foreign_member_id=row.get(self.COL_FOREIGN_MEMBER_ID, ""),
            primary_member_id=row.get(self.COL_PRIMARY_MEMBER_ID, ""),
            valid_from=row.get(self.COL_VALID_FROM, ""),
            valid_to=row.get(self.COL_VALID_TO, ""),
            is_linked=is_linked
        )


def filter_by_cube(entries: List[MemberLinkEntry], cube_name: str) -> List[MemberLinkEntry]:
    """Filter member link entries by target cube name.

    Args:
        entries: List of MemberLinkEntry objects
        cube_name: Target cube name to filter for

    Returns:
        Filtered list of entries
    """
    return [e for e in entries if e.target_cube == cube_name]


def filter_by_target_variable(entries: List[MemberLinkEntry], var_name: str) -> List[MemberLinkEntry]:
    """Filter member link entries by target variable name.

    Args:
        entries: List of MemberLinkEntry objects
        var_name: Target variable name to filter for

    Returns:
        Filtered list of entries
    """
    return [e for e in entries if e.target_variable == var_name]


def filter_by_source_variable(entries: List[MemberLinkEntry], var_name: str) -> List[MemberLinkEntry]:
    """Filter member link entries by source variable name.

    Args:
        entries: List of MemberLinkEntry objects
        var_name: Source variable name to filter for

    Returns:
        Filtered list of entries
    """
    return [e for e in entries if e.source_variable == var_name]


def filter_active_only(entries: List[MemberLinkEntry]) -> List[MemberLinkEntry]:
    """Filter to only include active (is_linked=True) entries.

    Args:
        entries: List of MemberLinkEntry objects

    Returns:
        Filtered list of active entries
    """
    return [e for e in entries if e.is_linked]


def get_unique_source_variables(entries: List[MemberLinkEntry]) -> List[str]:
    """Get unique source variable names from entries.

    Args:
        entries: List of MemberLinkEntry objects

    Returns:
        Sorted list of unique source variable names
    """
    return sorted(set(e.source_variable for e in entries if e.source_variable))


def get_unique_source_tables(entries: List[MemberLinkEntry]) -> List[str]:
    """Get unique source table names from entries.

    Args:
        entries: List of MemberLinkEntry objects

    Returns:
        Sorted list of unique source table names
    """
    return sorted(set(e.source_table for e in entries if e.source_table))
