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
Data models for member link derivation generation.

This module contains dataclasses representing member link entries
and derivation mappings used throughout the derivation generation pipeline.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MemberLinkEntry:
    """Represents a single member link entry from the CSV.

    Attributes:
        cube_structure_item_link_id: Full link ID from CSV
        foreign_member_id: Target/output member value
        primary_member_id: Source/input member value
        valid_from: Validity start date
        valid_to: Validity end date
        is_linked: Whether the link is active

        Parsed fields (extracted from cube_structure_item_link_id):
        target_cube: The target cube name (e.g., ANCRDT_INSTRMNT_C)
        source_table: The source table name (e.g., BIRD_INSTRMNT_EIL)
        source_variable: The source variable name (e.g., INSTRMNT_TYP_PRDCT)
        target_variable: The target variable name (e.g., TYP_INSTRMNT)
        instance: The instance number from the link ID
    """
    cube_structure_item_link_id: str
    foreign_member_id: str
    primary_member_id: str
    valid_from: str
    valid_to: str
    is_linked: bool

    # Parsed fields - populated by parser
    target_cube: str = ""
    source_table: str = ""
    source_variable: str = ""
    target_variable: str = ""
    instance: str = ""

    def __post_init__(self):
        """Parse the cube_structure_item_link_id to extract components."""
        self._parse_link_id()

    def _parse_link_id(self):
        """Extract target cube, source table, and variable names from link ID.

        Link ID format:
        GEN_L__<TARGET_CUBE>__<TARGET_CUBE>__<SOURCE_TABLE>__<INSTANCE>__<SOURCE_VAR>__<TARGET_VAR>

        Example:
        GEN_L__ANCRDT_INSTRMNT_C__GEN_L__ANCRDT_INSTRMNT_C__BIRD_INSTRMNT_EIL__1__INSTRMNT_TYP_PRDCT__TYP_INSTRMNT
        """
        if not self.cube_structure_item_link_id:
            return

        parts = self.cube_structure_item_link_id.split("__")

        # Expected minimum parts: GEN_L, CUBE, GEN_L, CUBE, TABLE, INSTANCE, SRC_VAR, TGT_VAR
        if len(parts) >= 8:
            # Find the BIRD_ table which indicates start of source info
            bird_idx = None
            for i, part in enumerate(parts):
                if part.startswith("BIRD_"):
                    bird_idx = i
                    break

            if bird_idx is not None and bird_idx + 3 < len(parts):
                # Extract target cube (appears after GEN_L)
                self.target_cube = parts[1]
                self.source_table = parts[bird_idx]
                self.instance = parts[bird_idx + 1]
                self.source_variable = parts[bird_idx + 2]
                self.target_variable = parts[bird_idx + 3]


@dataclass
class DerivationMapping:
    """Represents a group of member mappings for a single source variable.

    Attributes:
        source_variable: The name of the source variable
        source_table: The source table this mapping comes from
        target_variable: The name of the target variable
        mappings: Dict mapping input member values to output member values
        specificity_score: Score indicating how specific this mapping is
        entries: The original MemberLinkEntry objects for reference
    """
    source_variable: str
    source_table: str
    target_variable: str
    mappings: Dict[str, str] = field(default_factory=dict)
    specificity_score: int = 0
    entries: List[MemberLinkEntry] = field(default_factory=list)

    def add_mapping(self, input_member: str, output_member: str, entry: MemberLinkEntry):
        """Add a member mapping to this derivation.

        Args:
            input_member: The source/input member value
            output_member: The target/output member value
            entry: The original MemberLinkEntry
        """
        self.mappings[input_member] = output_member
        self.entries.append(entry)

    def get_output_for_input(self, input_member: str) -> Optional[str]:
        """Get the output member for a given input member.

        Args:
            input_member: The input member value to look up

        Returns:
            The corresponding output member value, or None if not found
        """
        return self.mappings.get(input_member)


@dataclass
class DerivationConfig:
    """Configuration for derivation generation.

    Attributes:
        csv_path: Path to the member_link.csv file
        output_dir: Directory to write generated code
        target_cube: The cube to filter for (e.g., ANCRDT_INSTRMNT_C)
        target_variable: The target variable to derive (e.g., TYP_INSTRMNT)
        output_variable_name: Name for the generated property (e.g., TYP_INSTRMNT_ANCRDT)
        class_name: Name of the class to generate the property for (e.g., INSTRMNT)
        models_path: Path to Django models.py file for AST introspection
    """
    csv_path: str = "results/ancrdt_csv/member_link.csv"
    output_dir: str = "resources/derivation_files/generated_from_member_links/"
    target_cube: str = "ANCRDT_INSTRMNT_C"
    target_variable: str = "TYP_INSTRMNT"
    output_variable_name: str = "TYP_INSTRMNT_ANCRDT"
    class_name: str = "INSTRMNT"
    models_path: str = "results/database_configuration_files/models.py"
