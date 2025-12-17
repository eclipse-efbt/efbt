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
Specificity Resolver for member link derivations.

This module handles the resolution of conflicts when multiple member link
entries map the same input to different outputs. It uses a specificity-based
scoring system to determine which mapping should take precedence.
"""

from collections import defaultdict
from typing import Dict, List, Tuple

from .dataclasses import DerivationMapping, MemberLinkEntry


class SpecificityResolver:
    """Resolves conflicts in member link mappings using specificity scoring.

    Specificity is determined by:
    1. Source table specificity (more specific tables like BIRD_INSTRMNT_EIL
       score higher than generic tables)
    2. Whether the mapping is a direct domain match vs cross-domain
    3. The presence of additional context in the link ID
    """

    # Table specificity scores - higher is more specific
    TABLE_SPECIFICITY = {
        "BIRD_INSTRMNT_EIL": 100,
        "BIRD_INSTRMNT_RL_EIL": 90,
        "BIRD_INSTRMNT_ENTTY_RL_ASSGNMNT_EIL": 85,
        "BIRD_PRTY_EIL": 80,
        "BIRD_SCRTY_EXCHNG_TRDBL_DRVTV_EIL": 75,
        "BIRD_SCRTY_PSTN_EIL": 70,
        "BIRD_CLLTRL_RL_EIL": 65,
        "BIRD_NN_FNNCL_ASST_EIL": 60,
        "BIRD_EXCHNG_TRDBL_DRVTV_PSTN_RL_EIL": 55,
    }

    # Default specificity for unknown tables
    DEFAULT_TABLE_SPECIFICITY = 50

    def __init__(self):
        """Initialize the resolver."""
        self._conflict_log: List[str] = []

    @property
    def conflict_log(self) -> List[str]:
        """Get the log of resolved conflicts."""
        return self._conflict_log

    def calculate_specificity(self, entry: MemberLinkEntry) -> int:
        """Calculate the specificity score for a member link entry.

        Args:
            entry: The MemberLinkEntry to score

        Returns:
            Integer specificity score (higher = more specific)
        """
        score = 0

        # Table specificity
        table_score = self.TABLE_SPECIFICITY.get(
            entry.source_table,
            self.DEFAULT_TABLE_SPECIFICITY
        )
        score += table_score

        # Bonus for direct variable domain match
        # (when source and target variables share the same domain prefix)
        if self._is_same_domain(entry.source_variable, entry.target_variable):
            score += 20

        # Bonus for more specific link IDs (longer paths = more context)
        link_parts = entry.cube_structure_item_link_id.count("__")
        score += min(link_parts, 10)  # Cap at 10 bonus points

        return score

    def _is_same_domain(self, source_var: str, target_var: str) -> bool:
        """Check if source and target variables share the same domain.

        Args:
            source_var: Source variable name
            target_var: Target variable name

        Returns:
            True if they appear to be from the same domain
        """
        # Extract the base domain name (before any suffixes)
        source_base = source_var.split("_")[0] if source_var else ""
        target_base = target_var.split("_")[0] if target_var else ""
        return source_base == target_base and source_base != ""

    def group_by_source_variable(
        self,
        entries: List[MemberLinkEntry]
    ) -> Dict[str, List[MemberLinkEntry]]:
        """Group member link entries by their source variable.

        Args:
            entries: List of MemberLinkEntry objects

        Returns:
            Dictionary mapping source variable names to lists of entries
        """
        grouped: Dict[str, List[MemberLinkEntry]] = defaultdict(list)
        for entry in entries:
            if entry.source_variable:
                grouped[entry.source_variable].append(entry)
        return dict(grouped)

    def group_by_source_table_and_variable(
        self,
        entries: List[MemberLinkEntry]
    ) -> Dict[Tuple[str, str], List[MemberLinkEntry]]:
        """Group entries by (source_table, source_variable) tuple.

        Args:
            entries: List of MemberLinkEntry objects

        Returns:
            Dictionary mapping (table, variable) tuples to lists of entries
        """
        grouped: Dict[Tuple[str, str], List[MemberLinkEntry]] = defaultdict(list)
        for entry in entries:
            if entry.source_table and entry.source_variable:
                key = (entry.source_table, entry.source_variable)
                grouped[key].append(entry)
        return dict(grouped)

    def resolve_conflicts(
        self,
        entries: List[MemberLinkEntry]
    ) -> List[DerivationMapping]:
        """Resolve conflicts and create DerivationMapping objects.

        When multiple entries map the same input member to different outputs,
        this method selects the mapping with the highest specificity score.

        Args:
            entries: List of MemberLinkEntry objects

        Returns:
            List of DerivationMapping objects with conflicts resolved
        """
        self._conflict_log = []

        # Group by (source_table, source_variable) for more precise mappings
        grouped = self.group_by_source_table_and_variable(entries)

        derivation_mappings: List[DerivationMapping] = []

        for (source_table, source_var), group_entries in grouped.items():
            # Create a DerivationMapping for this group
            mapping = DerivationMapping(
                source_variable=source_var,
                source_table=source_table,
                target_variable=group_entries[0].target_variable if group_entries else ""
            )

            # Track input members that map to multiple outputs
            input_to_entries: Dict[str, List[MemberLinkEntry]] = defaultdict(list)
            for entry in group_entries:
                input_to_entries[entry.primary_member_id].append(entry)

            # Resolve conflicts for each input member
            for input_member, candidate_entries in input_to_entries.items():
                if len(candidate_entries) == 1:
                    # No conflict - direct mapping
                    entry = candidate_entries[0]
                    mapping.add_mapping(input_member, entry.foreign_member_id, entry)
                else:
                    # Conflict - resolve by specificity
                    resolved_entry = self._resolve_single_conflict(candidate_entries)
                    mapping.add_mapping(
                        input_member,
                        resolved_entry.foreign_member_id,
                        resolved_entry
                    )

            # Calculate overall specificity for this mapping group
            if mapping.entries:
                mapping.specificity_score = max(
                    self.calculate_specificity(e) for e in mapping.entries
                )

            derivation_mappings.append(mapping)

        # Sort by specificity (highest first)
        derivation_mappings.sort(key=lambda m: m.specificity_score, reverse=True)

        return derivation_mappings

    def _resolve_single_conflict(
        self,
        candidates: List[MemberLinkEntry]
    ) -> MemberLinkEntry:
        """Resolve a conflict where one input maps to multiple outputs.

        Args:
            candidates: List of conflicting MemberLinkEntry objects

        Returns:
            The entry with the highest specificity score
        """
        # Score each candidate
        scored = [(self.calculate_specificity(e), e) for e in candidates]
        scored.sort(key=lambda x: x[0], reverse=True)

        winner = scored[0][1]
        losers = [e for _, e in scored[1:]]

        # Log the conflict resolution
        if losers:
            loser_outputs = [e.foreign_member_id for e in losers]
            self._conflict_log.append(
                f"Conflict resolved for {winner.primary_member_id}: "
                f"selected {winner.foreign_member_id} (specificity={scored[0][0]}) "
                f"over {loser_outputs}"
            )

        return winner

    def merge_mappings_by_variable(
        self,
        mappings: List[DerivationMapping]
    ) -> Dict[str, DerivationMapping]:
        """Merge mappings from different tables for the same source variable.

        When the same source variable appears in multiple source tables,
        this method merges them, preferring higher-specificity mappings.

        Args:
            mappings: List of DerivationMapping objects

        Returns:
            Dictionary mapping source variable names to merged DerivationMapping
        """
        merged: Dict[str, DerivationMapping] = {}

        for mapping in mappings:
            var_name = mapping.source_variable

            if var_name not in merged:
                # First mapping for this variable
                merged[var_name] = DerivationMapping(
                    source_variable=var_name,
                    source_table=mapping.source_table,
                    target_variable=mapping.target_variable,
                    specificity_score=mapping.specificity_score
                )
                merged[var_name].mappings = mapping.mappings.copy()
                merged[var_name].entries = mapping.entries.copy()
            else:
                # Merge with existing, preferring higher specificity for conflicts
                existing = merged[var_name]
                for input_member, output_member in mapping.mappings.items():
                    if input_member not in existing.mappings:
                        # No conflict - add the mapping
                        existing.mappings[input_member] = output_member
                    elif mapping.specificity_score > existing.specificity_score:
                        # Higher specificity - override
                        existing.mappings[input_member] = output_member
                        self._conflict_log.append(
                            f"Cross-table merge: {var_name}.{input_member} "
                            f"updated to {output_member} from {mapping.source_table}"
                        )

                # Update entries and specificity if this mapping is more specific
                if mapping.specificity_score > existing.specificity_score:
                    existing.specificity_score = mapping.specificity_score
                    existing.source_table = mapping.source_table

                existing.entries.extend(mapping.entries)

        return merged
