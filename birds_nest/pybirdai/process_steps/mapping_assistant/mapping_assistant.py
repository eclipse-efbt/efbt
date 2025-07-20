# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

from typing import List, Dict, Tuple, Set, Optional
from dataclasses import dataclass
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class MappingProposal:
    """Represents a proposed mapping with confidence score"""
    source_variable_id: str
    source_member_id: str
    target_variable_id: str
    target_member_id: str
    confidence_score: float
    member_mapping_id: str
    member_mapping_row: str
    reasoning: str = ""


@dataclass
class CombinationData:
    """Represents a combination with its items"""
    combination_id: str
    items: List[Dict[str, str]]  # List of {variable_id, member_id, subdomain_id}


class MappingAssistant:
    """
    Intelligent mapping assistant that suggests mappings based on combinations
    and historical mapping patterns.
    """

    def __init__(self):
        self.memoization_dictionary = {}
        self.pattern_cache = {}
        self.initialise_memoization_dictionary()

    def initialise_memoization_dictionary(self):
        self.memoization_dictionary["similarity_scores"] = {}
        self.memoization_dictionary["mapping_found"] = {}
        self.memoization_dictionary["candidates"] = {}
        self.memoization_dictionary["patterns"] = {}

    def find_mapping(self,
        source_set_of_tuple: tuple,
        source_mapping_set_of_tuple: tuple
    ) -> bool:
        is_mapping = False
        if source_set_of_tuple == source_mapping_set_of_tuple:
            is_mapping = True
        return is_mapping

    def compute_similarity(self, source_set_of_tuple: tuple, source_mapping_set_of_tuple: tuple) -> float:
        """Compute similarity between two sets of tuples"""
        cache_key = (source_set_of_tuple, source_mapping_set_of_tuple)

        if cache_key in self.memoization_dictionary["similarity_scores"]:
            return self.memoization_dictionary["similarity_scores"][cache_key]

        if source_set_of_tuple == source_mapping_set_of_tuple:
            self.memoization_dictionary["similarity_scores"][cache_key] = 1.0
            return 1.0

        intersect_len = len(set(source_set_of_tuple).intersection(set(source_mapping_set_of_tuple)))
        max_len = max(len(source_set_of_tuple), len(source_mapping_set_of_tuple))

        if max_len == 0:
            similarity = 0.0
        else:
            similarity = intersect_len / max_len

        self.memoization_dictionary["similarity_scores"][cache_key] = similarity
        return similarity

    def return_mapping_or_candidates_for_source_set(self, source_set_of_tuple: set, mapping_sets_of_tuple: list, threshold: float = 0.8) -> Set[tuple]:
        """Find exact mapping or candidates above threshold"""
        if source_set_of_tuple in self.memoization_dictionary["mapping_found"]:
            return {self.memoization_dictionary["mapping_found"][source_set_of_tuple]}

        if source_set_of_tuple in self.memoization_dictionary["candidates"]:
            return self.memoization_dictionary["candidates"][source_set_of_tuple]

        candidates = set()
        for mapping_set_of_tuple in mapping_sets_of_tuple:
            if source_set_of_tuple == mapping_set_of_tuple:
                self.memoization_dictionary["mapping_found"][source_set_of_tuple] = mapping_set_of_tuple
                return {mapping_set_of_tuple}

            similarity = self.compute_similarity(source_set_of_tuple, mapping_set_of_tuple)
            if similarity >= threshold:
                candidates.add(mapping_set_of_tuple)

        self.memoization_dictionary["candidates"][source_set_of_tuple] = candidates
        return candidates

    def extract_combination_patterns(self, combinations: List[CombinationData]) -> Dict[str, Set[Tuple[str, str]]]:
        """Extract patterns from combinations for matching"""
        patterns = defaultdict(set)

        for combination in combinations:
            for item in combination.items:
                variable_id = item.get('variable_id')
                member_id = item.get('member_id')
                if variable_id and member_id:
                    patterns[combination.combination_id].add((variable_id, member_id))

        return dict(patterns)

    def analyze_member_mappings(self, member_mapping_items: List[Dict]) -> Dict[str, List[Dict]]:
        """Analyze member mapping items to extract patterns"""
        mapping_patterns = defaultdict(list)

        for item in member_mapping_items:
            mapping_id = item.get('member_mapping_id')
            row = item.get('member_mapping_row', '')
            is_source = item.get('is_source', 'false').lower() == 'true'

            key = f"{mapping_id}_{row}"
            mapping_patterns[key].append({
                'variable_id': item.get('variable_id'),
                'member_id': item.get('member_id'),
                'is_source': is_source,
                'mapping_id': mapping_id,
                'row': row
            })

        return dict(mapping_patterns)

    def match_combination_to_mapping(self,
                                   combination_pattern: Set[Tuple[str, str]],
                                   mapping_patterns: Dict[str, List[Dict]],
                                   variable_mapping_filter: Optional[Dict] = None) -> List[MappingProposal]:
        """Match a combination pattern to potential mappings"""
        proposals = []

        for mapping_key, mapping_items in mapping_patterns.items():
            # Extract source items from mapping
            source_items = [(item['variable_id'], item['member_id'])
                          for item in mapping_items if item['is_source']]

            if not source_items:
                continue

            # Convert to tuple for comparison
            source_tuple = tuple(sorted(source_items))
            combination_tuple = tuple(sorted(combination_pattern))

            # Calculate similarity
            similarity = self.compute_similarity(combination_tuple, source_tuple)

            if similarity >= 0.7:  # Threshold for considering a match
                # Extract target items
                target_items = [item for item in mapping_items if not item['is_source']]

                # Apply variable mapping filter if provided
                # if variable_mapping_filter:
                #     # Filter based on variable mapping constraints
                #     valid_targets = []
                #     for target in target_items:
                #         if self._is_valid_target(target, variable_mapping_filter):
                #             valid_targets.append(target)
                #     target_items = valid_targets

                # Create proposals for each source-target pair
                for source in source_items:
                    for target in target_items:
                        if source[0] in [item[0] for item in combination_pattern]:
                            proposal = MappingProposal(
                                source_variable_id=source[0],
                                source_member_id=source[1],
                                target_variable_id=target['variable_id'],
                                target_member_id=target['member_id'],
                                confidence_score=similarity,
                                member_mapping_id=mapping_items[0]['mapping_id'],
                                member_mapping_row=mapping_items[0]['row'],
                                reasoning=f"Pattern match with {similarity:.0%} similarity"
                            )
                            proposals.append(proposal)

        return proposals

    def _is_valid_target(self, target_item: Dict, variable_mapping_filter: Dict) -> bool:
        """Check if a target item is valid based on variable mapping filter"""
        # Implementation depends on the structure of variable_mapping_filter
        # For now, return True to accept all targets
        return True

    def generate_mapping_proposals(self,
                                 combinations: List[CombinationData],
                                 member_mapping_items: List[Dict],
                                 variable_mapping_items: Optional[List[Dict]] = None,
                                 confidence_threshold: float = 0.7) -> Dict[str, List[MappingProposal]]:
        """
        Generate mapping proposals for given combinations based on existing mappings.

        Args:
            combinations: List of combinations with their items
            member_mapping_items: List of existing member mapping items
            variable_mapping_items: Optional list of variable mapping items for filtering
            confidence_threshold: Minimum confidence score for proposals

        Returns:
            Dictionary mapping combination_id to list of proposals
        """
        logger.info(f"Generating mapping proposals for {len(combinations)} combinations")

        # Extract patterns from combinations
        combination_patterns = self.extract_combination_patterns(combinations)

        # Analyze existing mappings
        mapping_patterns = self.analyze_member_mappings(member_mapping_items)

        # Build variable mapping filter if provided
        variable_filter = None
        if variable_mapping_items:
            variable_filter = self._build_variable_filter(variable_mapping_items)

        # Generate proposals for each combination
        all_proposals = {}

        for combination in combinations:
            combination_id = combination.combination_id
            pattern = combination_patterns.get(combination_id, set())

            if not pattern:
                logger.warning(f"No pattern found for combination {combination_id}")
                continue

            # Find matching mappings
            proposals = self.match_combination_to_mapping(
                pattern,
                mapping_patterns,
                variable_filter
            )

            # Filter by confidence threshold
            filtered_proposals = [p for p in proposals if p.confidence_score >= confidence_threshold]

            # Sort by confidence score
            filtered_proposals.sort(key=lambda p: p.confidence_score, reverse=True)

            all_proposals[combination_id] = filtered_proposals

            logger.info(f"Generated {len(filtered_proposals)} proposals for combination {combination_id}")

        return all_proposals

    def _build_variable_filter(self, variable_mapping_items: List[Dict]) -> Dict:
        """Build a filter from variable mapping items"""
        filter_dict = defaultdict(set)

        for item in variable_mapping_items:
            mapping_id = item.get('variable_mapping_id')
            variable_id = item.get('variable_id')
            is_source = item.get('is_source', 'false').lower() == 'true'

            if mapping_id and variable_id:
                filter_dict[mapping_id].add((variable_id, is_source))

        return dict(filter_dict)

    def learn_from_feedback(self, accepted_proposals: List[MappingProposal], rejected_proposals: List[MappingProposal]):
        """Learn from user feedback to improve future proposals"""
        # Store accepted patterns for future use
        for proposal in accepted_proposals:
            pattern_key = (proposal.source_variable_id, proposal.source_member_id)
            target_key = (proposal.target_variable_id, proposal.target_member_id)

            if pattern_key not in self.pattern_cache:
                self.pattern_cache[pattern_key] = []
            self.pattern_cache[pattern_key].append(target_key)

        logger.info(f"Learned from {len(accepted_proposals)} accepted and {len(rejected_proposals)} rejected proposals")
