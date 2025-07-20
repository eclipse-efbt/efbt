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
#
"""Entry point for the Mapping Assistant feature."""

import os
import json
from typing import List, Dict, Optional
from pathlib import Path

import django
from django.apps import AppConfig
from django.conf import settings

from pybirdai.bird_meta_data_model import (
    COMBINATION, COMBINATION_ITEM, MEMBER_MAPPING, MEMBER_MAPPING_ITEM,
    VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM, FRAMEWORK, CUBE, CUBE_TO_COMBINATION
)


class RunMappingAssistant(AppConfig):
    """Django AppConfig for running the Mapping Assistant."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def generate_mapping_proposals(
        combination_ids: List[str],
        variable_mapping_ids: Optional[List[str]] = None,
        confidence_threshold: float = 0.7
    ) -> Dict:
        """
        Generate mapping proposals for given combinations.
        
        Args:
            combination_ids: List of combination IDs to process
            variable_mapping_ids: Optional list of variable mapping IDs for filtering
            confidence_threshold: Minimum confidence score for proposals
            
        Returns:
            Dictionary containing proposals and metadata
        """
        from pybirdai.process_steps.mapping_assistant import (
            MappingAssistant, CombinationData, MappingProposal
        )
        
        assistant = MappingAssistant()
        
        # Fetch combinations and their items
        combinations_data = []
        for combination_id in combination_ids:
            try:
                combination = COMBINATION.objects.get(combination_id=combination_id)
                items = COMBINATION_ITEM.objects.filter(combination_id=combination)
                
                # Convert to CombinationData format
                items_data = []
                for item in items:
                    item_dict = {
                        'variable_id': item.variable_id.variable_id if item.variable_id else None,
                        'member_id': item.member_id.member_id if item.member_id else None,
                        'subdomain_id': item.subdomain_id.subdomain_id if item.subdomain_id else None
                    }
                    items_data.append(item_dict)
                
                combo_data = CombinationData(
                    combination_id=combination_id,
                    items=items_data
                )
                combinations_data.append(combo_data)
                
            except COMBINATION.DoesNotExist:
                print(f"Warning: Combination {combination_id} not found")
                continue
        
        # Fetch all member mapping items
        member_mapping_items = []
        for item in MEMBER_MAPPING_ITEM.objects.all().select_related('member_mapping_id', 'variable_id', 'member_id'):
            item_dict = {
                'member_mapping_id': item.member_mapping_id.member_mapping_id if item.member_mapping_id else None,
                'member_mapping_row': item.member_mapping_row,
                'variable_id': item.variable_id.variable_id if item.variable_id else None,
                'member_id': item.member_id.member_id if item.member_id else None,
                'is_source': item.is_source
            }
            member_mapping_items.append(item_dict)
        
        # Fetch variable mapping items if specified
        variable_mapping_items = None
        if variable_mapping_ids:
            variable_mapping_items = []
            for vm_id in variable_mapping_ids:
                items = VARIABLE_MAPPING_ITEM.objects.filter(
                    variable_mapping_id__variable_mapping_id=vm_id
                ).select_related('variable_mapping_id', 'variable_id')
                
                for item in items:
                    item_dict = {
                        'variable_mapping_id': item.variable_mapping_id.variable_mapping_id if item.variable_mapping_id else None,
                        'variable_id': item.variable_id.variable_id if item.variable_id else None,
                        'is_source': item.is_source
                    }
                    variable_mapping_items.append(item_dict)
        
        # Generate proposals
        proposals = assistant.generate_mapping_proposals(
            combinations=combinations_data,
            member_mapping_items=member_mapping_items,
            variable_mapping_items=variable_mapping_items,
            confidence_threshold=confidence_threshold
        )
        
        # Convert proposals to serializable format
        result = {
            'proposals': {},
            'summary': {
                'total_combinations': len(combinations_data),
                'combinations_with_proposals': 0,
                'total_proposals': 0
            }
        }
        
        for combination_id, proposal_list in proposals.items():
            if proposal_list:
                result['summary']['combinations_with_proposals'] += 1
                result['summary']['total_proposals'] += len(proposal_list)
                
            result['proposals'][combination_id] = []
            for proposal in proposal_list:
                result['proposals'][combination_id].append({
                    'source_variable_id': proposal.source_variable_id,
                    'source_member_id': proposal.source_member_id,
                    'target_variable_id': proposal.target_variable_id,
                    'target_member_id': proposal.target_member_id,
                    'confidence_score': proposal.confidence_score,
                    'member_mapping_id': proposal.member_mapping_id,
                    'member_mapping_row': proposal.member_mapping_row,
                    'reasoning': proposal.reasoning
                })
        
        return result

    @staticmethod
    def save_proposals_to_file(proposals: Dict, output_file: str):
        """Save proposals to a JSON file."""
        output_path = Path(settings.BASE_DIR) / 'results' / 'mapping_assistant' / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(proposals, f, indent=2)
        
        print(f"Proposals saved to: {output_path}")
        return str(output_path)

    @staticmethod
    def accept_proposals(accepted_proposal_ids: List[Dict]) -> Dict:
        """
        Accept and save selected proposals as new member mappings.
        
        Args:
            accepted_proposal_ids: List of proposal dictionaries to accept
            
        Returns:
            Dictionary with success status and created mappings
        """
        created_mappings = []
        errors = []
        
        for proposal in accepted_proposal_ids:
            try:
                # Check if mapping already exists
                existing = MEMBER_MAPPING_ITEM.objects.filter(
                    member_mapping_id__member_mapping_id=proposal['member_mapping_id'],
                    member_mapping_row=proposal['member_mapping_row'],
                    variable_id__variable_id=proposal['source_variable_id'],
                    member_id__member_id=proposal['source_member_id'],
                    is_source='true'
                ).exists()
                
                if not existing:
                    # Create source mapping item
                    source_item = MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id_id=proposal['member_mapping_id'],
                        member_mapping_row=proposal['member_mapping_row'],
                        variable_id_id=proposal['source_variable_id'],
                        member_id_id=proposal['source_member_id'],
                        is_source='true'
                    )
                    created_mappings.append(f"Source: {proposal['source_variable_id']}/{proposal['source_member_id']}")
                
                # Check if target mapping already exists
                existing_target = MEMBER_MAPPING_ITEM.objects.filter(
                    member_mapping_id__member_mapping_id=proposal['member_mapping_id'],
                    member_mapping_row=proposal['member_mapping_row'],
                    variable_id__variable_id=proposal['target_variable_id'],
                    member_id__member_id=proposal['target_member_id'],
                    is_source='false'
                ).exists()
                
                if not existing_target:
                    # Create target mapping item
                    target_item = MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id_id=proposal['member_mapping_id'],
                        member_mapping_row=proposal['member_mapping_row'],
                        variable_id_id=proposal['target_variable_id'],
                        member_id_id=proposal['target_member_id'],
                        is_source='false'
                    )
                    created_mappings.append(f"Target: {proposal['target_variable_id']}/{proposal['target_member_id']}")
                    
            except Exception as e:
                errors.append(f"Error processing proposal: {str(e)}")
        
        return {
            'success': len(errors) == 0,
            'created_mappings': created_mappings,
            'errors': errors
        }

    @staticmethod
    def get_combinations_for_template(cube_id: str) -> List[str]:
        """Get all combination IDs for a specific template/cube."""
        cube_combinations = CUBE_TO_COMBINATION.objects.filter(
            cube_id__cube_id=cube_id
        ).select_related('combination_id')
        
        return [cc.combination_id.combination_id for cc in cube_combinations if cc.combination_id]

    @staticmethod
    def get_combinations_for_framework(framework_id: str) -> List[str]:
        """Get all combination IDs for all templates in a framework."""
        # Get all cubes for the framework
        cubes = CUBE.objects.filter(framework_id__framework_id=framework_id)
        
        all_combinations = []
        for cube in cubes:
            cube_combinations = CUBE_TO_COMBINATION.objects.filter(
                cube_id=cube
            ).select_related('combination_id')
            
            for cc in cube_combinations:
                if cc.combination_id:
                    all_combinations.append(cc.combination_id.combination_id)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_combinations = []
        for combo_id in all_combinations:
            if combo_id not in seen:
                seen.add(combo_id)
                unique_combinations.append(combo_id)
        
        return unique_combinations

    @staticmethod
    def generate_proposals_for_template(
        cube_id: str,
        variable_mapping_ids: Optional[List[str]] = None,
        confidence_threshold: float = 0.7
    ) -> Dict:
        """
        Generate mapping proposals for all combinations in a specific template/cube.
        
        Args:
            cube_id: The cube ID representing the template
            variable_mapping_ids: Optional list of variable mapping IDs for filtering
            confidence_threshold: Minimum confidence score for proposals
            
        Returns:
            Dictionary containing proposals and metadata
        """
        # Get all combinations for this template
        combination_ids = RunMappingAssistant.get_combinations_for_template(cube_id)
        
        if not combination_ids:
            return {
                'proposals': {},
                'summary': {
                    'template_id': cube_id,
                    'total_combinations': 0,
                    'combinations_with_proposals': 0,
                    'total_proposals': 0
                }
            }
        
        # Generate proposals using existing method
        result = RunMappingAssistant.generate_mapping_proposals(
            combination_ids=combination_ids,
            variable_mapping_ids=variable_mapping_ids,
            confidence_threshold=confidence_threshold
        )
        
        # Add template-specific metadata
        result['summary']['template_id'] = cube_id
        
        return result

    @staticmethod
    def generate_proposals_for_framework(
        framework_id: str,
        variable_mapping_ids: Optional[List[str]] = None,
        confidence_threshold: float = 0.7,
        ensure_consistency: bool = True
    ) -> Dict:
        """
        Generate mapping proposals for all combinations across all templates in a framework.
        
        Args:
            framework_id: The framework ID
            variable_mapping_ids: Optional list of variable mapping IDs for filtering
            confidence_threshold: Minimum confidence score for proposals
            ensure_consistency: Whether to ensure mapping consistency across templates
            
        Returns:
            Dictionary containing proposals and metadata
        """
        # Get all combinations for this framework
        combination_ids = RunMappingAssistant.get_combinations_for_framework(framework_id)
        
        if not combination_ids:
            return {
                'proposals': {},
                'summary': {
                    'framework_id': framework_id,
                    'total_combinations': 0,
                    'combinations_with_proposals': 0,
                    'total_proposals': 0,
                    'templates_processed': 0
                }
            }
        
        # Generate proposals using existing method
        result = RunMappingAssistant.generate_mapping_proposals(
            combination_ids=combination_ids,
            variable_mapping_ids=variable_mapping_ids,
            confidence_threshold=confidence_threshold
        )
        
        # Add framework-specific metadata
        templates = CUBE.objects.filter(framework_id__framework_id=framework_id)
        result['summary']['framework_id'] = framework_id
        result['summary']['templates_processed'] = templates.count()
        
        # If consistency is required, apply framework-wide conflict resolution
        if ensure_consistency:
            result = RunMappingAssistant._apply_framework_consistency(result, framework_id)
        
        return result

    @staticmethod
    def _apply_framework_consistency(proposals: Dict, framework_id: str) -> Dict:
        """
        Apply consistency rules across all proposals in a framework.
        
        This method resolves conflicts where the same source variable-member pair
        has different target mappings across different combinations in the framework.
        """
        from collections import defaultdict
        
        # Group proposals by source (variable_id, member_id) across all combinations
        source_to_targets = defaultdict(list)
        
        for combination_id, proposal_list in proposals['proposals'].items():
            for proposal in proposal_list:
                source_key = (proposal['source_variable_id'], proposal['source_member_id'])
                source_to_targets[source_key].append({
                    'combination_id': combination_id,
                    'proposal': proposal
                })
        
        # Resolve conflicts by selecting highest confidence proposal for each source
        resolved_mappings = {}
        conflicts_resolved = 0
        
        for source_key, target_list in source_to_targets.items():
            if len(target_list) > 1:
                # Multiple targets for same source - resolve by highest confidence
                best_proposal = max(target_list, key=lambda x: x['proposal']['confidence_score'])
                resolved_mappings[source_key] = best_proposal['proposal']
                conflicts_resolved += 1
            else:
                # Single target - keep as is
                resolved_mappings[source_key] = target_list[0]['proposal']
        
        # Rebuild proposals with consistent mappings
        consistent_proposals = {}
        for combination_id, proposal_list in proposals['proposals'].items():
            consistent_list = []
            for proposal in proposal_list:
                source_key = (proposal['source_variable_id'], proposal['source_member_id'])
                resolved = resolved_mappings[source_key]
                
                # Only include if this proposal matches the resolved mapping
                if (resolved['target_variable_id'] == proposal['target_variable_id'] and
                    resolved['target_member_id'] == proposal['target_member_id']):
                    consistent_list.append(proposal)
            
            consistent_proposals[combination_id] = consistent_list
        
        # Update result
        proposals['proposals'] = consistent_proposals
        proposals['summary']['conflicts_resolved'] = conflicts_resolved
        proposals['summary']['consistency_applied'] = True
        
        return proposals

    @staticmethod
    def get_framework_summary(framework_id: str) -> Dict:
        """Get summary information for a framework."""
        try:
            framework = FRAMEWORK.objects.get(framework_id=framework_id)
            templates = CUBE.objects.filter(framework_id=framework)
            
            total_combinations = 0
            template_info = []
            
            for template in templates:
                combo_count = CUBE_TO_COMBINATION.objects.filter(cube_id=template).count()
                total_combinations += combo_count
                
                template_info.append({
                    'cube_id': template.cube_id,
                    'name': template.name,
                    'combination_count': combo_count
                })
            
            return {
                'framework_id': framework_id,
                'framework_name': framework.name,
                'total_templates': templates.count(),
                'total_combinations': total_combinations,
                'templates': template_info
            }
            
        except FRAMEWORK.DoesNotExist:
            return {
                'error': f'Framework {framework_id} not found'
            }

def ready(self):
    # This method is still needed for Django's AppConfig
    pass