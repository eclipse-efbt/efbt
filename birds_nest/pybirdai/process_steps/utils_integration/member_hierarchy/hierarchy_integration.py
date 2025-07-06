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

import json
import logging
from typing import Dict, List, Any, Optional
from pybirdai.bird_meta_data_model import MEMBER, MEMBER_HIERARCHY, DOMAIN

logger = logging.getLogger(__name__)

BOX_WIDTH = 300
BOX_HEIGHT = 120


class HierarchyIntegrationProcessStep:
    """
    Process step for member hierarchy integration and visualization.
    Refactored from utils.member_hierarchy_editor.django_hierarchy_integration to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the hierarchy integration process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "convert_to_visualization", 
                hierarchy_id: str = None, visualization_data: str = None,
                **kwargs) -> Dict[str, Any]:
        """
        Execute hierarchy integration operations.
        
        Args:
            operation (str): Operation type - "convert_to_visualization", "convert_from_visualization", "get_hierarchy"
            hierarchy_id (str): Hierarchy ID for specific operations
            visualization_data (str): JSON visualization data for conversion
            **kwargs: Additional parameters for specific operations
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            integration = HierarchyIntegration()
            
            if operation == "convert_to_visualization":
                if not hierarchy_id:
                    raise ValueError("hierarchy_id is required for convert_to_visualization operation")
                
                result = integration.get_hierarchy_integration(hierarchy_id)
                
                return {
                    'success': True,
                    'operation': 'convert_to_visualization',
                    'hierarchy_id': hierarchy_id,
                    'visualization_data': result,
                    'message': f'Hierarchy {hierarchy_id} converted to visualization format'
                }
            
            elif operation == "convert_from_visualization":
                if not visualization_data:
                    raise ValueError("visualization_data is required for convert_from_visualization operation")
                
                result = integration.convert_from_visualization(visualization_data)
                
                return {
                    'success': True,
                    'operation': 'convert_from_visualization',
                    'conversion_result': result,
                    'message': 'Visualization data converted to hierarchy format'
                }
            
            elif operation == "get_hierarchy":
                if not hierarchy_id:
                    raise ValueError("hierarchy_id is required for get_hierarchy operation")
                
                result = integration.get_hierarchy_data(hierarchy_id)
                
                return {
                    'success': True,
                    'operation': 'get_hierarchy',
                    'hierarchy_id': hierarchy_id,
                    'hierarchy_data': result,
                    'message': f'Hierarchy data retrieved for {hierarchy_id}'
                }
            
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.hierarchy_integration = integration
                
        except Exception as e:
            logger.error(f"Failed to execute hierarchy integration: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Hierarchy integration operation failed'
            }


class HierarchyNodeDTO:
    """Data Transfer Object for hierarchy nodes"""
    
    def __init__(self, id_: str = "My Identifier", x: int = 0, y: int = 0, 
                 width: int = BOX_WIDTH, height: int = BOX_HEIGHT,
                 name: str = "My Name", text: str = "My Description"):
        self.id_ = id_
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.name = name
        self.text = text

    @classmethod
    def from_dict(cls, dict_data):
        """Create HierarchyNodeDTO from dictionary data"""
        return cls(
            id_=str(dict_data.get('id', '')),
            x=dict_data.get('x', 0),
            y=dict_data.get('y', 0),
            width=dict_data.get('width', BOX_WIDTH),
            height=dict_data.get('height', BOX_HEIGHT),
            name=dict_data.get('name', ''),
            text=dict_data.get('text', '')
        )

    @property
    def to_dict(self) -> dict:
        """Convert to dictionary format expected by visualization tool"""
        return {
            'id': self.id_,
            'x': self.x,
            'y': self.y,
            'width': self.width,
            'height': self.height,
            'name': self.name,
            'text': self.text
        }


class HierarchyIntegration:
    """
    Enhanced hierarchy integration with process step support.
    Refactored from utils.member_hierarchy_editor.django_hierarchy_integration.
    """
    
    def __init__(self):
        """Initialize the hierarchy integration."""
        self.model_converter = None
        logger.info("HierarchyIntegration initialized")
    
    def get_hierarchy_integration(self, hierarchy_id: str) -> str:
        """
        Convert Django hierarchy models to visualization format.
        
        Args:
            hierarchy_id (str): The hierarchy ID to convert
            
        Returns:
            str: JSON string for visualization
        """
        logger.info(f"Converting hierarchy {hierarchy_id} to visualization format")
        
        try:
            # Get the hierarchy
            hierarchy = MEMBER_HIERARCHY.objects.get(member_hierarchy_id=hierarchy_id)
            
            # Get all members in this hierarchy
            from pybirdai.bird_meta_data_model import MEMBER_HIERARCHY_NODE
            nodes = MEMBER_HIERARCHY_NODE.objects.filter(
                member_hierarchy_id=hierarchy
            ).order_by('level', 'order') if hasattr(MEMBER_HIERARCHY_NODE.objects.first(), 'order') else MEMBER_HIERARCHY_NODE.objects.filter(
                member_hierarchy_id=hierarchy
            ).order_by('level')
            
            # Convert to visualization format
            visualization_nodes = []
            node_positions = self._calculate_node_positions(nodes)
            
            for i, node in enumerate(nodes):
                member = node.member_id
                position = node_positions.get(i, {'x': 50, 'y': 50})
                
                node_dto = HierarchyNodeDTO(
                    id_=member.member_id if member else f"node_{i}",
                    name=member.name if member else "Unknown",
                    text=member.description if member and member.description else "",
                    x=position['x'],
                    y=position['y']
                )
                visualization_nodes.append(node_dto.to_dict)
            
            # Create the complete visualization structure
            visualization_data = {
                'hierarchy_id': hierarchy_id,
                'hierarchy_name': hierarchy.name,
                'nodes': visualization_nodes,
                'metadata': {
                    'total_nodes': len(visualization_nodes),
                    'domain_id': hierarchy.domain_id.domain_id if hierarchy.domain_id else None,
                    'description': hierarchy.description
                }
            }
            
            result_json = json.dumps(visualization_data, indent=2)
            logger.info(f"Successfully converted hierarchy {hierarchy_id} with {len(visualization_nodes)} nodes")
            
            return result_json
            
        except MEMBER_HIERARCHY.DoesNotExist:
            logger.error(f"Hierarchy {hierarchy_id} not found")
            raise ValueError(f"Hierarchy {hierarchy_id} not found")
        except Exception as e:
            logger.error(f"Failed to convert hierarchy {hierarchy_id}: {e}")
            raise
    
    def _calculate_node_positions(self, nodes) -> Dict[int, Dict[str, int]]:
        """
        Calculate positions for hierarchy nodes in visualization.
        
        Args:
            nodes: QuerySet of hierarchy nodes
            
        Returns:
            dict: Node index to position mapping
        """
        positions = {}
        level_counts = {}
        level_current = {}
        
        # Count nodes per level
        for node in nodes:
            level = getattr(node, 'level', 0) or 0
            level_counts[level] = level_counts.get(level, 0) + 1
            level_current[level] = 0
        
        # Calculate positions
        for i, node in enumerate(nodes):
            level = getattr(node, 'level', 0) or 0
            
            # Calculate Y position based on level
            y = 50 + (level * (BOX_HEIGHT + 50))
            
            # Calculate X position based on position within level
            nodes_in_level = level_counts.get(level, 1)
            current_in_level = level_current[level]
            
            if nodes_in_level == 1:
                x = 400  # Center single nodes
            else:
                # Distribute nodes across the width
                spacing = 800 / max(nodes_in_level - 1, 1)
                x = 50 + (current_in_level * spacing)
            
            positions[i] = {'x': int(x), 'y': int(y)}
            level_current[level] += 1
        
        return positions
    
    def convert_from_visualization(self, visualization_json: str) -> Dict[str, Any]:
        """
        Convert visualization format back to Django hierarchy models.
        
        Args:
            visualization_json (str): JSON visualization data
            
        Returns:
            dict: Conversion results
        """
        logger.info("Converting visualization data to hierarchy format")
        
        try:
            visualization_data = json.loads(visualization_json)
            
            results = {
                'hierarchy_id': visualization_data.get('hierarchy_id'),
                'nodes_processed': 0,
                'nodes_created': 0,
                'nodes_updated': 0,
                'errors': []
            }
            
            hierarchy_id = visualization_data.get('hierarchy_id')
            if not hierarchy_id:
                raise ValueError("hierarchy_id is required in visualization data")
            
            # Get or create hierarchy
            hierarchy, created = MEMBER_HIERARCHY.objects.get_or_create(
                member_hierarchy_id=hierarchy_id,
                defaults={
                    'name': visualization_data.get('hierarchy_name', 'Converted Hierarchy'),
                    'description': visualization_data.get('metadata', {}).get('description', '')
                }
            )
            
            if created:
                logger.info(f"Created new hierarchy: {hierarchy_id}")
            
            # Process nodes
            nodes_data = visualization_data.get('nodes', [])
            for node_data in nodes_data:
                try:
                    result = self._process_visualization_node(hierarchy, node_data)
                    results['nodes_processed'] += 1
                    
                    if result.get('created'):
                        results['nodes_created'] += 1
                    else:
                        results['nodes_updated'] += 1
                        
                except Exception as e:
                    error_msg = f"Failed to process node {node_data.get('id', 'unknown')}: {e}"
                    results['errors'].append(error_msg)
                    logger.warning(error_msg)
            
            logger.info(f"Conversion completed: {results['nodes_processed']} nodes processed")
            return results
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in visualization data: {e}")
            raise ValueError(f"Invalid JSON: {e}")
        except Exception as e:
            logger.error(f"Failed to convert visualization data: {e}")
            raise
    
    def _process_visualization_node(self, hierarchy, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single visualization node and create/update corresponding models."""
        member_id = node_data.get('id')
        if not member_id:
            raise ValueError("Node must have an 'id' field")
        
        # Get or create member
        member, member_created = MEMBER.objects.get_or_create(
            member_id=member_id,
            defaults={
                'name': node_data.get('name', ''),
                'description': node_data.get('text', ''),
                'code': node_data.get('name', '')
            }
        )
        
        # Calculate level from Y position
        y_position = node_data.get('y', 0)
        level = max(0, (y_position - 50) // (BOX_HEIGHT + 50))
        
        # Create or update hierarchy node
        from pybirdai.bird_meta_data_model import MEMBER_HIERARCHY_NODE
        node, node_created = MEMBER_HIERARCHY_NODE.objects.get_or_create(
            member_hierarchy_id=hierarchy,
            member_id=member,
            defaults={
                'level': level
            }
        )
        
        if not node_created:
            # Update existing node
            node.level = level
            node.save()
        
        return {
            'member_created': member_created,
            'node_created': node_created,
            'created': member_created or node_created
        }
    
    def get_hierarchy_data(self, hierarchy_id: str) -> Dict[str, Any]:
        """
        Get complete hierarchy data including nodes and relationships.
        
        Args:
            hierarchy_id (str): The hierarchy ID
            
        Returns:
            dict: Complete hierarchy data
        """
        logger.info(f"Retrieving hierarchy data for {hierarchy_id}")
        
        try:
            hierarchy = MEMBER_HIERARCHY.objects.get(member_hierarchy_id=hierarchy_id)
            
            # Get all nodes
            from pybirdai.bird_meta_data_model import MEMBER_HIERARCHY_NODE
            nodes = MEMBER_HIERARCHY_NODE.objects.filter(
                member_hierarchy_id=hierarchy
            ).select_related('member_id', 'parent_member_id')
            
            # Build hierarchy data
            hierarchy_data = {
                'hierarchy_id': hierarchy_id,
                'name': hierarchy.name,
                'description': hierarchy.description,
                'domain_id': hierarchy.domain_id.domain_id if hierarchy.domain_id else None,
                'is_main_hierarchy': getattr(hierarchy, 'is_main_hierarchy', False),
                'total_nodes': nodes.count(),
                'nodes': [],
                'levels': {}
            }
            
            # Process nodes
            level_counts = {}
            for node in nodes:
                member = node.member_id
                level = getattr(node, 'level', 0) or 0
                
                level_counts[level] = level_counts.get(level, 0) + 1
                
                node_data = {
                    'member_id': member.member_id if member else None,
                    'member_name': member.name if member else None,
                    'member_code': member.code if member else None,
                    'member_description': member.description if member else None,
                    'level': level,
                    'parent_member_id': node.parent_member_id.member_id if node.parent_member_id else None,
                    'comparator': getattr(node, 'comparator', None),
                    'operator': getattr(node, 'operator', None)
                }
                
                hierarchy_data['nodes'].append(node_data)
            
            # Add level statistics
            hierarchy_data['levels'] = {
                str(level): count for level, count in level_counts.items()
            }
            hierarchy_data['max_level'] = max(level_counts.keys()) if level_counts else 0
            
            logger.info(f"Retrieved hierarchy data with {len(hierarchy_data['nodes'])} nodes across {len(level_counts)} levels")
            return hierarchy_data
            
        except MEMBER_HIERARCHY.DoesNotExist:
            logger.error(f"Hierarchy {hierarchy_id} not found")
            raise ValueError(f"Hierarchy {hierarchy_id} not found")
        except Exception as e:
            logger.error(f"Failed to retrieve hierarchy data: {e}")
            raise
    
    def validate_hierarchy(self, hierarchy_id: str) -> Dict[str, Any]:
        """
        Validate hierarchy structure and relationships.
        
        Args:
            hierarchy_id (str): The hierarchy ID to validate
            
        Returns:
            dict: Validation results
        """
        logger.info(f"Validating hierarchy {hierarchy_id}")
        
        validation_result = {
            'hierarchy_id': hierarchy_id,
            'valid': True,
            'issues': [],
            'statistics': {}
        }
        
        try:
            hierarchy_data = self.get_hierarchy_data(hierarchy_id)
            nodes = hierarchy_data['nodes']
            
            # Basic statistics
            validation_result['statistics'] = {
                'total_nodes': len(nodes),
                'levels': hierarchy_data['levels'],
                'max_level': hierarchy_data['max_level']
            }
            
            # Validate node relationships
            member_ids = {node['member_id'] for node in nodes if node['member_id']}
            
            for node in nodes:
                # Check for orphaned parent references
                parent_id = node.get('parent_member_id')
                if parent_id and parent_id not in member_ids:
                    validation_result['valid'] = False
                    validation_result['issues'].append(
                        f"Node {node['member_id']} references non-existent parent {parent_id}"
                    )
                
                # Check for self-references
                if parent_id == node['member_id']:
                    validation_result['valid'] = False
                    validation_result['issues'].append(
                        f"Node {node['member_id']} references itself as parent"
                    )
            
            # Check for circular references (simplified check)
            circular_refs = self._detect_circular_references(nodes)
            if circular_refs:
                validation_result['valid'] = False
                validation_result['issues'].extend([
                    f"Circular reference detected: {' -> '.join(ref)}" 
                    for ref in circular_refs
                ])
            
            status = 'Valid' if validation_result['valid'] else 'Invalid'
            logger.info(f"Hierarchy validation completed: {status} ({len(validation_result['issues'])} issues)")
            
        except Exception as e:
            validation_result['valid'] = False
            validation_result['issues'].append(f"Validation failed: {e}")
            logger.error(f"Hierarchy validation failed: {e}")
        
        return validation_result
    
    def _detect_circular_references(self, nodes: List[Dict[str, Any]]) -> List[List[str]]:
        """Detect circular references in hierarchy nodes."""
        # Build parent-child mapping
        parent_map = {}
        for node in nodes:
            member_id = node['member_id']
            parent_id = node.get('parent_member_id')
            if member_id and parent_id:
                parent_map[member_id] = parent_id
        
        circular_refs = []
        visited = set()
        
        def find_cycle(node_id, path):
            if node_id in path:
                # Found a cycle
                cycle_start = path.index(node_id)
                circular_refs.append(path[cycle_start:] + [node_id])
                return
            
            if node_id in visited or node_id not in parent_map:
                return
            
            visited.add(node_id)
            find_cycle(parent_map[node_id], path + [node_id])
        
        for node_id in parent_map:
            if node_id not in visited:
                find_cycle(node_id, [])
        
        return circular_refs