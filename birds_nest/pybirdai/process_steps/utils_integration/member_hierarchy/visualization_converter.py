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
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Any, Optional

logger = logging.getLogger(__name__)

BOX_WIDTH = 300
BOX_HEIGHT = 120


class VisualizationConverterProcessStep:
    """
    Process step for converting between visualization formats and member hierarchy data.
    Refactored from utils.member_hierarchy_editor.from_* files to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the visualization converter process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "to_visualization", 
                data_source: Any = None, hierarchy_id: str = None,
                **kwargs) -> Dict[str, Any]:
        """
        Execute visualization conversion operations.
        
        Args:
            operation (str): Operation type - "to_visualization", "from_visualization", "pandas_to_visualization", "json_to_nodes"
            data_source: Data source (DataFrame, dict, file path, etc.)
            hierarchy_id (str): Hierarchy ID for specific operations
            **kwargs: Additional parameters for specific operations
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            converter = VisualizationConverter()
            
            if operation == "to_visualization":
                if not isinstance(data_source, dict):
                    raise ValueError("data_source must be a dictionary for to_visualization operation")
                
                result = converter.hierarchy_to_visualization(data_source)
                
                return {
                    'success': True,
                    'operation': 'to_visualization',
                    'visualization_data': result,
                    'message': 'Hierarchy data converted to visualization format'
                }
            
            elif operation == "from_visualization":
                if not isinstance(data_source, dict):
                    raise ValueError("data_source must be a dictionary for from_visualization operation")
                
                result = converter.visualization_to_hierarchy_nodes(data_source)
                
                return {
                    'success': True,
                    'operation': 'from_visualization',
                    'hierarchy_nodes': result,
                    'message': 'Visualization data converted to hierarchy nodes'
                }
            
            elif operation == "pandas_to_visualization":
                if not hierarchy_id:
                    raise ValueError("hierarchy_id is required for pandas_to_visualization operation")
                
                members_df = kwargs.get('members_df')
                hierarchies_df = kwargs.get('hierarchies_df')
                hierarchy_nodes_df = kwargs.get('hierarchy_nodes_df')
                
                if not all([isinstance(df, pd.DataFrame) for df in [members_df, hierarchies_df, hierarchy_nodes_df]]):
                    raise ValueError("members_df, hierarchies_df, and hierarchy_nodes_df must be pandas DataFrames")
                
                integration = MemberHierarchyIntegration(members_df, hierarchies_df, hierarchy_nodes_df)
                result = integration.get_hierarchy_by_id(hierarchy_id)
                
                return {
                    'success': True,
                    'operation': 'pandas_to_visualization',
                    'hierarchy_id': hierarchy_id,
                    'visualization_data': result,
                    'message': f'Pandas data converted to visualization format for hierarchy {hierarchy_id}'
                }
            
            elif operation == "json_to_nodes":
                if isinstance(data_source, str):
                    # Assume it's a file path
                    result = converter.load_and_convert_json_file(data_source)
                elif isinstance(data_source, dict):
                    # Direct data
                    result = converter.visualization_to_hierarchy_nodes(data_source)
                else:
                    raise ValueError("data_source must be a file path (str) or dictionary for json_to_nodes operation")
                
                return {
                    'success': True,
                    'operation': 'json_to_nodes',
                    'hierarchy_nodes': result,
                    'message': 'JSON visualization data converted to hierarchy nodes'
                }
            
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.visualization_converter = converter
                
        except Exception as e:
            logger.error(f"Failed to execute visualization converter: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Visualization converter operation failed'
            }


@dataclass
class HierarchyNodeDTO:
    """Data Transfer Object for hierarchy nodes"""
    id_: str = "My Identifier"
    x: int = 0
    y: int = 0
    width: int = BOX_WIDTH
    height: int = BOX_HEIGHT
    name: str = "My Name"
    text: str = "My Description"

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

    @property
    def to_json(self) -> str:
        return json.dumps(self.to_dict)


@dataclass
class HierarchyArrowDTO:
    """Data Transfer Object for hierarchy arrows"""
    from_: str = ""  # child
    to_: str = ""    # parent

    @classmethod
    def from_dict(cls, dict_data):
        """Create HierarchyArrowDTO from dictionary data"""
        return cls(
            from_=str(dict_data.get("from", "")),
            to_=str(dict_data.get("to", ""))
        )

    @property
    def to_dict(self) -> dict:
        """Convert to dictionary format expected by visualization tool"""
        return {
            "from": self.from_,
            "to": self.to_
        }

    @property
    def to_json(self) -> str:
        return json.dumps(self.to_dict)


class VisualizationConverter:
    """
    Enhanced visualization converter with process step integration.
    Combines functionality from visualization conversion utilities.
    """
    
    def __init__(self):
        """Initialize the visualization converter."""
        logger.info("VisualizationConverter initialized")
    
    def hierarchy_to_visualization(self, hierarchy_data: Dict) -> Dict:
        """
        Convert hierarchy data to visualization format.
        
        Args:
            hierarchy_data (dict): Hierarchy data
            
        Returns:
            dict: Visualization format data
        """
        logger.info("Converting hierarchy data to visualization format")
        
        # This method can be extended based on specific hierarchy data format
        # For now, it assumes the data is already in a compatible format
        return hierarchy_data
    
    def visualization_to_hierarchy_nodes(self, hierarchy_data: dict) -> List[dict]:
        """
        Convert visualization data to MEMBER_HIERARCHY_NODE format.
        
        Args:
            hierarchy_data (dict): Hierarchy visualization data
            
        Returns:
            list: List of member hierarchy node dictionaries
        """
        logger.info("Converting visualization data to MEMBER_HIERARCHY_NODE format")
        
        boxes = hierarchy_data.get('boxes', [])
        arrows = hierarchy_data.get('arrows', [])
        
        # Get all unique node IDs
        all_nodes = set()
        for box in boxes:
            all_nodes.add(box['id'])
        
        # Build hierarchy relationships
        children_map, parent_map = self._build_hierarchy_graph(arrows)
        
        # Find root nodes and calculate levels
        root_nodes = self._find_root_nodes(all_nodes, parent_map)
        levels = self._calculate_levels(root_nodes, children_map)
        
        # Create MEMBER_HIERARCHY_NODE entries
        member_nodes = []
        
        for node in all_nodes:
            # Get node properties
            comparator, operator = self._determine_node_properties(node, children_map, parent_map)
            level = levels.get(node, 1)
            name = self._get_node_name(node, boxes)
            
            # Create the member hierarchy node
            member_node = {
                "member_code": node,
                "member_name": name,
                "parent_member_code": None,
                "level": level,
                "comparator": comparator,
                "operator": operator
            }
            
            # Set parent (if any) - use the first parent if multiple exist
            if node in parent_map and parent_map[node]:
                member_node["parent_member_code"] = list(parent_map[node])[0]
            
            member_nodes.append(member_node)
        
        # Sort by level and then by member_code for consistent output
        member_nodes.sort(key=lambda x: (x['level'], x['member_code']))
        
        logger.info(f"Converted {len(member_nodes)} nodes to MEMBER_HIERARCHY_NODE format")
        return member_nodes
    
    def load_and_convert_json_file(self, file_path: str) -> List[dict]:
        """
        Load hierarchy JSON file and convert to MEMBER_HIERARCHY_NODE format.
        
        Args:
            file_path (str): Path to the JSON file
            
        Returns:
            list: List of member hierarchy node dictionaries
        """
        logger.info(f"Loading and converting JSON file: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                hierarchy_data = json.load(f)
            
            return self.visualization_to_hierarchy_nodes(hierarchy_data)
            
        except Exception as e:
            logger.error(f"Failed to load and convert JSON file {file_path}: {e}")
            raise
    
    def save_hierarchy_nodes_to_file(self, member_nodes: List[dict], output_file: str):
        """
        Save the MEMBER_HIERARCHY_NODE data to a JSON file.
        
        Args:
            member_nodes (list): List of member hierarchy nodes
            output_file (str): Output file path
        """
        logger.info(f"Saving {len(member_nodes)} hierarchy nodes to {output_file}")
        
        output_data = {
            "MEMBER_HIERARCHY_NODE": member_nodes
        }
        
        try:
            with open(output_file, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            logger.info(f"Successfully saved hierarchy nodes to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to save hierarchy nodes to {output_file}: {e}")
            raise
    
    def _build_hierarchy_graph(self, arrows: List[dict]) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """Build parent-child relationships from arrows."""
        children_map = {}  # parent -> set of children
        parent_map = {}    # child -> set of parents
        
        for arrow in arrows:
            child = arrow['from']
            parent = arrow['to']
            
            if parent not in children_map:
                children_map[parent] = set()
            children_map[parent].add(child)
            
            if child not in parent_map:
                parent_map[child] = set()
            parent_map[child].add(parent)
        
        return children_map, parent_map
    
    def _find_root_nodes(self, all_nodes: Set[str], parent_map: Dict[str, Set[str]]) -> Set[str]:
        """Find nodes that have no parents (root nodes)."""
        return {node for node in all_nodes if node not in parent_map}
    
    def _calculate_levels(self, root_nodes: Set[str], children_map: Dict[str, Set[str]]) -> Dict[str, int]:
        """Calculate the level of each node starting from 1."""
        levels = {}
        
        def dfs(node, level):
            if node in levels:
                levels[node] = max(levels[node], level)
            else:
                levels[node] = level
            
            if node in children_map:
                for child in children_map[node]:
                    dfs(child, level + 1)
        
        for root in root_nodes:
            dfs(root, 1)
        
        return levels
    
    def _determine_node_properties(self, node: str, children_map: Dict[str, Set[str]], 
                                 parent_map: Dict[str, Set[str]]) -> Tuple[str, str]:
        """Determine comparator and operator based on node type."""
        has_children = node in children_map and len(children_map[node]) > 0
        has_parents = node in parent_map and len(parent_map[node]) > 0
        
        if has_children and not has_parents:
            # Rule 1: parent and not a child
            return "=", ""
        elif has_children and has_parents:
            # Rule 2: parent and child
            return "=", "+"
        elif not has_children:
            # Rule 3: leaf node
            return "", "+"
        else:
            # Default case
            return "", ""
    
    def _get_node_name(self, node_id: str, boxes: List[dict]) -> str:
        """Get the name of a node from the boxes list."""
        for box in boxes:
            if box['id'] == node_id:
                return box.get('name', node_id)
        return node_id
    
    def get_conversion_summary(self, member_nodes: List[dict]) -> Dict[str, Any]:
        """
        Generate a summary of the conversion.
        
        Args:
            member_nodes (list): List of member hierarchy nodes
            
        Returns:
            dict: Summary statistics
        """
        total_nodes = len(member_nodes)
        
        # Level distribution
        level_counts = {}
        for node in member_nodes:
            level = node['level']
            level_counts[level] = level_counts.get(level, 0) + 1
        
        # Node type distribution
        type_counts = {"Root (=, '')": 0, "Intermediate (=, +)": 0, "Leaf ('', +)": 0}
        for node in member_nodes:
            comp = node['comparator']
            op = node['operator']
            if comp == "=" and op == "":
                type_counts["Root (=, '')"] += 1
            elif comp == "=" and op == "+":
                type_counts["Intermediate (=, +)"] += 1
            elif comp == "" and op == "+":
                type_counts["Leaf ('', +)"] += 1
        
        summary = {
            'total_nodes': total_nodes,
            'level_distribution': level_counts,
            'type_distribution': type_counts,
            'max_level': max(level_counts.keys()) if level_counts else 0
        }
        
        logger.info(f"Conversion summary: {total_nodes} nodes, {len(level_counts)} levels")
        return summary


class MemberHierarchyIntegration:
    """
    Integration class to convert member hierarchy CSV data into visualization JSON format.
    Enhanced version with process step support.
    """

    def __init__(self, members_df: pd.DataFrame, hierarchies_df: pd.DataFrame,
                 hierarchy_nodes_df: pd.DataFrame):
        """
        Initialize with pandas DataFrames.
        
        Args:
            members_df: DataFrame with member data
            hierarchies_df: DataFrame with hierarchy data
            hierarchy_nodes_df: DataFrame with hierarchy node data
        """
        self.members_df = members_df
        self.hierarchies_df = hierarchies_df
        self.hierarchy_nodes_df = hierarchy_nodes_df
        logger.info("MemberHierarchyIntegration initialized")

    def get_hierarchy_by_id(self, hierarchy_id: str) -> Dict:
        """
        Convert a specific member hierarchy to visualization format.
        
        Args:
            hierarchy_id (str): The hierarchy ID to convert
            
        Returns:
            dict: Visualization format data
        """
        logger.info(f"Converting hierarchy {hierarchy_id} to visualization format")
        
        # Filter nodes for this hierarchy
        hierarchy_nodes = self.hierarchy_nodes_df[
            self.hierarchy_nodes_df['MEMBER_HIERARCHY_ID'] == hierarchy_id
        ].copy()

        if hierarchy_nodes.empty:
            logger.warning(f"No nodes found for hierarchy {hierarchy_id}")
            return {"boxes": [], "arrows": [], "nextId": 1}

        # Get hierarchy info
        hierarchy_info = self.hierarchies_df[
            self.hierarchies_df['MEMBER_HIERARCHY_ID'] == hierarchy_id
        ].iloc[0] if not self.hierarchies_df[
            self.hierarchies_df['MEMBER_HIERARCHY_ID'] == hierarchy_id
        ].empty else None

        # Create nodes
        nodes = []
        arrows = []

        # Layout configuration
        level_spacing_y = 200 + BOX_HEIGHT * 2
        node_spacing_x = 250 + BOX_WIDTH
        start_x = 100
        start_y = 200

        # Group nodes by level for positioning
        levels = hierarchy_nodes.groupby('LEVEL')

        for level, level_nodes in levels:
            level_nodes = level_nodes.sort_values('MEMBER_ID')
            
            for i, (_, node_row) in enumerate(level_nodes.iterrows()):
                # Get member information
                member_info = self.members_df[
                    self.members_df['MEMBER_ID'] == node_row['MEMBER_ID']
                ]

                if not member_info.empty:
                    member = member_info.iloc[0]
                    name = member['NAME']
                    description = member['DESCRIPTION'] if isinstance(member['DESCRIPTION'], str) else name
                else:
                    name = node_row['MEMBER_ID']
                    description = "No description available"

                # Calculate position
                x = start_x + (i * node_spacing_x)
                y = start_y + ((level - 1) * level_spacing_y)

                node = HierarchyNodeDTO(
                    id_=node_row['MEMBER_ID'],
                    x=x,
                    y=y,
                    width=BOX_WIDTH,
                    height=BOX_HEIGHT,
                    name=name,
                    text=description
                )
                nodes.append(node)

                # Create arrow if there's a parent
                if pd.notna(node_row['PARENT_MEMBER_ID']):
                    arrow = HierarchyArrowDTO(
                        from_=node_row['MEMBER_ID'],
                        to_=node_row['PARENT_MEMBER_ID']
                    )
                    arrows.append(arrow)

        # Convert to dict format
        boxes = [node.to_dict for node in nodes]
        arrow_dicts = [arrow.to_dict for arrow in arrows]

        # Calculate next available ID
        next_id = len(boxes) + 1

        result = {
            "boxes": boxes,
            "arrows": arrow_dicts,
            "nextId": next_id,
            "hierarchy_info": {
                "id": hierarchy_id,
                "name": hierarchy_info['NAME'] if hierarchy_info is not None else hierarchy_id,
                "description": hierarchy_info['DESCRIPTION'] if hierarchy_info is not None else "",
                "domain": hierarchy_info['DOMAIN_ID'] if hierarchy_info is not None else "",
                "allowed_members": self.members_df[self.members_df['DOMAIN_ID'] == hierarchy_info['DOMAIN_ID']].set_index("MEMBER_ID")["NAME"].to_dict() if hierarchy_info is not None else ""
            }
        }
        
        logger.info(f"Converted hierarchy {hierarchy_id}: {len(boxes)} boxes, {len(arrow_dicts)} arrows")
        return result

    def get_all_hierarchies(self) -> Dict[str, Dict]:
        """Get all hierarchies as a dictionary."""
        logger.info("Getting all hierarchies")
        
        all_hierarchies = {}
        hierarchy_ids = self.hierarchy_nodes_df['MEMBER_HIERARCHY_ID'].unique()

        for hierarchy_id in hierarchy_ids:
            all_hierarchies[hierarchy_id] = self.get_hierarchy_by_id(hierarchy_id)

        logger.info(f"Retrieved {len(all_hierarchies)} hierarchies")
        return all_hierarchies

    def get_hierarchies_by_domain(self, domain_id: str) -> Dict[str, Dict]:
        """
        Get all hierarchies for a specific domain.
        
        Args:
            domain_id (str): The domain ID
            
        Returns:
            dict: Hierarchies for the domain
        """
        logger.info(f"Getting hierarchies for domain {domain_id}")
        
        domain_hierarchies = self.hierarchies_df[
            self.hierarchies_df['DOMAIN_ID'] == domain_id
        ]['MEMBER_HIERARCHY_ID'].tolist()

        result = {}
        for hierarchy_id in domain_hierarchies:
            result[hierarchy_id] = self.get_hierarchy_by_id(hierarchy_id)

        logger.info(f"Retrieved {len(result)} hierarchies for domain {domain_id}")
        return result

    def save_hierarchy_json(self, hierarchy_id: str, output_path: str):
        """
        Save a specific hierarchy to JSON file.
        
        Args:
            hierarchy_id (str): The hierarchy ID
            output_path (str): Output file path
        """
        logger.info(f"Saving hierarchy {hierarchy_id} to {output_path}")
        
        hierarchy_data = self.get_hierarchy_by_id(hierarchy_id)
        with open(output_path, 'w') as f:
            json.dump(hierarchy_data, f, indent=2)
        
        logger.info(f"Successfully saved hierarchy {hierarchy_id} to {output_path}")

    def get_available_hierarchies(self) -> pd.DataFrame:
        """Get a summary of all available hierarchies."""
        return self.hierarchies_df[['MEMBER_HIERARCHY_ID', 'NAME', 'DOMAIN_ID', 'DESCRIPTION', 'IS_MAIN_HIERARCHY']]