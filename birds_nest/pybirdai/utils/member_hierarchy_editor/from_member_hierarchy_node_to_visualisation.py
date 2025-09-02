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
import pandas as pd
import json
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import math

BOX_WIDTH = 300
BOX_HEIGHT = 120

@dataclass
class HierarchyNodeDTO:
    id_: str = "My Identifier"
    x: int = 0
    y: int = 0
    width: int = BOX_WIDTH
    height: int = BOX_HEIGHT
    name: str = "My Name"
    text: str = "My Description"

    @classmethod
    def from_dict(cls, dict_data):
        """
        Create HierarchyNodeDTO from dictionary data
        Expected format:
        {'id': 1,
         'x': 800,
         'y': 93,
         'width': 180,
         'height': 120,
         'name': 'Concept 1',
         'text': 'Enter description here...'}
        """
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
        """
        Convert to dictionary format expected by visualization tool
        """
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
    from_: str = ""  # child
    to_: str = ""    # parent

    @classmethod
    def from_dict(cls, dict_data):
        """
        Create HierarchyArrowDTO from dictionary data
        """
        return cls(
            from_=str(dict_data.get("from", "")),
            to_=str(dict_data.get("to", ""))
        )

    @property
    def to_dict(self) -> dict:
        """
        Convert to dictionary format expected by visualization tool
        """
        return {
            "from": self.from_,
            "to": self.to_
        }

    @property
    def to_json(self) -> str:
        return json.dumps(self.to_dict)


class MemberHierarchyIntegration:
    """
    Integration class to convert member hierarchy CSV data into visualization JSON format
    """

    def __init__(self, members_df: pd.DataFrame, hierarchies_df: pd.DataFrame,
                 hierarchy_nodes_df: pd.DataFrame):
        self.members_df = members_df
        self.hierarchies_df = hierarchies_df
        self.hierarchy_nodes_df = hierarchy_nodes_df

    def get_hierarchy_by_id(self, hierarchy_id: str) -> Dict:
        """
        Convert a specific member hierarchy to visualization format
        """
        # Filter nodes for this hierarchy
        hierarchy_nodes = self.hierarchy_nodes_df[
            self.hierarchy_nodes_df['MEMBER_HIERARCHY_ID'] == hierarchy_id
        ].copy()

        if hierarchy_nodes.empty:
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

        # Calculate positions based on hierarchy level
        level_spacing_y = 200 + BOX_HEIGHT * 2
        node_spacing_x = 250 + BOX_WIDTH
        start_x = 100
        start_y = 200

        # Group nodes by level for positioning
        levels = hierarchy_nodes.groupby('LEVEL')

        for level, level_nodes in levels:
            level_nodes = level_nodes.sort_values('MEMBER_ID')
            nodes_in_level = len(level_nodes)

            for i, (_, node_row) in enumerate(level_nodes.iterrows()):
                # Get member information
                member_info = self.members_df[
                    self.members_df['MEMBER_ID'] == node_row['MEMBER_ID']
                ]

                if not member_info.empty:
                    member = member_info.iloc[0]
                    name = member['NAME']
                    description = member['DESCRIPTION'] if isinstance(member['DESCRIPTION'], str) else name
                    code = member['CODE']
                else:
                    name = node_row['MEMBER_ID']
                    description = "No description available"
                    code = ""

                # Calculate position
                x = start_x + (i * node_spacing_x)
                y = start_y + ((level - 1) * level_spacing_y)

                text = description

                node = HierarchyNodeDTO(
                    id_=node_row['MEMBER_ID'],
                    x=x,
                    y=y,
                    width=BOX_WIDTH,
                    height=BOX_HEIGHT,
                    name=name,
                    text=text
                )
                nodes.append(node)

                # Create arrow if there's a parent
                if pd.notna(node_row['PARENT_MEMBER_ID']):
                    arrow = HierarchyArrowDTO(
                        from_=node_row['MEMBER_ID'],  # parent points to child
                        to_=node_row['PARENT_MEMBER_ID']
                    )
                    arrows.append(arrow)

        # Convert to dict format
        boxes = [node.to_dict for node in nodes]
        arrow_dicts = [arrow.to_dict for arrow in arrows]

        # Calculate next available ID
        next_id = len(boxes) + 1

        return {
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

    def get_all_hierarchies(self) -> Dict[str, Dict]:
        """
        Get all hierarchies as a dictionary
        """
        all_hierarchies = {}
        hierarchy_ids = self.hierarchy_nodes_df['MEMBER_HIERARCHY_ID'].unique()

        for hierarchy_id in hierarchy_ids:
            all_hierarchies[hierarchy_id] = self.get_hierarchy_by_id(hierarchy_id)

        return all_hierarchies

    def get_hierarchies_by_domain(self, domain_id: str) -> Dict[str, Dict]:
        """
        Get all hierarchies for a specific domain
        """
        domain_hierarchies = self.hierarchies_df[
            self.hierarchies_df['DOMAIN_ID'] == domain_id
        ]['MEMBER_HIERARCHY_ID'].tolist()

        result = {}
        for hierarchy_id in domain_hierarchies:
            result[hierarchy_id] = self.get_hierarchy_by_id(hierarchy_id)

        return result

    def save_hierarchy_json(self, hierarchy_id: str, output_path: str):
        """
        Save a specific hierarchy to JSON file
        """
        hierarchy_data = self.get_hierarchy_by_id(hierarchy_id)
        with open(output_path, 'w') as f:
            json.dump(hierarchy_data, f, indent=2)

    def get_available_hierarchies(self) -> pd.DataFrame:
        """
        Get a summary of all available hierarchies
        """
        return self.hierarchies_df[['MEMBER_HIERARCHY_ID', 'NAME', 'DOMAIN_ID', 'DESCRIPTION', 'IS_MAIN_HIERARCHY']]


# Convenience functions for easy usage
def load_member_hierarchy_data(entities_path: str = "entities/") -> MemberHierarchyIntegration:
    """
    Load member hierarchy data from CSV files and return integration object
    """
    members = pd.read_csv(f"{entities_path}member.csv")
    member_hierarchies = pd.read_csv(f"{entities_path}member_hierarchy.csv")
    member_hierarchy_nodes = pd.read_csv(f"{entities_path}member_hierarchy_node.csv")

    return MemberHierarchyIntegration(members, member_hierarchies, member_hierarchy_nodes)


def generate_hierarchy_json(hierarchy_id: str, output_path: str = "hierarchy.json",
                          entities_path: str = "entities/"):
    """
    Generate hierarchy JSON for a specific hierarchy ID
    """
    integration = load_member_hierarchy_data(entities_path)
    integration.save_hierarchy_json(hierarchy_id, output_path)
    print(f"Generated hierarchy JSON for {hierarchy_id} at {output_path}")


def list_available_hierarchies(entities_path: str = "entities/") -> pd.DataFrame:
    """
    List all available hierarchies
    """
    integration = load_member_hierarchy_data(entities_path)
    return integration.get_available_hierarchies()


# Example usage
if __name__ == "__main__":
    # Load the data
    integration = load_member_hierarchy_data()

    hierarchies = integration.get_available_hierarchies()

    first_hierarchy_id = "TYP_INSTRMNT_HIER_2"
    print(f"\nGenerating JSON for hierarchy: {first_hierarchy_id}")

    hierarchy_json = integration.get_hierarchy_by_id(first_hierarchy_id)

    # Save to file
    with open("generated_hierarchy.json", "w") as f:
        json.dump(hierarchy_json, f, indent=2)

    print(f"Saved hierarchy JSON to generated_hierarchy.json")
    print(f"Generated {len(hierarchy_json['boxes'])} nodes and {len(hierarchy_json['arrows'])} arrows")
