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
import json
import csv
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import math
from collections import defaultdict

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

    def __init__(self, members_data: List[Dict], hierarchies_data: List[Dict],
                 hierarchy_nodes_data: List[Dict]):
        self.members_data = members_data
        self.hierarchies_data = hierarchies_data
        self.hierarchy_nodes_data = hierarchy_nodes_data

    def get_hierarchy_by_id(self, hierarchy_id: str) -> Dict:
        """
        Convert a specific member hierarchy to visualization format
        """
        # Filter nodes for this hierarchy
        hierarchy_nodes = [
            node for node in self.hierarchy_nodes_data
            if node['MEMBER_HIERARCHY_ID'] == hierarchy_id
        ]

        if not hierarchy_nodes:
            return {"boxes": [], "arrows": [], "nextId": 1}

        # Get hierarchy info
        hierarchy_info = None
        for hierarchy in self.hierarchies_data:
            if hierarchy['MEMBER_HIERARCHY_ID'] == hierarchy_id:
                hierarchy_info = hierarchy
                break

        # Create nodes
        nodes = []
        arrows = []

        # Calculate positions based on hierarchy level
        level_spacing_y = 200 + BOX_HEIGHT * 2
        node_spacing_x = 250 + BOX_WIDTH
        start_x = 100
        start_y = 200

        # Group nodes by level for positioning
        levels = defaultdict(list)
        for node in hierarchy_nodes:
            level = int(node['LEVEL']) if node['LEVEL'] else 1
            levels[level].append(node)

        for level, level_nodes in levels.items():
            # Sort nodes by MEMBER_ID
            level_nodes.sort(key=lambda x: x['MEMBER_ID'])
            nodes_in_level = len(level_nodes)

            for i, node_row in enumerate(level_nodes):
                # Get member information
                member_info = None
                for member in self.members_data:
                    if member['MEMBER_ID'] == node_row['MEMBER_ID']:
                        member_info = member
                        break

                if member_info:
                    name = member_info['NAME']
                    description = member_info['DESCRIPTION'] if isinstance(member_info['DESCRIPTION'], str) else name
                    code = member_info['CODE']
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
                if node_row['PARENT_MEMBER_ID'] is not None and node_row['PARENT_MEMBER_ID'] != '':
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

        # Create allowed_members dictionary
        allowed_members = {}
        if hierarchy_info is not None:
            domain_id = hierarchy_info['DOMAIN_ID']
            for member in self.members_data:
                if member['DOMAIN_ID'] == domain_id:
                    allowed_members[member['MEMBER_ID']] = member['NAME']

        return {
            "boxes": boxes,
            "arrows": arrow_dicts,
            "nextId": next_id,
            "hierarchy_info": {
                "id": hierarchy_id,
                "name": hierarchy_info['NAME'] if hierarchy_info is not None else hierarchy_id,
                "description": hierarchy_info['DESCRIPTION'] if hierarchy_info is not None else "",
                "domain": hierarchy_info['DOMAIN_ID'] if hierarchy_info is not None else "",
                "allowed_members": allowed_members
            }
        }

    def get_all_hierarchies(self) -> Dict[str, Dict]:
        """
        Get all hierarchies as a dictionary
        """
        all_hierarchies = {}
        hierarchy_ids = list(set(node['MEMBER_HIERARCHY_ID'] for node in self.hierarchy_nodes_data))

        for hierarchy_id in hierarchy_ids:
            all_hierarchies[hierarchy_id] = self.get_hierarchy_by_id(hierarchy_id)

        return all_hierarchies

    def get_hierarchies_by_domain(self, domain_id: str) -> Dict[str, Dict]:
        """
        Get all hierarchies for a specific domain
        """
        domain_hierarchies = [
            hierarchy['MEMBER_HIERARCHY_ID']
            for hierarchy in self.hierarchies_data
            if hierarchy['DOMAIN_ID'] == domain_id
        ]

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

    def get_available_hierarchies(self) -> List[Dict]:
        """
        Get a summary of all available hierarchies
        """
        return [
            {
                'MEMBER_HIERARCHY_ID': hierarchy['MEMBER_HIERARCHY_ID'],
                'NAME': hierarchy['NAME'],
                'DOMAIN_ID': hierarchy['DOMAIN_ID'],
                'DESCRIPTION': hierarchy['DESCRIPTION'],
                'IS_MAIN_HIERARCHY': hierarchy.get('IS_MAIN_HIERARCHY', '')
            }
            for hierarchy in self.hierarchies_data
        ]


# Convenience functions for easy usage
def load_member_hierarchy_data(entities_path: str = "entities/") -> MemberHierarchyIntegration:
    """
    Load member hierarchy data from CSV files and return integration object
    """
    def read_csv_as_dict_list(filepath: str) -> List[Dict]:
        """Read CSV file and return as list of dictionaries"""
        data = []
        with open(filepath, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        return data

    members = read_csv_as_dict_list(f"{entities_path}member.csv")
    member_hierarchies = read_csv_as_dict_list(f"{entities_path}member_hierarchy.csv")
    member_hierarchy_nodes = read_csv_as_dict_list(f"{entities_path}member_hierarchy_node.csv")

    return MemberHierarchyIntegration(members, member_hierarchies, member_hierarchy_nodes)


def generate_hierarchy_json(hierarchy_id: str, output_path: str = "hierarchy.json",
                          entities_path: str = "entities/"):
    """
    Generate hierarchy JSON for a specific hierarchy ID
    """
    integration = load_member_hierarchy_data(entities_path)
    integration.save_hierarchy_json(hierarchy_id, output_path)
    print(f"Generated hierarchy JSON for {hierarchy_id} at {output_path}")


def list_available_hierarchies(entities_path: str = "entities/") -> List[Dict]:
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
