import json
from typing import Dict, List
from ...models.bird_meta_data_model import (
    MEMBER, MEMBER_HIERARCHY, DOMAIN
)
from .django_model_converter import DjangoModelConverter

BOX_WIDTH = 300
BOX_HEIGHT = 120


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

    @property
    def to_json(self) -> str:
        return json.dumps(self.to_dict)


class HierarchyArrowDTO:
    """Data Transfer Object for hierarchy arrows"""

    def __init__(self, from_: str = "", to_: str = ""):
        self.from_ = from_  # child
        self.to_ = to_      # parent

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


class DjangoMemberHierarchyIntegration:
    """Django model-based integration for member hierarchy visualization"""

    def __init__(self):
        self.converter = DjangoModelConverter()

    def get_hierarchy_by_id(self, hierarchy_id: str) -> Dict:
        """Convert a specific member hierarchy to visualization format"""
        return self.converter.django_nodes_to_visualization(hierarchy_id)

    def get_all_hierarchies(self) -> Dict[str, Dict]:
        """Get all hierarchies as a dictionary"""
        all_hierarchies = {}
        hierarchies = MEMBER_HIERARCHY.objects.all()

        for hierarchy in hierarchies:
            all_hierarchies[hierarchy.member_hierarchy_id] = self.get_hierarchy_by_id(
                hierarchy.member_hierarchy_id
            )

        return all_hierarchies

    def get_hierarchies_by_domain(self, domain_id: str) -> Dict[str, Dict]:
        """Get all hierarchies for a specific domain"""
        try:
            domain = DOMAIN.objects.get(domain_id=domain_id)
            hierarchies = MEMBER_HIERARCHY.objects.filter(domain_id=domain)

            result = {}
            for hierarchy in hierarchies:
                result[hierarchy.member_hierarchy_id] = self.get_hierarchy_by_id(
                    hierarchy.member_hierarchy_id
                )

            return result
        except DOMAIN.DoesNotExist:
            return {}

    def save_hierarchy_from_visualization(self, hierarchy_id: str, visualization_data: dict) -> bool:
        """Save visualization data back to Django models"""
        # Validate the structure first
        is_valid, errors = self.converter.validate_hierarchy_structure(visualization_data)
        if not is_valid:
            print(f"Hierarchy validation failed: {errors}")
            return False

        return self.converter.visualization_to_django_nodes(hierarchy_id, visualization_data)

    def get_available_hierarchies(self) -> List[Dict]:
        """Get a summary of all available hierarchies"""
        hierarchies = MEMBER_HIERARCHY.objects.select_related('domain_id').all()

        return [
            {
                'member_hierarchy_id': h.member_hierarchy_id,
                'name': h.name,
                'domain_id': h.domain_id.domain_id if h.domain_id else None,
                'description': h.description,
                'is_main_hierarchy': getattr(h, 'is_main_hierarchy', False)
            }
            for h in hierarchies
        ]

    def get_domain_members(self, domain_id: str) -> List[Dict]:
        """Get all members for a specific domain"""
        try:
            domain = DOMAIN.objects.get(domain_id=domain_id)
            members = MEMBER.objects.filter(domain_id=domain)

            return [
                {
                    'member_id': m.member_id,
                    'name': m.name or m.member_id,
                    'code': m.code or m.member_id,
                    'description': m.description or ""
                }
                for m in members
            ]
        except DOMAIN.DoesNotExist:
            return []


# Convenience functions for Django views
def get_hierarchy_integration() -> DjangoMemberHierarchyIntegration:
    """Get a new hierarchy integration instance"""
    return DjangoMemberHierarchyIntegration()


def get_hierarchy_json_response(hierarchy_id: str) -> dict:
    """Get hierarchy data as dict for JSON responses"""
    integration = get_hierarchy_integration()
    hierarchy_data = integration.get_hierarchy_by_id(hierarchy_id)
    return hierarchy_data


def save_hierarchy_json(hierarchy_id: str, visualization_data: dict) -> dict:
    """Save hierarchy from visualization data and return response dict"""
    integration = get_hierarchy_integration()

    # Validate structure first
    is_valid, errors = integration.converter.validate_hierarchy_structure(visualization_data)
    if not is_valid:
        return {
            'success': False,
            'message': 'Hierarchy validation failed',
            'errors': errors
        }

    success = integration.save_hierarchy_from_visualization(hierarchy_id, visualization_data)

    return {
        'success': success,
        'message': 'Hierarchy saved successfully' if success else 'Failed to save hierarchy'
    }
