import json
from typing import Dict, List, Set, Tuple
from django.core.exceptions import ObjectDoesNotExist
from ...bird_meta_data_model import MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE


class DjangoModelConverter:
    """Converts between visualization data and Django MEMBER_HIERARCHY_NODE models"""
    
    def visualization_to_django_nodes(self, hierarchy_id: str, visualization_data: dict) -> bool:
        """Convert visualization data to Django MEMBER_HIERARCHY_NODE instances"""
        try:
            hierarchy = MEMBER_HIERARCHY.objects.get(member_hierarchy_id=hierarchy_id)
            
            # Clear existing nodes for this hierarchy
            MEMBER_HIERARCHY_NODE.objects.filter(member_hierarchy_id=hierarchy).delete()
            
            boxes = visualization_data.get('boxes', [])
            arrows = visualization_data.get('arrows', [])
            
            # Build parent-child relationships from arrows
            parent_map = {}
            children_map = {}
            
            for arrow in arrows:
                child_id = arrow['from']
                parent_id = arrow['to']
                parent_map[child_id] = parent_id
                
                if parent_id not in children_map:
                    children_map[parent_id] = []
                children_map[parent_id].append(child_id)
            
            # Calculate levels for each node
            def calculate_level(node_id, visited=None):
                if visited is None:
                    visited = set()
                
                if node_id in visited:
                    return 1  # Circular reference, treat as root
                
                visited.add(node_id)
                
                if node_id not in parent_map:
                    return 1  # Root node
                
                parent_id = parent_map[node_id]
                return calculate_level(parent_id, visited.copy()) + 1
            
            # Determine node properties based on hierarchy rules
            def determine_node_properties(node_id):
                has_children = node_id in children_map and len(children_map[node_id]) > 0
                has_parent = node_id in parent_map
                
                if has_children and not has_parent:
                    # Root node with children
                    return "=", ""
                elif has_children and has_parent:
                    # Intermediate node
                    return "=", "+"
                elif not has_children and has_parent:
                    # Leaf node
                    return "", "+"
                else:
                    # Standalone node (should not happen in well-formed hierarchies)
                    return "", ""
            
            # Create new nodes
            created_nodes = []
            for box in boxes:
                try:
                    member = MEMBER.objects.get(member_id=box['id'])
                    parent_member = None
                    
                    if box['id'] in parent_map:
                        try:
                            parent_member = MEMBER.objects.get(member_id=parent_map[box['id']])
                        except MEMBER.DoesNotExist:
                            continue  # Skip if parent member doesn't exist
                    
                    level = calculate_level(box['id'])
                    comparator, operator = determine_node_properties(box['id'])
                    
                    node = MEMBER_HIERARCHY_NODE.objects.create(
                        member_hierarchy_id=hierarchy,
                        member_id=member,
                        parent_member_id=parent_member,
                        level=level,
                        comparator=comparator,
                        operator=operator
                    )
                    created_nodes.append(node)
                    
                except MEMBER.DoesNotExist:
                    continue  # Skip invalid members
            
            return len(created_nodes) > 0
            
        except MEMBER_HIERARCHY.DoesNotExist:
            return False
        except Exception as e:
            print(f"Error converting visualization to Django nodes: {e}")
            return False
    
    def django_nodes_to_visualization(self, hierarchy_id: str) -> Dict:
        """Convert Django MEMBER_HIERARCHY_NODE instances to visualization format"""
        try:
            hierarchy = MEMBER_HIERARCHY.objects.get(member_hierarchy_id=hierarchy_id)
        except MEMBER_HIERARCHY.DoesNotExist:
            return {"boxes": [], "arrows": [], "nextId": 1, "error": "Hierarchy not found"}
        
        # Get all nodes for this hierarchy
        hierarchy_nodes = MEMBER_HIERARCHY_NODE.objects.filter(
            member_hierarchy_id=hierarchy
        ).select_related('member_id', 'parent_member_id').order_by('level', 'member_id__member_id')
        
        if not hierarchy_nodes.exists():
            return {"boxes": [], "arrows": [], "nextId": 1}
        
        boxes = []
        arrows = []
        
        # Layout configuration
        BOX_WIDTH = 300
        BOX_HEIGHT = 120
        level_spacing_y = 200 + BOX_HEIGHT * 2
        node_spacing_x = 250 + BOX_WIDTH
        start_x = 100
        start_y = 200
        
        # Group nodes by level for positioning
        level_groups = {}
        for node in hierarchy_nodes:
            level = node.level
            if level not in level_groups:
                level_groups[level] = []
            level_groups[level].append(node)
        
        for level, level_nodes in level_groups.items():
            level_nodes.sort(key=lambda x: x.member_id.member_id if x.member_id else "")
            
            for i, node_row in enumerate(level_nodes):
                # Get member information
                member = node_row.member_id
                if member:
                    name = member.name or member.member_id
                    description = member.description or name
                else:
                    name = "Unknown Member"
                    description = "No description available"
                
                # Calculate position
                x = start_x + (i * node_spacing_x)
                y = start_y + ((level - 1) * level_spacing_y)
                
                # Create box
                box = {
                    'id': member.member_id if member else f"unknown_{node_row.id}",
                    'x': x,
                    'y': y,
                    'width': BOX_WIDTH,
                    'height': BOX_HEIGHT,
                    'name': name,
                    'text': description
                }
                boxes.append(box)
                
                # Create arrow if there's a parent
                if node_row.parent_member_id:
                    arrow = {
                        'from': member.member_id if member else f"unknown_{node_row.id}",
                        'to': node_row.parent_member_id.member_id
                    }
                    arrows.append(arrow)
        
        # Calculate next available ID
        next_id = len(boxes) + 1
        
        # Get allowed members for this hierarchy's domain
        allowed_members = {}
        if hierarchy.domain_id:
            domain_members = MEMBER.objects.filter(domain_id=hierarchy.domain_id)
            allowed_members = {
                member.member_id: member.name or member.member_id 
                for member in domain_members
            }
        
        return {
            "boxes": boxes,
            "arrows": arrows,
            "nextId": next_id,
            "hierarchy_info": {
                "id": hierarchy_id,
                "name": hierarchy.name or hierarchy_id,
                "description": hierarchy.description or "",
                "domain": hierarchy.domain_id.domain_id if hierarchy.domain_id else "",
                "allowed_members": allowed_members
            }
        }
    
    def validate_hierarchy_structure(self, visualization_data: dict) -> Tuple[bool, List[str]]:
        """Validate the hierarchy structure for logical consistency"""
        boxes = visualization_data.get('boxes', [])
        arrows = visualization_data.get('arrows', [])
        
        errors = []
        
        # Check for basic structure
        if not boxes:
            errors.append("Hierarchy must contain at least one member")
            return False, errors
        
        # Build graph structure
        all_nodes = {box['id'] for box in boxes}
        parent_map = {}
        children_map = {}
        
        for arrow in arrows:
            child_id = arrow['from']
            parent_id = arrow['to']
            
            # Validate that both nodes exist
            if child_id not in all_nodes:
                errors.append(f"Arrow references non-existent child node: {child_id}")
                continue
            if parent_id not in all_nodes:
                errors.append(f"Arrow references non-existent parent node: {parent_id}")
                continue
            
            # Check for self-reference
            if child_id == parent_id:
                errors.append(f"Node cannot be parent of itself: {child_id}")
                continue
            
            # Check for duplicate relationships
            if child_id in parent_map:
                errors.append(f"Node {child_id} has multiple parents: {parent_map[child_id]} and {parent_id}")
                continue
            
            parent_map[child_id] = parent_id
            
            if parent_id not in children_map:
                children_map[parent_id] = []
            children_map[parent_id].append(child_id)
        
        # Check for cycles
        def has_cycle(node_id, visited, rec_stack):
            visited.add(node_id)
            rec_stack.add(node_id)
            
            if node_id in children_map:
                for child in children_map[node_id]:
                    if child not in visited:
                        if has_cycle(child, visited, rec_stack):
                            return True
                    elif child in rec_stack:
                        return True
            
            rec_stack.remove(node_id)
            return False
        
        visited = set()
        for node_id in all_nodes:
            if node_id not in visited:
                if has_cycle(node_id, visited, set()):
                    errors.append("Hierarchy contains circular references")
                    break
        
        # Check for multiple root nodes (warning, not error)
        root_nodes = [node_id for node_id in all_nodes if node_id not in parent_map]
        if len(root_nodes) > 1:
            errors.append(f"Warning: Hierarchy has multiple root nodes: {', '.join(root_nodes)}")
        elif len(root_nodes) == 0 and all_nodes:
            errors.append("Error: No root nodes found - all nodes have parents")
        
        return len([e for e in errors if e.startswith("Error:")]) == 0, errors
    
    def get_hierarchy_statistics(self, hierarchy_id: str) -> Dict:
        """Get statistics about a hierarchy"""
        try:
            hierarchy = MEMBER_HIERARCHY.objects.get(member_hierarchy_id=hierarchy_id)
            nodes = MEMBER_HIERARCHY_NODE.objects.filter(member_hierarchy_id=hierarchy)
            
            total_nodes = nodes.count()
            levels = nodes.values_list('level', flat=True).distinct()
            max_level = max(levels) if levels else 0
            
            # Count nodes by level
            level_counts = {}
            for level in levels:
                level_counts[level] = nodes.filter(level=level).count()
            
            # Count by node types
            root_nodes = nodes.filter(parent_member_id__isnull=True).count()
            leaf_nodes = nodes.exclude(
                member_id__in=nodes.values_list('parent_member_id', flat=True)
            ).count()
            intermediate_nodes = total_nodes - root_nodes - leaf_nodes
            
            return {
                'total_nodes': total_nodes,
                'max_level': max_level,
                'level_counts': level_counts,
                'root_nodes': root_nodes,
                'leaf_nodes': leaf_nodes,
                'intermediate_nodes': intermediate_nodes,
                'hierarchy_name': hierarchy.name,
                'domain': hierarchy.domain_id.domain_id if hierarchy.domain_id else None
            }
            
        except MEMBER_HIERARCHY.DoesNotExist:
            return {'error': 'Hierarchy not found'}