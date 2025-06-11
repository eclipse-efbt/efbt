import json
import sys
from typing import Dict, List, Set, Tuple

def load_hierarchy_json(file_path: str) -> dict:
    """Load the hierarchy JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def build_hierarchy_graph(arrows: List[dict]) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
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

def find_root_nodes(all_nodes: Set[str], parent_map: Dict[str, Set[str]]) -> Set[str]:
    """Find nodes that have no parents (root nodes)."""
    return {node for node in all_nodes if node not in parent_map}

def calculate_levels(root_nodes: Set[str], children_map: Dict[str, Set[str]]) -> Dict[str, int]:
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

def determine_node_properties(node: str, children_map: Dict[str, Set[str]], parent_map: Dict[str, Set[str]]) -> Tuple[str, str]:
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

def get_node_name(node_id: str, boxes: List[dict]) -> str:
    """Get the name of a node from the boxes list."""
    for box in boxes:
        if box['id'] == node_id:
            return box.get('name', node_id)
    return node_id

def convert_to_member_hierarchy_nodes(hierarchy_data: dict) -> List[dict]:
    """Convert hierarchy data to MEMBER_HIERARCHY_NODE format."""
    boxes = hierarchy_data.get('boxes', [])
    arrows = hierarchy_data.get('arrows', [])
    
    # Get all unique node IDs
    all_nodes = set()
    for box in boxes:
        all_nodes.add(box['id'])
    
    # Build hierarchy relationships
    children_map, parent_map = build_hierarchy_graph(arrows)
    
    # Find root nodes and calculate levels
    root_nodes = find_root_nodes(all_nodes, parent_map)
    levels = calculate_levels(root_nodes, children_map)
    
    # Create MEMBER_HIERARCHY_NODE entries
    member_nodes = []
    
    for node in all_nodes:
        # Get node properties
        comparator, operator = determine_node_properties(node, children_map, parent_map)
        level = levels.get(node, 1)
        name = get_node_name(node, boxes)
        
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
    
    return member_nodes

def save_member_hierarchy_nodes(member_nodes: List[dict], output_file: str):
    """Save the MEMBER_HIERARCHY_NODE data to a JSON file."""
    output_data = {
        "MEMBER_HIERARCHY_NODE": member_nodes
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)

def print_summary(member_nodes: List[dict]):
    """Print a summary of the conversion."""
    print(f"Converted {len(member_nodes)} nodes to MEMBER_HIERARCHY_NODE format")
    print("\nLevel distribution:")
    level_counts = {}
    for node in member_nodes:
        level = node['level']
        level_counts[level] = level_counts.get(level, 0) + 1
    
    for level in sorted(level_counts.keys()):
        print(f"  Level {level}: {level_counts[level]} nodes")
    
    print("\nNode type distribution:")
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
    
    for type_name, count in type_counts.items():
        print(f"  {type_name}: {count} nodes")

def main():
    """Main function to run the conversion."""
    input_file = "generated_hierarchy.json"
    output_file = "member_hierarchy_nodes.json"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    try:
        # Load the hierarchy data
        print(f"Loading hierarchy from {input_file}...")
        hierarchy_data = load_hierarchy_json(input_file)
        
        # Convert to MEMBER_HIERARCHY_NODE format
        print("Converting to MEMBER_HIERARCHY_NODE format...")
        member_nodes = convert_to_member_hierarchy_nodes(hierarchy_data)
        
        # Save the result
        print(f"Saving to {output_file}...")
        save_member_hierarchy_nodes(member_nodes, output_file)
        
        # Print summary
        print_summary(member_nodes)
        
        print(f"\nConversion completed successfully!")
        print(f"Output saved to: {output_file}")
        
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()