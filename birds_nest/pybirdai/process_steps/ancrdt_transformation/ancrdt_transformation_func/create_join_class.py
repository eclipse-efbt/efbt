# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create Join Class

Builds join class with attributes and mapping methods.
"""

import ast
from .ast_builders import create_class, create_simple_method, create_attribute


def create_join_class(rolc_id: str, join_identifier: str, cube_structure_item_links, logger) -> ast.ClassDef:
    """
    Create join class with cross-cube references.
    
    Args:
        rolc_id: Role-based cube identifier
        join_identifier: Join identifier
        cube_structure_item_links: List of cube structure item links
        logger: Logger instance
        
    Returns:
        AST ClassDef node for the join class
    """
    body_nodes = []
    
    # Add primary cube attributes (deduplicated)
    primary_cubes_added = []
    for link in cube_structure_item_links:
        primary_cube_id = link.cube_link_id.primary_cube_id.cube_id
        if primary_cube_id not in primary_cubes_added:
            attr = create_attribute(primary_cube_id, "None")
            body_nodes.append(attr)
            primary_cubes_added.append(primary_cube_id)
    
    # Add mapping methods
    for link in cube_structure_item_links:
        primary_cube_id = link.cube_link_id.primary_cube_id.cube_id
        primary_var = link.primary_cube_variable_code.variable_id.variable_id
        foreign_var = link.foreign_cube_variable_code.variable_id.variable_id
        
        method = create_simple_method(
            name=foreign_var,
            return_type=None,  # Will be inferred
            body_expr=f"self.{primary_cube_id}.{primary_var}",
            has_self=True,
            decorators=[f'lineage(dependencies={{"{primary_cube_id}.{primary_var}"}})']
        )
        body_nodes.append(method)
    
    # Create class
    return create_class(
        name=join_identifier.replace(' ', '_'),
        bases=[f"{rolc_id}_Base"],
        body=body_nodes if body_nodes else [ast.Pass()]
    )
