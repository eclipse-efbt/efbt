# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create Join Table Class

Builds join table class with stub calc and init methods.
"""

import ast
from .ast_builders import (
    create_class, create_method_with_body, create_attribute,
    create_assignment, create_expr_stmt, create_return
)


def create_join_table_class(rolc_id: str, join_id: str, cube_structure_item_links, logger) -> ast.ClassDef:
    """
    Create join table class.
    
    Args:
        rolc_id: Role-based cube identifier
        join_id: Join identifier
        cube_structure_item_links: List of cube structure item links
        logger: Logger instance
        
    Returns:
        AST ClassDef node for the join table class
    """
    body_nodes = []
    
    # Add primary cube table attributes (deduplicated)
    primary_cubes_added = []
    for link in cube_structure_item_links:
        primary_cube_id = link.cube_link_id.primary_cube_id.cube_id
        if primary_cube_id not in primary_cubes_added:
            attr = create_attribute(f"{primary_cube_id}_Table", "None")
            body_nodes.append(attr)
            primary_cubes_added.append(primary_cube_id)
    
    # Items attribute
    join_id_clean = join_id.replace(' ', '_')
    items_attr = create_attribute(f"{join_id_clean}s", "[]")
    body_nodes.append(items_attr)
    
    # calc method (stub with comments)
    calc_body = [
        create_assignment("items", "[]"),
        # Comments as expression statements (will show in generated code)
        create_return("items")
    ]
    
    calc_method = create_method_with_body(
        name=f"calc_{join_id_clean}s",
        return_type=None,
        body_stmts=calc_body
    )
    body_nodes.append(calc_method)
    
    # init method
    init_body = [
        create_expr_stmt("Orchestration().init(self)"),
        create_assignment(f"self.{join_id_clean}s", "[]"),
        create_expr_stmt(f"self.{join_id_clean}s.extend(self.calc_{join_id_clean}s())"),
        create_expr_stmt("CSVConverter.persist_object_as_csv(self, True)"),
        create_return("None")
    ]
    
    init_method = create_method_with_body(
        name="init",
        return_type=None,
        body_stmts=init_body,
        decorators=["track_table_init"]
    )
    body_nodes.append(init_method)
    
    # Create class
    return create_class(
        name=f"{rolc_id}_{join_id_clean}_Table",
        bases=[],
        body=body_nodes
    )
