# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create UnionTable Class

Builds UnionTable class with dynamic attributes and calc/init methods.
"""

import ast
from .ast_builders import (
    create_class, create_method_with_body, create_attribute,
    create_assignment, create_for_loop, create_expr_stmt, create_return
)


def create_union_table_class(rolc_id: str, cube_link_to_join_for_report_id_map, logger) -> ast.ClassDef:
    """
    Create UnionTable class.
    
    Args:
        rolc_id: Role-based cube identifier
        cube_link_to_join_for_report_id_map: Dict mapping join_for_rolc_id to cube_links
        logger: Logger instance
        
    Returns:
        AST ClassDef node for the UnionTable class
    """
    body_nodes = []
    
    # UnionItems attribute
    union_items_attr = create_attribute(f"{rolc_id}_UnionItems", "[]")
    body_nodes.append(union_items_attr)
    
    # Add dynamic attributes for each join (deduplicated)
    join_ids_added = []
    for join_for_rolc_id, cube_links in cube_link_to_join_for_report_id_map.items():
        for cube_link in cube_links:
            the_rolc_id = cube_link.foreign_cube_id.cube_id
            if the_rolc_id == rolc_id:
                join_id = cube_link.join_identifier
                if join_id not in join_ids_added:
                    join_id_clean = join_id.replace(' ', '_')
                    attr = create_attribute(f"{rolc_id}_{join_id_clean}_Table", "None")
                    body_nodes.append(attr)
                    join_ids_added.append(join_id)
    
    # Build calc method with nested for loops
    calc_body = [create_assignment("items", "[]")]
    
    join_ids_added = []
    for join_for_rolc_id, cube_links in cube_link_to_join_for_report_id_map.items():
        for cube_link in cube_links:
            the_rolc_id = cube_link.foreign_cube_id.cube_id
            if the_rolc_id == rolc_id:
                join_id = cube_link.join_identifier
                if join_id not in join_ids_added:
                    join_id_clean = join_id.replace(' ', '_')

                    for_loop = create_for_loop(
                        var_name="item",
                        iter_expr=f"self.{rolc_id}_{join_id_clean}_Table.{join_id_clean}s",
                        body=[
                            create_assignment("newItem", f"{rolc_id}_UnionItem()"),
                            create_expr_stmt("newItem.base = item"),
                            create_expr_stmt("items.append(newItem)")
                        ]
                    )
                    calc_body.append(for_loop)
                    join_ids_added.append(join_id)
    
    calc_body.append(create_return("items"))
    
    calc_method = create_method_with_body(
        name=f"calc_{rolc_id}_UnionItems",
        return_type=f"list[{rolc_id}_UnionItem]",
        body_stmts=calc_body
    )
    body_nodes.append(calc_method)
    
    # init method
    init_body = [
        create_expr_stmt("Orchestration().init(self)"),
        create_assignment(f"self.{rolc_id}_UnionItems", "[]"),
        create_expr_stmt(f"self.{rolc_id}_UnionItems.extend(self.calc_{rolc_id}_UnionItems())"),
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
        name=f"{rolc_id}_UnionTable",
        bases=[],
        body=body_nodes
    )
