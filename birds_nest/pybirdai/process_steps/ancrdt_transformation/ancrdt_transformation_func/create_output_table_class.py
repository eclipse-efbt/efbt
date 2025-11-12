# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create Output Table Class

Builds output table class with calc and init methods.
"""

import ast
from .ast_builders import (
    create_class, create_method_with_body, create_attribute,
    create_assignment, create_for_loop, create_expr_stmt, create_return
)


def create_output_table_class(rolc_id: str) -> ast.ClassDef:
    """
    Create output table class.
    
    Args:
        rolc_id: Role-based cube identifier
        
    Returns:
        AST ClassDef node for the output table class
    """
    # Attributes
    union_table_attr = create_attribute(f"{rolc_id}_UnionTable", "None")
    items_attr = create_attribute(f"{rolc_id}s", "[]")
    
    # calc method body
    calc_body = [
        create_assignment("items", "[]"),
        create_for_loop(
            var_name="item",
            iter_expr=f"self.{rolc_id}_UnionTable.{rolc_id}_UnionItems",
            body=[
                create_assignment("newItem", f"{rolc_id}()"),
                create_expr_stmt("newItem.unionOfLayers = item"),
                create_expr_stmt("items.append(newItem)")
            ]
        ),
        create_return("items")
    ]
    
    calc_method = create_method_with_body(
        name=f"calc_{rolc_id}s",
        return_type=f"list[{rolc_id}]",
        body_stmts=calc_body
    )
    
    # init method body
    init_body = [
        create_expr_stmt("Orchestration().init(self)"),
        create_assignment(f"self.{rolc_id}s", "[]"),
        create_expr_stmt(f"self.{rolc_id}s.extend(self.calc_{rolc_id}s())"),
        create_expr_stmt("CSVConverter.persist_object_as_csv(self, True)"),
        create_return("None")
    ]
    
    init_method = create_method_with_body(
        name="init",
        return_type=None,
        body_stmts=init_body,
        decorators=["track_table_init"]
    )
    
    # Create class
    return create_class(
        name=f"{rolc_id}_Table",
        bases=[],
        body=[union_table_attr, items_attr, calc_method, init_method]
    )
