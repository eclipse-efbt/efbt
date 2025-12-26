# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create UnionItem Class

Builds UnionItem class that delegates to base implementation.
"""

import ast
from .ast_builders import create_class, create_simple_method, create_attribute, DOMAIN_TYPE_MAP


def create_union_item_class(rolc_id: str, cube_structure_items) -> ast.ClassDef:
    """
    Create UnionItem class with delegation methods.
    
    Args:
        rolc_id: Role-based cube identifier
        cube_structure_items: List of cube structure items
        
    Returns:
        AST ClassDef node for the UnionItem class
    """
    # Base attribute
    base_attr = create_attribute("base", "None", f"{rolc_id}_Base")
    
    # Delegation methods
    methods = []
    for cube_structure_item in cube_structure_items:
        variable = cube_structure_item.variable_id
        if not variable:
            continue
        if variable.variable_id == "NEVS":
            continue

        # Handle case where variable has no domain
        domain = variable.domain_id.domain_id if variable.domain_id else 'String'
        return_type = DOMAIN_TYPE_MAP.get(domain, 'str')

        # Capture variable_id once to ensure consistency across method name, body, and decorator
        var_id = variable.variable_id

        # Create method with lineage decorator
        method = create_simple_method(
            name=var_id,
            return_type=return_type,
            body_expr=f"self.base.{var_id}()",
            has_self=True,
            decorators=[f'lineage(dependencies={{"base.{var_id}"}})']
        )
        methods.append(method)
    
    # Create class
    return create_class(
        name=f"{rolc_id}_UnionItem",
        bases=[],
        body=[base_attr] + methods
    )
