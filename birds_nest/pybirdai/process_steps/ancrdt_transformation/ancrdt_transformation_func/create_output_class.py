# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create Output Class

Builds output class that delegates to unionOfLayers.
"""

import ast
from .ast_builders import create_class, create_simple_method, create_attribute, DOMAIN_TYPE_MAP


def create_output_class(rolc_id: str, cube_structure_items) -> ast.ClassDef:
    """
    Create output class with unionOfLayers delegation.
    
    Args:
        rolc_id: Role-based cube identifier
        cube_structure_items: List of cube structure items
        
    Returns:
        AST ClassDef node for the output class
    """
    # unionOfLayers attribute
    union_attr = create_attribute("unionOfLayers", "None", f"{rolc_id}_UnionItem")
    
    # Delegation methods
    methods = []
    for cube_structure_item in cube_structure_items:
        variable = cube_structure_item.variable_id
        if variable.variable_id == "NEVS":
            continue
        
        domain = variable.domain_id.domain_id
        return_type = DOMAIN_TYPE_MAP.get(domain, 'str')
        
        # Create method with lineage decorator
        method = create_simple_method(
            name=variable.variable_id,
            return_type=return_type,
            body_expr=f"self.unionOfLayers.{variable.variable_id}()",
            has_self=True,
            decorators=[f'lineage(dependencies={{"unionOfLayers.{variable.variable_id}"}})']
        )
        methods.append(method)
    
    # Create class
    return create_class(
        name=rolc_id,
        bases=[],
        body=[union_attr] + methods
    )
