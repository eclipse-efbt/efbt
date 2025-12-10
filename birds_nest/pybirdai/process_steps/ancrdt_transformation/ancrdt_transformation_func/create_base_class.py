# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create Base Class

Builds the Base class with stub methods (pass statements).
"""

import ast
from .ast_builders import create_class, create_simple_method, DOMAIN_TYPE_MAP


def create_base_class(rolc_id: str, cube_structure_items) -> ast.ClassDef:
    """
    Create Base class with stub methods.
    
    Args:
        rolc_id: Role-based cube identifier
        cube_structure_items: List of cube structure items
        
    Returns:
        AST ClassDef node for the Base class
    """
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
        
        # Create stub method (with self, just pass)
        method = create_simple_method(
            name=variable.variable_id,
            return_type=return_type,
            body_expr=None,  # Will generate 'pass'
            has_self=True,  # Fixed: methods need self parameter
            docstring=None if domain in DOMAIN_TYPE_MAP else f'return string from {domain} enumeration'
        )
        methods.append(method)
    
    # Create class
    return create_class(
        name=f"{rolc_id}_Base",
        bases=[],
        body=methods if methods else [ast.Pass()]
    )
