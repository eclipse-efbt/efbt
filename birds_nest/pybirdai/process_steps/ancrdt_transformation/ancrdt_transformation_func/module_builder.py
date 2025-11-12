# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Module Builder

Assembles complete Python modules from imports and class definitions.
"""

import ast
from typing import List
from .ast_builders import fix_ast_locations, unparse_with_header


def build_complete_module(
    imports: List[ast.ImportFrom],
    classes: List[ast.ClassDef],
    filename: str
) -> str:
    """
    Build a complete Python module from imports and classes.
    
    Args:
        imports: List of import statements
        classes: List of class definitions
        filename: Name of the file (for header comment)
        
    Returns:
        Complete Python source code as string
    """
    # Combine imports and classes into module body
    module_body = imports + classes
    
    # Create module node
    module = ast.Module(body=module_body, type_ignores=[])
    
    # Fix locations
    fix_ast_locations(module)
    
    # Convert to source with header
    return unparse_with_header(module, filename)
