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

    This creates a class that:
    1. Has attributes for each primary (input) cube (set to None initially)
    2. Has mapping methods for each linked variable
    3. Has pass-through methods for variables from related cubes (with None checks)

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

    # Track which foreign variables we've already added methods for
    methods_added = set()

    # Add identity field methods for all primary cubes
    # This ensures fields like INSTRMNT_ID, CLLTRL_ID are accessible even when not explicitly linked
    for i, cube_id in enumerate(primary_cubes_added):
        identity_field = f"{cube_id}_ID"
        # Main cube (first) doesn't need None check, secondary cubes do
        is_optional = i > 0
        if is_optional:
            body_expr = f"self.{cube_id}.{identity_field} if self.{cube_id} else None"
        else:
            body_expr = f"self.{cube_id}.{identity_field}"

        identity_method = create_simple_method(
            name=identity_field,
            return_type=None,
            body_expr=body_expr,
            has_self=True,
            decorators=[f'lineage(dependencies={{"{cube_id}.{identity_field}"}})']
        )
        body_nodes.append(identity_method)
        methods_added.add(identity_field)

    # Add mapping methods for linked variables
    for link in cube_structure_item_links:
        primary_cube_id = link.cube_link_id.primary_cube_id.cube_id
        primary_var = link.primary_cube_variable_code.variable_id.variable_id
        foreign_var = link.foreign_cube_variable_code.variable_id.variable_id

        if foreign_var in methods_added:
            continue

        # Determine if this cube might be optional (not the main cube)
        # Main cube is typically the first one; others get None checks
        is_optional = primary_cube_id != primary_cubes_added[0] if primary_cubes_added else False

        if is_optional:
            # Use conditional return with None check
            body_expr = f"self.{primary_cube_id}.{primary_var} if self.{primary_cube_id} else None"
        else:
            body_expr = f"self.{primary_cube_id}.{primary_var}"

        method = create_simple_method(
            name=foreign_var,
            return_type=None,  # Will be inferred
            body_expr=body_expr,
            has_self=True,
            decorators=[f'lineage(dependencies={{"{primary_cube_id}.{primary_var}"}})']
        )
        body_nodes.append(method)
        methods_added.add(foreign_var)

    # Create class
    return create_class(
        name=join_identifier.replace(' ', '_'),
        bases=[f"{rolc_id}_Base"],
        body=body_nodes if body_nodes else [ast.Pass()]
    )
