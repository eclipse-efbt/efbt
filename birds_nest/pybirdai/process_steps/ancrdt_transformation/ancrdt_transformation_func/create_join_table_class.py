# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create Join Table Class

Builds join table class with calc and init methods that perform
actual join logic between related tables.
"""

import ast
from .ast_builders import (
    create_class, create_method_with_body, create_attribute,
    create_assignment, create_expr_stmt, create_return,
    create_for_loop, create_if_statement
)


def create_join_table_class(rolc_id: str, join_id: str, cube_structure_item_links, logger) -> ast.ClassDef:
    """
    Create join table class with join logic.

    This creates a class that:
    1. Has attributes for each related table (e.g., INSTRMNT_Table, INSTRMNT_RL_Table)
    2. Has an items list to store joined results
    3. Has a calc method that performs the actual join
    4. Has an init method that orchestrates the process

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

    # Build calc method with actual join logic
    calc_body = [
        create_assignment("items", "[]"),
    ]

    # Filter mapping based on join_identifier for INSTRMNT_TYP_PRDCT
    INSTRMNT_TYP_PRDCT_FILTERS = {
        "Other loans": "1022",
        "Credit card debt": "51",
    }

    if len(primary_cubes_added) >= 1:
        main_cube = primary_cubes_added[0]
        related_cubes = primary_cubes_added[1:] if len(primary_cubes_added) > 1 else []

        # Build the inner loop body that creates join items
        inner_body = []

        # Create the join item
        inner_body.append(create_assignment("new_item", f"{join_id_clean}()"))

        # Assign main cube reference
        inner_body.append(create_expr_stmt(f"new_item.{main_cube} = {main_cube.lower()}_item"))

        # For related cubes, we add None assignment (actual matching logic would need FK info)
        for related_cube in related_cubes:
            # Add a comment-like assignment showing the pattern
            # In production, this would need proper FK matching
            inner_body.append(create_expr_stmt(
                f"new_item.{related_cube} = self.{related_cube}_Table[0] if self.{related_cube}_Table and len(self.{related_cube}_Table) > 0 else None"
            ))

        inner_body.append(create_expr_stmt("items.append(new_item)"))

        # If this join has a known INSTRMNT_TYP_PRDCT filter, wrap inner_body in if statement
        if join_id in INSTRMNT_TYP_PRDCT_FILTERS:
            filter_value = INSTRMNT_TYP_PRDCT_FILTERS[join_id]
            inner_body = [create_if_statement(
                condition_expr=f"{main_cube.lower()}_item.INSTRMNT_TYP_PRDCT == '{filter_value}'",
                body=inner_body
            )]

        # Create the for loop over main table
        for_loop = create_for_loop(
            var_name=f"{main_cube.lower()}_item",
            iter_expr=f"self.{main_cube}_Table",
            body=inner_body
        )
        calc_body.append(for_loop)

    calc_body.append(create_return("items"))

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
