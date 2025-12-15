# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Create Filtered/Aggregated Class Pair

Builds filtered and aggregated table class and item class with filter logic and mapping methods.
"""

import ast
import logging
from typing import Dict, List, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)
from .ast_builders import (
    create_class, create_simple_method, create_method_with_body,
    create_attribute, create_assignment, create_for_loop,
    create_if_statement, create_expr_stmt, create_return, DOMAIN_TYPE_MAP, fix_ast_locations
)


def _create_mapping_method_ast(method_name: str, source_expr: str, mapping_dict: list) -> ast.FunctionDef:
    """Create dictionary-based mapping method (from orchestrator)"""
    mapping_count = len(mapping_dict) if mapping_dict else 0
    logger.info(f"_create_mapping_method_ast called - method='{method_name}', mapping_entries={mapping_count}")
    source_value_ast = ast.parse(source_expr, mode='eval').body

    body = []
    if mapping_dict and len(mapping_dict) > 0:
        mapping_keys = [ast.Constant(value=row.get('source')) for row in mapping_dict]
        mapping_values = [ast.Constant(value=row.get('target')) for row in mapping_dict]


        body = [
            ast.Assign(
                targets=[ast.Name(id='source', ctx=ast.Store())],
                value=source_value_ast
            ),
            ast.Assign(
                targets=[ast.Name(id='mapping', ctx=ast.Store())],
                value=ast.Dict(keys=mapping_keys, values=mapping_values)
            ),
            ast.Return(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id='mapping', ctx=ast.Load()),
                        attr='get',
                        ctx=ast.Load()
                    ),
                    args=[
                        ast.Name(id='source', ctx=ast.Load()),
                        ast.Constant(value=None)
                    ],
                    keywords=[]
                )
            )
        ]
    else:
        body = [
            ast.Assign(
                targets=[ast.Name(id='source', ctx=ast.Store())],
                value=source_value_ast
            ),

            ast.Return(
                value=ast.Name(id='source', ctx=ast.Load())
                )

        ]

    func = ast.FunctionDef(
        name=method_name,
        args=ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg='self', annotation=None)],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[]
        ),
        body=body,
        decorator_list=[],
        returns=ast.Name(id='str', ctx=ast.Load())
    )

    fix_ast_locations(func)
    logger.info(f"_create_mapping_method_ast completed - generated method:\n{ast.unparse(func)}")

    return func


def create_filtered_class_pair(
    rolc_id: str,
    join_id: str,
    cube_structure_items,
    cube_links,
    links_by_cube: Dict,
    assignment_dicts: Dict,
    filter_builder_module,
    CUBE_STRUCTURE_ITEM_LINK,
    logger
) -> Tuple[ast.ClassDef, ast.ClassDef]:
    """
    Create filtered/aggregated table class and item class.

    Returns:
        Tuple of (table_class, item_class)
    """
    logger.info(f"create_filtered_class_pair called - rolc_id='{rolc_id}', join_id='{join_id}'")
    join_id_clean = join_id.replace(' ', '_')

    # ===== TABLE CLASS =====
    logger.info(f"Building table class with {len(cube_structure_items)} cube_structure_items")
    table_body = []

    # Add class attributes (Fixed: these were missing)
    table_body.append(create_attribute(f"{rolc_id}_{join_id_clean}_Table", "None"))
    table_body.append(create_attribute(f"{rolc_id}_{join_id_clean}_filtered_and_aggregateds", "[]"))

    # Add delegation methods for each variable
    for cube_structure_item in cube_structure_items:
        variable = cube_structure_item.variable_id
        if not variable:
            continue
        if variable.variable_id == "NEVS":
            continue

        # Handle case where variable has no domain
        domain = variable.domain_id.domain_id if variable.domain_id else 'String'
        return_type = DOMAIN_TYPE_MAP.get(domain, 'str')

        method = create_simple_method(
            name=variable.variable_id,
            return_type=return_type,
            body_expr=f"self.base.{variable.variable_id}()",
            has_self=True,
            decorators=[f'lineage(dependencies={{"base.{variable.variable_id}"}})']
        )
        table_body.append(method)

    # Build calc method with filter logic
    filter_assignments = []
    filter_bool_vars = []

    for cube_link in cube_links:
        cube_structure_item_link_ids = links_by_cube.get(cube_link, [])
        for cube_structure_item_link in cube_structure_item_link_ids:
            assignments, bool_var_names = filter_builder_module.define_filter_from_structure_link(
                cube_structure_item_link.cube_structure_item_link_id
            )
            filter_assignments.extend(assignments)
            filter_bool_vars.extend(bool_var_names)

    logger.info(f"Processing {len(cube_links)} cube_links, generated {len(filter_assignments)} filter assignments, {len(filter_bool_vars)} bool vars")

    if filter_bool_vars:
        # Create combined condition
        if len(filter_bool_vars) > 1:
            bool_list = ', '.join(filter_bool_vars)
            combined_condition = f"all([{bool_list}])"
        elif len(filter_bool_vars) == 1:
            combined_condition = filter_bool_vars[0]
        else:
            combined_condition = "True"

        # Build for loop body with filter
        for_loop_body = []

        # Add filter assignments as AST nodes
        for assignment_str in filter_assignments:
            assign_node = ast.parse(assignment_str, mode='exec').body[0]
            for_loop_body.append(assign_node)

        # Add if statement with combined condition
        if_body = [
            create_assignment("newItem", f"{rolc_id}_{join_id_clean}_filtered_and_aggregated()"),
            # Fixed: use descriptive attribute name instead of generic 'source'
            create_expr_stmt(f"newItem.{rolc_id}_{join_id_clean} = item"),
            create_expr_stmt("items.append(newItem)")
        ]

        if_stmt = create_if_statement(combined_condition, if_body)
        for_loop_body.append(if_stmt)

        # Build calc method
        # Fixed: collection name should be join_id_cleans, not rolc_id_join_id_cleans
        calc_body = [
            create_assignment("items", "[]"),
            create_for_loop(
                var_name="item",
                iter_expr=f"self.{rolc_id}_{join_id_clean}_Table.{join_id_clean}s",
                body=for_loop_body
            ),
            create_return("items")
        ]

        calc_method = create_method_with_body(
            name=f"calc_{rolc_id}_{join_id_clean}_filtered_and_aggregated",
            return_type="str",
            body_stmts=calc_body
        )
        table_body.append(calc_method)

    # Add init method (Fixed: was missing)
    init_body = [
        create_expr_stmt("Orchestration().init(self)"),
        create_assignment(f"self.{rolc_id}_{join_id_clean}_filtered_and_aggregateds", "[]"),
        create_expr_stmt(f"self.{rolc_id}_{join_id_clean}_filtered_and_aggregateds.extend(self.calc_{rolc_id}_{join_id_clean}_filtered_and_aggregated())"),
        create_expr_stmt("CSVConverter.persist_object_as_csv(self, True)"),
        create_return("None")
    ]

    init_method = create_method_with_body(
        name="init",
        return_type=None,
        body_stmts=init_body,
        decorators=["track_table_init"]
    )
    table_body.append(init_method)

    table_class = create_class(
        name=f"{rolc_id}_{join_id_clean}_filtered_and_aggregated_Table",
        bases=[],
        body=table_body if table_body else [ast.Pass()]
    )

    # ===== ITEM CLASS =====
    logger.info(f"Building item class with {len(assignment_dicts)} assignment dicts")
    # Build mapping methods using AST
    mapping_methods = []
    for var, source_target_dict in assignment_dicts.items():
        source_expr = f'self.{rolc_id}_{join_id_clean}.{var}()'
        method_ast = _create_mapping_method_ast(var, source_expr, source_target_dict)
        fix_ast_locations(method_ast)
        mapping_methods.append(method_ast)

    item_class = create_class(
        name=f"{rolc_id}_{join_id_clean}_filtered_and_aggregated",
        bases=[f"{rolc_id}_Base"],
        body=mapping_methods if mapping_methods else [ast.Pass()]
    )

    logger.info(f"create_filtered_class_pair completed - table_class='{rolc_id}_{join_id_clean}_filtered_and_aggregated_Table', item_class='{rolc_id}_{join_id_clean}_filtered_and_aggregated'")

    return (table_class, item_class)
