# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
Core AST Builder Functions

Provides low-level AST building blocks for code generation.
All functions return AST nodes that can be combined into complete modules.
"""

import ast
from typing import List, Optional, Union, Dict
from datetime import datetime


# Domain to Python type mapping
DOMAIN_TYPE_MAP = {
    'String': 'str',
    'Integer': 'int',
    'Date': 'datetime',
    'Float': 'float',
    'Boolean': 'bool'
}


def create_import(module: str, names: List[str] = None) -> ast.ImportFrom:
    """
    Create import statement: from module import names (or *).

    For relative imports, pass module with dots (e.g., '.module_name', '..package.module').
    The function will extract the level and strip dots from module name.
    """
    if names is None:
        names_list = [ast.alias(name='*', asname=None)]
    else:
        names_list = [ast.alias(name=name, asname=None) for name in names]

    # Handle relative imports: count leading dots, then strip them from module name
    if module and module.startswith('.'):
        level = len(module) - len(module.lstrip('.'))
        module_name = module.lstrip('.')
    else:
        level = 0
        module_name = module

    return ast.ImportFrom(module=module_name if module_name else None, names=names_list, level=level)


def create_class(name: str, bases: List[str] = None, body: List[ast.stmt] = None) -> ast.ClassDef:
    """Create class definition"""
    base_exprs = [ast.Name(id=base, ctx=ast.Load()) for base in (bases or [])]
    body_nodes = body if body else [ast.Pass()]
    
    return ast.ClassDef(
        name=name,
        bases=base_exprs,
        keywords=[],
        body=body_nodes,
        decorator_list=[]
    )


def create_attribute(name: str, value_expr: str, comment: str = None) -> ast.Assign:
    """Create class attribute assignment"""
    try:
        value_ast = ast.parse(value_expr, mode='eval').body
    except:
        value_ast = ast.Constant(value=value_expr)
    
    return ast.Assign(
        targets=[ast.Name(id=name, ctx=ast.Store())],
        value=value_ast
    )


def create_simple_method(
    name: str,
    return_type: str = None,
    body_expr: str = None,
    has_self: bool = True,
    decorators: List[str] = None,
    docstring: str = None
) -> ast.FunctionDef:
    """Create simple method with single return statement"""
    args_list = [ast.arg(arg='self', annotation=None)] if has_self else []
    args = ast.arguments(
        posonlyargs=[],
        args=args_list,
        kwonlyargs=[],
        kw_defaults=[],
        defaults=[]
    )
    
    body = []
    if docstring:
        body.append(ast.Expr(value=ast.Constant(value=docstring)))
    
    if body_expr:
        expr_ast = ast.parse(body_expr, mode='eval').body
        body.append(ast.Return(value=expr_ast))
    else:
        body.append(ast.Pass())
    
    decorator_list = []
    if decorators:
        for dec in decorators:
            if '(' in dec:
                decorator_list.append(ast.parse(dec, mode='eval').body)
            else:
                decorator_list.append(ast.Name(id=dec, ctx=ast.Load()))
    
    returns = ast.Name(id=return_type, ctx=ast.Load()) if return_type else None
    
    func = ast.FunctionDef(
        name=name,
        args=args,
        body=body,
        decorator_list=decorator_list,
        returns=returns
    )
    
    return func


def create_method_with_body(
    name: str,
    return_type: str,
    body_stmts: List[ast.stmt],
    has_self: bool = True,
    decorators: List[str] = None
) -> ast.FunctionDef:
    """Create method with multiple statements in body"""
    args_list = [ast.arg(arg='self', annotation=None)] if has_self else []
    args = ast.arguments(
        posonlyargs=[],
        args=args_list,
        kwonlyargs=[],
        kw_defaults=[],
        defaults=[]
    )
    
    decorator_list = []
    if decorators:
        for dec in decorators:
            if '(' in dec:
                decorator_list.append(ast.parse(dec, mode='eval').body)
            else:
                decorator_list.append(ast.Name(id=dec, ctx=ast.Load()))

    # Handle return type annotation
    if return_type is None:
        returns = None
    elif return_type.startswith('list['):
        inner_type = return_type[5:-1]
        returns = ast.Subscript(
            value=ast.Name(id='list', ctx=ast.Load()),
            slice=ast.Name(id=inner_type, ctx=ast.Load()),
            ctx=ast.Load()
        )
    else:
        returns = ast.Name(id=return_type, ctx=ast.Load())
    
    func = ast.FunctionDef(
        name=name,
        args=args,
        body=body_stmts if body_stmts else [ast.Pass()],
        decorator_list=decorator_list,
        returns=returns
    )
    
    return func


def create_assignment(target: str, value_expr: str) -> ast.Assign:
    """Create assignment statement: target = value_expr"""
    try:
        value_ast = ast.parse(value_expr, mode='eval').body
    except:
        value_ast = ast.Constant(value=value_expr)
    
    return ast.Assign(
        targets=[ast.Name(id=target, ctx=ast.Store())],
        value=value_ast
    )


def create_return(expr: str) -> ast.Return:
    """Create return statement"""
    if expr:
        value_ast = ast.parse(expr, mode='eval').body
        return ast.Return(value=value_ast)
    else:
        return ast.Return(value=None)


def create_for_loop(var_name: str, iter_expr: str, body: List[ast.stmt]) -> ast.For:
    """Create for loop statement"""
    iter_ast = ast.parse(iter_expr, mode='eval').body
    
    return ast.For(
        target=ast.Name(id=var_name, ctx=ast.Store()),
        iter=iter_ast,
        body=body if body else [ast.Pass()],
        orelse=[]
    )


def create_if_statement(condition_expr: str, body: List[ast.stmt], orelse: List[ast.stmt] = None) -> ast.If:
    """Create if statement"""
    cond_ast = ast.parse(condition_expr, mode='eval').body
    
    return ast.If(
        test=cond_ast,
        body=body if body else [ast.Pass()],
        orelse=orelse if orelse else []
    )


def create_expr_stmt(expr: str) -> ast.stmt:
    """
    Create statement node from string expression.

    Handles both expressions (e.g., 'func()') and assignments (e.g., 'x = y').

    Args:
        expr: String expression or statement

    Returns:
        AST statement node (ast.Expr for expressions, ast.Assign for assignments, etc.)
    """
    # Parse as statement (mode='exec') to handle assignments
    parsed = ast.parse(expr, mode='exec')
    if parsed.body:
        return parsed.body[0]
    else:
        # Empty statement, return Pass
        return ast.Pass()


def fix_ast_locations(node: ast.AST) -> ast.AST:
    """Fix missing location information in AST"""
    return ast.fix_missing_locations(node)


def unparse_with_header(module: ast.Module, filename: str) -> str:
    """Convert AST module to source code with generation header"""
    timestamp = datetime.now().isoformat()
    header = f"""# Generated: {timestamp}
# Generator: ANCRDT Transformation (AST-based)
# File: {filename}
# DO NOT EDIT THIS FILE DIRECTLY - Edit via web UI to preserve changes

"""
    code = ast.unparse(module)
    return header + code
