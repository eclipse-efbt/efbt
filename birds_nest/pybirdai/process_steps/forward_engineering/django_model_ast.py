# coding=UTF-8
# Copyright (c) 2026 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
"""AST helpers for generated Django model files.

The SQLDeveloper import path writes very large Python files. Importing those
files just to inspect their model structure is brittle because it requires a
configured Django app. These helpers read the generated source with Python's AST
and preserve enough source text to emit a new generated model file.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class ModelStatement:
    """A source statement inside a generated model class."""

    name: str
    source: str
    kind: str
    line_number: int
    field_type: str | None = None
    related_model: str | None = None
    choices_name: str | None = None
    primary_key: bool = False


@dataclass
class ModelClass:
    """A parsed Django model class."""

    name: str
    bases: list[str]
    statements: list[ModelStatement] = field(default_factory=list)
    annotations: dict = field(default_factory=dict)
    meta_source: str | None = None
    line_number: int = 0

    @property
    def fields(self) -> dict[str, ModelStatement]:
        return {statement.name: statement for statement in self.statements if statement.kind == "field"}

    @property
    def choices(self) -> dict[str, ModelStatement]:
        return {statement.name: statement for statement in self.statements if statement.kind == "choice"}

    def is_root_model(self) -> bool:
        return self.bases in (["models.Model"], ["Model"])


@dataclass
class DjangoModelModule:
    """A parsed generated Django model module."""

    path: Path
    classes: dict[str, ModelClass]
    class_order: list[str]

    def iter_classes(self, names: Iterable[str] | None = None) -> Iterable[ModelClass]:
        if names is None:
            names = self.class_order
        for name in names:
            model_class = self.classes.get(name)
            if model_class is not None:
                yield model_class


def parse_django_model(path: str | Path) -> DjangoModelModule:
    """Parse a generated Django model module without importing Django."""

    model_path = Path(path)
    source = model_path.read_text(encoding="utf-8")
    source_lines = source.splitlines(keepends=True)
    tree = ast.parse(source, filename=str(model_path))

    classes: dict[str, ModelClass] = {}
    class_order: list[str] = []

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue

        model_class = ModelClass(
            name=node.name,
            bases=[_base_name(base) for base in node.bases],
            line_number=node.lineno,
        )

        for statement in node.body:
            parsed_annotations = _parse_class_annotations(statement)
            if parsed_annotations is not None:
                model_class.annotations.update(parsed_annotations)
                continue

            parsed_statement = _parse_class_statement(source_lines, statement)
            if parsed_statement is not None:
                model_class.statements.append(parsed_statement)
                continue

            if isinstance(statement, ast.ClassDef) and statement.name == "Meta":
                model_class.meta_source = _source_segment(source_lines, statement)

        classes[model_class.name] = model_class
        class_order.append(model_class.name)

    return DjangoModelModule(path=model_path, classes=classes, class_order=class_order)


def _parse_class_annotations(statement: ast.stmt) -> dict | None:
    if not isinstance(statement, ast.Assign):
        return None
    if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
        return None
    if statement.targets[0].id != "__bird_annotations__":
        return None
    if not isinstance(statement.value, ast.Dict):
        return {}
    try:
        parsed_value = ast.literal_eval(statement.value)
    except (ValueError, SyntaxError):
        return {}
    if not isinstance(parsed_value, dict):
        return {}
    return parsed_value


def _parse_class_statement(source: str | list[str], statement: ast.stmt) -> ModelStatement | None:
    if not isinstance(statement, ast.Assign):
        return None
    if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
        return None

    name = statement.targets[0].id
    if name == "__bird_annotations__":
        return None
    source_lines = source if isinstance(source, list) else source.splitlines(keepends=True)
    statement_source = _source_segment(source_lines, statement)
    if statement_source is None:
        return None

    if isinstance(statement.value, ast.Dict):
        return ModelStatement(
            name=name,
            source=statement_source,
            kind="choice",
            line_number=statement.lineno,
        )

    if not isinstance(statement.value, ast.Call):
        return None

    field_type = _model_field_type(statement.value.func)
    if field_type is None:
        return None

    return ModelStatement(
        name=name,
        source=statement_source,
        kind="field",
        line_number=statement.lineno,
        field_type=field_type,
        related_model=_foreign_key_related_model(statement.value),
        choices_name=_keyword_name(statement.value, "choices"),
        primary_key=_keyword_bool(statement.value, "primary_key"),
    )


def _source_segment(source_lines: list[str], node: ast.AST) -> str | None:
    if (
        not hasattr(node, "lineno")
        or not hasattr(node, "end_lineno")
        or not hasattr(node, "col_offset")
        or not hasattr(node, "end_col_offset")
    ):
        return None
    lines = source_lines[node.lineno - 1:node.end_lineno]
    if not lines:
        return None
    if len(lines) == 1:
        segment = lines[0][node.col_offset:node.end_col_offset]
    else:
        segment_lines = list(lines)
        segment_lines[0] = segment_lines[0][node.col_offset:]
        segment_lines[-1] = segment_lines[-1][:node.end_col_offset]
        segment = "".join(segment_lines)
    return segment.rstrip("\r\n")


def _base_name(base: ast.expr) -> str:
    if isinstance(base, ast.Name):
        return base.id
    if isinstance(base, ast.Attribute):
        parts = [base.attr]
        value = base.value
        while isinstance(value, ast.Attribute):
            parts.append(value.attr)
            value = value.value
        if isinstance(value, ast.Name):
            parts.append(value.id)
        return ".".join(reversed(parts))
    return ast.unparse(base)


def _model_field_type(func: ast.expr) -> str | None:
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id == "models":
        return func.attr
    return None


def _foreign_key_related_model(call: ast.Call) -> str | None:
    if _model_field_type(call.func) != "ForeignKey" or not call.args:
        return None
    first_arg = call.args[0]
    if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
        return first_arg.value
    if isinstance(first_arg, ast.Name):
        return first_arg.id
    return None


def _keyword_name(call: ast.Call, keyword_name: str) -> str | None:
    for keyword in call.keywords:
        if keyword.arg == keyword_name and isinstance(keyword.value, ast.Name):
            return keyword.value.id
    return None


def _keyword_bool(call: ast.Call, keyword_name: str) -> bool:
    for keyword in call.keywords:
        if keyword.arg == keyword_name and isinstance(keyword.value, ast.Constant):
            return bool(keyword.value.value)
    return False
