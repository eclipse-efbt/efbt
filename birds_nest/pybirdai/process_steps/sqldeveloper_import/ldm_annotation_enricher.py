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
"""Enrich generated Django LDM files with passive SQLDeveloper metadata.

The Django model remains executable Django code. The annotations written here are
plain class variables used by downstream tooling to recover SQLDeveloper source
facts such as composite key groups, relation optionality, and entity ids.
"""

from __future__ import annotations

import argparse
import ast
import csv
import sys
from pathlib import Path
from pprint import pformat
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pybirdai.process_steps.utils import Utils  # noqa: E402


def enrich_django_ldm_annotations(
    model_path: str | Path,
    resources_directory: str | Path,
    output_path: str | Path | None = None,
) -> dict[str, int]:
    """Add SQLDeveloper entity and relation metadata to a Django LDM file."""

    model_path = Path(model_path)
    output_path = Path(output_path) if output_path is not None else model_path
    resources_directory = Path(resources_directory)

    entity_metadata_by_class, class_name_by_entity_id = load_ldm_entity_metadata(resources_directory)
    relation_metadata_by_id = load_ldm_relation_metadata(resources_directory, class_name_by_entity_id)

    source = model_path.read_text(encoding="utf-8")
    source_lines = source.splitlines(keepends=True)
    line_separator = "\r\n" if source_lines and source_lines[0].endswith("\r\n") else "\n"
    tree = ast.parse(source, filename=str(model_path))

    edits: list[tuple[int, int, list[str]]] = []
    changed_class_count = 0
    enriched_foreign_key_count = 0

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue

        annotation_node = _annotation_assignment_node(node)
        annotations = _literal_annotation_value(annotation_node) if annotation_node is not None else {}
        updated_annotations, enriched_foreign_keys = _enriched_annotations_for_class(
            class_name=node.name,
            annotations=annotations,
            entity_metadata_by_class=entity_metadata_by_class,
            relation_metadata_by_id=relation_metadata_by_id,
        )
        if updated_annotations == annotations:
            continue

        changed_class_count += 1
        enriched_foreign_key_count += enriched_foreign_keys
        indent = _class_body_indent(node, annotation_node, source_lines)
        rendered_annotation = _render_annotation_assignment(updated_annotations, line_separator, indent)
        if annotation_node is not None:
            edits.append((annotation_node.lineno - 1, annotation_node.end_lineno, rendered_annotation))
        else:
            edits.append((node.lineno, node.lineno, rendered_annotation))

    for start_line, end_line, replacement_lines in reversed(edits):
        source_lines[start_line:end_line] = replacement_lines

    output_path.write_text("".join(source_lines), encoding="utf-8")
    return {
        "changed_class_count": changed_class_count,
        "enriched_foreign_key_count": enriched_foreign_key_count,
        "entity_metadata_available_count": len(entity_metadata_by_class),
        "relation_metadata_available_count": len(relation_metadata_by_id),
    }


def load_ldm_entity_metadata(resources_directory: str | Path) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Read entity metadata from SQLDeveloper DM_Entities.csv."""

    resources_directory = Path(resources_directory)
    classification_types = load_ldm_classification_types(resources_directory)
    entity_metadata_by_class: dict[str, dict[str, Any]] = {}
    class_name_by_entity_id: dict[str, str] = {}

    with _open_ldm_csv(resources_directory, "DM_Entities.csv") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            preferred_abbreviation = row.get("Preferred_Abbreviation", "")
            entity_id = row.get("ObjectID", "")
            if not preferred_abbreviation or not entity_id:
                continue

            class_name = Utils.make_valid_id(preferred_abbreviation)
            class_name_by_entity_id[entity_id] = class_name
            entity_metadata_by_class[class_name] = _without_empty_values(
                {
                    "entity_id": entity_id,
                    "entity_name": Utils.make_valid_id(row.get("Entity_Name", "")),
                    "classification_type": classification_types.get(row.get("Classification_Type", ""), ""),
                    "engineering_strategy": row.get("Engineering_Strategy", ""),
                    "supertype_entity_id": row.get("SuperTypeEntity_ID", ""),
                    "num_supertype_entity_id": _parse_int_or_none(row.get("Num_SuperTypeEntity_ID", "")),
                }
            )

    return entity_metadata_by_class, class_name_by_entity_id


def load_ldm_classification_types(resources_directory: str | Path) -> dict[str, str]:
    """Read classification type names from SQLDeveloper DM_Classification_Types.csv."""

    resources_directory = Path(resources_directory)
    classification_types: dict[str, str] = {}
    path = resources_directory / "ldm" / "DM_Classification_Types.csv"
    if not path.exists():
        return classification_types

    with path.open(encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            object_id = row.get("ObjectID", "") or row.get("Type_ID", "")
            name = row.get("Classification_Type_Name", "") or row.get("Type_Name", "")
            if object_id and name:
                classification_types[object_id] = name
    return classification_types


def load_ldm_relation_metadata(
    resources_directory: str | Path,
    class_name_by_entity_id: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Read relation endpoint and optionality metadata from SQLDeveloper DM_Relations.csv."""

    resources_directory = Path(resources_directory)
    relation_metadata: dict[str, dict[str, Any]] = {}
    with _open_ldm_csv(resources_directory, "DM_Relations.csv") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            relation_id = row.get("ObjectID", "")
            if not relation_id:
                continue

            source_id = row.get("Source_ID", "")
            target_id = row.get("Target_ID", "")
            source_to_target_cardinality = row.get("SourceTo_Target_Cardinality", "")
            target_to_source_cardinality = row.get("TargetTo_Source_Cardinality", "")
            relation_metadata[relation_id] = _without_empty_values(
                {
                    "relation_id": relation_id,
                    "relation_name": Utils.make_valid_id(row.get("Relation_Name", "")),
                    "source_entity": Utils.make_valid_id(row.get("Source_Entity_Name", "")),
                    "source_class": class_name_by_entity_id.get(source_id, ""),
                    "source_id": source_id,
                    "target_entity": Utils.make_valid_id(row.get("Target_Entity_Name", "")),
                    "target_class": class_name_by_entity_id.get(target_id, ""),
                    "target_id": target_id,
                    "identifying": row.get("Identifying", ""),
                    "number_of_attributes": _parse_int_or_none(row.get("Number_Of_Attributes", "")),
                    "source_to_target_cardinality": source_to_target_cardinality,
                    "target_to_source_cardinality": target_to_source_cardinality,
                    "source_optional": row.get("Source_Optional", ""),
                    "target_optional": row.get("Target_Optional", ""),
                    "one_to_one": (
                        source_to_target_cardinality.strip() == "1"
                        and target_to_source_cardinality.strip() == "1"
                    ),
                }
            )
    return relation_metadata


def _enriched_annotations_for_class(
    class_name: str,
    annotations: dict[str, Any],
    entity_metadata_by_class: dict[str, dict[str, Any]],
    relation_metadata_by_id: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], int]:
    sql_developer_annotations = dict(annotations.get("sql_developer", {}))
    changed = False

    entity_metadata = entity_metadata_by_class.get(class_name, {})
    for key, value in entity_metadata.items():
        if key not in sql_developer_annotations:
            sql_developer_annotations[key] = value
            changed = True

    enriched_foreign_key_count = 0
    foreign_keys = sql_developer_annotations.get("foreign_keys")
    if isinstance(foreign_keys, list):
        enriched_foreign_keys: list[Any] = []
        for foreign_key in foreign_keys:
            if not isinstance(foreign_key, dict):
                enriched_foreign_keys.append(foreign_key)
                continue
            enriched_foreign_key = dict(foreign_key)
            relation_metadata = relation_metadata_by_id.get(str(foreign_key.get("relation_id", "")), {})
            if relation_metadata:
                before = dict(enriched_foreign_key)
                for key, value in relation_metadata.items():
                    if key not in enriched_foreign_key:
                        enriched_foreign_key[key] = value
                for key, value in _referenced_relation_endpoint(relation_metadata, class_name).items():
                    if key not in enriched_foreign_key:
                        enriched_foreign_key[key] = value
                if enriched_foreign_key != before:
                    enriched_foreign_key_count += 1
                    changed = True
            enriched_foreign_keys.append(_without_empty_values(enriched_foreign_key))
        sql_developer_annotations["foreign_keys"] = enriched_foreign_keys

    if not changed:
        return annotations, 0

    updated_annotations = dict(annotations)
    updated_annotations["sql_developer"] = _without_empty_values(sql_developer_annotations)
    return updated_annotations, enriched_foreign_key_count


def _referenced_relation_endpoint(relation_metadata: dict[str, Any], class_name: str) -> dict[str, Any]:
    if relation_metadata.get("source_class") == class_name:
        return _without_empty_values(
            {
                "relation_side": "source",
                "referenced_entity": relation_metadata.get("target_entity", ""),
                "referenced_class": relation_metadata.get("target_class", ""),
                "referenced_id": relation_metadata.get("target_id", ""),
            }
        )
    if relation_metadata.get("target_class") == class_name:
        return _without_empty_values(
            {
                "relation_side": "target",
                "referenced_entity": relation_metadata.get("source_entity", ""),
                "referenced_class": relation_metadata.get("source_class", ""),
                "referenced_id": relation_metadata.get("source_id", ""),
            }
        )
    return {}


def _annotation_assignment_node(class_node: ast.ClassDef) -> ast.Assign | None:
    for statement in class_node.body:
        if not isinstance(statement, ast.Assign):
            continue
        if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
            continue
        if statement.targets[0].id == "__bird_annotations__":
            return statement
    return None


def _literal_annotation_value(annotation_node: ast.Assign | None) -> dict[str, Any]:
    if annotation_node is None:
        return {}
    try:
        value = ast.literal_eval(annotation_node.value)
    except (ValueError, SyntaxError):
        return {}
    return value if isinstance(value, dict) else {}


def _class_body_indent(
    class_node: ast.ClassDef,
    annotation_node: ast.Assign | None,
    source_lines: list[str],
) -> str:
    if annotation_node is not None:
        return _leading_whitespace(source_lines[annotation_node.lineno - 1])
    for statement in class_node.body:
        if hasattr(statement, "lineno"):
            return _leading_whitespace(source_lines[statement.lineno - 1])
    return "\t"


def _leading_whitespace(line: str) -> str:
    return line[: len(line) - len(line.lstrip(" \t"))]


def _render_annotation_assignment(annotations: dict[str, Any], line_separator: str, indent: str) -> list[str]:
    rendered = pformat(annotations, width=120, sort_dicts=False)
    lines = rendered.splitlines()
    return [
        (f"{indent}__bird_annotations__ = " if index == 0 else indent) + line + line_separator
        for index, line in enumerate(lines)
    ]


def _open_ldm_csv(resources_directory: Path, file_name: str):
    return (resources_directory / "ldm" / file_name).open(encoding="utf-8-sig", newline="")


def _without_empty_values(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if item is not None and item != ""
    }


def _parse_int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, type=Path, help="Django LDM model to enrich")
    parser.add_argument("--resources", required=True, type=Path, help="Directory containing the ldm CSV folder")
    parser.add_argument("--output", type=Path, help="Output path. Defaults to overwriting --model")
    args = parser.parse_args(argv)

    summary = enrich_django_ldm_annotations(args.model, args.resources, args.output)
    print(pformat(summary, width=120, sort_dicts=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
