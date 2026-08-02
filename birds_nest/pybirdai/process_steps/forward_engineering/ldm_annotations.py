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
"""The ``__bird_annotations__["ldm"]`` contract shared by writers and readers.

``specs/BIRD_LDM_ANNOTATIONS_SPEC.md`` defines a hand- and tool-editable
annotation contract for the Django LDM classes. This module is the single place
that knows the contract:

* readers (forward engineering) use the accessors to get normalized values,
* writers (the SQLDeveloper importer and the annotation enricher) use
  :func:`canonical_annotations` so every authoring source emits the same shape,
* linters use :func:`validate_annotations`.

During the migration window the accessors still read the legacy
``sql_developer`` namespace and the legacy value spellings (``"Y"``/``"N"``
flags, ``value``/``label`` entity member keys). Writers never emit them.
"""

from __future__ import annotations

import ast
from pprint import pformat
from typing import Any, Callable, Iterable

LDM_NAMESPACE = "ldm"
LEGACY_NAMESPACE = "sql_developer"

#: Keys of the ``ldm`` section that are part of the contract.
SECTION_KEYS = (
    "primary_key",
    "primary_key_fields",
    "foreign_keys",
    "fields",
    "entity_member",
    "attribute_inheritance_type",
)

#: Contract keys of a ``foreign_keys[]`` entry.
FOREIGN_KEY_KEYS = (
    "relation_name",
    "identifying",
    "relation_side",
    "source_class",
    "target_class",
    "referenced_class",
    "source_entity",
    "referenced_entity",
    "fields",
    "field_entries",
    "number_of_attributes",
    "one_to_one",
    "source_optional",
)

#: Contract keys of a ``foreign_keys[].field_entries[]`` entry.
FOREIGN_KEY_FIELD_ENTRY_KEYS = ("field", "sequence", "primary_key")

#: Contract keys of a ``primary_key_fields[]`` entry.
PRIMARY_KEY_FIELD_ENTRY_KEYS = ("field", "sequence")

#: Contract keys of a ``fields.<FIELD_NAME>`` entry.
FIELD_KEYS = (
    "domain_synonym",
    "domain_field_name",
    "domain_name",
    "domain_id",
    "primary_key",
    "foreign_key",
    "add_not_applicable_candidate",
    "hierarchy_sibling_missing_field",
)

#: Contract keys of the ``entity_member`` object.
ENTITY_MEMBER_KEYS = (
    "discriminator_field",
    "domain_synonym",
    "domain_name",
    "member_code",
    "member_label",
)

#: Foreign key flags written as booleans, read leniently while migrating.
FOREIGN_KEY_FLAG_KEYS = ("identifying", "one_to_one", "source_optional")

#: Field flags written as booleans, read leniently while migrating.
FIELD_FLAG_KEYS = (
    "primary_key",
    "foreign_key",
    "add_not_applicable_candidate",
    "hierarchy_sibling_missing_field",
)

#: Legacy ``entity_member`` spellings accepted on read only.
LEGACY_MEMBER_CODE_KEYS = ("member_code", "value")
LEGACY_MEMBER_LABEL_KEYS = (
    "member_label",
    "label",
    "member_description",
    "source_member_description",
)

#: Legacy spellings of ``attribute_inheritance_type`` accepted on read only.
LEGACY_INHERITANCE_TYPE_KEYS = (
    "attribute_inheritance_type",
    "attribute_inher_type",
    "attribute_inheritance",
    "inheritance_type",
)

_INHERITANCE_TYPES_IGNORING_ATTRIBUTES = frozenset({"all atributes", "all attributes"})
_TRUE_STRINGS = frozenset({"y", "yes", "true", "1"})
_RELATION_SIDES = frozenset({"source", "target"})
_LARGE_SEQUENCE = 10**9


def annotations_of(model_class: Any) -> dict:
    """Return the raw ``__bird_annotations__`` dict of a parsed model class."""

    if isinstance(model_class, dict):
        return model_class
    annotations = getattr(model_class, "annotations", None)
    return annotations if isinstance(annotations, dict) else {}


def ldm_section(model_class: Any) -> dict:
    """Return ``__bird_annotations__["ldm"]``.

    Falls back to the legacy ``sql_developer`` namespace for annotations that
    have not been migrated yet.
    """

    annotations = annotations_of(model_class)
    for namespace in (LDM_NAMESPACE, LEGACY_NAMESPACE):
        section = annotations.get(namespace)
        if isinstance(section, dict) and section:
            return section
    return {}


def primary_key(model_class: Any) -> list[str]:
    """Return the ``primary_key`` field names, unordered by contract."""

    names = ldm_section(model_class).get("primary_key", [])
    return names if isinstance(names, list) else []


def ordered_primary_key_fields(model_class: Any) -> list[str]:
    """Return the primary key field names in ``sequence`` order.

    ``primary_key_fields`` wins when present because it carries the component
    order that composite foreign keys are mapped onto.
    """

    entries = ldm_section(model_class).get("primary_key_fields", [])
    if isinstance(entries, list) and entries:
        return _ordered_field_entries(entries)
    return [name for name in primary_key(model_class) if isinstance(name, str)]


def foreign_keys(model_class: Any) -> list[dict]:
    """Return the ``foreign_keys`` entries of a class."""

    entries = ldm_section(model_class).get("foreign_keys", [])
    return entries if isinstance(entries, list) else []


def ordered_foreign_key_fields(foreign_key: dict) -> list[str]:
    """Return one foreign key's component field names in ``sequence`` order."""

    if not isinstance(foreign_key, dict):
        return []
    field_entries = foreign_key.get("field_entries", [])
    if isinstance(field_entries, list) and field_entries:
        return _ordered_field_entries(field_entries)
    fields = foreign_key.get("fields", [])
    return [name for name in fields if isinstance(name, str)] if isinstance(fields, list) else []


def field_annotations(model_class: Any, field_name: str) -> dict:
    """Return the ``fields`` entry for one Django field name."""

    fields = ldm_section(model_class).get("fields", {})
    if not isinstance(fields, dict):
        return {}
    entry = fields.get(field_name, {})
    return entry if isinstance(entry, dict) else {}


def entity_member(model_class: Any) -> dict:
    """Return the ``entity_member`` discriminator membership of a subtype."""

    member = ldm_section(model_class).get("entity_member", {})
    return member if isinstance(member, dict) else {}


def entity_member_code_and_label(member: dict) -> tuple[str, str] | None:
    """Return the ``(member_code, member_label)`` pair of an entity member."""

    if not isinstance(member, dict):
        return None
    code = _first_present(member, LEGACY_MEMBER_CODE_KEYS)
    label = _first_present(member, LEGACY_MEMBER_LABEL_KEYS)
    if code is None or label is None:
        return None
    return str(code), str(label)


def flag(source: dict, *keys: str) -> bool:
    """Return the first flag found under ``keys`` as a boolean.

    Booleans are the contract. ``"Y"``/``"N"`` strings and ``1``/``0`` integers
    are still accepted so unmigrated annotations keep working.
    """

    if not isinstance(source, dict):
        return False
    for key in keys:
        if key not in source:
            continue
        value = source[key]
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in _TRUE_STRINGS
        if isinstance(value, int):
            return value == 1
    return False


def is_identifying(foreign_key: dict) -> bool:
    """Return whether a foreign key annotation describes an identifying relation."""

    return flag(foreign_key, "identifying")


def ignores_attribute_inheritance(model_class: Any) -> bool:
    """Return whether the class opts out of ``not applicable`` inheritance rules."""

    section = ldm_section(model_class)
    inheritance_type = _first_present(section, LEGACY_INHERITANCE_TYPE_KEYS)
    if inheritance_type is None:
        return False
    return str(inheritance_type).strip().lower() in _INHERITANCE_TYPES_IGNORING_ATTRIBUTES


def canonical_annotations(annotations: dict) -> dict:
    """Return ``annotations`` reduced to the canonical ``{"ldm": ...}`` form.

    Non-contract keys are dropped, legacy flag spellings become booleans, and
    the legacy ``sql_developer`` namespace is folded into ``ldm``. Annotations
    that carry nothing in contract come back empty so callers can omit the
    ``__bird_annotations__`` assignment entirely.
    """

    if not isinstance(annotations, dict):
        return {}
    merged: dict[str, Any] = {}
    for namespace in (LEGACY_NAMESPACE, LDM_NAMESPACE):
        section = annotations.get(namespace)
        if isinstance(section, dict):
            merged.update(section)
    section = canonical_section(merged)
    return {LDM_NAMESPACE: section} if section else {}


def canonical_section(section: dict) -> dict:
    """Return one ``ldm`` section reduced to the canonical contract."""

    if not isinstance(section, dict):
        return {}

    canonical: dict[str, Any] = {}

    names = [name for name in section.get("primary_key", []) or [] if isinstance(name, str)]
    if names:
        canonical["primary_key"] = names

    primary_key_fields = _canonical_key_entries(
        section.get("primary_key_fields"),
        PRIMARY_KEY_FIELD_ENTRY_KEYS,
        flag_keys=(),
    )
    if primary_key_fields:
        canonical["primary_key_fields"] = primary_key_fields

    canonical_foreign_keys = [
        entry
        for entry in (
            _canonical_foreign_key(foreign_key)
            for foreign_key in section.get("foreign_keys", []) or []
        )
        if entry
    ]
    if canonical_foreign_keys:
        canonical["foreign_keys"] = canonical_foreign_keys

    fields = section.get("fields")
    if isinstance(fields, dict):
        canonical_fields = {
            field_name: canonical_field
            for field_name, field_metadata in fields.items()
            if (canonical_field := _canonical_field(field_metadata))
        }
        if canonical_fields:
            canonical["fields"] = canonical_fields

    member = _canonical_entity_member(section.get("entity_member"))
    if member:
        canonical["entity_member"] = member

    inheritance_type = _first_present(section, LEGACY_INHERITANCE_TYPE_KEYS)
    if inheritance_type not in (None, ""):
        canonical["attribute_inheritance_type"] = str(inheritance_type)

    return canonical


def validate_annotations(annotations: dict, class_name: str = "") -> list[str]:
    """Return contract violations found in one class's annotations.

    The checks follow the validation checklist of the specification. Django
    field consistency is not checked here because the contract allows logical
    key components that FE resolves by name.
    """

    prefix = f"{class_name}: " if class_name else ""
    issues: list[str] = []
    if not isinstance(annotations, dict) or not annotations:
        return issues

    if LEGACY_NAMESPACE in annotations:
        issues.append(f"{prefix}legacy '{LEGACY_NAMESPACE}' namespace is not part of the contract")
    section = annotations.get(LDM_NAMESPACE)
    if section is None:
        issues.append(f"{prefix}__bird_annotations__ has no '{LDM_NAMESPACE}' namespace")
        return issues
    if not isinstance(section, dict):
        issues.append(f"{prefix}'{LDM_NAMESPACE}' must be a dict")
        return issues

    for key in sorted(set(section) - set(SECTION_KEYS)):
        issues.append(f"{prefix}'{key}' is not a contract key of '{LDM_NAMESPACE}'")

    primary_key_names = _as_list(section.get("primary_key"), "primary_key", prefix, issues)
    primary_key_entries = _as_list(section.get("primary_key_fields"), "primary_key_fields", prefix, issues)
    if primary_key_names and primary_key_entries:
        entry_names = [
            entry.get("field")
            for entry in primary_key_entries
            if isinstance(entry, dict)
        ]
        if set(entry_names) != set(primary_key_names):
            issues.append(f"{prefix}'primary_key' and 'primary_key_fields' name different fields")
    for entry in primary_key_entries:
        issues.extend(
            _entry_issues(entry, PRIMARY_KEY_FIELD_ENTRY_KEYS, "primary_key_fields[]", prefix)
        )

    for foreign_key in _as_list(section.get("foreign_keys"), "foreign_keys", prefix, issues):
        if not isinstance(foreign_key, dict):
            issues.append(f"{prefix}'foreign_keys[]' entries must be dicts")
            continue
        for key in sorted(set(foreign_key) - set(FOREIGN_KEY_KEYS)):
            issues.append(f"{prefix}'{key}' is not a contract key of 'foreign_keys[]'")
        for key in FOREIGN_KEY_FLAG_KEYS:
            if key in foreign_key and not isinstance(foreign_key[key], bool):
                issues.append(f"{prefix}'foreign_keys[].{key}' should be a boolean")
        relation_side = foreign_key.get("relation_side")
        if relation_side is not None and relation_side not in _RELATION_SIDES:
            issues.append(f"{prefix}'foreign_keys[].relation_side' must be 'source' or 'target'")
        field_entries = foreign_key.get("field_entries")
        if isinstance(field_entries, list) and field_entries:
            entry_names = {
                entry.get("field") for entry in field_entries if isinstance(entry, dict)
            }
            for field_name in _as_list(foreign_key.get("fields"), "foreign_keys[].fields", prefix, issues):
                if field_name not in entry_names:
                    issues.append(
                        f"{prefix}'foreign_keys[].fields' entry '{field_name}' is missing from 'field_entries'"
                    )
            for entry in field_entries:
                issues.extend(
                    _entry_issues(entry, FOREIGN_KEY_FIELD_ENTRY_KEYS, "field_entries[]", prefix)
                )

    fields = section.get("fields", {})
    if isinstance(fields, dict):
        for field_name, field_metadata in fields.items():
            if not isinstance(field_metadata, dict):
                issues.append(f"{prefix}'fields.{field_name}' must be a dict")
                continue
            for key in sorted(set(field_metadata) - set(FIELD_KEYS)):
                issues.append(f"{prefix}'{key}' is not a contract key of 'fields.{field_name}'")
            for key in FIELD_FLAG_KEYS:
                if key in field_metadata and not isinstance(field_metadata[key], bool):
                    issues.append(f"{prefix}'fields.{field_name}.{key}' should be a boolean")

    member = section.get("entity_member")
    if isinstance(member, dict):
        for key in sorted(set(member) - set(ENTITY_MEMBER_KEYS)):
            issues.append(f"{prefix}'{key}' is not a contract key of 'entity_member'")
        if entity_member_code_and_label(member) is None:
            issues.append(f"{prefix}'entity_member' needs both 'member_code' and 'member_label'")
        if not any(member.get(key) for key in ("discriminator_field", "domain_synonym", "domain_name")):
            issues.append(f"{prefix}'entity_member' needs a discriminator identity")
    elif member is not None:
        issues.append(f"{prefix}'entity_member' must be a dict")

    return issues


def _as_list(value: Any, location: str, prefix: str, issues: list[str]) -> list:
    if value is None:
        return []
    if not isinstance(value, list):
        issues.append(f"{prefix}'{location}' must be a list")
        return []
    return value


def rewrite_annotations(
    source: str,
    transform: Callable[[str, dict], dict],
) -> tuple[str, int]:
    """Rewrite the ``__bird_annotations__`` of every class in a model source.

    ``transform`` is called with the class name and its current annotations and
    returns the annotations to write. Returning an empty dict removes the
    assignment; returning the annotations unchanged leaves the source untouched,
    so unrelated formatting in the file is preserved.
    """

    source_lines = source.splitlines(keepends=True)
    line_separator = "\r\n" if source_lines and source_lines[0].endswith("\r\n") else "\n"
    tree = ast.parse(source)

    edits: list[tuple[int, int, list[str]]] = []
    changed_class_count = 0

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue

        annotation_node = _annotation_assignment_node(node)
        annotations = _literal_annotation_value(annotation_node)
        updated_annotations = transform(node.name, annotations)
        if updated_annotations == annotations:
            continue

        changed_class_count += 1
        indent = _class_body_indent(node, annotation_node, source_lines)
        rendered_annotation = (
            _render_annotation_assignment(updated_annotations, line_separator, indent)
            if updated_annotations
            else []
        )
        if annotation_node is not None:
            edits.append((annotation_node.lineno - 1, annotation_node.end_lineno, rendered_annotation))
        else:
            edits.append((node.lineno, node.lineno, rendered_annotation))

    for start_line, end_line, replacement_lines in reversed(edits):
        source_lines[start_line:end_line] = replacement_lines

    return "".join(source_lines), changed_class_count


def _annotation_assignment_node(class_node: ast.ClassDef) -> ast.Assign | None:
    for statement in class_node.body:
        if not isinstance(statement, ast.Assign):
            continue
        if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
            continue
        if statement.targets[0].id == "__bird_annotations__":
            return statement
    return None


def _literal_annotation_value(annotation_node: ast.Assign | None) -> dict:
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


def _render_annotation_assignment(annotations: dict, line_separator: str, indent: str) -> list[str]:
    assignment = "__bird_annotations__ = "
    # pformat aligns as if the value started at column 0, so every wrapped line
    # is shifted by the same amount to keep annotations readable and editable.
    continuation_indent = indent + " " * len(assignment)
    rendered = pformat(annotations, width=max(60, 120 - len(continuation_indent)), sort_dicts=False)
    return [
        (indent + assignment if index == 0 else continuation_indent) + line + line_separator
        for index, line in enumerate(rendered.splitlines())
    ]


def _canonical_foreign_key(foreign_key: Any) -> dict:
    if not isinstance(foreign_key, dict):
        return {}
    canonical = _canonical_keys(foreign_key, FOREIGN_KEY_KEYS, FOREIGN_KEY_FLAG_KEYS)
    fields = [name for name in foreign_key.get("fields", []) or [] if isinstance(name, str)]
    if fields:
        canonical["fields"] = fields
    else:
        canonical.pop("fields", None)
    field_entries = _canonical_key_entries(
        foreign_key.get("field_entries"),
        FOREIGN_KEY_FIELD_ENTRY_KEYS,
        flag_keys=("primary_key",),
    )
    if field_entries:
        canonical["field_entries"] = field_entries
    else:
        canonical.pop("field_entries", None)
    return canonical


def _canonical_field(field_metadata: Any) -> dict:
    if not isinstance(field_metadata, dict):
        return {}
    return _canonical_keys(field_metadata, FIELD_KEYS, FIELD_FLAG_KEYS)


def _canonical_entity_member(member: Any) -> dict:
    if not isinstance(member, dict):
        return {}
    canonical = _canonical_keys(member, ENTITY_MEMBER_KEYS, flag_keys=())
    code_and_label = entity_member_code_and_label(member)
    if code_and_label is None:
        return {}
    canonical["member_code"], canonical["member_label"] = code_and_label
    return canonical


def _canonical_key_entries(
    entries: Any,
    contract_keys: Iterable[str],
    flag_keys: Iterable[str],
) -> list[dict]:
    if not isinstance(entries, list):
        return []
    canonical_entries = []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("field"), str):
            continue
        canonical_entries.append(_canonical_keys(entry, contract_keys, flag_keys))
    return canonical_entries


def _canonical_keys(
    source: dict,
    contract_keys: Iterable[str],
    flag_keys: Iterable[str],
) -> dict:
    flag_key_set = set(flag_keys)
    canonical: dict[str, Any] = {}
    for key in contract_keys:
        if key not in source:
            continue
        value = source[key]
        if key in flag_key_set:
            canonical[key] = flag(source, key)
        elif value not in (None, ""):
            canonical[key] = value
    return canonical


def _entry_issues(entry: Any, contract_keys: Iterable[str], location: str, prefix: str) -> list[str]:
    if not isinstance(entry, dict):
        return [f"{prefix}'{location}' entries must be dicts"]
    issues = [
        f"{prefix}'{key}' is not a contract key of '{location}'"
        for key in sorted(set(entry) - set(contract_keys))
    ]
    if not isinstance(entry.get("field"), str):
        issues.append(f"{prefix}'{location}' entries need a 'field' name")
    return issues


def _ordered_field_entries(entries: Any) -> list[str]:
    if not isinstance(entries, list):
        return []
    ordered = sorted(
        (
            entry
            for entry in entries
            if isinstance(entry, dict) and isinstance(entry.get("field"), str)
        ),
        key=lambda entry: (
            entry.get("sequence") if isinstance(entry.get("sequence"), int) else _LARGE_SEQUENCE,
            entry.get("field", ""),
        ),
    )
    return [entry["field"] for entry in ordered]


def _first_present(source: dict, keys: Iterable[str]) -> Any:
    if not isinstance(source, dict):
        return None
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None
