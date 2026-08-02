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
plain class variables used by downstream tooling to recover LDM source facts such
as composite key groups, relation optionality, and subtype discriminators.

Everything written lands under ``__bird_annotations__["ldm"]`` in the shape
defined by ``specs/BIRD_LDM_ANNOTATIONS_SPEC.md``. SQLDeveloper metadata that the
contract does not cover is read here but never written.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from pprint import pformat
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pybirdai.process_steps.forward_engineering import ldm_annotations  # noqa: E402
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
    relation_metadata_by_name = _relation_metadata_by_name(relation_metadata_by_id)
    field_metadata_by_class, entity_member_metadata_by_class = load_ldm_forward_engineering_metadata(
        resources_directory,
        class_name_by_entity_id,
    )

    enriched_foreign_key_count = 0

    def enrich_class(class_name: str, annotations: dict[str, Any]) -> dict[str, Any]:
        nonlocal enriched_foreign_key_count
        updated_annotations, enriched_foreign_keys = _enriched_annotations_for_class(
            class_name=class_name,
            annotations=annotations,
            relation_metadata_by_id=relation_metadata_by_id,
            relation_metadata_by_name=relation_metadata_by_name,
            field_metadata_by_class=field_metadata_by_class,
            entity_member_metadata_by_class=entity_member_metadata_by_class,
        )
        enriched_foreign_key_count += enriched_foreign_keys
        return updated_annotations

    enriched_source, changed_class_count = ldm_annotations.rewrite_annotations(
        model_path.read_text(encoding="utf-8"),
        enrich_class,
    )

    output_path.write_text(enriched_source, encoding="utf-8")
    return {
        "changed_class_count": changed_class_count,
        "enriched_foreign_key_count": enriched_foreign_key_count,
        "entity_metadata_available_count": len(entity_metadata_by_class),
        "relation_metadata_available_count": len(relation_metadata_by_id),
        "field_metadata_available_count": sum(len(fields) for fields in field_metadata_by_class.values()),
        "entity_member_metadata_available_count": len(entity_member_metadata_by_class),
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


def load_ldm_forward_engineering_metadata(
    resources_directory: str | Path,
    class_name_by_entity_id: dict[str, str],
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, Any]]]:
    """Read source metadata needed to reproduce SQLDeveloper forward engineering."""

    resources_directory = Path(resources_directory)
    entity_rows_by_id = load_ldm_entity_rows(resources_directory)
    domain_metadata_by_id = load_ldm_domain_metadata(resources_directory)
    field_metadata_by_class, attributes_by_entity_id = load_ldm_field_metadata(
        resources_directory,
        class_name_by_entity_id,
        domain_metadata_by_id,
        entity_rows_by_id,
    )
    entity_member_metadata_by_class = load_ldm_entity_member_metadata(
        resources_directory,
        class_name_by_entity_id,
        entity_rows_by_id,
        attributes_by_entity_id,
        domain_metadata_by_id,
    )
    return field_metadata_by_class, entity_member_metadata_by_class


def load_ldm_entity_rows(resources_directory: str | Path) -> dict[str, dict[str, Any]]:
    """Read raw entity rows keyed by SQLDeveloper entity id."""

    resources_directory = Path(resources_directory)
    entity_rows_by_id: dict[str, dict[str, Any]] = {}
    with _open_ldm_csv(resources_directory, "DM_Entities.csv") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            entity_id = row.get("ObjectID", "")
            if not entity_id:
                continue
            entity_rows_by_id[entity_id] = {
                "entity_id": entity_id,
                "entity_name": row.get("Entity_Name", ""),
                "class_name": Utils.make_valid_id(row.get("Preferred_Abbreviation", "")),
                "supertype_entity_id": row.get("SuperTypeEntity_ID", ""),
            }
    return entity_rows_by_id


def load_ldm_domain_metadata(resources_directory: str | Path) -> dict[str, dict[str, Any]]:
    """Read domain names, synonyms, and member values from SQLDeveloper CSVs."""

    resources_directory = Path(resources_directory)
    domain_metadata_by_id: dict[str, dict[str, Any]] = {}
    domains_path = resources_directory / "ldm" / "DM_Domains.csv"
    if not domains_path.exists():
        return domain_metadata_by_id

    with domains_path.open(encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            domain_id = row.get("Domain_ID", "")
            if not domain_id:
                continue
            domain_metadata_by_id[domain_id] = _without_empty_values(
                {
                    "domain_id": domain_id,
                    "domain_name": row.get("Domain_Name", ""),
                    "domain_synonym": row.get("Synonyms", ""),
                    "domain_field_name": (
                        Utils.make_valid_id(row.get("Synonyms", "")) + "_domain"
                        if row.get("Synonyms", "")
                        else ""
                    ),
                    "members": {},
                }
            )

    domain_avt_path = resources_directory / "ldm" / "DM_Domain_AVT.csv"
    if not domain_avt_path.exists():
        return domain_metadata_by_id

    with domain_avt_path.open(encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            domain_id = row.get("Domain_ID", "")
            value = row.get("Value", "")
            if not domain_id or value == "":
                continue
            domain_metadata = domain_metadata_by_id.setdefault(
                domain_id,
                {"domain_id": domain_id, "members": {}},
            )
            members = domain_metadata.setdefault("members", {})
            members[value] = {
                "value": value,
                "label": Utils.make_valid_id(row.get("Short_Description", "")),
                "description": row.get("Short_Description", ""),
                "sequence": _parse_int_or_none(row.get("Sequence", "")),
            }

    return domain_metadata_by_id


def load_ldm_field_metadata(
    resources_directory: str | Path,
    class_name_by_entity_id: dict[str, str],
    domain_metadata_by_id: dict[str, dict[str, Any]],
    entity_rows_by_id: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    """Read per-field source domain metadata and SQLDeveloper 0-add candidates."""

    resources_directory = Path(resources_directory)
    attributes_by_entity_id: dict[str, list[dict[str, Any]]] = {}
    attributes_path = resources_directory / "ldm" / "DM_Attributes.csv"
    if not attributes_path.exists():
        return {}, attributes_by_entity_id

    with attributes_path.open(encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            entity_id = row.get("ContainerID", "")
            field_name = Utils.make_valid_id(row.get("Preferred_Abbreviation", "") or row.get("Attribute_Name", ""))
            if not entity_id or not field_name:
                continue
            domain_id = row.get("Domain_ID", "")
            domain_metadata = domain_metadata_by_id.get(domain_id, {})
            attribute_metadata = _without_empty_values(
                {
                    "attribute_id": row.get("ObjectID", ""),
                    "attribute_name": row.get("Attribute_Name", ""),
                    "field_name": field_name,
                    "domain_id": domain_id,
                    "domain_name": row.get("Domain_Name", "") or domain_metadata.get("domain_name", ""),
                    "domain_synonym": domain_metadata.get("domain_synonym", ""),
                    "domain_field_name": domain_metadata.get("domain_field_name", ""),
                    "relation_id": row.get("Relation_ID", ""),
                    "relation_name": Utils.make_valid_id(row.get("Relation_Name", "")),
                    "primary_key": row.get("PK_Flag", "").strip().upper() == "P",
                    "foreign_key": row.get("FK_Flag", "").strip().upper() == "F",
                    "sequence": _parse_int_or_none(row.get("Sequence", "")),
                }
            )
            not_applicable_member = domain_metadata.get("members", {}).get("0", {})
            attribute_metadata["not_applicable_present"] = bool(not_applicable_member)
            if not_applicable_member:
                attribute_metadata["not_applicable_label"] = not_applicable_member.get("label", "")
                attribute_metadata["not_applicable_description"] = not_applicable_member.get("description", "")
            attributes_by_entity_id.setdefault(entity_id, []).append(attribute_metadata)

    child_entity_ids_by_parent_id: dict[str, list[str]] = {}
    for entity_id, entity_row in entity_rows_by_id.items():
        parent_id = entity_row.get("supertype_entity_id", "")
        if parent_id:
            child_entity_ids_by_parent_id.setdefault(parent_id, []).append(entity_id)

    raw_attribute_names_by_entity_id = {
        entity_id: {str(attribute.get("attribute_name", "")) for attribute in attributes}
        for entity_id, attributes in attributes_by_entity_id.items()
    }

    field_metadata_by_class: dict[str, dict[str, dict[str, Any]]] = {}
    for entity_id, attributes in attributes_by_entity_id.items():
        class_name = class_name_by_entity_id.get(entity_id)
        if not class_name:
            continue
        for attribute_metadata in attributes:
            hierarchy_sibling_missing_field = _hierarchy_sibling_lacks_raw_attribute(
                entity_id=entity_id,
                attribute_name=str(attribute_metadata.get("attribute_name", "")),
                entity_rows_by_id=entity_rows_by_id,
                child_entity_ids_by_parent_id=child_entity_ids_by_parent_id,
                raw_attribute_names_by_entity_id=raw_attribute_names_by_entity_id,
            )
            attribute_metadata["hierarchy_sibling_missing_field"] = hierarchy_sibling_missing_field
            attribute_metadata["add_not_applicable_candidate"] = (
                hierarchy_sibling_missing_field and not attribute_metadata.get("not_applicable_present", False)
            )
            field_name = str(attribute_metadata["field_name"])
            field_metadata_by_class.setdefault(class_name, {})[field_name] = _without_empty_values(attribute_metadata)

    return field_metadata_by_class, attributes_by_entity_id


def load_ldm_entity_member_metadata(
    resources_directory: str | Path,
    class_name_by_entity_id: dict[str, str],
    entity_rows_by_id: dict[str, dict[str, Any]],
    attributes_by_entity_id: dict[str, list[dict[str, Any]]],
    domain_metadata_by_id: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Recreate SQLDeveloper's dynamic entity_member_map from logical CSVs."""

    arcs_by_entity_name = load_ldm_arcs_by_entity_name(resources_directory)
    entity_member_metadata_by_class: dict[str, dict[str, Any]] = {}
    for entity_id, entity_row in entity_rows_by_id.items():
        parent_id = entity_row.get("supertype_entity_id", "")
        class_name = class_name_by_entity_id.get(entity_id)
        if not parent_id or not class_name:
            continue
        parent_row = entity_rows_by_id.get(parent_id, {})
        parent_entity_name = str(parent_row.get("entity_name", ""))
        discriminator_names = set(arcs_by_entity_name.get(parent_entity_name, []))
        if not discriminator_names:
            discriminator_names = {
                str(attribute.get("attribute_name", ""))
                for attribute in attributes_by_entity_id.get(parent_id, [])
                if " type" in str(attribute.get("attribute_name", ""))
            }
        for attribute in attributes_by_entity_id.get(parent_id, []):
            if str(attribute.get("attribute_name", "")) not in discriminator_names:
                continue
            domain_metadata = domain_metadata_by_id.get(str(attribute.get("domain_id", "")), {})
            matched_member = _matching_entity_member(
                entity_name=str(entity_row.get("entity_name", "")),
                domain_name=str(domain_metadata.get("domain_name", "")),
                members=domain_metadata.get("members", {}),
            )
            if matched_member is None:
                continue
            member_name, member = matched_member
            entity_member_metadata_by_class[class_name] = _without_empty_values(
                {
                    "entity_name": entity_row.get("entity_name", ""),
                    "discriminator_attribute": attribute.get("attribute_name", ""),
                    "discriminator_field": attribute.get("field_name", ""),
                    "domain_id": domain_metadata.get("domain_id", ""),
                    "domain_name": domain_metadata.get("domain_name", ""),
                    "domain_synonym": domain_metadata.get("domain_synonym", ""),
                    "member_code": member.get("value", ""),
                    "member_label": Utils.make_valid_id(member_name),
                    "member_description": member_name,
                    "source_member_description": member.get("description", ""),
                }
            )
            break
    return entity_member_metadata_by_class


def load_ldm_arcs_by_entity_name(resources_directory: str | Path) -> dict[str, list[str]]:
    resources_directory = Path(resources_directory)
    arcs_path = resources_directory / "ldm" / "arcs.csv"
    if not arcs_path.exists():
        return {}
    arcs_by_entity_name: dict[str, list[str]] = {}
    with arcs_path.open(encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in reader:
            entity_name = row.get("entity_name", "")
            arc_name = row.get("arc_name", "")
            if entity_name and arc_name:
                arcs_by_entity_name.setdefault(entity_name, []).append(arc_name)
    return arcs_by_entity_name


def _matching_entity_member(
    entity_name: str,
    domain_name: str,
    members: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any]] | None:
    for member in members.values():
        description = str(member.get("description", ""))
        if domain_name != "Forbearance measure type":
            member_name = description.split(":", 1)[0]
        else:
            member_name = description
        if member_name == entity_name or member_name == entity_name + "s":
            return member_name, member
    return None


def _hierarchy_sibling_lacks_raw_attribute(
    entity_id: str,
    attribute_name: str,
    entity_rows_by_id: dict[str, dict[str, Any]],
    child_entity_ids_by_parent_id: dict[str, list[str]],
    raw_attribute_names_by_entity_id: dict[str, set[str]],
) -> bool:
    current_entity_id = entity_id
    while current_entity_id in entity_rows_by_id:
        parent_id = str(entity_rows_by_id[current_entity_id].get("supertype_entity_id", ""))
        if not parent_id:
            return False
        for sibling_entity_id in child_entity_ids_by_parent_id.get(parent_id, []):
            if attribute_name not in raw_attribute_names_by_entity_id.get(sibling_entity_id, set()):
                return True
        current_entity_id = parent_id
    return False


def _enriched_annotations_for_class(
    class_name: str,
    annotations: dict[str, Any],
    relation_metadata_by_id: dict[str, dict[str, Any]],
    relation_metadata_by_name: dict[str, dict[str, Any]],
    field_metadata_by_class: dict[str, dict[str, dict[str, Any]]],
    entity_member_metadata_by_class: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], int]:
    """Add LDM source metadata to one class and return canonical annotations."""

    section = _merged_ldm_section(annotations)

    entity_member_metadata = entity_member_metadata_by_class.get(class_name)
    if entity_member_metadata and "entity_member" not in section:
        section["entity_member"] = entity_member_metadata

    fields_metadata = field_metadata_by_class.get(class_name, {})
    if fields_metadata:
        existing_fields = section.get("fields", {})
        if not isinstance(existing_fields, dict):
            existing_fields = {}
        enriched_fields = dict(existing_fields)
        for field_name, field_metadata in fields_metadata.items():
            existing_field_metadata = enriched_fields.get(field_name, {})
            if not isinstance(existing_field_metadata, dict):
                existing_field_metadata = {}
            enriched_field_metadata = dict(existing_field_metadata)
            for key, value in field_metadata.items():
                enriched_field_metadata.setdefault(key, value)
            enriched_fields[field_name] = enriched_field_metadata
        section["fields"] = enriched_fields

    enriched_foreign_key_count = 0
    foreign_keys = section.get("foreign_keys")
    if isinstance(foreign_keys, list):
        enriched_foreign_keys: list[Any] = []
        for foreign_key in foreign_keys:
            if not isinstance(foreign_key, dict):
                enriched_foreign_keys.append(foreign_key)
                continue
            enriched_foreign_key = dict(foreign_key)
            relation_metadata = _relation_metadata_for_foreign_key(
                foreign_key,
                relation_metadata_by_id,
                relation_metadata_by_name,
            )
            if relation_metadata:
                before = dict(enriched_foreign_key)
                for key, value in relation_metadata.items():
                    enriched_foreign_key.setdefault(key, value)
                for key, value in _referenced_relation_endpoint(relation_metadata, class_name).items():
                    enriched_foreign_key.setdefault(key, value)
                if enriched_foreign_key != before:
                    enriched_foreign_key_count += 1
            enriched_foreign_keys.append(enriched_foreign_key)
        section["foreign_keys"] = enriched_foreign_keys

    updated_annotations = {
        key: value
        for key, value in annotations.items()
        if key not in (ldm_annotations.LDM_NAMESPACE, ldm_annotations.LEGACY_NAMESPACE)
    }
    updated_annotations.update(ldm_annotations.canonical_annotations({ldm_annotations.LDM_NAMESPACE: section}))
    if updated_annotations == annotations:
        return annotations, 0
    return updated_annotations, enriched_foreign_key_count


def _merged_ldm_section(annotations: dict[str, Any]) -> dict[str, Any]:
    """Return the annotation payload to enrich, folding the legacy namespace in.

    Non-contract keys such as ``relation_id`` are kept here because they are the
    join keys back to the SQLDeveloper CSVs. They are dropped again when the
    result is canonicalized.
    """

    section: dict[str, Any] = {}
    for namespace in (ldm_annotations.LEGACY_NAMESPACE, ldm_annotations.LDM_NAMESPACE):
        namespace_section = annotations.get(namespace)
        if isinstance(namespace_section, dict):
            section.update(namespace_section)
    return section


def _relation_metadata_by_name(
    relation_metadata_by_id: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Index relations by name so canonical annotations can still be matched.

    Names that are not unique are left out rather than matched ambiguously.
    """

    by_name: dict[str, dict[str, Any]] = {}
    ambiguous_names: set[str] = set()
    for relation_metadata in relation_metadata_by_id.values():
        relation_name = str(relation_metadata.get("relation_name", ""))
        if not relation_name:
            continue
        if relation_name in by_name:
            ambiguous_names.add(relation_name)
            continue
        by_name[relation_name] = relation_metadata
    for relation_name in ambiguous_names:
        by_name.pop(relation_name, None)
    return by_name


def _relation_metadata_for_foreign_key(
    foreign_key: dict[str, Any],
    relation_metadata_by_id: dict[str, dict[str, Any]],
    relation_metadata_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    relation_id = str(foreign_key.get("relation_id", ""))
    if relation_id:
        return relation_metadata_by_id.get(relation_id, {})
    relation_name = str(foreign_key.get("relation_name", ""))
    if relation_name:
        return relation_metadata_by_name.get(relation_name, {})
    return {}


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
