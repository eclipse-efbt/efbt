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
"""Forward-engineer a Django BIRD LDM model into an EIL-style Django model.

This module intentionally works from generated Django source files instead of
importing model classes. The generated BIRD model files are large, and importing
them requires a fully configured Django environment.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from pprint import pformat
from typing import Iterable

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pybirdai.process_steps.forward_engineering.django_model_ast import (  # noqa: E402
    DjangoModelModule,
    ModelClass,
    ModelStatement,
    parse_django_model,
)


@dataclass(frozen=True)
class ForwardEngineeringOptions:
    """Configuration for a forward-engineering run."""

    ldm_model_path: Path
    output_model_path: Path
    reference_model_path: Path | None = None
    report_path: Path | None = None
    field_lineage_path: Path | None = None
    column_validation_rules_path: Path | None = None
    relationship_validation_rules_path: Path | None = None
    choice_comparison_summary_path: Path | None = None
    include_reference_fallback: bool = False


@dataclass(frozen=True)
class SQLDeveloperForwardEngineeringPolicy:
    """Editable SQLDeveloper forward-engineering policy copied into Python."""

    include_entity_names: frozenset[str]
    include_class_names: frozenset[str]
    merge_entity_names: frozenset[str]
    merge_class_names: frozenset[str]
    folded_class_names: frozenset[str]
    suppressed_field_names_by_target: dict[str, frozenset[str]]
    field_name_overrides_by_target: dict[str, dict[str, str]]
    final_suppressed_field_names_by_target: dict[str, frozenset[str]]
    relationship_identifier_fields_by_target_table: dict[str, str]
    self_relationship_field_names_by_target: dict[str, frozenset[str]]
    source_field_injections_by_target: dict[str, dict[str, tuple[str, str]]]
    synthetic_char_fields_by_target: dict[str, frozenset[str]]
    preserved_reduced_field_names_by_target: dict[str, frozenset[str]]
    discriminator_names_not_merged: frozenset[str]


@dataclass
class ClassEngineeringReport:
    """Trace information for one generated EIL class."""

    target_class: str
    ldm_source_classes: list[str]
    generated_fields: list[str]
    folded_fields: list[str]
    synthetic_fields: list[str]
    reference_fallback_fields: list[str]
    missing_reference_fields: list[str] = field(default_factory=list)
    extra_generated_fields: list[str] = field(default_factory=list)


@dataclass
class DerivedFieldSet:
    """Fields inferred from the LDM, with metadata for synthetic relationships."""

    field_names: set[str] = field(default_factory=set)
    relationship_targets: dict[str, str] = field(default_factory=dict)
    source_field_names: dict[tuple[str, str], str] = field(default_factory=dict)
    source_field_injections: dict[str, tuple[str, str]] = field(default_factory=dict)
    synthetic_char_fields: set[str] = field(default_factory=set)
    skipped_source_fields: set[tuple[str, str]] = field(default_factory=set)
    not_applicable_choice_fields: set[str] = field(default_factory=set)
    choice_values_by_field: dict[str, dict[str, str]] = field(default_factory=dict)
    field_lineage: dict[str, list[dict[str, str]]] = field(default_factory=dict)


@dataclass(frozen=True)
class AnnotatedRelationshipKeyComponent:
    """A SQLDeveloper FK component mapped to the generated relationship key."""

    relationship_target: str
    canonical_field_name: str | None


@dataclass
class ForwardEngineeringResult:
    """The result of a forward-engineering run."""

    generated_source: str
    report: dict
    field_lineage: dict
    column_validation_rules: list[dict] = field(default_factory=list)
    relationship_validation_rules: list[dict] = field(default_factory=list)
    choice_comparison_summary: dict = field(default_factory=dict)


@dataclass
class ClassValidationContext:
    """Metadata needed to recreate SQLDeveloper validation-rule artifacts."""

    target_class: str
    ldm_source_classes: list[str]
    generated_field_names: set[str]
    derived_field_set: DerivedFieldSet


@dataclass(frozen=True)
class EntityMemberInfo:
    """A discriminator member in SQLDeveloper's validation-rule shape."""

    code: str
    label: str
    entity_name: str
    class_name: str


def run_forward_engineering(options: ForwardEngineeringOptions) -> ForwardEngineeringResult:
    """Run Python forward engineering and write the requested output files."""

    ldm_module = parse_django_model(options.ldm_model_path)
    reference_module = parse_django_model(options.reference_model_path) if options.reference_model_path else None

    (
        generated_source,
        report,
        field_lineage,
        column_validation_rules,
        relationship_validation_rules,
    ) = _generate_forward_engineering_artifacts(
        ldm_module=ldm_module,
        reference_module=reference_module,
        include_reference_fallback=options.include_reference_fallback,
    )

    options.output_model_path.parent.mkdir(parents=True, exist_ok=True)
    options.output_model_path.write_text(generated_source, encoding="utf-8")

    if options.report_path is not None:
        options.report_path.parent.mkdir(parents=True, exist_ok=True)
        options.report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if options.field_lineage_path is not None:
        options.field_lineage_path.parent.mkdir(parents=True, exist_ok=True)
        options.field_lineage_path.write_text(
            json.dumps(field_lineage, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    if options.column_validation_rules_path is not None:
        options.column_validation_rules_path.parent.mkdir(parents=True, exist_ok=True)
        options.column_validation_rules_path.write_text(
            json.dumps(column_validation_rules, indent=2) + "\n",
            encoding="utf-8",
        )

    if options.relationship_validation_rules_path is not None:
        options.relationship_validation_rules_path.parent.mkdir(parents=True, exist_ok=True)
        options.relationship_validation_rules_path.write_text(
            json.dumps(relationship_validation_rules, indent=2) + "\n",
            encoding="utf-8",
        )

    choice_comparison_summary = _build_choice_comparison_summary(report.get("comparison", {}))
    if options.choice_comparison_summary_path is not None:
        options.choice_comparison_summary_path.parent.mkdir(parents=True, exist_ok=True)
        options.choice_comparison_summary_path.write_text(
            json.dumps(choice_comparison_summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    return ForwardEngineeringResult(
        generated_source=generated_source,
        report=report,
        field_lineage=field_lineage,
        column_validation_rules=column_validation_rules,
        relationship_validation_rules=relationship_validation_rules,
        choice_comparison_summary=choice_comparison_summary,
    )


def generate_forward_engineered_source(
    ldm_module: DjangoModelModule,
    reference_module: DjangoModelModule | None = None,
    include_reference_fallback: bool = False,
) -> tuple[str, dict]:
    """Generate an EIL-style Django model source file from an LDM model."""

    (
        generated_source,
        report,
        _field_lineage,
        _column_rules,
        _relationship_rules,
    ) = _generate_forward_engineering_artifacts(
        ldm_module=ldm_module,
        reference_module=reference_module,
        include_reference_fallback=include_reference_fallback,
    )
    return generated_source, report


def _generate_forward_engineering_artifacts(
    ldm_module: DjangoModelModule,
    reference_module: DjangoModelModule | None = None,
    include_reference_fallback: bool = False,
) -> tuple[str, dict, dict, list[dict], list[dict]]:
    """Generate model source, report, field-lineage, and validation-rule artifacts."""

    graph = _ClassGraph(ldm_module)

    if reference_module is not None:
        target_class_order = [name for name in reference_module.class_order if name in ldm_module.classes]
    else:
        target_class_order = _target_class_order_from_ldm(ldm_module)

    target_classes = set(target_class_order)
    lines: list[str] = [
        "from pybirdai.annotations.decorators import lineage",
        "from django.db import models",
        "",
    ]
    class_reports: list[ClassEngineeringReport] = []
    class_field_lineage: dict[str, dict] = {}
    validation_contexts: dict[str, ClassValidationContext] = {}

    for target_class_name in target_class_order:
        ldm_class = ldm_module.classes[target_class_name]
        reference_class = reference_module.classes.get(target_class_name) if reference_module is not None else None
        ldm_source_classes = graph.forward_engineering_source_classes(target_class_name, target_classes)
        derived_field_set = _derive_fields_for_target(
            target_class_name=target_class_name,
            ldm_source_classes=ldm_source_classes,
            ldm_module=ldm_module,
            reference_class=reference_class,
            graph=graph,
            target_classes=target_classes,
        )
        derived_fields = (
            derived_field_set.field_names
            | set(derived_field_set.source_field_injections)
            | derived_field_set.synthetic_char_fields
        )
        synthetic_fields = (
            {"test_id", f"{target_class_name}_uniqueID"}
            | set(derived_field_set.source_field_injections)
            | derived_field_set.synthetic_char_fields
        )

        if reference_class is not None:
            reference_field_names = set(reference_class.fields)
            generated_field_names = (derived_fields | synthetic_fields) & reference_field_names
            if include_reference_fallback:
                generated_field_names = set(reference_field_names)
            key_annotations = _build_forward_engineered_key_annotations(
                target_class_name=target_class_name,
                ldm_source_classes=ldm_source_classes,
                generated_field_names=generated_field_names,
                derived_field_set=derived_field_set,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            )
            class_lines = _render_class_from_reference(
                reference_class=reference_class,
                included_fields=generated_field_names,
                annotations=key_annotations,
            )
        else:
            generated_field_names = derived_fields | synthetic_fields
            key_annotations = _build_forward_engineered_key_annotations(
                target_class_name=target_class_name,
                ldm_source_classes=ldm_source_classes,
                generated_field_names=generated_field_names,
                derived_field_set=derived_field_set,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            )
            class_lines = _render_class_from_ldm(
                target_class_name=target_class_name,
                ldm_class=ldm_class,
                source_class_names=ldm_source_classes,
                ldm_module=ldm_module,
                generated_field_names=generated_field_names,
                relationship_targets=derived_field_set.relationship_targets,
                source_field_names=derived_field_set.source_field_names,
                source_field_injections=derived_field_set.source_field_injections,
                synthetic_char_fields=derived_field_set.synthetic_char_fields,
                not_applicable_choice_fields=derived_field_set.not_applicable_choice_fields,
                choice_values_by_field=derived_field_set.choice_values_by_field,
                skipped_source_fields=derived_field_set.skipped_source_fields,
                graph=graph,
                target_classes=target_classes,
                annotations=key_annotations,
            )

        lines.extend(class_lines)
        lines.append("")

        validation_contexts[target_class_name] = ClassValidationContext(
            target_class=target_class_name,
            ldm_source_classes=ldm_source_classes,
            generated_field_names=set(generated_field_names),
            derived_field_set=derived_field_set,
        )

        reference_fields = set(reference_class.fields) if reference_class is not None else set()
        class_reports.append(
            ClassEngineeringReport(
                target_class=target_class_name,
                ldm_source_classes=ldm_source_classes,
                generated_fields=_sorted_in_reference_order(generated_field_names, reference_class),
                folded_fields=_sorted_in_reference_order(derived_fields & generated_field_names, reference_class),
                synthetic_fields=_sorted_in_reference_order(synthetic_fields & generated_field_names, reference_class),
                reference_fallback_fields=_sorted_in_reference_order(
                    generated_field_names - derived_fields - synthetic_fields,
                    reference_class,
                ),
                missing_reference_fields=_sorted_in_reference_order(reference_fields - generated_field_names, reference_class),
                extra_generated_fields=sorted(generated_field_names - reference_fields) if reference_fields else [],
            )
        )
        class_field_lineage[target_class_name] = _build_class_field_lineage(
            target_class_name=target_class_name,
            ldm_source_classes=ldm_source_classes,
            generated_field_names=generated_field_names,
            derived_field_set=derived_field_set,
            reference_class=reference_class,
            derived_fields=derived_fields,
            synthetic_fields=synthetic_fields,
        )

    generated_source = "\n".join(lines).rstrip() + "\n"
    generated_module = _parse_generated_source(generated_source)
    comparison = compare_model_modules(generated_module, reference_module) if reference_module is not None else {}

    report = _build_report(
        ldm_module=ldm_module,
        reference_module=reference_module,
        target_class_order=target_class_order,
        class_reports=class_reports,
        comparison=comparison,
        include_reference_fallback=include_reference_fallback,
    )
    field_lineage = _build_field_lineage_report(
        ldm_module=ldm_module,
        reference_module=reference_module,
        target_class_order=target_class_order,
        class_reports=class_reports,
        class_field_lineage=class_field_lineage,
        include_reference_fallback=include_reference_fallback,
    )
    column_validation_rules = _build_column_validation_rules(ldm_module, graph, validation_contexts)
    relationship_validation_rules = _build_relationship_validation_rules(
        ldm_module=ldm_module,
        graph=graph,
        validation_contexts=validation_contexts,
        target_classes=target_classes,
    )
    report["summary"]["column_validation_rule_count"] = len(column_validation_rules)
    report["summary"]["relationship_validation_rule_count"] = len(relationship_validation_rules)
    return generated_source, report, field_lineage, column_validation_rules, relationship_validation_rules


def compare_model_modules(generated_module: DjangoModelModule, reference_module: DjangoModelModule | None) -> dict:
    """Compare generated model class, field names, and field choice values."""

    if reference_module is None:
        return {}

    generated_classes = set(generated_module.classes)
    reference_classes = set(reference_module.classes)
    per_class: dict[str, dict] = {}

    for class_name in reference_module.class_order:
        if class_name not in generated_module.classes:
            continue
        generated_class = generated_module.classes[class_name]
        reference_class = reference_module.classes[class_name]
        generated_fields = set(generated_class.fields)
        reference_fields = set(reference_class.fields)
        choice_comparison = _compare_class_field_choices(generated_class, reference_class)
        per_class[class_name] = {
            "generated_field_count": len(generated_fields),
            "reference_field_count": len(reference_fields),
            "matching_fields": len(generated_fields & reference_fields),
            "missing_fields": sorted(reference_fields - generated_fields),
            "extra_fields": sorted(generated_fields - reference_fields),
            "generated_choice_field_count": choice_comparison["generated_choice_field_count"],
            "reference_choice_field_count": choice_comparison["reference_choice_field_count"],
            "matching_choice_fields": choice_comparison["matching_choice_fields"],
            "choice_differences": choice_comparison["choice_differences"],
        }

    generated_field_count = sum(len(model_class.fields) for model_class in generated_module.classes.values())
    reference_field_count = sum(len(model_class.fields) for model_class in reference_module.classes.values())
    matching_field_count = sum(class_report["matching_fields"] for class_report in per_class.values())
    generated_choice_field_count = sum(
        class_report["generated_choice_field_count"] for class_report in per_class.values()
    )
    reference_choice_field_count = sum(
        class_report["reference_choice_field_count"] for class_report in per_class.values()
    )
    matching_choice_field_count = sum(class_report["matching_choice_fields"] for class_report in per_class.values())
    choice_difference_count = sum(
        len(class_report["choice_differences"]) for class_report in per_class.values()
    )

    return {
        "generated_class_count": len(generated_classes),
        "reference_class_count": len(reference_classes),
        "matching_class_count": len(generated_classes & reference_classes),
        "missing_classes": sorted(reference_classes - generated_classes),
        "extra_classes": sorted(generated_classes - reference_classes),
        "generated_field_count": generated_field_count,
        "reference_field_count": reference_field_count,
        "matching_field_count": matching_field_count,
        "field_match_ratio": matching_field_count / reference_field_count if reference_field_count else 1.0,
        "generated_choice_field_count": generated_choice_field_count,
        "reference_choice_field_count": reference_choice_field_count,
        "matching_choice_field_count": matching_choice_field_count,
        "choice_difference_count": choice_difference_count,
        "choice_match_ratio": (
            matching_choice_field_count / reference_choice_field_count if reference_choice_field_count else 1.0
        ),
        "classes": per_class,
    }


def _compare_class_field_choices(generated_class: ModelClass, reference_class: ModelClass) -> dict:
    generated_fields = generated_class.fields
    reference_fields = reference_class.fields
    common_field_names = set(generated_fields) & set(reference_fields)
    generated_choice_fields = {
        field_name for field_name, field in generated_fields.items() if field.choices_name is not None
    }
    reference_choice_fields = {
        field_name for field_name, field in reference_fields.items() if field.choices_name is not None
    }
    choice_differences: dict[str, dict] = {}
    matching_choice_fields = 0

    for field_name in sorted(common_field_names & (generated_choice_fields | reference_choice_fields)):
        generated_field = generated_fields[field_name]
        reference_field = reference_fields[field_name]
        generated_choices = _field_choice_values(generated_class, generated_field)
        reference_choices = _field_choice_values(reference_class, reference_field)
        if generated_choices == reference_choices:
            matching_choice_fields += 1
            continue

        generated_values = set(generated_choices)
        reference_values = set(reference_choices)
        differing_labels = {
            value: {
                "generated": generated_choices[value],
                "reference": reference_choices[value],
            }
            for value in sorted(generated_values & reference_values)
            if generated_choices[value] != reference_choices[value]
        }
        choice_differences[field_name] = {
            "generated_choices_name": generated_field.choices_name,
            "reference_choices_name": reference_field.choices_name,
            "generated_choice_count": len(generated_choices),
            "reference_choice_count": len(reference_choices),
            "missing_values": sorted(reference_values - generated_values),
            "extra_values": sorted(generated_values - reference_values),
            "differing_labels": differing_labels,
        }

    for field_name in sorted((reference_choice_fields - generated_choice_fields) & common_field_names):
        if field_name in choice_differences:
            continue
        reference_field = reference_fields[field_name]
        reference_choices = _field_choice_values(reference_class, reference_field)
        choice_differences[field_name] = {
            "generated_choices_name": None,
            "reference_choices_name": reference_field.choices_name,
            "generated_choice_count": 0,
            "reference_choice_count": len(reference_choices),
            "missing_values": sorted(reference_choices),
            "extra_values": [],
            "differing_labels": {},
        }

    for field_name in sorted((generated_choice_fields - reference_choice_fields) & common_field_names):
        if field_name in choice_differences:
            continue
        generated_field = generated_fields[field_name]
        generated_choices = _field_choice_values(generated_class, generated_field)
        choice_differences[field_name] = {
            "generated_choices_name": generated_field.choices_name,
            "reference_choices_name": None,
            "generated_choice_count": len(generated_choices),
            "reference_choice_count": 0,
            "missing_values": [],
            "extra_values": sorted(generated_choices),
            "differing_labels": {},
        }

    return {
        "generated_choice_field_count": len(generated_choice_fields),
        "reference_choice_field_count": len(reference_choice_fields),
        "matching_choice_fields": matching_choice_fields,
        "choice_differences": choice_differences,
    }


def _field_choice_values(model_class: ModelClass, field: ModelStatement) -> dict[str, str]:
    choice_statement = _choice_statement_for_field(model_class, field)
    if choice_statement is None:
        return {}
    return _literal_choice_values(choice_statement.source)


def _choice_statement_for_field(model_class: ModelClass, field: ModelStatement) -> ModelStatement | None:
    if field.choices_name is None:
        return None
    preceding_choice_statements = [
        statement
        for statement in model_class.statements
        if statement.kind == "choice"
        and statement.name == field.choices_name
        and statement.line_number < field.line_number
    ]
    if preceding_choice_statements:
        return preceding_choice_statements[-1]
    return model_class.choices.get(field.choices_name)


def _literal_choice_values(choice_source: str) -> dict[str, str]:
    try:
        parsed = ast.parse(choice_source)
    except SyntaxError:
        return {}
    if not parsed.body or not isinstance(parsed.body[0], ast.Assign):
        return {}
    try:
        literal_value = ast.literal_eval(parsed.body[0].value)
    except (ValueError, SyntaxError):
        return {}
    if not isinstance(literal_value, dict):
        return {}
    return {str(key): str(value) for key, value in literal_value.items()}


def _target_class_order_from_ldm(ldm_module: DjangoModelModule) -> list[str]:
    sql_developer_policy = _editable_sqldeveloper_forward_engineering_policy()
    sql_developer_target_classes = _sql_developer_policy_classes(
        ldm_module,
        entity_names=sql_developer_policy.include_entity_names,
        class_names=sql_developer_policy.include_class_names,
    )
    sql_developer_merge_classes = _sql_developer_policy_classes(
        ldm_module,
        entity_names=sql_developer_policy.merge_entity_names,
        class_names=sql_developer_policy.merge_class_names,
    )

    target_class_order: list[str] = []
    for name in ldm_module.class_order:
        model_class = ldm_module.classes[name]
        if name in sql_developer_merge_classes or name in sql_developer_policy.folded_class_names:
            continue
        if name in sql_developer_target_classes:
            target_class_order.append(name)
            continue
        if not model_class.is_root_model():
            continue
        if _looks_like_helper_or_domain_class(name):
            continue
        if _is_folded_sql_developer_extension(name, model_class):
            continue
        if _is_unreferenced_derived_data_class(name, model_class):
            continue
        target_class_order.append(name)
    return target_class_order


def _is_folded_sql_developer_extension(class_name: str, model_class: ModelClass) -> bool:
    """Return True for one-to-one extension tables that SQLDeveloper merges."""

    if class_name.endswith("_DRVD_DT"):
        if _has_model_context_identity(model_class):
            return False
        return _has_identifying_source_reference(model_class) or _has_primary_key_source_reference(model_class)
    if class_name.endswith("_RSK_DT"):
        return _has_standard_identifying_source_reference(model_class)
    return False


def _is_unreferenced_derived_data_class(class_name: str, model_class: ModelClass) -> bool:
    if not class_name.endswith("_DRVD_DT"):
        return False
    if _has_model_context_identity(model_class):
        return False
    return True


def _editable_sqldeveloper_forward_engineering_policy() -> SQLDeveloperForwardEngineeringPolicy:
    """Return the SQLDeveloper FE policy copied from the scripts.

    Edit this function when the SQLDeveloper Subtree generation include list or
    Merge one-to-one list changes. The entity-name lists are copied from
    ``fe_6_6.xml``; the class-name lists are Python-side overrides for abbreviated
    Django names or for SQLDeveloper effects that are not visible from the
    generated LDM structure alone.
    """

    include_entity_names = {
        # Party related
        "Country",
        "Party",
        "Entity role",
        "Party code",
        # Rating system related
        "Rating system",
        "Rating grade",
        # Group related
        "Group",
        "Internal group role",
        # Assignments
        "Instrument Entity role assignment",
        "Credit facility Entity role assignment",
        "Security Entity role assignment",
        "Security position",
        "Long security position Prudential portfolio assignment",
        "Long security position Prudential Portfolio assignment Accounting classification for financial assets assignment",
        "Short security position prudential portfolio assignment",
        "Prudential portfolio",
        "Accounting classification",
        "Fair value option designation",
        "Master agreement Entity role assignment",
        "Exchange tradable derivative position",
        "Exchange tradable derivative position role",
        "Subsidiary, joint venture and associate Other organisation role assignment",
        "Instrument Collateral instrument assignment",
        "Instrument Collateral assignment",
        "Transferred asset leg Instrument assigment",
        "Instrument Protection arrangement assignment",
        "Exchange tradable derivative position Protection arrangement assignment",
        "Group Key management personnel assignment",
        # Instrument related
        "Instrument",
        "Instrument role",
        "Repurchase agreement component",
        "Security borrowing and lending transaction including cash as collateral component",
        "Security collateral Security leg assignment",
        "Over the counter (OTC) Derivative as a hedge",
        "Master agreement",
        "Protection arrangement",
        "Protection arrangement role",
        "Security and exchange tradable derivative",
        # Securitisation
        "Asset pool (subject to a Securitisation and other credit transfer)",
        "Covered bond programme",
        "Traditional securitisation",
        "Synthetic securitisation",
        "Credit transfer other than securitisation and covered bond programme",
        "Tranche in a synthetic securitisation without SSPE",
        "Asset pool Instrument assignment",
        # BSI
        "Non-financial asset",
        "Cash on hand",
        "Investment property taken into possession ",
        "Non-financial liability",
        # Additonal from second option draft
        "Financial asset instrument Collateral received instrument assignment",
        "Financial asset instrument role",
        "Financial liability instrument role",
        "Instrument hedged by Over the counter (OTC) Derivative",
        # Additional master types
        "Default status",
        "Forbearance measure",
        "Model Context",
        "Accounting classification",
        "Subordinated debt type",
        # Address
        "Postal code",
        "Address (used for reporting)",
        "Financial contract",
        # Security
        "Debt security issued (by the reporting agent)",
        "Security position hedged by Over the counter (OTC) derivative",
        # Tranche
        "Tranche in a Traditional securitisation",
        "Tranche in a synthetic securitisation without securitisation special purpose entity (SSPE) being a deposit",
        "Tranche in a synthetic securitisation without securitisation special purpose entity (SSPE) being a financial guarantee",
        # Collateral
        "Collateral",
        "Collateral role",
        # FRTB
        "Risk factor for standardised approach",
        "Fundamental review of the trading book standard approach risk measure for OTC positions",
        "Fundamental review of the trading book standard approach risk measure for ETD positions",
        "Fundamental review of the trading book standard approach risk measure for security positions",
        "Fundamental review of the trading book standard approach risk measure",
    }

    include_class_names = {
        # Current Django abbreviations for include-list entries that do not
        # resolve cleanly from Meta.verbose_name.
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT",
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT",
        "SHRT_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT",
        "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST",
        "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT",
    }

    merge_entity_names = {
        "Party_risk_data",
        "Credit_facility_risk_data",
        "Credit_facility_with_interest_rate",
        "Organisation_risk_data",
        "Factoring_cash_reserve",
        "Discount_or_excess_spread",
    }
    merge_class_names = {
        "PRTY_RSK_DT",
        "CRDT_FCLTY_RSK_DT",
        "CRDT_FCLTY_INTRST_RT",
        "ORGNSTN_RSK_DT",
        "FCTRNG_CSH_RSRV",
        "DSCRNT_EXCSS_SPRD",
    }

    folded_class_names = {
        # SQLDeveloper keeps the more concrete included hierarchy tables here.
        "FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR",
        "NN_FNNCL_ASST_NN_FNNCL_LBLTY",
        "SCRTSTN_TRNCH",
        "SCRTSN_OTHR_CRDT_TRNSFR",
        "SCRTY_EXCHNG_TRDBL_DRVTV_PSTN",
    }

    suppressed_field_names_by_target = {
        # These are SQLDeveloper "Reduce discriminators" style choices: the
        # broader table keeps the shared semantic field and not every subtype
        # flag or subtype-specific key that survived the Django LDM import.
        "CLLTRL": frozenset(
            {
                "AGRCLTR_LND_INDCTR",
                "CLLTRL_ANNX_PRTCTN_ID",
                "CNTRY_CD",
                "EXCHNG_TRDBL_DRVT_ID",
                "OFFC_CMMRCL_PRMS_CLLTRL_INDCTR",
                "TYP_PRTCTN_VL_APPRCH_FR_VL",
                "TYP_PRTCTN_VL_APPRCH_NTNL_AMNT",
            }
        ),
        "INSTRMNT": frozenset(
            {
                "MNMM_RSRV_INDCTR",
                "theEQT_INSTRMNT_NT_SCRT_HDG",
                "theINSTRMNT_ENTTY_RL_ASSGNMNT",
                "theLN_AND_ADVNC_HDG",
                "theRPRCHS_TRNSCTN_GLD_GVN_ASSGNMNT",
                "theSCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL",
                "theTRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST",
                "theTRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT",
            }
        ),
        "INSTRMNT_RL": frozenset(
            {
                "FDCRY_INSTRMT_INDCTR",
                "GRSS_CRRYNG_AMNT",
                "NN_BLNC_SHT_RCGNSD_FNNCL_ASST_INSTRMNT_BY_ACCNTNG_STNDRD",
                "OFF_BLNC_SHT_ITM_GVN_INSTRMNT_FRBRNC_STTS_TYP",
            }
        ),
        "SCRTY_EXCHNG_TRDBL_DRVTV": frozenset(
            {
                "ACCRD_INTRST_MRKT_VL_INDCTR",
                "ERLY_RDMPTN_INCLSN_INDCTR",
                "RNGTTD_DBT_SCRTY_WTH_FRBRNC_MSR_INDCTR",
                "SCRTY_TYP_BY_PRDCT",
                "theCVRD_BND_ISSNC",
                "theRSK_FAC_SA",
                "theSCRTY_ENTTY_RL_ASSGNMNT",
                "theTRNCH_TRDTNL_SCRTSTN",
            }
        ),
        "RSK_FAC_SA": frozenset(
            {
                "ENTTY_RL_TYP",
                "MN_CRRNCY",
                "PRTY_ID",
                "RSK_CRRNCY",
                "SCND_CRRNCY",
                "SCRTY_ID",
            }
        ),
        "CRDT_FCLTY": frozenset(
            {
                "CRDT_FCLTY_RSK_DT_ID",
                "FNNCL_CNTRCT_ID",
                "RNGTTD_CRDT_FCLTY_FRBRNC_MSR_INDCTR",
                "theCRDT_FCLTY_ENTTY_RL_ASSGNMNT",
                "theTRDTNL_SCRTSTN",
            }
        ),
        "LNG_SHRT_BLNC_SHT_RCGNSD_SCRTY_PSTN": frozenset(
            {
                "ACCNTNG_CLSSFCTN",
                "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_ACCNTNG_STNDRD_TYP",
                "SHRT_BLNC_SHT_RCGNSD_SCRTY_PSTN_ACCNTNG_STNDRD_TYP",
                "theLNG_SHRT_BLNC_SHT_RCGNSD_SCRTY_PSTN_HDG",
                "theSCRTY_PSTN",
            }
        ),
        "TRDTNL_SCRTSTN": frozenset({"ASST_PL_ID", "CRDT_FCLTY_ID"}),
        "SNTHTC_SCRTSTN": frozenset({"ASST_PL_ID"}),
        "OTHR_PRTY_ID": frozenset({"OTHR_PRTY_CD_TYP", "PRTY_CD_ID"}),
        "MSTR_AGRMNT_ENTTY_RL_ASSGNMNT": frozenset(
            {
                "CLRNG_MMBR_ORGNSTN_PRTY_ID",
                "CLRNG_MMBR_ORGNSTN_RL_TYP",
                "NN_QCCP_ORGNSTN_PRTY_ID",
                "NN_QCCP_ORGNSTN_RL_TYP",
                "QCCP_ORGNSTN_PRTY_ID",
                "QCCP_ORGNSTN_RL_TYP",
            }
        ),
        "SBSDRY_JNT_VNTR_ASSCT_OTHR_ORGNSTN_ASSGNMNT": frozenset(
            {
                "ASSCT_OTHR_ORGNSTN_PRTY_ID",
                "ENTTY_RL_TYP",
                "JNT_VNTR_OTHR_ORGNSTN_PRTY_ID",
                "SBSDRY_JNT_VNTR_ASSCT_GRP_ID",
                "SBSDRY_OTHR_ORGNSTN_PRTY_ID",
            }
        ),
        "PRTNR_ENTRPRS_ASSGNMNT": frozenset({"ENTTY_RL_TYP", "PRTY_ID"}),
        "LNKD_ENTRPRS_ASSGNMNT": frozenset({"ENTTY_RL_TYP", "PRTY_ID"}),
        "SCRTY_LNDNG_CMPNNT_SCRTY_ASSGNMNT": frozenset(
            {"SCRTY_BRRWNG_LNDNG_TRNSCTN_ID", "SCRTY_EXCHNG_TRDBL_DRVTV_ID"}
        ),
        "CVRD_BND_ISSNC": frozenset({"SCRTY_EXCHNG_TRDBL_DRVTV_ID"}),
    }

    field_name_overrides_by_target = {
        "CVRD_BND_ISSNC": {
            "CVRD_BND_ID": "SCRTY_ID",
        },
        "FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ETD_PSTNS": {
            "RSK_FCTR_ID": "FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ID",
        },
        "FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_FR_SCRTY_PSTNS": {
            "RSK_FCTR_ID": "FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ID",
        },
        "FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_OTC_PSTNS": {
            "RSK_FCTR_ID": "FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR_ID",
        },
        "LNG_SHRT_BLNC_SHT_RCGNSD_SCRTY_PSTN": {
            "SHRT_PSTN_ACCNTNG_CLSSFCTN": "SHRT_PSTN_ACCNTNG_CLSSFCTN",
        },
        "GRP_KY_MNGMNT_PRSNLL_ASSGNMNT": {
            "KY_MNGMNT_PRSNNL_PRTY_ID": "PRTY_ID",
            "KY_MNGMNT_PRSNLL_TYP": "NTRL_PRSN_GRP_RL_TYP",
        },
        "IMMDT_PRNT_ENTRPRS_ASSGNMNT": {
            "IMMDT_PRNT_ENTRPRS_PRTY_RL_TYP": "IMMDT_PRNT_ENTRPRS_ENTTY_RL_TYP",
        },
        "INSTRMNT_CLLTRL_INSTRMNT_ASSGNMNT": {
            "CLLTRL_GVN_INSTRMNT_ID": "CLLTRL_INSTRMNT_ID",
            "CLLTRL_RCVD_INSTRMNT_ID": "CLLTRL_INSTRMNT_ID",
        },
        "LNKD_ENTRPRS_ASSGNMNT": {
            "CNTR_BNK_PRVT_SCTR_CMPNY_PRTY_ID": "CNTR_BNK_PRVT_SCTR_CMPNY_PRTY_ID",
            "LNKD_ENTRPRS_PRTY_ID": "LNKD_ENTRPRS_PRTY_ID",
            "LNKD_ENTRPRS_PRTY_RL_TYP": "LNKD_ENTRPRS_ENTTY_RL_TYP",
        },
        "NTRL_PRSN_KY_MNGMNT_PRSNLL_ASSGNMNT": {
            "KY_MNGMNT_PRSNNL_PRTY_ID": "KY_MNGMNT_PRSNNL_PRTY_ID",
            "NTRL_PRSN_PRTY_ID": "NTRL_PRSN_PRTY_ID",
        },
        "OTC_DRVTV_HDG": {
            "OTC_DRVTV_ID": "INSTRMNT_ID",
        },
        "MSTR_AGRMNT_ENTTY_RL_ASSGNMNT": {
            "CLRNG_MMBR_ORGNSTN_PRTY_ID": "PRTY_ID",
            "CLRNG_MMBR_ORGNSTN_RL_TYP": "ORGNSTN_RL_TYP",
            "NN_QCCP_ORGNSTN_PRTY_ID": "PRTY_ID",
            "NN_QCCP_ORGNSTN_RL_TYP": "ORGNSTN_RL_TYP",
            "QCCP_ORGNSTN_PRTY_ID": "PRTY_ID",
            "QCCP_ORGNSTN_RL_TYP": "ORGNSTN_RL_TYP",
        },
        "OTHR_PRTY_ID": {
            "OTHR_PRTY_CD_ID": "PRTY_ID",
            "OTHR_PRTY_CD_TYP": "PRTY_CD_TYP",
        },
        "PRTNR_ENTRPRS_ASSGNMNT": {
            "CNTR_BNK_PRVT_SCTR_CMPNY_PRTY_ID": "CNTR_BNK_PRVT_SCTR_CMPNY_PRTY_ID",
            "PRTNR_ENTRPRS_PRTY_ID": "PRTNR_ENTRPRS_PRTY_ID",
            "PRTNR_ENTRPRS_PRTY_RL_TYP": "OTHR_ORGNSTN_RL_TYP",
        },
        "RSK_FAC_SA": {
            "RSK_FCTR_ID": "RSK_FCTR_ID",
        },
        "RPRCHS_TRNSCTN_GLD_GVN_ASSGNMNT": {
            "RPRCHS_TRNSCTN_ID": "RPRCHS_TRNSCTN_ID",
        },
        "RPRCHS_TRNSCTN_LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_GVN_ASSGNMNT": {
            "RPRCHS_TRNSCTN_ID": "RPRCHS_TRNSCTN_ID",
        },
        "SBSDRY_JNT_VNTR_ASSCT_OTHR_ORGNSTN_ASSGNMNT": {
            "ASSCT_OTHR_ORGNSTN_PRTY_ID": "PRTY_ID",
            "ASSCT_RL_TYP": "OTHR_ORGNSTN_RL_TYP",
            "JNT_VNTR_OTHR_ORGNSTN_PRTY_ID": "PRTY_ID",
            "JNT_VNTR_RL_TYP": "OTHR_ORGNSTN_RL_TYP",
            "SBSDRY_JNT_VNTR_ASSCT_GRP_ID": "GRP_ID",
            "SBSDRY_OTHR_ORGNSTN_PRTY_ID": "PRTY_ID",
            "SBSDRY_RL_TYP": "OTHR_ORGNSTN_RL_TYP",
        },
        "SCRTY_LNDNG_CMPNNT_SCRTY_ASSGNMNT": {
            "SCRTY_BRRWNG_LNDNG_TRNSCTN_ID": "INSTRMNT_ID",
            "SCRTY_ID": "SCRTY_ID",
        },
        "SCRTY_CLLTRL_LNDNG_CMPNNT_SCRTY_CLLTRL_ASSGNMNT": {
            "SCRTY_BRRWNG_LNDNG_TRNSCTN_ID": "INSTRMNT_ID",
        },
        "SCRTY_PSTN_HDGD_OTC_DRVTV": {
            "INVSTR_RL_TYP": "INVSTR_RL_TYP",
        },
        "SNTHTC_SCRTSTN": {
            "SCRTSTN_ACCNTNG_CNSLDTN_LVL": "ACCNTNG_CNSLDTN_LVL",
            "SCRTSTN_ACCNTNG_STNDRD": "ACCNTNG_STNDRD",
            "SCRTSTN_ID": "SCRTSTN_ID",
            "SCRTSTN_OTHR_CRDT_TRNSFR_TYP": "SCRTSTN_OTHR_CRDT_TRNSFR_TYP",
            "SCRTSTN_RFRNC_DT": "DT_RFRNC",
            "SCRTSTN_RPRTNG_AGNT_ID": "RPRTNG_AGNT_ID",
        },
        "TRDTNL_SCRTSTN": {
            "SCRTSTN_ACCNTNG_CNSLDTN_LVL": "ACCNTNG_CNSLDTN_LVL",
            "SCRTSTN_ACCNTNG_STNDRD": "ACCNTNG_STNDRD",
            "SCRTSTN_ID": "SCRTSTN_ID",
            "SCRTSTN_OTHR_CRDT_TRNSFR_TYP": "SCRTSTN_OTHR_CRDT_TRNSFR_TYP",
        },
    }

    final_suppressed_field_names_by_target = {
        # These are single-field SQLDeveloper output differences. They should
        # not enable the broader target cleanup used by the reduce-discriminator
        # entries above.
        "ASST_PL": frozenset({"theCRDT_TRNSFR_OTHR_SCRTSTN_CVRD_BND_PRGRM", "theCVRD_BND_PRGRM"}),
        "ASST_PL_INSTRMNT_ASSGNMNT": frozenset({"LN_ID", "SCRTY_ID"}),
        "CLLTRL_RL": frozenset({"CLLTRL_RCVD_ID"}),
        "CRDT_TRNSFR_OTHR_SCRTSTN_CVRD_BND_PRGRM": frozenset({"ASST_PL_ID"}),
        "CVRD_BND_PRGRM": frozenset({"ASST_PL_ID", "theCVRD_BND_PRGRMM_RLVNT_RGM_EXCSS"}),
        "DBT_SCRTY_ISSD": frozenset({"theDBT_SCRTY_ISSD_HDG"}),
        "ENTTY_RL": frozenset(
            {
                "theINSTRMNT_ENTTY_RL_ASSGNMNT",
                "theSBSDRY_JNT_VNTR_ASSCT_OTHR_ORGNSTN_ASSGNMNT",
            }
        ),
        "ETD_LBLTY_PSTN_SNTHTC_SCRTSTN_ASSGNMNT": frozenset({"theEXCHNG_TRDBL_DRVTV_PSTN"}),
        "EXCHNG_TRDBL_DRVTV_PSTN_RL": frozenset(
            {
                "BLNC_SHT_RCGNSD_EXCHNG_TRDBL_DRVTV_ASST_PSTN_TKN_PSSSSN_ID",
                "ETD_ASST_PSTN_TYP",
                "ETD_LBLTY_PSTN_TYP",
                "EXCHNG_TRDBL_DRVTV_PSTN_HDG_ACCNTNG_STNDRD_TYP",
            }
        ),
        "FNNCL_CNTRCT": frozenset({"SYNDCTD_CNTRCT_ID"}),
        "GRP": frozenset({"DMSTC_INSTTTNL_UNT_INDCTR"}),
        "INSTRMNT_CLLTRL_INSTRMNT_ASSGNMNT": frozenset({"OTC_CRDT_DFLT_SWP_INSTRMNT_RL_TYP"}),
        "INSTRMNT_CLLTRL_ASSGNMNT": frozenset(
            {"CLLTRL_RCVD_ID", "CLLTRL_RCVD_RL_TYP", "LN_AND_ADVNC_ID", "theCLLTRL"}
        ),
        "INSTRMNT_ENTTY_RL_ASSGNMNT": frozenset({"PYMNT_AGNT_ID", "SCRTY_ID"}),
        "INSTRMNT_HDGD_EXCHNG_TRDBL_DRVTV": frozenset({"theEXCHNG_TRDBL_DRVTV_PSTN"}),
        "INVSTMNT_PRPRTY_TKN_PSSSSN": frozenset({"HLD_SL"}),
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT": frozenset(
            {"CLLTRL_RCVD_ID", "theCLLTRL"}
        ),
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRTCTN_ARRNGMNT_RCVD_ASSGNMNT": frozenset(
            {"thePRTCTN_ARRNGMNT"}
        ),
        "LNG_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT": frozenset({"theCLLTRL"}),
        "LNG_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRTCTN_ARRNGMNT_RCVD_ASSGNMNT": frozenset(
            {"thePRTCTN_ARRNGMNT"}
        ),
        "MSTR_AGRMNT": frozenset({"theMSTR_AGRMNT_ENTTY_RL_ASSGNMNT"}),
        "NN_FNNCL_ASST": frozenset({"NN_FNNCL_ASST_NN_FNNCL_LBLTY_TYP"}),
        "NN_FNNCL_LBLTY": frozenset({"NN_FNNCL_ASST_NN_FNNCL_LBLTY_TYP"}),
        "PRTCTN_ARRNGMNT_RL": frozenset({"PRTCTN_ARRNGMNT_RCVD_ID"}),
        "PRTY": frozenset(
            {
                "CRDT_INSTTTN_ID",
                "DMSTC_BRNCH_INDCTR",
                "FRGN_BRNCH_ID",
                "INSTTTNL_UNT_GRP_ID",
                "INSTTTNL_UNT_ID",
            }
        ),
        "RPRCHS_TRNSCTN_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN_ASSGNMNT": frozenset({"INSTRMNT_ID"}),
        "SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL": frozenset(
            {"SCRTY_BRRWNG_LNDNG_TRNSCTN_CMPNNT_TYP_BY_SCRTY_TYP", "SCRTY_LNDNG_CMPNNT_INDCTR"}
        ),
        "SCRTY_ENTTY_RL_ASSGNMNT": frozenset({"SCRTY_ID"}),
        "SCRTY_HDGD_EXCHNG_TRDBL_DRVTV": frozenset({"theEXCHNG_TRDBL_DRVTV_PSTN"}),
        "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST": frozenset({"TRNCH_SYNTHTC_SCRTSTN_TYP"}),
        "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT": frozenset({"TRNCH_SYNTHTC_SCRTSTN_TYP"}),
    }

    relationship_identifier_fields_by_target_table = {
        # The SQLDeveloper EIL keeps a scalar non-financial asset key alongside
        # the relationship; the Django LDM import only exposes the relationship.
        "NN_FNNCL_ASST": "NN_FNNCL_ASST_ID",
    }

    self_relationship_field_names_by_target = {
        # Most self-target relationships are noise after folding. Party keeps
        # these two organisation/party recursive relationships in the EIL.
        "PRTY": frozenset(
            {
                "Organisation_comprises_Organisational_unit_s",
                "Organisation_is_ultimate_parent_of_Organisation_s",
            }
        ),
    }

    source_field_injections_by_target = {
        # SQLDeveloper places source-of-encumbrance on these concrete views as
        # well as the broader prudential-portfolio accounting-classification
        # assignment. In the Django LDM the source field lives on the folded
        # derived-data class, so copy that field definition explicitly.
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT_NGAAP_FDCRY_ITM": {
            "SRC_ENCMBRNC": (
                "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT_DRVD_DT",
                "SRC_ENCMBRNC",
            ),
        },
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT_TKN_PSSSN": {
            "SRC_ENCMBRNC": (
                "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT_DRVD_DT",
                "SRC_ENCMBRNC",
            ),
        },
    }

    synthetic_char_fields_by_target = {
        "NN_FNNCL_ASST": frozenset({"NN_FNNCL_ASST_ID"}),
        "NN_FNNCL_LBLTY": frozenset({"NN_FNNCL_LBLTY_ID"}),
    }

    preserved_reduced_field_names_by_target = {
        "SNTHTC_SCRTSTN": frozenset({"SCRTSTN_OTHR_CRDT_TRNSFR_TYP", "SCRTSTN_TYP"}),
        "TRDTNL_SCRTSTN": frozenset({"SCRTSTN_OTHR_CRDT_TRNSFR_TYP", "SCRTSTN_TYP"}),
    }
    discriminator_names_not_merged = frozenset(
        name for pair in _editable_sqldeveloper_discriminators_not_merged() for name in pair
    )

    return SQLDeveloperForwardEngineeringPolicy(
        include_entity_names=frozenset(include_entity_names),
        include_class_names=frozenset(include_class_names),
        merge_entity_names=frozenset(merge_entity_names),
        merge_class_names=frozenset(merge_class_names),
        folded_class_names=frozenset(folded_class_names),
        suppressed_field_names_by_target=suppressed_field_names_by_target,
        field_name_overrides_by_target=field_name_overrides_by_target,
        final_suppressed_field_names_by_target=final_suppressed_field_names_by_target,
        relationship_identifier_fields_by_target_table=relationship_identifier_fields_by_target_table,
        self_relationship_field_names_by_target=self_relationship_field_names_by_target,
        source_field_injections_by_target=source_field_injections_by_target,
        synthetic_char_fields_by_target=synthetic_char_fields_by_target,
        preserved_reduced_field_names_by_target=preserved_reduced_field_names_by_target,
        discriminator_names_not_merged=discriminator_names_not_merged,
    )


def _editable_sqldeveloper_discriminators_not_merged() -> tuple[tuple[str, str], ...]:
    """Disjoint discriminators copied from SQLDeveloper Reduce discriminators."""

    return (
        ("ORGNSTN_TYP_BY_PRCDNG_STTS", "Organisation_type_by_legal_proceeding_status"),
        ("INSTRMNT_TYP_ORGN", "Instrument_type_by_origin"),
        ("SNDCTN_SB_PRTCPTN_MMBR_INSTRMNT_INDCTR", "Syndication or sub-participation member instrument indicator"),
        ("SCRTY_TYP_BY_IDNTFR", "Security_type_by_identifier"),
        (
            "SCRTY_BRRWNG_LNDNG_TRNSCTN_CMPNNT_TYP_BY_DRCTN",
            "Security_borrowing_and_lending_transaction_component_type_by_direction",
        ),
        ("DBT_SCRTY_PRFRMNG_STTS_TYP", "Debt_security_by_Performing_status_type"),
        ("PRPTL_DBT_SCRTY_INDCTR", "Perpetual_debt_security_indicator"),
        ("DBT_SCRTY_ACCNTNG_STNDRD", "Debt_security_by_accounting_standard"),
        ("DBT_SCRTY_ISSD_PRDNTL_PRTFL_TYP", "Debt_security_issued_prudential_portfolio_type"),
        ("SCRTY_PSTN_BY_ACCNTNG_STNDRD", "Security position by accounting standard"),
        ("LNG_SCRTY_PSTN_PRDNTL_PRTFL_TYP", "Long_security_position_Prudential_portfolio_type"),
        (
            "LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ACCNTNG_CLSSFCTN_FNNCL_ASSTS_ASSGNMNT_TKN_PSSSSN_TYP",
            "Long security position Prudential Portfolio assignment Accounting classification for financial assets taken into possession type",
        ),
        ("PST_DU_DBT_SCRTY_INDCTR", "Past due debt security indicator"),
        (
            "BLNC_SHT_RCGNSD_EXCHNG_TRDBL_DRVTV_ASST_PSTN_BY_ACCNTNG_STNDRD",
            "Balance sheet recognised exchange tradable derivative asset position by accounting standard",
        ),
        (
            "BLNC_SHT_RCGNSD_EXCHNG_TRDBL_DRVTV_ASST_PSTN_TKN_PSSSSN_TYP",
            "Balance sheet recognised exchange tradable derivative asset position taken into possession type",
        ),
        ("EXCHNG_TRDBL_DRVTV_TYP_BY_IDNTFR", "Exchange tradable derivative type by identifier"),
        ("RTNG_SYSTM_TYP_BY_NTR", "Rating_system_type_by_nature_(Grade_vs._Numeric)"),
        (
            "FNNCL_ASST_INSTRMNT_TYP_CRR_123",
            "Financial_asset_instrument_type_by_CRR,_Article_123_(Retail_exposure)",
        ),
        ("FNNCL_ASST_INSTRMNT_TYP_INTRST_RT_ONL", "Financial_asset_instrument_type_by_interest_rate_only"),
        ("FNNCL_ASST_INSTRMNT_TYP_FXD_INTRST_RT", "Financial_asset_instrument_type_by_fixed_interest_rate"),
        ("FNNCL_ASST_INSTRMNT_TYP_RNGTTN_STTS", "Financial_asset_instrument_type_by_renegotiation_status"),
        ("ABSTRCT_INSTRMNT_RL_TYP", "Abstract_instrument_role_type"),
        (
            "BLNC_SHT_RCGNSD_FFNCL_ASST_INSTRMNT_FR_VL_TYP",
            "Balance_sheet_recognised_financial_asset_instrument_by_fair_value_type",
        ),
        (
            "BLNC_SHT_RCGNSD_FNNCL_ASST_INSTRMNT_FR_VL_TYP",
            "Balance_sheet_recognised_financial_asset_instrument_by_fair_value_type",
        ),
        ("PST_DU_FNNCL_ASST_INSTRMNT_INDCTR", "Past_due_financial_asset_instrument_indicator"),
        (
            "BLNC_SHT_RCGNSD_FNNCL_ASST_INSTRMNT_TKN_PSSSSN_TYP",
            "Balance sheet recognised financial asset instrument taken into possession type",
        ),
        ("PRTY_TYP_ADDRS", "Party_type_by_address"),
        ("LSTD_CNTRL_BNK_PRVT_SCTR_CMPNY_INDCTR", "Listed_central_bank_and_private_sector_company_indicator"),
        ("ORGNSTN_TYP_BY_PRCDNG_STTS", "Organisation type by legal proceeding status"),
        ("RL_ESTT_CLLTRL_LCTN_TYP", "Real_estate_collateral_location_type"),
    )


def _sql_developer_policy_classes(
    ldm_module: DjangoModelModule,
    entity_names: frozenset[str],
    class_names: frozenset[str],
) -> set[str]:
    normalized_entity_names = {_normalize_sql_developer_entity_name(name) for name in entity_names}
    policy_classes = {class_name for class_name in class_names if class_name in ldm_module.classes}
    for class_name, model_class in ldm_module.classes.items():
        if any(
            _normalize_sql_developer_entity_name(logical_name) in normalized_entity_names
            for logical_name in _model_class_logical_names(class_name, model_class)
        ):
            policy_classes.add(class_name)
    return policy_classes


def _model_class_logical_names(class_name: str, model_class: ModelClass) -> tuple[str, ...]:
    logical_names = [class_name]
    verbose_name = _model_verbose_name(model_class)
    if verbose_name is not None:
        logical_names.append(verbose_name)
    return tuple(logical_names)


def _model_verbose_name(model_class: ModelClass) -> str | None:
    if model_class.meta_source is None:
        return None
    match = re.search(r"verbose_name\s*=\s*['\"]([^'\"]+)['\"]", model_class.meta_source)
    return match.group(1) if match else None


def _normalize_sql_developer_entity_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def _has_identifying_source_reference(model_class: ModelClass) -> bool:
    return bool(_identifying_source_references(model_class))


def _has_standard_identifying_source_reference(model_class: ModelClass) -> bool:
    identifying_references = _identifying_source_references(model_class)
    if len(identifying_references) != 1:
        return False
    foreign_key = identifying_references[0]
    fields = foreign_key.get("fields", [])
    number_of_attributes = foreign_key.get("number_of_attributes")
    return number_of_attributes == 5 and isinstance(fields, list) and len(fields) == 5


def _identifying_source_references(model_class: ModelClass) -> list[dict]:
    identifying_references: list[dict] = []
    for foreign_key in _sql_developer_foreign_keys(model_class):
        if foreign_key.get("identifying") != "Y":
            continue
        if foreign_key.get("relation_side") not in {"source", "target"}:
            continue
        if foreign_key.get("source_class") or foreign_key.get("referenced_class"):
            identifying_references.append(foreign_key)
    return identifying_references


def _has_primary_key_source_reference(model_class: ModelClass) -> bool:
    return bool(_primary_key_source_references(model_class))


def _primary_key_source_references(model_class: ModelClass) -> list[dict]:
    primary_key = set(_sql_developer_primary_key(model_class))
    if not primary_key:
        return []

    primary_key_references: list[dict] = []
    for foreign_key in _sql_developer_foreign_keys(model_class):
        referenced_class = foreign_key.get("referenced_class")
        if not referenced_class:
            continue
        fields = foreign_key.get("fields", [])
        if not isinstance(fields, list) or set(fields) != primary_key:
            continue
        primary_key_references.append(foreign_key)
    return primary_key_references


def _sql_developer_primary_key(model_class: ModelClass) -> list[str]:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    primary_key = sql_developer_annotations.get("primary_key", [])
    return primary_key if isinstance(primary_key, list) else []


def _has_sql_developer_relationship_for_model_field(
    model_class: ModelClass,
    field_name: str,
    related_model_name: str,
) -> bool:
    foreign_keys = _sql_developer_foreign_keys(model_class)
    if not foreign_keys:
        return True

    for foreign_key in foreign_keys:
        if foreign_key.get("relation_name") == field_name:
            return True
        if related_model_name in {
            foreign_key.get("source_class"),
            foreign_key.get("target_class"),
            foreign_key.get("referenced_class"),
        }:
            return True
    return False


def _is_non_primary_relationship_key_component(
    source_class: ModelClass,
    field_name: str,
    relationship_target: str,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    primary_key_status = _relationship_key_component_primary_key_status(
        source_class=source_class,
        field_name=field_name,
        relationship_target=relationship_target,
        graph=graph,
        target_classes=target_classes,
    )
    return primary_key_status is False


def _relationship_key_component_primary_key_status(
    source_class: ModelClass,
    field_name: str,
    relationship_target: str,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool | None:
    for foreign_key in _sql_developer_foreign_keys(source_class):
        fields = foreign_key.get("fields", [])
        if not isinstance(fields, list) or field_name not in fields:
            continue
        if not _foreign_key_targets_relationship_target(foreign_key, relationship_target, graph, target_classes):
            continue
        for field_entry in foreign_key.get("field_entries", []):
            if field_entry.get("field") == field_name:
                return bool(field_entry.get("primary_key"))
        return field_name in _sql_developer_primary_key(source_class)
    return None


def _foreign_key_targets_relationship_target(
    foreign_key: dict,
    relationship_target: str,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    referenced_class = foreign_key.get("referenced_class")
    if referenced_class:
        return relationship_target in graph.relationship_target_tables(referenced_class, target_classes)

    for class_name_key in ("source_class", "target_class"):
        class_name = foreign_key.get(class_name_key)
        if class_name:
            if relationship_target in graph.relationship_target_tables(class_name, target_classes):
                return True
    return False


def _add_reduced_discriminator_choice_values(
    derived_field_set: DerivedFieldSet,
    target_class_name: str,
    ldm_source_classes: list[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
    preserved_field_names: frozenset[str] = frozenset(),
) -> None:
    for field_name in list(derived_field_set.choice_values_by_field):
        if field_name in preserved_field_names:
            continue
        current_choice_values = derived_field_set.choice_values_by_field[field_name]
        base_class_name = _not_merged_discriminator_base_class(field_name, ldm_module)
        if base_class_name is not None:
            hierarchy_choice_values = _not_merged_discriminator_leaf_choice_values(
                base_class_name=base_class_name,
                current_choice_values=current_choice_values,
                ldm_module=ldm_module,
                graph=graph,
            )
            if hierarchy_choice_values:
                derived_field_set.choice_values_by_field[field_name] = hierarchy_choice_values
            continue

        base_class_name = _reduced_discriminator_base_class(field_name, target_class_name, ldm_module)
        include_source_class_members = target_class_name == base_class_name
        if base_class_name is None:
            base_class_name = _annotated_reduced_discriminator_base_class(
                field_name=field_name,
                ldm_source_classes=ldm_source_classes,
                ldm_module=ldm_module,
                graph=graph,
            )
            include_source_class_members = base_class_name is not None
        if base_class_name is None:
            continue
        if not graph.children.get(base_class_name) and not include_source_class_members:
            continue
        if _target_is_reduced_discriminator_leaf(target_class_name, base_class_name, graph):
            continue
        hierarchy_choice_values = _reduced_discriminator_leaf_choice_values(
            field_name=field_name,
            base_class_name=base_class_name,
            restrict_source_members_to_base_hierarchy=base_class_name != target_class_name,
            ldm_source_classes=ldm_source_classes,
            ldm_module=ldm_module,
            graph=graph,
            target_classes=target_classes,
            include_source_class_members=include_source_class_members,
        )
        if hierarchy_choice_values:
            derived_field_set.choice_values_by_field[field_name] = hierarchy_choice_values


def _add_relationship_copy_reduced_discriminator_choice_values(
    derived_field_set: DerivedFieldSet,
    target_class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> None:
    base_choice_values_by_field: dict[str, tuple[dict[str, str], bool]] = {}
    for (source_class_name, source_field_name), output_name in list(derived_field_set.source_field_names.items()):
        if output_name not in derived_field_set.field_names:
            continue
        if output_name not in derived_field_set.choice_values_by_field:
            continue

        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        source_field = source_class.fields.get(source_field_name)
        if source_field is None or source_field.choices_name is None:
            continue

        base_class_name = _reduced_discriminator_base_class(output_name, target_class_name, ldm_module)
        if base_class_name is None or base_class_name not in target_classes:
            continue
        if target_class_name == base_class_name:
            continue
        if output_name != f"{base_class_name}_TYP":
            continue
        if not _is_relationship_copy_reduced_discriminator_component(
            source_class=source_class,
            source_field=source_field,
            source_field_name=source_field_name,
            output_name=output_name,
            base_class_name=base_class_name,
            ldm_module=ldm_module,
            graph=graph,
        ):
            continue

        cached_base_choice_values = base_choice_values_by_field.get(output_name)
        if cached_base_choice_values is None:
            base_source_classes = graph.forward_engineering_source_classes(base_class_name, target_classes)
            base_choice_values = _reduced_discriminator_leaf_choice_values(
                field_name=output_name,
                base_class_name=base_class_name,
                restrict_source_members_to_base_hierarchy=False,
                ldm_source_classes=base_source_classes,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
                include_source_class_members=True,
            )
            base_adds_not_applicable = _base_reduced_discriminator_adds_not_applicable(
                field_name=output_name,
                base_class_name=base_class_name,
                base_source_classes=base_source_classes,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            )
            base_choice_values_by_field[output_name] = (base_choice_values, base_adds_not_applicable)
        else:
            base_choice_values, base_adds_not_applicable = cached_base_choice_values
        if not base_choice_values:
            continue

        derived_field_set.choice_values_by_field[output_name] = dict(base_choice_values)
        if base_adds_not_applicable or "0" in base_choice_values:
            derived_field_set.not_applicable_choice_fields.add(output_name)
        else:
            derived_field_set.not_applicable_choice_fields.discard(output_name)


def _add_entity_role_copy_choice_values(
    derived_field_set: DerivedFieldSet,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> None:
    if "ENTTY_RL" not in ldm_module.classes:
        return

    entity_role_choice_values: dict[str, str] | None = None
    for (source_class_name, source_field_name), output_name in list(derived_field_set.source_field_names.items()):
        if output_name not in derived_field_set.field_names:
            continue
        if output_name not in derived_field_set.choice_values_by_field:
            continue
        if not output_name.endswith("_RL_TYP"):
            continue

        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        source_field = source_class.fields.get(source_field_name)
        if source_field is None or source_field.choices_name is None:
            continue
        if not _is_entity_role_copy_component(
            source_class=source_class,
            source_field=source_field,
            source_field_name=source_field_name,
            output_name=output_name,
            ldm_module=ldm_module,
            graph=graph,
        ):
            continue

        if entity_role_choice_values is None:
            entity_role_source_classes = graph.forward_engineering_source_classes("ENTTY_RL", target_classes)
            entity_role_choice_values = _reduced_discriminator_leaf_choice_values(
                field_name="ENTTY_RL_TYP",
                base_class_name="ENTTY_RL",
                restrict_source_members_to_base_hierarchy=False,
                ldm_source_classes=entity_role_source_classes,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
                include_source_class_members=True,
            )
        if not entity_role_choice_values:
            continue

        derived_field_set.choice_values_by_field[output_name] = dict(entity_role_choice_values)
        derived_field_set.not_applicable_choice_fields.add(output_name)


def _is_entity_role_copy_component(
    source_class: ModelClass,
    source_field: ModelStatement,
    source_field_name: str,
    output_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> bool:
    has_role_type_domain = _source_field_has_role_type_domain(source_class, source_field, source_field_name)
    if not _source_field_domain_matches_output_name(
        source_class,
        source_field,
        source_field_name,
        output_name,
    ) and not (output_name == "ENTTY_RL_TYP" and has_role_type_domain) and not has_role_type_domain:
        return False
    for foreign_key in _sql_developer_foreign_keys(source_class):
        if foreign_key.get("identifying") != "Y":
            continue
        if not _foreign_key_contains_field(foreign_key, source_field_name):
            continue
        referenced_class = foreign_key.get("referenced_class")
        if isinstance(referenced_class, str) and _class_is_or_descends_from(referenced_class, "ENTTY_RL", graph):
            return True
    if has_role_type_domain:
        return _identifying_key_chain_reaches_entity_role(
            source_class.name,
            source_field_name,
            ldm_module,
            graph,
            visited=set(),
        )
    return False


def _identifying_key_chain_reaches_entity_role(
    class_name: str,
    field_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    visited: set[tuple[str, str]],
) -> bool:
    if (class_name, field_name) in visited:
        return False
    visited.add((class_name, field_name))

    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return False

    for foreign_key in _sql_developer_foreign_keys(model_class):
        if foreign_key.get("identifying") != "Y":
            continue
        if not _foreign_key_contains_field(foreign_key, field_name):
            continue
        referenced_class = foreign_key.get("referenced_class")
        if not isinstance(referenced_class, str) or referenced_class not in ldm_module.classes:
            continue
        if _class_is_or_descends_from(referenced_class, "ENTTY_RL", graph):
            return True
        if _identifying_key_chain_reaches_entity_role(
            referenced_class,
            field_name,
            ldm_module,
            graph,
            visited,
        ):
            return True

    for base_class_name in model_class.bases:
        if base_class_name not in ldm_module.classes:
            continue
        if _identifying_key_chain_reaches_entity_role(
            base_class_name,
            field_name,
            ldm_module,
            graph,
            visited,
        ):
            return True

    return False


def _source_field_has_role_type_domain(
    source_class: ModelClass,
    source_field: ModelStatement,
    source_field_name: str,
) -> bool:
    field_annotations = _sql_developer_field_annotations(source_class, source_field_name)
    for candidate in (
        field_annotations.get("domain_synonym"),
        field_annotations.get("domain_field_name"),
        source_field.choices_name,
        source_field_name,
    ):
        if candidate and str(candidate).removesuffix("_domain").endswith("_RL_TYP"):
            return True
    return False


def _add_entity_role_not_applicable_choice_values(derived_field_set: DerivedFieldSet) -> None:
    choice_values = derived_field_set.choice_values_by_field.get("ENTTY_RL_TYP")
    if choice_values is None:
        return
    choice_values.setdefault("0", "Not_applicable")


def _add_sql_developer_input_domain_not_applicable_choice_values(
    derived_field_set: DerivedFieldSet,
    target_class_name: str,
    ldm_module: DjangoModelModule,
) -> None:
    input_domain_synonyms = _editable_sqldeveloper_input_domain_not_applicable_synonyms()
    for (source_class_name, source_field_name), output_name in list(derived_field_set.source_field_names.items()):
        choice_values = derived_field_set.choice_values_by_field.get(output_name)
        if choice_values is None or "0" in choice_values:
            continue
        if source_class_name == target_class_name:
            continue

        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        source_field = source_class.fields.get(source_field_name)
        if source_field is None or source_field.choices_name is None:
            continue

        domain_synonyms = _source_field_domain_synonyms(source_class, source_field, source_field_name)
        if not input_domain_synonyms.intersection(domain_synonyms):
            continue
        if not _is_folded_classifier_foreign_key_field(source_class, source_field_name):
            continue
        if not _is_sql_developer_input_domain_folded_source(target_class_name, source_class_name, domain_synonyms):
            continue

        choice_values["0"] = "Not_Applicable"


def _add_sql_developer_folded_input_domain_choice_values(
    derived_field_set: DerivedFieldSet,
    target_class_name: str,
    ldm_source_classes: list[str],
) -> None:
    input_domain_choice_values = _editable_sqldeveloper_folded_input_domain_choice_values_by_target()
    suppressed_choice_values = _editable_sqldeveloper_folded_input_domain_suppressed_values_by_target()
    source_class_set = set(ldm_source_classes)
    for field_name, choice_values_by_source_class in input_domain_choice_values.get(target_class_name, {}).items():
        if field_name not in derived_field_set.field_names:
            continue
        merged_choice_values = derived_field_set.choice_values_by_field.setdefault(field_name, {})
        for source_class_name, choice_values in choice_values_by_source_class.items():
            if source_class_name not in source_class_set:
                continue
            merged_choice_values.update(choice_values)
    for field_name, suppressed_values_by_source_class in suppressed_choice_values.get(target_class_name, {}).items():
        if field_name not in derived_field_set.field_names:
            continue
        merged_choice_values = derived_field_set.choice_values_by_field.get(field_name)
        if merged_choice_values is None:
            continue
        for source_class_name, suppressed_values in suppressed_values_by_source_class.items():
            if source_class_name not in source_class_set:
                continue
            for suppressed_value in suppressed_values:
                merged_choice_values.pop(suppressed_value, None)
                if suppressed_value == "0":
                    derived_field_set.not_applicable_choice_fields.discard(field_name)


def _add_sql_developer_input_domain_choice_label_overrides(derived_field_set: DerivedFieldSet) -> None:
    for field_name, label_overrides in _editable_sqldeveloper_input_domain_choice_label_overrides_by_field().items():
        choice_values = derived_field_set.choice_values_by_field.get(field_name)
        if choice_values is None:
            continue
        for value, label in label_overrides.items():
            if value in choice_values or (value == "0" and field_name in derived_field_set.not_applicable_choice_fields):
                choice_values[value] = label


def _editable_sqldeveloper_input_domain_choice_label_overrides_by_field() -> dict[str, dict[str, str]]:
    """SQLDeveloper input-domain labels that differ from base-domain labels."""

    return {
        "ELGBL_CNTRL_BNK_FNDNG_INDCTR": {
            "0": "Not_applicable_To_be_used_if_central_bank_eligibility_does_not_apply",
        },
        "FVO_DSGNTN": {
            "0": "Not_Applicable",
        },
        "LBLTY_ENCMBRNC_RSDL_MTRTY_BND": {
            "999": "Open_Maturity",
        },
        "LGL_FRM": {
            "AT609": "GesbR_Gesellschaft_des_burgerlichen_Rechts_Partnership_under_civil_code",
        },
        "INSTRMNT_CLLTRL_ASSGNMNT_TYP": {
            "7": "_Reverse_repurchase_transaction_gold_collateral_received_assignment",
        },
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT_TYP": {
            "1": "Long_balance_sheet_recognised_debt_security_position_prudential_portfolio_Accounting_c_9e2c07",
            "2": "Long_balance_sheet_recognised_equity_or_fund_security_position_prudential_portfolio_Ac_80a46c",
        },
        "PRMRY_ISSR_INDCTR": {
            "0": "Not_Applicable",
        },
        "PST_DU_DBT_SCRTY_INDCTR": {
            "0": "Not_Applicable",
        },
        "RNGTTD_DBT_SCRTY_TYP": {
            "0": "Not_Applicable",
        },
        "RSDL_MTRTY_CNTRCT_BND": {
            "999": "Open_Maturity",
        },
        "RTNG_GRD_TYP": {
            "3": "Rating_grade_for_issuer_based_rating_systems_for_non_Central_government",
            "4": "Rating_grade_for_issuer_based_rating_systems_for_Central_government",
        },
        "RPRCHS_TRNSCTN_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN_ASSGNMNT_TYP": {
            "1": "_Reverse_Repurchase_transaction_Non_balance_sheet_recognised_security_position_receive_3a48d4",
        },
        "SCRTY_TYP_BY_IDNTFR": {
            "8": "International_securities_identification_number_ISIN_security",
            "9": "Non_International_securities_identification_number_Non_ISIN_security",
        },
        "TYP_RSK": {
            "0": "Not_Applicable",
        },
    }


def _editable_sqldeveloper_folded_input_domain_choice_values_by_target() -> dict[str, dict[str, dict[str, dict[str, str]]]]:
    """SQLDeveloper input-domain members that are lost as Django LDM metadata.

    These are source metadata bridges, kept in one editable place. They come from
    SQLDeveloper/Xcore input-domain output and Reduce discriminators behavior where
    lower-level subtype members are folded into a higher-level input domain.
    """

    return {
        "INSTRMNT": {
            "INSTRMNT_TYP_PRDCT": {
                "CRDT_CRD_DBT": {"51": "Credit_card_debt"},
                "FNNCL_LS": {"80": "Finance_leases"},
                "RPRCHS_TRNSCTN": {"1003": "Reverse_repurchase_agreement_instrument"},
                "FCTRNG": {"1020": "Factoring"},
                "OTHR_LN": {"1022": "Other_loan"},
                "OTHR_TRD_RCVBL": {"1023": "Other_trade_receivables"},
                "LN_DMND_MNMM_RSRV": {"1201": "Loan_on_demand_used_for_minimum_reserve"},
                "OPN_RPRCHS_TRNSCTN": {"162": "Open_repurchase_agreement_instrument"},
                "TRM_RPRCHS_TRNSCTN": {"163": "Term_repurchase_agreement_instrument"},
            },
        },
        "EXCHNG_TRDBL_DRVTV_PSTN_RL": {
            "ACCNTNG_CLSSFCTN": {
                "NN_BLNC_SHT_RCGNSD_EXCHNG_TRDBL_DRVTV_ASST_PSTN": {
                    "90": "Under_IFRS_9_impairment_Off_balance_sheet_accounting_classification_under_IFRS_9_impairment",
                    "911": "Measured_under_IAS_37_Off_balance_sheet_accounting_classification_measured_under_IAS_37",
                    "912": "Measured_under_IFRS_4_Off_balance_sheet_accounting_classification_measured_under_IFRS_4",
                    "92": "Measured_at_fair_value_through_profit_or_loss_Off_balance_sheet_accounting_classificat_360a76",
                    "93": "Under_nGAAP_Off_balance_sheet_accounting_classification_measured_under_nGAAP_based_on_BAD",
                },
                "NN_BLNC_SHT_RCGNSD_ETD_LBLTY_PSTN": {
                    "90": "Under_IFRS_9_impairment_Off_balance_sheet_accounting_classification_under_IFRS_9_impairment",
                    "911": "Measured_under_IAS_37_Off_balance_sheet_accounting_classification_measured_under_IAS_37",
                    "912": "Measured_under_IFRS_4_Off_balance_sheet_accounting_classification_measured_under_IFRS_4",
                    "92": "Measured_at_fair_value_through_profit_or_loss_Off_balance_sheet_accounting_classificat_360a76",
                    "93": "Under_nGAAP_Off_balance_sheet_accounting_classification_measured_under_nGAAP_based_on_BAD",
                },
            },
        },
        "CLLTRL": {
            "CLLTRL_TYP": {
                "LND_EXCLDNG_AGRCLTR": {"107": "Land_excluding_agriculture"},
                "LND_INCLDNG_AGRCLTR": {"108": "Land_including_agriculture"},
            },
        },
        "BLNC_SHT_RCGNSD_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN": {
            "SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP": {
                "NN_BLNC_SHT_RCGNSD_SCRTY_PSTN": {
                    "2": "Non_Balance_sheet_recognised_security_position",
                },
            },
        },
        "SCRTY_EXCHNG_TRDBL_DRVTV": {
            "SCRTY_EXCHNG_TRDBL_DRVTV_TYP": {
                "EXCHNG_TRDBL_OPTN": {"3": "Exchange_tradable_option"},
                "EXCHNG_TRDBL_FTR": {"4": "Exchange_tradable_future"},
            },
        },
        "NN_FNNCL_LBLTY": {
            "NN_FNNCL_LBLTY_TYP": {
                "OTHR_NN_FNNCL_LBLTY": {
                    "1301": "Non_financial_liabilites_other_than_Tax_liability_Share_capital_repayable_on_demand_or_dfd225",
                },
                "FNDS_GNRL_BNKNG_RSK": {
                    "701": "Provisions_Funds_for_general_banking_risks",
                },
                "OTHR_EMPLY_BNFT": {
                    "702": "Provisions_Employee_benefits_Other_than_pension_and_other_post_employment_defined_bene_258d25",
                },
                "PNSN_OTHR_PST_EMPLYMNT_BNFT_OBLGTN": {
                    "703": "Provisions_Employee_benefits_Pension_and_other_post_employment_defined_benefit_obligations",
                },
                "RSTRCTRNG": {
                    "704": "Provisions_Restructuring",
                },
                "PNDNG_LGL_ISSS_TX_LTGTN": {
                    "705": "Provisions_Pending_legal_issues_and_tax_litigation",
                },
                "OTHR_PRVSN": {
                    "707": "Provisions_Other_than_Employee_benefits_Restructuring_Pending_legal_issues_and_tax_lit_905d67",
                },
                "CRRNT_TX_LBLTY": {
                    "710": "Current_tax_liabilities",
                },
                "DFRRD_TX_LBLTY": {
                    "720": "Deferred_tax_liabilities",
                },
                "SHR_CPTL_RPYBL_DMND": {
                    "730": "Share_capital_repayable_on_demand",
                },
            },
        },
        "NN_FNNCL_ASST": {
            "MSRMNT_MTHD": {
                "INVSTMNT_PRPRTY": {
                    "1": "Cost_model_IAS_17_49",
                    "3": "Revaluation_model_IAS_17_49",
                },
                "PRPRTY_PLNT_EQPMNT": {
                    "1": "Cost_model_IAS_17_49",
                    "3": "Revaluation_model_IAS_17_49",
                },
            },
            "NN_FNNCL_ASST_TYP": {
                "OTHR_NN_FNNCL_ASST": {
                    "1300": "Non_financial_assets_other_than_Goodwill_Tax_asset_Investment_property_Other_intangibl_4aa924",
                },
            },
        },
        "CRDT_FCLTY": {
            "PRFRMNG_FRBRN_EXPSR_UNDR_PRBTN_RCLSSFD_NN_PRFRMNG_INDCTR": {
                "CRDT_FCLTY_RSK_DT": {
                    "1": "Non_performing_prior_to_forbearance",
                    "2": "Not_non_performing_prior_to_forbearance",
                },
            },
        },
    }


def _editable_sqldeveloper_folded_input_domain_suppressed_values_by_target() -> dict[str, dict[str, dict[str, frozenset[str]]]]:
    """Intermediate subtype members removed when SQLDeveloper builds input domains."""

    return {
        "CLLTRL": {
            "CLLTRL_TYP": {
                "RL_ESTT_CLLTRL": frozenset({"82"}),
                "OFFCS_CMMRCL_PRMSS_RLTD_LND_CLLTRL": frozenset({"105"}),
            },
        },
        "BLNC_SHT_RCGNSD_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN": {
            "SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP": {
                "NN_BLNC_SHT_RCGNSD_DBT_SCRTY_PSTN": frozenset({"3"}),
                "NN_BLNC_SHT_RCGNSD_EQTY_FND_SCRTY_PSTN": frozenset({"4"}),
            },
        },
        "SCRTY_EXCHNG_TRDBL_DRVTV": {
            "SCRTY_EXCHNG_TRDBL_DRVTV_TYP": {
                "EXCHNG_TRDBL_DRVTV": frozenset({"1"}),
                "SCRTY": frozenset({"2"}),
            },
            "CRRNCY_TRNSCTN_RPRTD": {
                "SCRTY": frozenset({"0"}),
            },
            "EXCHNG_TRDBL_DRVTV_TYP_BY_IDNTFR": {
                "EXCHNG_TRDBL_DRVTV": frozenset({"0"}),
            },
        },
        "NN_FNNCL_LBLTY": {
            "NN_FNNCL_LBLTY_TYP": {
                "NN_FNNCL_LBLTY": frozenset({"0"}),
                "EMPLY_BNFT": frozenset({"1303"}),
            },
        },
        "NN_FNNCL_ASST": {
            "NN_FNNCL_ASST_TYP": {
                "NN_FNNCL_ASST": frozenset({"0"}),
            },
        },
        "SNTHTC_SCRTSTN": {
            "RSCRTSTN_INDCTR": {
                "SCRTSTN": frozenset({"0"}),
            },
            "SCRTSTN_TYP": {
                "SCRTSTN": frozenset({"0"}),
            },
            "SGNFCNT_RSK_TRNSFR_INDCTR": {
                "SGNFCNT_RSK_TRNSFR_SCRTSTN": frozenset({"0"}),
            },
            "SNTHTC_SCRTSTN_TYP": {
                "SNTHTC_SCRTSTN": frozenset({"0"}),
                "SGNFCNT_RSK_TRNSFR_SCRTSTN": frozenset({"1"}),
                "NT_SGNFCNT_RSK_TRNSFR_SCRTSTN": frozenset({"2"}),
            },
            "STS_SCRTSTN_INDCTR": {
                "SCRTSTN": frozenset({"0"}),
            },
        },
        "TRDTNL_SCRTSTN": {
            "RSCRTSTN_INDCTR": {
                "SCRTSTN": frozenset({"0"}),
            },
            "SCRTSTN_TYP": {
                "SCRTSTN": frozenset({"0"}),
            },
            "SGNFCNT_RSK_TRNSFR_INDCTR": {
                "SGNFCNT_RSK_TRNSFR_SCRTSTN": frozenset({"0"}),
            },
            "STS_SCRTSTN_INDCTR": {
                "SCRTSTN": frozenset({"0"}),
            },
        },
        "DBT_SCRTY_ISSD": {
            "ACCNTNG_STNDRD": {
                "DBT_SCRTY_ISSD_TRDNG_BK": frozenset({"23", "24"}),
            },
        },
        "INSTRMNT_RL": {
            "ACCNTNG_STNDRD": {
                "FR_VLD_BLNC_SHT_RCGNSD_FNNCL_LBLTY_INSTRMNT": frozenset({"46", "47"}),
            },
            "DFLT_STTS": {
                "OFF_BLNC_SHT_ITM_GVN_INSTRMNT": frozenset({"14"}),
            },
        },
        "SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL": {
            "SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL_TYP": {
                "SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL": frozenset({"2"}),
            },
        },
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT": {
            "SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP": {
                "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT": frozenset({"0"}),
            },
        },
        "LNG_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT": {
            "SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP": {
                "LNG_NN_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT": frozenset({"0"}),
            },
        },
        "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT": {
            "AVLBL_ENCMBRNC_INDCTR": {
                "LNG_BLNC_SHT_RCGNSD_DBT_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT": frozenset({"0"}),
            },
            "NN_PRFRMNG_PRR_FRBRNC_INDCTR": {
                "LNG_BLNC_SHT_RCGNSD_DBT_SCRTY_PSTN_PRDNTL_PRTFL_ACCNTNG_CLSSFCTN_ASSGNMNT": frozenset({"0"}),
            },
        },
        "PRTY": {
            "GVRND_CNTRY_CD": {
                "PRTY": frozenset({"0"}),
            },
            "INSTTTNL_SCTR": {
                "PRTY": frozenset({"0"}),
            },
        },
        "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST": {
            "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_TYP": {
                "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE": frozenset({"0"}),
            },
        },
        "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_FNNCL_GRNT": {
            "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_TYP": {
                "TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE": frozenset({"0"}),
            },
        },
    }


def _add_sql_developer_synthetic_choice_values(
    derived_field_set: DerivedFieldSet,
    ldm_module: DjangoModelModule,
) -> None:
    synthetic_choice_values_by_field = _editable_sqldeveloper_synthetic_choice_values_by_field()
    for (source_class_name, source_field_name), output_name in list(derived_field_set.source_field_names.items()):
        if output_name not in derived_field_set.field_names:
            continue
        if output_name in derived_field_set.choice_values_by_field:
            continue
        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        source_field = source_class.fields.get(source_field_name)
        if source_field is None or source_field.choices_name is not None:
            continue
        field_annotations = _sql_developer_field_annotations(source_class, source_field_name)
        choice_values = synthetic_choice_values_by_field.get(source_field_name)
        if choice_values is None:
            continue
        if not _synthetic_choice_metadata_matches_source(field_annotations, choice_values):
            continue
        derived_field_set.choice_values_by_field[output_name] = dict(choice_values)


def _editable_sqldeveloper_synthetic_choice_values_by_field() -> dict[str, dict[str, str]]:
    """SQLDeveloper domains missing as Django choices after CSV-to-Django import.

    These values are source metadata bridges, not EIL-mined fallbacks. The listed
    indicator values come from the SQLDeveloper Reduce discriminators member map
    and BLN_TF domain. The own-company-investment values come from the exported
    SQLDeveloper domain output while the Django LDM currently retains only the
    placeholder DOM3000004 identifier.
    """

    return {
        "LSTD_INDCTR": {
            "0": "Not_applicable",
            "F": "Non_listed",
            "T": "Listed",
        },
        "OWN_CMPNY_INVSTMNT_INDCTR": {
            "0": "Not_applicable",
            "1": "Own_company_investment",
            "2": "Non_own_company_investment",
        },
    }


def _synthetic_choice_metadata_matches_source(field_annotations: dict, choice_values: dict[str, str]) -> bool:
    if "F" in choice_values and field_annotations.get("domain_synonym") == "BLN_TF":
        return True
    return field_annotations.get("domain_id") == "DOM3000004"


def _add_held_for_sale_not_applicable_choice_values(
    derived_field_set: DerivedFieldSet,
    target_class_name: str,
) -> None:
    if target_class_name not in _editable_sqldeveloper_held_for_sale_input_domain_targets():
        return
    choice_values = derived_field_set.choice_values_by_field.get("HLD_SL_INDCTR")
    if choice_values is None:
        return
    choice_values.setdefault("0", "Not_applicable")


def _editable_sqldeveloper_held_for_sale_input_domain_targets() -> frozenset[str]:
    """Targets where SQLDeveloper turns held-for-sale into an input domain."""

    return frozenset(
        {
            "INVSTMNT_PRPRTY_TKN_PSSSSN",
            "PRPRTY_PLNT_EQPMNT_TKN_PSSSSN",
            "RPRCHS_TRNSCTN_GLD_GVN_ASSGNMNT",
        }
    )


def _editable_sqldeveloper_input_domain_not_applicable_synonyms() -> frozenset[str]:
    """Domains where SQLDeveloper creates an input-domain variant while folding.

    SQLDeveloper's "Amend columns domain and remove duplicated columns" step
    works on relational column mappings that are not fully present in the Django
    LDM. Keep the affected domain synonyms explicit and editable here while the
    application still gates them through source-field metadata.
    """

    return frozenset(
        {
            "CMMRCL_RL_ESTT_LN_INDCTR",
            "DRVD_DFLT_STTS",
            "OBSRVD_AGNT_INDCTR_ANCRDT_RPRTNG",
            "PRTCTN_VL_TYP",
            "RSDL_MTRTY_BND",
            "TM_PST_DU_BND",
            "TM_SNC_INTL_RCGNTN",
            "TRNSFR_IMPRMNT_STGS_F_12.02_TRNSFR_IMPRMNT_STGS_REF",
            "TRNSFR_IMPRMNT_STGS_F_12_02_TRNSFR_IMPRMNT_STGS_REF",
        }
    )


def _source_field_domain_synonyms(
    source_class: ModelClass,
    source_field: ModelStatement,
    source_field_name: str,
) -> set[str]:
    field_annotations = _sql_developer_field_annotations(source_class, source_field_name)
    candidates = {
        field_annotations.get("domain_synonym"),
        field_annotations.get("domain_field_name"),
        source_field.choices_name,
        source_field_name,
    }
    normalized_candidates: set[str] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        normalized = str(candidate).removesuffix("_domain")
        normalized_candidates.add(normalized)
        normalized_candidates.add(normalized.replace(".", "_"))
    return normalized_candidates


def _is_folded_classifier_foreign_key_field(source_class: ModelClass, source_field_name: str) -> bool:
    field_annotations = _sql_developer_field_annotations(source_class, source_field_name)
    if field_annotations.get("primary_key") is True:
        return False
    if field_annotations.get("foreign_key") is True:
        return True
    for foreign_key in _sql_developer_foreign_keys(source_class):
        if foreign_key.get("identifying") == "Y":
            continue
        if _foreign_key_contains_field(foreign_key, source_field_name):
            return True
    return False


def _is_sql_developer_input_domain_folded_source(
    target_class_name: str,
    source_class_name: str,
    domain_synonyms: set[str],
) -> bool:
    if not source_class_name.endswith(("_DRVD_DT", "_RSK_DT")):
        return False
    if target_class_name.endswith("_ASSGNMNT") and "PRTCTN_VL_TYP" in domain_synonyms:
        return False
    if (
        (target_class_name.endswith("_TKN_PSSSSN") or target_class_name.endswith("_TKN_PSSSN"))
        and (source_class_name.endswith("_TKN_PSSSSN_DRVD_DT") or source_class_name.endswith("_TKN_PSSSN_DRVD_DT"))
    ):
        return False
    return True


def _add_accounting_context_not_applicable_choice_values(
    derived_field_set: DerivedFieldSet,
    target_class_name: str,
) -> None:
    base_domain_fields = _editable_sqldeveloper_accounting_context_base_domain_fields_by_target()
    for field_name in derived_field_set.choice_values_by_field:
        if field_name in {"ACCNTNG_CNSLDTN_LVL", "ACCNTNG_STNDRD"}:
            if field_name in base_domain_fields.get(target_class_name, frozenset()):
                derived_field_set.not_applicable_choice_fields.discard(field_name)
                continue
            derived_field_set.not_applicable_choice_fields.add(field_name)


def _editable_sqldeveloper_accounting_context_base_domain_fields_by_target() -> dict[str, frozenset[str]]:
    """Accounting context fields where SQLDeveloper keeps the base domain.

    Most folded accounting context columns become input domains with 0: Not
    applicable. These target fields remain on ACCNTNG_* base domains, so the
    synthetic input-domain 0 should not be rendered.
    """

    context_fields = frozenset({"ACCNTNG_CNSLDTN_LVL", "ACCNTNG_STNDRD"})
    return {
        "CSH_HND": context_fields,
        "FR_VL_DCRS_CNTNGNT_ENCMBRNC": context_fields,
        "INTRST_RT_RSK_HDG_PRTFL": context_fields,
        "KB_PR_BCKT_DRVD_DT": context_fields,
        "MSTR_AGRMNT": context_fields,
        "SGNFCNT_CRRNCY_DPRCTN_CNTNGNT_ENCMBRNC": context_fields,
    }


def _add_directional_role_not_applicable_choice_values(derived_field_set: DerivedFieldSet) -> None:
    for field_name, choice_values in derived_field_set.choice_values_by_field.items():
        if not field_name.endswith("_RL_TYP"):
            continue
        normalized_labels = {_normalize_sql_developer_entity_name(label) for label in choice_values.values()}
        if not normalized_labels:
            continue
        if not all("received" in label or "given" in label for label in normalized_labels):
            continue
        has_received = any("received" in label for label in normalized_labels)
        has_given = any("given" in label for label in normalized_labels)
        if has_received and has_given:
            derived_field_set.not_applicable_choice_fields.add(field_name)


def _base_reduced_discriminator_adds_not_applicable(
    field_name: str,
    base_class_name: str,
    base_source_classes: list[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    for source_class_name in base_source_classes:
        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        for source_field_name, source_field in source_class.fields.items():
            if source_field.choices_name is None:
                continue
            source_field_candidates = _field_name_candidates(
                source_field_name,
                base_class_name,
                source_class_name,
                target_classes,
            )
            if field_name not in source_field_candidates:
                continue
            if _should_add_not_applicable_to_choice_field(
                source_class_name=source_class_name,
                field_name=source_field_name,
                ldm_module=ldm_module,
                graph=graph,
            ):
                return True
            if _should_add_not_applicable_to_optional_identifying_fk_output(
                output_name=field_name,
                source_class=source_class,
                source_field_name=source_field_name,
                ldm_source_classes=base_source_classes,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            ):
                return True
    return False


def _is_relationship_copy_reduced_discriminator_component(
    source_class: ModelClass,
    source_field: ModelStatement,
    source_field_name: str,
    output_name: str,
    base_class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> bool:
    if not _source_field_domain_matches_output_name(source_class, source_field, source_field_name, output_name):
        return False

    for foreign_key in _sql_developer_foreign_keys(source_class):
        if foreign_key.get("identifying") != "Y":
            continue
        if not _foreign_key_contains_field(foreign_key, source_field_name):
            continue
        referenced_class = foreign_key.get("referenced_class")
        if isinstance(referenced_class, str) and _class_is_or_descends_from(referenced_class, base_class_name, graph):
            return True
    return False


def _source_field_domain_matches_output_name(
    source_class: ModelClass,
    source_field: ModelStatement,
    source_field_name: str,
    output_name: str,
) -> bool:
    normalized_output_name = _normalize_sql_developer_entity_name(output_name)
    field_annotations = _sql_developer_field_annotations(source_class, source_field_name)
    for candidate in (
        field_annotations.get("domain_synonym"),
        field_annotations.get("domain_name"),
        field_annotations.get("domain_field_name"),
        source_field.choices_name,
        source_field_name,
    ):
        if not candidate:
            continue
        candidate_name = str(candidate).removesuffix("_domain")
        if _normalize_sql_developer_entity_name(candidate_name) == normalized_output_name:
            return True
    return False


def _class_is_or_descends_from(
    class_name: str,
    ancestor_class_name: str,
    graph: _ClassGraph,
) -> bool:
    return class_name == ancestor_class_name or ancestor_class_name in graph.ancestors(class_name)


def _target_is_reduced_discriminator_leaf(
    target_class_name: str,
    base_class_name: str,
    graph: _ClassGraph,
) -> bool:
    return target_class_name != base_class_name and base_class_name in graph.ancestors(target_class_name)


def _reduced_discriminator_leaf_choice_values(
    field_name: str,
    base_class_name: str,
    restrict_source_members_to_base_hierarchy: bool,
    ldm_source_classes: list[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
    include_source_class_members: bool,
) -> dict[str, str]:
    source_class_set = set(ldm_source_classes)
    choice_values: dict[str, str] = {}
    for leaf_class_name in _hierarchy_leaf_descendants(base_class_name, graph):
        if _has_folded_reduced_discriminator_successor(
            class_name=leaf_class_name,
            field_name=field_name,
            source_class_set=source_class_set,
            ldm_module=ldm_module,
            graph=graph,
            target_classes=target_classes,
        ):
            continue
        member = _entity_member_for_class_for_discriminator(
            class_name=leaf_class_name,
            field_name=field_name,
            ldm_module=ldm_module,
            graph=graph,
        )
        if member is None:
            member = _entity_member_for_class_from_any_choice(leaf_class_name, ldm_module, graph)
        if member is None:
            continue
        value, label = member
        choice_values[value] = label
    if include_source_class_members and (field_name.endswith("_TYP") or field_name.endswith("_INDCTR")):
        for value, label in _annotated_source_reduced_discriminator_choice_values(
            field_name=field_name,
            base_class_name=base_class_name,
            restrict_to_base_hierarchy=restrict_source_members_to_base_hierarchy,
            ldm_source_classes=ldm_source_classes,
            ldm_module=ldm_module,
            graph=graph,
        ).items():
            choice_values.setdefault(value, label)
    return choice_values


def _annotated_reduced_discriminator_base_class(
    field_name: str,
    ldm_source_classes: list[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> str | None:
    source_class_set = set(ldm_source_classes)
    for source_class_name in ldm_source_classes:
        if not graph.children.get(source_class_name):
            continue
        for descendant_name in graph.folded_descendants(source_class_name, target_classes=set()):
            if descendant_name not in source_class_set:
                continue
            descendant_class = ldm_module.classes.get(descendant_name)
            if descendant_class is None:
                continue
            if _annotated_member_discriminator_matches_field(
                _entity_member_annotation(descendant_class),
                field_name,
            ):
                return source_class_name
    return None


def _entity_member_for_class_for_discriminator(
    class_name: str,
    field_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> tuple[str, str] | None:
    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return None

    entity_member = _entity_member_annotation(model_class)
    if entity_member and _annotated_member_discriminator_matches_field(entity_member, field_name):
        annotated_member = _annotated_entity_member_for_class(model_class)
        if annotated_member is not None:
            return annotated_member

    logical_names = _model_class_logical_names(class_name, model_class)
    manual_member = _manual_entity_member_for_logical_names(logical_names, ldm_module)
    if manual_member is not None:
        return manual_member

    annotated_member = _annotated_entity_member_for_class(model_class)
    if annotated_member is not None:
        return annotated_member

    return _automatic_entity_member_for_class(class_name, logical_names, ldm_module, graph)


def _entity_member_annotation(model_class: ModelClass) -> dict:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    entity_member = sql_developer_annotations.get("entity_member", {})
    return entity_member if isinstance(entity_member, dict) else {}


def _annotated_member_discriminator_is_not_merged(
    entity_member: dict,
    class_name: str,
    ldm_module: DjangoModelModule,
) -> bool:
    for discriminator_name in (
        entity_member.get("discriminator_field"),
        entity_member.get("domain_synonym"),
        entity_member.get("domain_name"),
    ):
        if discriminator_name and _is_sql_developer_discriminator_not_merged(
            str(discriminator_name),
            class_name,
            ldm_module,
        ):
            return True
    return False


def _annotated_member_discriminator_matches_field(entity_member: dict, field_name: str) -> bool:
    return any(
        discriminator_name
        and _normalize_sql_developer_entity_name(str(discriminator_name)) == _normalize_sql_developer_entity_name(field_name)
        for discriminator_name in (
            entity_member.get("discriminator_field"),
            entity_member.get("domain_synonym"),
            entity_member.get("domain_name"),
        )
    )


def _annotated_source_reduced_discriminator_choice_values(
    field_name: str,
    base_class_name: str,
    restrict_to_base_hierarchy: bool,
    ldm_source_classes: list[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> dict[str, str]:
    source_class_set = set(ldm_source_classes)
    choice_values: dict[str, str] = {}
    for source_class_name in ldm_source_classes:
        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        if (
            restrict_to_base_hierarchy
            and source_class_name != base_class_name
            and base_class_name not in graph.ancestors(source_class_name)
        ):
            continue
        if not _has_reducible_annotated_entity_member(
            class_name=source_class_name,
            model_class=source_class,
            field_name=field_name,
            source_class_set=source_class_set,
            ldm_module=ldm_module,
            graph=graph,
        ):
            continue
        if _has_reducible_annotated_source_successor(
            class_name=source_class_name,
            field_name=field_name,
            source_class_set=source_class_set,
            ldm_module=ldm_module,
            graph=graph,
        ):
            continue
        member = _annotated_entity_member_for_class(source_class)
        if member is None:
            continue
        value, label = member
        choice_values[value] = label
    return choice_values


def _has_reducible_annotated_entity_member(
    class_name: str,
    model_class: ModelClass,
    field_name: str,
    source_class_set: set[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> bool:
    entity_member = _entity_member_annotation(model_class)
    if not entity_member:
        return False
    if not any(entity_member.get(key) for key in ("discriminator_field", "domain_synonym", "domain_name")):
        return False
    if _annotated_member_discriminator_matches_field(entity_member, field_name):
        return False
    if _annotated_member_discriminator_is_not_merged(entity_member, class_name, ldm_module):
        return False
    return not _has_not_merged_annotated_source_predecessor(
        class_name=class_name,
        field_name=field_name,
        source_class_set=source_class_set,
        ldm_module=ldm_module,
        graph=graph,
    )


def _has_not_merged_annotated_source_predecessor(
    class_name: str,
    field_name: str,
    source_class_set: set[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> bool:
    pending = list(graph.ancestors(class_name))
    for ancestor_name in [class_name, *graph.ancestors(class_name)]:
        pending.extend(graph.delegate_owners.get(ancestor_name, []))
    seen: set[str] = set()
    while pending:
        predecessor_name = pending.pop(0)
        if predecessor_name in seen:
            continue
        seen.add(predecessor_name)
        predecessor_class = ldm_module.classes.get(predecessor_name)
        if predecessor_class is None:
            continue
        if predecessor_name in source_class_set and _is_sql_developer_discriminator_not_merged(
            predecessor_name,
            predecessor_name,
            ldm_module,
        ):
            return True
        if predecessor_name in source_class_set:
            entity_member = _entity_member_annotation(predecessor_class)
            if (
                entity_member
                and not _annotated_member_discriminator_matches_field(entity_member, field_name)
                and _annotated_member_discriminator_is_not_merged(entity_member, predecessor_name, ldm_module)
            ):
                return True
        pending.extend(graph.ancestors(predecessor_name))
        pending.extend(graph.delegate_owners.get(predecessor_name, []))
    return False


def _has_reducible_annotated_source_successor(
    class_name: str,
    field_name: str,
    source_class_set: set[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> bool:
    for successor_name in _reducible_annotated_source_successors(class_name, source_class_set, ldm_module, graph):
        successor_class = ldm_module.classes.get(successor_name)
        if successor_class is None:
            continue
        if _has_reducible_annotated_entity_member(
            class_name=successor_name,
            model_class=successor_class,
            field_name=field_name,
            source_class_set=source_class_set,
            ldm_module=ldm_module,
            graph=graph,
        ):
            return True
    return False


def _reducible_annotated_source_successors(
    class_name: str,
    source_class_set: set[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> list[str]:
    successors: list[str] = []
    pending = list(graph.children.get(class_name, []))
    source_class = ldm_module.classes.get(class_name)
    if source_class is not None:
        for field in source_class.fields.values():
            if field.related_model and field.name.endswith("_delegate"):
                pending.append(field.related_model)
    seen: set[str] = set()
    while pending:
        successor_name = pending.pop(0)
        if successor_name in seen:
            continue
        seen.add(successor_name)
        if successor_name in source_class_set:
            successors.append(successor_name)
        pending.extend(graph.children.get(successor_name, []))
    return successors


def _has_folded_reduced_discriminator_successor(
    class_name: str,
    field_name: str,
    source_class_set: set[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    for successor_name in graph.identifying_extensions.get(class_name, []):
        if successor_name.endswith(("_DRVD_DT", "_RSK_DT")):
            if _has_reducible_annotated_source_successor(
                class_name=class_name,
                field_name=field_name,
                source_class_set=source_class_set,
                ldm_module=ldm_module,
                graph=graph,
            ):
                return True
            continue
        if successor_name in source_class_set and successor_name not in target_classes:
            return True
    for successor_name in graph.delegate_owners.get(class_name, []):
        if successor_name in source_class_set and successor_name not in target_classes:
            return True
    return False


def _not_merged_discriminator_base_class(
    field_name: str,
    ldm_module: DjangoModelModule,
) -> str | None:
    normalized_field_name = _normalize_sql_developer_entity_name(field_name)
    for field_candidate, base_candidate in _editable_sqldeveloper_discriminators_not_merged():
        if normalized_field_name != _normalize_sql_developer_entity_name(field_candidate):
            continue
        return _class_name_for_sql_developer_name(base_candidate, ldm_module)
    return None


def _class_name_for_sql_developer_name(
    sql_developer_name: str,
    ldm_module: DjangoModelModule,
) -> str | None:
    normalized_name = _normalize_sql_developer_entity_name(sql_developer_name)
    for class_name, model_class in ldm_module.classes.items():
        if any(
            _normalize_sql_developer_entity_name(logical_name) == normalized_name
            for logical_name in _model_class_logical_names(class_name, model_class)
        ):
            return class_name
    return None


def _not_merged_discriminator_leaf_choice_values(
    base_class_name: str,
    current_choice_values: dict[str, str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> dict[str, str]:
    choice_values: dict[str, str] = {}
    for leaf_class_name in _hierarchy_leaf_descendants(base_class_name, graph):
        member = _current_choice_member_for_class(leaf_class_name, current_choice_values, ldm_module)
        if member is None:
            member = _entity_member_for_class_from_any_choice(leaf_class_name, ldm_module, graph)
        if member is None:
            continue
        value, label = member
        choice_values[value] = label
    return choice_values


def _entity_member_for_class_from_any_choice(
    class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> tuple[str, str] | None:
    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return None
    annotated_member = _annotated_entity_member_for_class(model_class)
    if annotated_member is not None:
        return annotated_member
    logical_names = _model_class_logical_names(class_name, model_class)
    manual_member = _manual_entity_member_for_logical_names(logical_names, ldm_module)
    if manual_member is not None:
        return manual_member
    return _automatic_entity_member_for_class(
        class_name,
        logical_names,
        ldm_module,
        graph,
        discriminator_fields_only=False,
    )


def _current_choice_member_for_class(
    class_name: str,
    current_choice_values: dict[str, str],
    ldm_module: DjangoModelModule,
) -> tuple[str, str] | None:
    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return None
    normalized_logical_names = {
        _normalize_sql_developer_entity_name(logical_name)
        for logical_name in _model_class_logical_names(class_name, model_class)
    }
    for value, label in current_choice_values.items():
        if _normalize_sql_developer_entity_name(label) in normalized_logical_names:
            return value, label
    return None


def _reduced_discriminator_base_class(
    field_name: str,
    target_class_name: str,
    ldm_module: DjangoModelModule,
) -> str | None:
    if field_name.endswith("ENTTY_RL_TYP") and "ENTTY_RL" in ldm_module.classes:
        return "ENTTY_RL"

    explicit_suffix_bases = {
        "CLLTRL_RL_TYP": "CLLTRL_RL",
        "EXCHNG_TRDBL_DRVTV_PSTN_RL_TYP": "EXCHNG_TRDBL_DRVTV_PSTN_RL",
        "INSTRMNT_RL_TYP": "INSTRMNT_RL",
        "PRTCTN_ARRNGMNT_RL_TYP": "PRTCTN_ARRNGMNT_RL",
    }
    for suffix, class_name in explicit_suffix_bases.items():
        if field_name.endswith(suffix) and class_name in ldm_module.classes:
            return class_name

    if field_name == f"{target_class_name}_TYP":
        return target_class_name

    if field_name.endswith("_TYP"):
        candidate_class_name = field_name[: -len("_TYP")]
        if candidate_class_name in ldm_module.classes:
            return candidate_class_name
    return None


def _hierarchy_leaf_member_choice_values(
    base_class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> dict[str, str]:
    choice_values: dict[str, str] = {}
    for leaf_class_name in _hierarchy_leaf_descendants(base_class_name, graph):
        member = _entity_member_for_class(leaf_class_name, ldm_module, graph)
        if member is None:
            continue
        value, label = member
        choice_values[value] = label
    return choice_values


def _hierarchy_leaf_descendants(base_class_name: str, graph: _ClassGraph) -> list[str]:
    leaves: list[str] = []

    def visit(class_name: str) -> None:
        children = graph.children.get(class_name, [])
        if not children:
            if class_name != base_class_name:
                leaves.append(class_name)
            return
        for child_name in children:
            visit(child_name)

    visit(base_class_name)
    return leaves


def _entity_member_for_class(
    class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> tuple[str, str] | None:
    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return None
    annotated_member = _annotated_entity_member_for_class(model_class)
    if annotated_member is not None:
        return annotated_member
    logical_names = _model_class_logical_names(class_name, model_class)
    manual_member = _manual_entity_member_for_logical_names(logical_names, ldm_module)
    if manual_member is not None:
        return manual_member
    return _automatic_entity_member_for_class(class_name, logical_names, ldm_module, graph)


def _annotated_entity_member_for_class(model_class: ModelClass) -> tuple[str, str] | None:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    entity_member = sql_developer_annotations.get("entity_member", {})
    if not isinstance(entity_member, dict):
        return None
    value = entity_member.get("member_code") or entity_member.get("value")
    label = (
        entity_member.get("member_label")
        or entity_member.get("label")
        or entity_member.get("member_description")
        or entity_member.get("source_member_description")
    )
    if value is None or label is None:
        return None
    return str(value), _sanitize_choice_label(str(label))


def _manual_entity_member_for_logical_names(
    logical_names: tuple[str, ...],
    ldm_module: DjangoModelModule,
) -> tuple[str, str] | None:
    manual_map = _editable_sqldeveloper_entity_member_map()
    normalized_logical_names = {_normalize_sql_developer_entity_name(name) for name in logical_names}
    for entity_name, member in manual_map.items():
        if _normalize_sql_developer_entity_name(entity_name) not in normalized_logical_names:
            continue
        value, label = member
        return value, _choice_label_for_manual_entity_member(value, label, ldm_module)
    return None


def _choice_label_for_manual_entity_member(value: str, label: str, ldm_module: DjangoModelModule) -> str:
    normalized_label = _normalize_sql_developer_entity_name(label)
    for model_class in ldm_module.classes.values():
        for choice_statement in model_class.choices.values():
            for candidate_value, candidate_label in _literal_choice_values(choice_statement.source).items():
                if candidate_value != value:
                    continue
                if _normalize_sql_developer_entity_name(candidate_label) == normalized_label:
                    return candidate_label
    return _sanitize_choice_label(label)


def _automatic_entity_member_for_class(
    class_name: str,
    logical_names: tuple[str, ...],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    discriminator_fields_only: bool = True,
) -> tuple[str, str] | None:
    normalized_logical_names = {_normalize_sql_developer_entity_name(name) for name in logical_names}
    candidate_classes = [class_name]
    candidate_classes.extend(reversed(graph.ancestors(class_name)))
    for candidate_class_name in candidate_classes:
        candidate_class = ldm_module.classes.get(candidate_class_name)
        if candidate_class is None:
            continue
        for field in candidate_class.fields.values():
            if field.choices_name is None:
                continue
            if discriminator_fields_only and not (field.name.endswith("_TYP") or field.name.endswith("_INDCTR")):
                continue
            choice_statement = _choice_statement_for_field(candidate_class, field)
            if choice_statement is None:
                continue
            for value, label in _literal_choice_values(choice_statement.source).items():
                if _normalize_sql_developer_entity_name(label) in normalized_logical_names:
                    return value, label
    return None


def _sanitize_choice_label(label: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", label.replace("\u00a0", " ")).strip("_")


def _editable_sqldeveloper_entity_member_map() -> dict[str, tuple[str, str]]:
    """Manual entity-to-discriminator-member map copied from SQLDeveloper Reduce discriminators."""

    return {
        "Non-financial asset": ("1302", "Non-financial liabilites"),
        "Non-financial liability": ("400", "Non-financial assets"),
        "Asset": ("499", "All assets"),
        "Banking book": ("2", "Non-trading book"),
        "Central counterparty client": ("26", "Central counterparty client"),
        "Credit risk mitigation arragement": ("6", "Credit risk mitigation arrangement"),
        "Currency collateral": ("77", "Currency"),
        "Current tax liability": ("710", "Current tax liabilities"),
        "Deferred tax liability": ("720", "Deferred tax liabilities"),
        "Deposit collateral": ("78", "Deposit"),
        "Deposit taking corporation": ("27", "Deposit taking corporation"),
        "Deposit with agreed maturity": (
            "522",
            "Deposits with agreed maturity - other than counterpart liability to non-derecognised loans",
        ),
        "Financial asset": ("40", "Financial instruments. Creditor"),
        "Financial guarantee instrument covering a Debt security": (
            "13",
            "Financial guarantee instrument for a Debt security",
        ),
        "Financial guarantee instrument not covering a Debt security": (
            "14",
            "Financial guarantee instrument not for a Debt security",
        ),
        "Financial guarantee protection item": ("74", "Financial guarantee"),
        "Financial lease": ("80", "Finance leases"),
        "Financial liability": ("1100", "Financial instruments. Debtor"),
        "Funds for general banking risk": ("701", "Provisions. Funds for general banking risks"),
        "Gold collateral": ("13", "Gold"),
        "Goodwill": ("420", "Intangible assets. Goodwill"),
        "Graded rating system": ("5", "Graded Rating System"),
        "International organisation or General government": ("24", "International organisation or general government"),
        "Investment property": ("413", "Tangible assets. Investment property"),
        "Liability": ("749", "All liabilities"),
        "Loan collateral": ("16", "Loans"),
        "Machinery and equipment collateral": ("85", "Machinery and equiptment collateral"),
        "Non-central government rating system": ("4", "Non-Central government rating system"),
        "Non-financial liabilty": ("1302", "Non-financial liabilites"),
        "Off-balance instrument": ("948", "Off balance sheet instruments"),
        "Original lender": ("21", "Original lender"),
        "Other deposit": ("551", "Other deposits not part of minimum reserve system IMF purposes"),
        "Other employee benefit": (
            "702",
            "Provisions. Employee benefits. Other than pension and other post-employment defined benefit obligations",
        ),
        "Other financial collateral": ("72", "Other financial protection"),
        "Other intangible asset": ("430", "Intangible assets other than Goodwill"),
        "Other non-financial asset": (
            "1300",
            "Non-financial assets other than Goodwill, Tax asset, Investment property, Other intangible asset or Property, plant and equipment",
        ),
        "Other non-financial liability": (
            "1301",
            "Non-financial liabilites other than Tax liability, Share capital repayable on demand or Provision",
        ),
        "Other over the counter (OTC) Derivative instrument": ("5", "Other OTC Derivative instrument"),
        "Other over the counter (OTC) Swap": ("8", "Other OTC Swap"),
        "Other provision": (
            "707",
            "Provisions. Other than Employee benefits, Restructuring, Pending legal issues and tax litigation, Off-balance sheet exposures subject to credit risk",
        ),
        "Other loans": ("1022", "Other loans"),
        "Over the counter (OTC) Credit default swap": ("7", "OTC Credit default swap"),
        "Over the counter (OTC) Credit spread option": ("9", "OTC Credit spread option"),
        "Over the counter (OTC) Forward": ("380", "Forward"),
        "Over the counter (OTC) Option": ("390", "Option"),
        "Over the counter (OTC) Option other than Over the counter (OTC) Credit spread option": (
            "10",
            "OTC Option other than OTC Credit spread option",
        ),
        "Over the counter (OTC) Swap": ("370", "Swap"),
        "Over the counter (OTC) Total return swap": ("6", "OTC Total return swap"),
        "Pending legal issues and tax litigation": ("705", "Provisions. Pending legal issues and tax litigation"),
        "Pension and other post-employment defined benefit obligation": (
            "703",
            "Provisions. Employee benefits. Pension and other post-employment defined benefit obligations",
        ),
        "Property, plant and equipment": ("416", "Tangible assets. Property, plant and equipment"),
        "Protection provider": ("24", "Protection provider"),
        "Rating grade for issuer based rating system for central government": (
            "4",
            "Rating grade for issuer based rating systems for Central government",
        ),
        "Rating grade for issuer based rating system for non-central government": (
            "3",
            "Rating grade for issuer based rating systems for non-Central government",
        ),
        "Reporting agent internal group role": ("1", "Reporting agent group"),
        "Restructuring": ("704", "Provisions. Restructuring"),
        "Security collateral": ("12", "Securities"),
        "Suspense item": ("130", "Suspence items"),
        "Swap provider": ("23", "Swap provider"),
        "Tax liability": ("721", "Tax liabilities"),
        "Traditional securitisation": ("1", "Traditional securititsation"),
        "Transferable deposit": ("511", "Tranferable deposit"),
        "Other overnight deposit": ("512", "Other overnight deposits"),
        "Subsidiary (of the reporting agent)": ("30", "Subsidiary"),
        "Joint venture (of the reporting agent)": ("31", "Joint venture"),
        "Associate (of the reporting agent)": ("32", "Associate"),
        "Trade receivable collateral": ("17", "Trade receivables"),
        "Life insurance policy pledged collateral": ("2", "Life insurance policies pledged"),
        "Listed central bank and private sector company": ("T", "Listed"),
        "Non-listed central bank and private sector company": ("F", "Non-listed"),
        "Long security position banking book assignment": ("5", "Long security position banking book assignment"),
        "Long security position trading book assignment": ("6", "Long security position trading book assignment"),
    }


def _should_add_not_applicable_to_choice_field(
    source_class_name: str,
    field_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> bool:
    """Mirror SQLDeveloper's addNotApplicable rule from the LDM structure we retain."""

    source_class = ldm_module.classes.get(source_class_name)
    if source_class is None:
        return False
    source_field = source_class.fields.get(field_name)
    if source_field is None or source_field.choices_name is None:
        return False
    if _sql_developer_ignores_attribute_inheritance(source_class):
        return False
    if _sql_developer_bool_annotation(
        _sql_developer_field_annotations(source_class, field_name),
        "add_not_applicable_candidate",
        "add_not_applicable_when_forward_engineered",
        "hierarchy_sibling_missing_field",
    ):
        return True
    return (
        _hierarchy_sibling_level_lacks_field(source_class_name, field_name, ldm_module, graph)
        or _has_optional_identifying_one_to_one_annotation(source_class_name, source_class, ldm_module)
    )


def _hierarchy_sibling_level_lacks_field(
    source_class_name: str,
    field_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> bool:
    current_class_name = source_class_name
    while current_class_name in ldm_module.classes:
        parent_class_name = _direct_model_parent(current_class_name, ldm_module)
        if parent_class_name is None:
            return False
        for sibling_class_name in graph.children.get(parent_class_name, []):
            sibling_class = ldm_module.classes.get(sibling_class_name)
            if sibling_class is not None and field_name not in sibling_class.fields:
                return True
        current_class_name = parent_class_name
    return False


def _has_optional_identifying_one_to_one_annotation(
    source_class_name: str,
    source_class: ModelClass,
    ldm_module: DjangoModelModule,
) -> bool:
    foreign_keys = _sql_developer_foreign_keys(source_class)
    if len(foreign_keys) != 1:
        return False
    foreign_key = foreign_keys[0]
    if foreign_key.get("identifying") != "Y":
        return False
    if not _sql_developer_bool_annotation(foreign_key, "one_to_one", "is_one_to_one"):
        return False
    is_optional = _sql_developer_bool_annotation(
        foreign_key,
        "optional_source",
        "source_optional",
        "is_optional_source",
        "optional",
    )
    is_subtype = _direct_model_parent(source_class_name, ldm_module) is not None
    return is_optional or is_subtype


def _is_optional_identifying_foreign_key_component(source_class: ModelClass, field_name: str) -> bool:
    for foreign_key in _sql_developer_foreign_keys(source_class):
        if foreign_key.get("identifying") != "Y":
            continue
        if not _sql_developer_bool_annotation(
            foreign_key,
            "optional_source",
            "source_optional",
            "is_optional_source",
            "optional",
        ):
            continue
        fields = foreign_key.get("fields", [])
        if isinstance(fields, list) and field_name in fields:
            return True
    return False


def _should_add_not_applicable_to_optional_identifying_fk_output(
    output_name: str,
    source_class: ModelClass,
    source_field_name: str,
    ldm_source_classes: list[str],
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    if output_name.endswith("ENTTY_RL_TYP") and _is_related_optional_entity_role_key_component(
        output_name=output_name,
        source_class=source_class,
        source_field_name=source_field_name,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    ):
        return True
    if not _is_optional_identifying_foreign_key_component(source_class, source_field_name):
        return False
    if _is_model_context_foreign_key_component(source_class, source_field_name):
        return len(set(ldm_source_classes)) > 1
    if _is_related_model_context_key_component(
        output_name=output_name,
        source_class=source_class,
        source_field_name=source_field_name,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    ):
        return True
    return output_name.endswith("ENTTY_RL_TYP")


def _is_related_optional_entity_role_key_component(
    output_name: str,
    source_class: ModelClass,
    source_field_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    source_field = source_class.fields.get(source_field_name)
    if source_field is None or source_field.choices_name is None:
        return False

    for foreign_key in _sql_developer_foreign_keys(source_class):
        if not _foreign_key_contains_field(foreign_key, source_field_name):
            continue
        for related_class_name in (foreign_key.get("source_class"), foreign_key.get("referenced_class")):
            if not isinstance(related_class_name, str) or related_class_name not in ldm_module.classes:
                continue
            for related_source_name in graph.forward_engineering_source_classes(related_class_name, target_classes):
                related_source = ldm_module.classes.get(related_source_name)
                if related_source is None or related_source.name == source_class.name:
                    continue
                if _class_has_optional_matching_entity_role_component(
                    output_name=output_name,
                    source_choices_name=source_field.choices_name,
                    related_target_name=related_class_name,
                    related_source=related_source,
                    target_classes=target_classes,
                ):
                    return True
    return False


def _class_has_optional_matching_entity_role_component(
    output_name: str,
    source_choices_name: str,
    related_target_name: str,
    related_source: ModelClass,
    target_classes: set[str],
) -> bool:
    for related_field in related_source.fields.values():
        if related_field.choices_name != source_choices_name:
            continue
        if not _is_optional_identifying_foreign_key_component(related_source, related_field.name):
            continue
        candidates = _field_name_candidates(
            related_field.name,
            related_target_name,
            related_source.name,
            target_classes,
        )
        if output_name in candidates:
            return True
    return False


def _is_related_model_context_key_component(
    output_name: str,
    source_class: ModelClass,
    source_field_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    source_field = source_class.fields.get(source_field_name)
    if source_field is None or source_field.choices_name is None:
        return False

    for foreign_key in _sql_developer_foreign_keys(source_class):
        if not _foreign_key_contains_field(foreign_key, source_field_name):
            continue
        for related_class_name in (foreign_key.get("source_class"), foreign_key.get("referenced_class")):
            if not isinstance(related_class_name, str) or related_class_name not in ldm_module.classes:
                continue
            for context_class_name in [*graph.ancestors(related_class_name), related_class_name]:
                related_class = ldm_module.classes.get(context_class_name)
                if related_class is None or related_class.name == source_class.name:
                    continue
                if _class_model_context_key_has_matching_component(
                    output_name=output_name,
                    source_choices_name=source_field.choices_name,
                    related_class=related_class,
                    target_classes=target_classes,
                ):
                    return True
    return False


def _class_model_context_key_has_matching_component(
    output_name: str,
    source_choices_name: str,
    related_class: ModelClass,
    target_classes: set[str],
) -> bool:
    for foreign_key in _sql_developer_foreign_keys(related_class):
        if not _is_model_context_foreign_key(foreign_key):
            continue
        fields = foreign_key.get("fields", [])
        if not isinstance(fields, list):
            continue
        for related_field_name in fields:
            related_field = related_class.fields.get(related_field_name)
            if related_field is None or related_field.choices_name != source_choices_name:
                continue
            if output_name in _field_name_candidates(
                related_field_name,
                related_class.name,
                related_class.name,
                target_classes,
            ):
                return True
    return False


def _is_model_context_foreign_key(foreign_key: dict) -> bool:
    relation_classes = {
        foreign_key.get("source_class"),
        foreign_key.get("referenced_class"),
    }
    if "MDL_CNTXT" in relation_classes:
        return True
    relation_entities = {
        _normalize_sql_developer_entity_name(str(foreign_key.get("source_entity", ""))),
        _normalize_sql_developer_entity_name(str(foreign_key.get("referenced_entity", ""))),
    }
    return (
        "model_context" in relation_entities
        or str(foreign_key.get("relation_name", "")).startswith("Model_Context")
    )


def _is_model_context_foreign_key_component(source_class: ModelClass, field_name: str) -> bool:
    for foreign_key in _sql_developer_foreign_keys(source_class):
        if not _foreign_key_contains_field(foreign_key, field_name):
            continue
        if _is_model_context_foreign_key(foreign_key):
            return True
    return False


def _foreign_key_contains_field(foreign_key: dict, field_name: str) -> bool:
    fields = foreign_key.get("fields", [])
    return isinstance(fields, list) and field_name in fields


def _sql_developer_ignores_attribute_inheritance(model_class: ModelClass) -> bool:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    inheritance_type = next(
        (
            sql_developer_annotations.get(key)
            for key in (
                "attribute_inheritance_type",
                "attribute_inher_type",
                "attribute_inheritance",
                "inheritance_type",
            )
            if key in sql_developer_annotations
        ),
        None,
    )
    if inheritance_type is None:
        return False
    normalized_inheritance_type = str(inheritance_type).strip().lower()
    return normalized_inheritance_type in {"all atributes", "all attributes"}


def _sql_developer_bool_annotation(source: dict, *keys: str) -> bool:
    for key in keys:
        if key not in source:
            continue
        value = source[key]
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"y", "yes", "true", "1"}
        if isinstance(value, int):
            return value == 1
    return False


def _direct_model_parent(class_name: str, ldm_module: DjangoModelModule) -> str | None:
    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return None
    return next((base for base in model_class.bases if base in ldm_module.classes), None)


def _is_folded_source_primary_key_component(
    source_class_name: str,
    target_class_name: str,
    source_class: ModelClass,
    field_name: str,
) -> bool:
    if source_class_name == target_class_name:
        return False
    return field_name in _sql_developer_primary_key(source_class)


def _has_model_context_identity(model_class: ModelClass) -> bool:
    for foreign_key in _sql_developer_foreign_keys(model_class):
        if foreign_key.get("identifying") != "Y":
            continue
        if foreign_key.get("relation_side") != "target":
            continue
        relation_classes = {
            foreign_key.get("source_class"),
            foreign_key.get("referenced_class"),
        }
        if "MDL_CNTXT" in relation_classes:
            return True
        relation_entities = {
            _normalize_sql_developer_entity_name(str(foreign_key.get("source_entity", ""))),
            _normalize_sql_developer_entity_name(str(foreign_key.get("referenced_entity", ""))),
        }
        if "model_context" in relation_entities:
            return True
        relation_name = foreign_key.get("relation_name", "")
        if relation_name.startswith("Model_Context"):
            return True
    return False


def _sql_developer_foreign_keys(model_class: ModelClass) -> list[dict]:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    foreign_keys = sql_developer_annotations.get("foreign_keys", [])
    return foreign_keys if isinstance(foreign_keys, list) else []


def _sql_developer_field_annotations(model_class: ModelClass, field_name: str) -> dict:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    fields = sql_developer_annotations.get("fields", {})
    if not isinstance(fields, dict):
        return {}
    field_annotations = fields.get(field_name, {})
    return field_annotations if isinstance(field_annotations, dict) else {}


class _ClassGraph:
    def __init__(self, module: DjangoModelModule):
        self.module = module
        self.children: dict[str, list[str]] = {class_name: [] for class_name in module.classes}
        self.delegate_owners: dict[str, list[str]] = {class_name: [] for class_name in module.classes}
        self.identifying_extensions: dict[str, list[str]] = {class_name: [] for class_name in module.classes}
        sql_developer_policy = _editable_sqldeveloper_forward_engineering_policy()
        sql_developer_merge_classes = _sql_developer_policy_classes(
            module,
            entity_names=sql_developer_policy.merge_entity_names,
            class_names=sql_developer_policy.merge_class_names,
        )
        for class_name, model_class in module.classes.items():
            for base in model_class.bases:
                if base in module.classes:
                    self.children.setdefault(base, []).append(class_name)
            for field in model_class.fields.values():
                if field.field_type != "ForeignKey" or not field.name.endswith("_delegate"):
                    continue
                if field.related_model is None or field.related_model not in module.classes:
                    continue
                self.delegate_owners.setdefault(field.related_model, []).append(class_name)
            if not _should_fold_identifying_extension(class_name, model_class, sql_developer_merge_classes):
                continue
            for referenced_class in _identifying_extension_referenced_classes(model_class):
                if referenced_class in module.classes:
                    self.identifying_extensions.setdefault(referenced_class, []).append(class_name)

    def ancestors(self, class_name: str) -> list[str]:
        ancestors: list[str] = []
        current = class_name
        while current in self.module.classes:
            parent = next((base for base in self.module.classes[current].bases if base in self.module.classes), None)
            if parent is None:
                break
            ancestors.append(parent)
            current = parent
        ancestors.reverse()
        return ancestors

    def folded_descendants(self, class_name: str, target_classes: set[str]) -> list[str]:
        descendants: list[str] = []
        for child_name in self.children.get(class_name, []):
            if child_name in target_classes:
                continue
            descendants.append(child_name)
            descendants.extend(self.folded_descendants(child_name, target_classes))
        return descendants

    def forward_engineering_source_classes(self, class_name: str, target_classes: set[str]) -> list[str]:
        source_classes: list[str] = []
        visited: set[str] = set()

        def add_source(source_class_name: str) -> None:
            if source_class_name in visited or source_class_name not in self.module.classes:
                return
            visited.add(source_class_name)
            source_classes.append(source_class_name)

        def add_folded_tree(root_class_name: str) -> None:
            for ancestor_name in self.ancestors(root_class_name):
                if ancestor_name not in target_classes:
                    add_source(ancestor_name)
            add_source(root_class_name)
            for descendant_name in self.folded_descendants(root_class_name, target_classes):
                for ancestor_name in self.ancestors(descendant_name):
                    if ancestor_name not in target_classes:
                        add_source(ancestor_name)
                add_source(descendant_name)

        for ancestor_name in self.ancestors(class_name):
            add_source(ancestor_name)
        add_folded_tree(class_name)

        # The LDM import represents SQLDeveloper disjoint subtyping arcs as
        # delegate foreign keys. Forward engineering folds the concrete classes
        # behind those delegates into the relational table, so they must be
        # source classes even though they are not Python subclasses of the table.
        pending_index = 0
        while pending_index < len(source_classes):
            source_class = self.module.classes[source_classes[pending_index]]
            pending_index += 1
            for extension_class_name in self.identifying_extensions.get(source_class.name, []):
                if extension_class_name not in target_classes:
                    add_source(extension_class_name)
            for owner_class_name in self.delegate_owners.get(source_class.name, []):
                for ancestor_name in self.ancestors(owner_class_name):
                    if ancestor_name not in target_classes:
                        add_source(ancestor_name)
                add_source(owner_class_name)
            for field in source_class.fields.values():
                if field.field_type != "ForeignKey" or not field.name.endswith("_delegate"):
                    continue
                if field.related_model is None or field.related_model not in self.module.classes:
                    continue
                add_folded_tree(field.related_model)

        return source_classes

    def nearest_target_ancestor(self, class_name: str, target_classes: set[str]) -> str | None:
        current = class_name
        while current in self.module.classes:
            if current in target_classes:
                return current
            parent = next((base for base in self.module.classes[current].bases if base in self.module.classes), None)
            if parent is None:
                return None
            current = parent
        return None

    def relationship_target_tables(self, class_name: str, target_classes: set[str]) -> list[str]:
        candidates: list[str] = []
        visited: set[str] = set()

        def add(candidate: str | None) -> None:
            if candidate is not None and candidate not in candidates:
                candidates.append(candidate)

        def visit(candidate_class_name: str) -> None:
            if candidate_class_name in visited or candidate_class_name not in self.module.classes:
                return
            visited.add(candidate_class_name)
            add(self.nearest_target_ancestor(candidate_class_name, target_classes))

            related_tree_classes = [candidate_class_name]
            related_tree_classes.extend(reversed(self.ancestors(candidate_class_name)))
            for related_tree_class in related_tree_classes:
                for owner_class_name in self.delegate_owners.get(related_tree_class, []):
                    visit(owner_class_name)

        visit(class_name)
        return candidates


def _identifying_extension_referenced_classes(model_class: ModelClass) -> tuple[str, ...]:
    referenced_classes: list[str] = []
    for foreign_key in _extension_source_references(model_class):
        referenced_class = foreign_key.get("referenced_class")
        if referenced_class and referenced_class not in referenced_classes:
            referenced_classes.append(referenced_class)
    return tuple(referenced_classes)


def _extension_source_references(model_class: ModelClass) -> list[dict]:
    references: list[dict] = []
    for foreign_key in _identifying_source_references(model_class):
        referenced_class = foreign_key.get("referenced_class")
        if referenced_class:
            references.append(foreign_key)

    for foreign_key in _primary_key_source_references(model_class):
        if foreign_key not in references:
            references.append(foreign_key)

    return references


def _should_fold_identifying_extension(
    class_name: str,
    model_class: ModelClass,
    sql_developer_merge_classes: set[str],
) -> bool:
    if class_name in sql_developer_merge_classes:
        return True
    if class_name.endswith("_RSK_DT") and _has_identifying_source_reference(model_class):
        return True
    return _is_folded_sql_developer_extension(class_name, model_class)


def _derive_fields_for_target(
    target_class_name: str,
    ldm_source_classes: list[str],
    ldm_module: DjangoModelModule,
    reference_class: ModelClass | None,
    graph: _ClassGraph,
    target_classes: set[str],
) -> DerivedFieldSet:
    reference_fields = set(reference_class.fields) if reference_class is not None else set()
    sql_developer_policy = _editable_sqldeveloper_forward_engineering_policy()
    cleanup_suppressed_field_names = sql_developer_policy.suppressed_field_names_by_target.get(
        target_class_name,
        frozenset(),
    )
    final_suppressed_field_names = sql_developer_policy.final_suppressed_field_names_by_target.get(
        target_class_name,
        frozenset(),
    )
    suppressed_field_names = cleanup_suppressed_field_names | final_suppressed_field_names
    field_name_overrides = sql_developer_policy.field_name_overrides_by_target.get(target_class_name, {})
    self_relationship_field_names = sql_developer_policy.self_relationship_field_names_by_target.get(
        target_class_name,
        frozenset(),
    )
    preserved_reduced_field_names = sql_developer_policy.preserved_reduced_field_names_by_target.get(
        target_class_name,
        frozenset(),
    )
    apply_sql_developer_target_cleanup = target_class_name in sql_developer_policy.suppressed_field_names_by_target
    derived_field_set = DerivedFieldSet()
    relationship_counts: dict[str, int] = {}
    relationship_fields_by_target: dict[str, str] = {}
    key_relationship_candidates: list[tuple[str, str]] = []
    target_class_names_by_length = sorted(target_classes, key=len, reverse=True)

    def add_relationship_field(
        target_table_name: str,
        allow_duplicate: bool = False,
        source_class_name: str | None = None,
        source_field_name: str | None = None,
    ) -> bool:
        if not reference_fields and not allow_duplicate and target_table_name in relationship_fields_by_target:
            _record_field_lineage(
                derived_field_set=derived_field_set,
                output_name=relationship_fields_by_target[target_table_name],
                source_class_name=source_class_name,
                source_field_name=source_field_name,
                source_kind="relationship_field",
                relationship_target=target_table_name,
            )
            return True
        relationship_index = relationship_counts.get(target_table_name, 0)
        field_name = f"the{target_table_name}{relationship_index if relationship_index else ''}"
        if reference_fields and field_name not in reference_fields:
            return False
        derived_field_set.field_names.add(field_name)
        derived_field_set.relationship_targets[field_name] = target_table_name
        _record_field_lineage(
            derived_field_set=derived_field_set,
            output_name=field_name,
            source_class_name=source_class_name,
            source_field_name=source_field_name,
            source_kind="relationship_field",
            relationship_target=target_table_name,
        )
        relationship_fields_by_target[target_table_name] = field_name
        relationship_counts[target_table_name] = relationship_index + 1
        return True

    def record_choice_values(output_name: str, source_class: ModelClass, source_field: ModelStatement) -> None:
        if source_field.choices_name is None:
            return
        choice_statement = _choice_statement_for_field(source_class, source_field)
        if choice_statement is None:
            return
        choice_values = _literal_choice_values(choice_statement.source)
        if not choice_values:
            return
        merged_choice_values = derived_field_set.choice_values_by_field.setdefault(output_name, {})
        merged_choice_values.update(choice_values)

    for source_class_name in ldm_source_classes:
        source_class = ldm_module.classes[source_class_name]
        source_relationship_prefixes: dict[str, str] = {}
        for field in source_class.fields.values():
            if field.field_type != "ForeignKey" or field.name.endswith("_delegate") or field.related_model is None:
                continue
            if (
                apply_sql_developer_target_cleanup
                and not _has_sql_developer_relationship_for_model_field(source_class, field.name, field.related_model)
            ):
                continue
            target_table_names = graph.relationship_target_tables(field.related_model, target_classes)
            if not target_table_names:
                continue
            for target_table_name in target_table_names:
                is_allowed_self_relationship = (
                    target_table_name == target_class_name and field.name in self_relationship_field_names
                )
                if target_table_name == target_class_name and not is_allowed_self_relationship:
                    continue
                for relationship_prefix in _relationship_field_prefixes(field.related_model, graph):
                    source_relationship_prefixes.setdefault(relationship_prefix, target_table_name)
                if add_relationship_field(
                    target_table_name,
                    allow_duplicate=(
                        is_allowed_self_relationship
                        or (
                            source_class_name == target_class_name
                            and _has_multiple_direct_entity_role_relationships(
                                source_class=source_class,
                                graph=graph,
                                target_classes=target_classes,
                            )
                            and _is_direct_entity_role_relationship(
                                source_class=source_class,
                                related_model_name=field.related_model,
                                target_table_name=target_table_name,
                            )
                        )
                    ),
                    source_class_name=source_class_name,
                    source_field_name=field.name,
                ):
                    break

        for field in source_class.fields.values():
            if field.field_type == "ForeignKey":
                continue
            if _is_folded_unique_id_field(source_class_name, target_class_name, field.name):
                continue
            if _is_reduced_lower_level_discriminator_field(
                field.name,
                source_class_name,
                target_class_name,
                graph,
                ldm_module,
            ) and field.name not in preserved_reduced_field_names:
                derived_field_set.skipped_source_fields.add((source_class_name, field.name))
                continue
            output_name = _normalize_field_name(
                field.name,
                target_class_name,
                source_class_name,
                reference_fields,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            )
            output_name = field_name_overrides.get(field.name, output_name)
            add_not_applicable_to_choices = _should_add_not_applicable_to_choice_field(
                source_class_name=source_class_name,
                field_name=field.name,
                ldm_module=ldm_module,
                graph=graph,
            )
            annotated_key_component = _annotated_relationship_key_component(
                source_class=source_class,
                field_name=field.name,
                target_class_name=target_class_name,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
                allowed_relationship_targets=set(source_relationship_prefixes.values()),
            )
            relationship_target = (
                annotated_key_component.relationship_target
                if annotated_key_component is not None
                else _key_field_relationship_target(
                    field.name,
                    target_class_name,
                    target_class_names_by_length,
                    source_relationship_prefixes,
                )
            )
            if relationship_target is not None:
                key_relationship_candidates.append((source_class_name, relationship_target))
                add_relationship_field(relationship_target)
                canonical_key_field = (
                    annotated_key_component.canonical_field_name
                    if annotated_key_component is not None
                    else _canonical_relationship_key_field_name(field.name, relationship_target)
                )
                preserves_direct_entity_role_key = _preserves_direct_entity_role_key(
                    field_name=field.name,
                    source_class_name=source_class_name,
                    target_class_name=target_class_name,
                    source_class=source_class,
                    relationship_target=relationship_target,
                    graph=graph,
                    target_classes=target_classes,
                )
                if (
                    apply_sql_developer_target_cleanup
                    and not preserves_direct_entity_role_key
                    and field.name not in field_name_overrides
                    and _is_non_primary_relationship_key_component(
                        source_class=source_class,
                        field_name=field.name,
                        relationship_target=relationship_target,
                        graph=graph,
                        target_classes=target_classes,
                    )
                ):
                    derived_field_set.skipped_source_fields.add((source_class_name, field.name))
                    continue
                if preserves_direct_entity_role_key:
                    relationship_key_field = output_name
                elif field.name in field_name_overrides:
                    relationship_key_field = output_name
                elif reference_fields and output_name in reference_fields:
                    relationship_key_field = output_name
                elif canonical_key_field is not None:
                    relationship_key_field = canonical_key_field
                else:
                    relationship_key_field = output_name
                derived_field_set.field_names.add(relationship_key_field)
                derived_field_set.source_field_names[(source_class_name, field.name)] = relationship_key_field
                _record_field_lineage(
                    derived_field_set=derived_field_set,
                    output_name=relationship_key_field,
                    source_class_name=source_class_name,
                    source_field_name=field.name,
                    source_kind="relationship_key_component",
                    relationship_target=relationship_target,
                )
                record_choice_values(relationship_key_field, source_class, field)
                if add_not_applicable_to_choices or _should_add_not_applicable_to_optional_identifying_fk_output(
                    output_name=relationship_key_field,
                    source_class=source_class,
                    source_field_name=field.name,
                    ldm_source_classes=ldm_source_classes,
                    ldm_module=ldm_module,
                    graph=graph,
                    target_classes=target_classes,
                ):
                    derived_field_set.not_applicable_choice_fields.add(relationship_key_field)
                continue
            if _is_folded_source_primary_key_component(
                source_class_name=source_class_name,
                target_class_name=target_class_name,
                source_class=source_class,
                field_name=field.name,
            ) and apply_sql_developer_target_cleanup and field.name not in field_name_overrides:
                derived_field_set.skipped_source_fields.add((source_class_name, field.name))
                continue
            derived_field_set.field_names.add(output_name)
            derived_field_set.source_field_names[(source_class_name, field.name)] = output_name
            _record_field_lineage(
                derived_field_set=derived_field_set,
                output_name=output_name,
                source_class_name=source_class_name,
                source_field_name=field.name,
                source_kind="ldm_field",
            )
            record_choice_values(output_name, source_class, field)
            if add_not_applicable_to_choices or _should_add_not_applicable_to_optional_identifying_fk_output(
                output_name=output_name,
                source_class=source_class,
                source_field_name=field.name,
                ldm_source_classes=ldm_source_classes,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            ):
                derived_field_set.not_applicable_choice_fields.add(output_name)

    seen_key_relationships: set[tuple[str, str]] = set()
    for source_class_name, target_table_name in key_relationship_candidates:
        relationship_key = (source_class_name, target_table_name)
        if relationship_key in seen_key_relationships:
            continue
        seen_key_relationships.add(relationship_key)
        add_relationship_field(target_table_name)

    preserved_redundant_key_fields = reference_fields | set(field_name_overrides.values())
    _remove_redundant_prefixed_key_fields(derived_field_set.field_names, preserved_redundant_key_fields)
    derived_field_set.field_names.difference_update(suppressed_field_names)
    derived_field_set.not_applicable_choice_fields.difference_update(suppressed_field_names)
    for field_name in suppressed_field_names:
        derived_field_set.relationship_targets.pop(field_name, None)
    derived_field_set.source_field_injections.update(
        sql_developer_policy.source_field_injections_by_target.get(target_class_name, {})
    )
    for output_name, (source_class_name, source_field_name) in derived_field_set.source_field_injections.items():
        _record_field_lineage(
            derived_field_set=derived_field_set,
            output_name=output_name,
            source_class_name=source_class_name,
            source_field_name=source_field_name,
            source_kind="source_field_injection",
        )
        source_class = ldm_module.classes.get(source_class_name)
        source_field = source_class.fields.get(source_field_name) if source_class is not None else None
        if source_class is not None and source_field is not None:
            record_choice_values(output_name, source_class, source_field)
        if _should_add_not_applicable_to_choice_field(
            source_class_name=source_class_name,
            field_name=source_field_name,
            ldm_module=ldm_module,
            graph=graph,
        ) or (
            source_class is not None
            and _should_add_not_applicable_to_optional_identifying_fk_output(
                output_name=output_name,
                source_class=source_class,
                source_field_name=source_field_name,
                ldm_source_classes=ldm_source_classes,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            )
        ):
            derived_field_set.not_applicable_choice_fields.add(output_name)
    derived_field_set.synthetic_char_fields.update(
        sql_developer_policy.synthetic_char_fields_by_target.get(target_class_name, frozenset())
    )
    for target_table_name in set(derived_field_set.relationship_targets.values()):
        identifier_field_name = sql_developer_policy.relationship_identifier_fields_by_target_table.get(target_table_name)
        if identifier_field_name is not None:
            derived_field_set.synthetic_char_fields.add(identifier_field_name)
    _add_reduced_discriminator_choice_values(
        derived_field_set=derived_field_set,
        target_class_name=target_class_name,
        ldm_source_classes=ldm_source_classes,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
        preserved_field_names=preserved_reduced_field_names,
    )
    _add_accounting_context_not_applicable_choice_values(
        derived_field_set=derived_field_set,
        target_class_name=target_class_name,
    )
    _add_relationship_copy_reduced_discriminator_choice_values(
        derived_field_set=derived_field_set,
        target_class_name=target_class_name,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    )
    _add_entity_role_copy_choice_values(
        derived_field_set=derived_field_set,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    )
    _add_entity_role_not_applicable_choice_values(derived_field_set)
    _add_sql_developer_input_domain_not_applicable_choice_values(
        derived_field_set=derived_field_set,
        target_class_name=target_class_name,
        ldm_module=ldm_module,
    )
    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name=target_class_name,
        ldm_source_classes=ldm_source_classes,
    )
    _add_sql_developer_synthetic_choice_values(
        derived_field_set=derived_field_set,
        ldm_module=ldm_module,
    )
    _add_held_for_sale_not_applicable_choice_values(
        derived_field_set=derived_field_set,
        target_class_name=target_class_name,
    )
    _add_directional_role_not_applicable_choice_values(derived_field_set)
    _add_sql_developer_input_domain_choice_label_overrides(derived_field_set)
    return derived_field_set


def _record_field_lineage(
    derived_field_set: DerivedFieldSet,
    output_name: str,
    source_class_name: str | None,
    source_field_name: str | None,
    source_kind: str,
    **extra: str,
) -> None:
    if source_class_name is None or source_field_name is None:
        return
    lineage_entry = {
        "ldm_class": source_class_name,
        "ldm_field": source_field_name,
        "source_kind": source_kind,
        **{key: value for key, value in extra.items() if value},
    }
    field_lineage = derived_field_set.field_lineage.setdefault(output_name, [])
    if lineage_entry not in field_lineage:
        field_lineage.append(lineage_entry)


def _build_class_field_lineage(
    target_class_name: str,
    ldm_source_classes: list[str],
    generated_field_names: set[str],
    derived_field_set: DerivedFieldSet,
    reference_class: ModelClass | None,
    derived_fields: set[str],
    synthetic_fields: set[str],
) -> dict:
    fields: dict[str, dict] = {}
    for field_name in _sorted_in_reference_order(generated_field_names, reference_class):
        field_info = {
            "generated_kind": _generated_field_kind(
                field_name=field_name,
                target_class_name=target_class_name,
                derived_field_set=derived_field_set,
                derived_fields=derived_fields,
                synthetic_fields=synthetic_fields,
            ),
            "sources": derived_field_set.field_lineage.get(field_name, []),
        }
        relationship_target = derived_field_set.relationship_targets.get(field_name)
        if relationship_target is not None:
            field_info["relationship_target"] = relationship_target
        fields[field_name] = field_info

    return {
        "ldm_source_classes": ldm_source_classes,
        "fields": fields,
    }


def _build_forward_engineered_key_annotations(
    target_class_name: str,
    ldm_source_classes: list[str],
    generated_field_names: set[str],
    derived_field_set: DerivedFieldSet,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> dict:
    """Build generated-field-only key hints for a forward-engineered class."""

    forward_engineering_annotations: dict = {}

    source_class = ldm_module.classes.get(target_class_name)
    if source_class is not None:
        primary_key_annotation = _mapped_primary_key_annotation(
            target_class_name=target_class_name,
            source_class_name=target_class_name,
            source_class=source_class,
            generated_field_names=generated_field_names,
            derived_field_set=derived_field_set,
            ldm_module=ldm_module,
            graph=graph,
            target_classes=target_classes,
        )
        forward_engineering_annotations.update(primary_key_annotation)

    foreign_keys: list[dict] = []
    seen_foreign_keys: set[str] = set()
    for source_class_name in ldm_source_classes:
        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        for foreign_key in _sql_developer_foreign_keys(source_class):
            mapped_foreign_key = _mapped_foreign_key_annotation(
                target_class_name=target_class_name,
                source_class_name=source_class_name,
                foreign_key=foreign_key,
                generated_field_names=generated_field_names,
                derived_field_set=derived_field_set,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            )
            if not mapped_foreign_key:
                continue
            signature = json.dumps(mapped_foreign_key, sort_keys=True)
            if signature in seen_foreign_keys:
                continue
            seen_foreign_keys.add(signature)
            foreign_keys.append(mapped_foreign_key)

    if foreign_keys:
        forward_engineering_annotations["candidate_foreign_keys"] = foreign_keys

    if not forward_engineering_annotations:
        return {}

    return {
        "forward_engineering": _without_empty_values(forward_engineering_annotations),
    }


def _mapped_primary_key_annotation(
    target_class_name: str,
    source_class_name: str,
    source_class: ModelClass,
    generated_field_names: set[str],
    derived_field_set: DerivedFieldSet,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> dict:
    primary_key_fields = _source_primary_key_field_entries(source_class)
    if not primary_key_fields:
        return {}

    mapped_fields: list[str] = []

    for field_entry in primary_key_fields:
        source_field_name = field_entry["field"]
        output_field_name = _generated_output_field_for_source_field(
            target_class_name=target_class_name,
            source_class_name=source_class_name,
            source_field_name=source_field_name,
            generated_field_names=generated_field_names,
            derived_field_set=derived_field_set,
            ldm_module=ldm_module,
            graph=graph,
            target_classes=target_classes,
        )
        if output_field_name is not None:
            _append_unique_value(mapped_fields, output_field_name)

    return _without_empty_values(
        {
            "candidate_primary_key": mapped_fields,
        }
    )


def _mapped_foreign_key_annotation(
    target_class_name: str,
    source_class_name: str,
    foreign_key: dict,
    generated_field_names: set[str],
    derived_field_set: DerivedFieldSet,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> dict:
    source_fields = _ordered_foreign_key_fields(foreign_key)
    if not source_fields:
        return {}

    mapped_fields: list[str] = []

    for source_field_name in source_fields:
        output_field_name = _generated_output_field_for_source_field(
            target_class_name=target_class_name,
            source_class_name=source_class_name,
            source_field_name=source_field_name,
            generated_field_names=generated_field_names,
            derived_field_set=derived_field_set,
            ldm_module=ldm_module,
            graph=graph,
            target_classes=target_classes,
        )
        if output_field_name is not None:
            _append_unique_value(mapped_fields, output_field_name)

    relationship_targets = _foreign_key_relationship_target_tables(
        foreign_key=foreign_key,
        graph=graph,
        target_classes=target_classes,
    )
    relationship_targets = tuple(
        relationship_target
        for relationship_target in relationship_targets
        if relationship_target != target_class_name
    )
    if not relationship_targets:
        relationship_targets = _foreign_key_target_table_fallbacks(
            foreign_key=foreign_key,
            current_target_class_name=target_class_name,
            target_classes=target_classes,
        )
    relationship_fields = [
        field_name
        for field_name, relationship_target in derived_field_set.relationship_targets.items()
        if field_name in generated_field_names and relationship_target in relationship_targets
    ]
    for relationship_field in relationship_fields:
        _append_unique_value(mapped_fields, relationship_field)

    if not mapped_fields:
        return {}
    if len(source_fields) <= 1 and len(mapped_fields) <= 1:
        return {}

    foreign_key_annotation = {"fields": mapped_fields}
    if len(relationship_targets) == 1:
        foreign_key_annotation["references"] = relationship_targets[0]
    elif relationship_targets:
        foreign_key_annotation["references"] = list(relationship_targets)
    if relationship_fields:
        foreign_key_annotation["relationship_fields"] = relationship_fields
    return _without_empty_values(foreign_key_annotation)


def _source_primary_key_field_entries(source_class: ModelClass) -> list[dict]:
    sql_developer_annotations = source_class.annotations.get("sql_developer", {})
    primary_key_fields = sql_developer_annotations.get("primary_key_fields", [])
    if isinstance(primary_key_fields, list) and primary_key_fields:
        entries = [
            dict(entry)
            for entry in primary_key_fields
            if isinstance(entry, dict) and isinstance(entry.get("field"), str)
        ]
        return sorted(
            entries,
            key=lambda entry: (
                entry.get("sequence") if isinstance(entry.get("sequence"), int) else 10**9,
                entry.get("field", ""),
            ),
        )
    return [{"field": field_name} for field_name in _ordered_sql_developer_primary_key_fields(source_class)]


def _foreign_key_target_table_fallbacks(
    foreign_key: dict,
    current_target_class_name: str,
    target_classes: set[str],
) -> tuple[str, ...]:
    candidates: list[str] = []
    for key in ("referenced_class", "source_class", "target_class"):
        class_name = foreign_key.get(key)
        if not isinstance(class_name, str):
            continue
        if class_name == current_target_class_name or class_name not in target_classes:
            continue
        if class_name not in candidates:
            candidates.append(class_name)
    return tuple(candidates)


def _generated_output_field_for_source_field(
    target_class_name: str,
    source_class_name: str,
    source_field_name: str,
    generated_field_names: set[str],
    derived_field_set: DerivedFieldSet,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> str | None:
    output_field_name = derived_field_set.source_field_names.get((source_class_name, source_field_name))
    if output_field_name in generated_field_names:
        return output_field_name

    for injected_output_name, source in derived_field_set.source_field_injections.items():
        if source == (source_class_name, source_field_name) and injected_output_name in generated_field_names:
            return injected_output_name

    for lineage_output_name, lineage_entries in derived_field_set.field_lineage.items():
        if lineage_output_name not in generated_field_names:
            continue
        if any(
            entry.get("ldm_class") == source_class_name and entry.get("ldm_field") == source_field_name
            for entry in lineage_entries
        ):
            return lineage_output_name

    if source_field_name in generated_field_names:
        return source_field_name

    normalized_field_name = _normalize_field_name(
        source_field_name,
        target_class_name=target_class_name,
        source_class_name=source_class_name,
        reference_fields=generated_field_names,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    )
    if normalized_field_name in generated_field_names:
        return normalized_field_name
    return None


def _generated_field_kind(
    field_name: str,
    target_class_name: str,
    derived_field_set: DerivedFieldSet,
    derived_fields: set[str],
    synthetic_fields: set[str],
) -> str:
    if field_name in derived_field_set.relationship_targets:
        return "relationship"
    if field_name in derived_field_set.source_field_injections:
        return "source_field_injection"
    if field_name in synthetic_fields and field_name not in derived_fields:
        return "synthetic"
    if field_name in derived_fields:
        return "ldm_field"
    if field_name in {"test_id", f"{target_class_name}_uniqueID"}:
        return "synthetic"
    return "reference_fallback"


def _render_class_from_reference(
    reference_class: ModelClass,
    included_fields: set[str],
    annotations: dict | None = None,
) -> list[str]:
    lines = [f"class {reference_class.name}(models.Model):"]
    pending_choices: list[ModelStatement] = []
    emitted_any_statement = False

    if annotations:
        lines.extend(_render_class_annotations(annotations))
        emitted_any_statement = True

    for statement in reference_class.statements:
        if statement.kind == "choice":
            pending_choices.append(statement)
            continue

        if statement.kind != "field":
            pending_choices.clear()
            continue

        if statement.name not in included_fields:
            pending_choices.clear()
            continue

        if statement.choices_name:
            for choice_statement in pending_choices:
                if choice_statement.name == statement.choices_name:
                    lines.append(_indent_source(choice_statement.source))
                    emitted_any_statement = True
        pending_choices.clear()

        lines.append(_indent_source(statement.source))
        emitted_any_statement = True

    if not emitted_any_statement:
        lines.append("    pass")

    if reference_class.meta_source:
        lines.append("")
        lines.append(_indent_source(reference_class.meta_source))
    else:
        lines.extend(_default_meta_lines(reference_class.name))

    return lines


def _render_class_from_ldm(
    target_class_name: str,
    ldm_class: ModelClass,
    source_class_names: Iterable[str],
    ldm_module: DjangoModelModule,
    generated_field_names: set[str],
    relationship_targets: dict[str, str],
    source_field_names: dict[tuple[str, str], str],
    source_field_injections: dict[str, tuple[str, str]],
    synthetic_char_fields: set[str],
    not_applicable_choice_fields: set[str],
    choice_values_by_field: dict[str, dict[str, str]],
    skipped_source_fields: set[tuple[str, str]],
    graph: _ClassGraph,
    target_classes: set[str],
    annotations: dict | None = None,
) -> list[str]:
    lines = [f"class {target_class_name}(models.Model):"]
    emitted_fields: set[str] = set()

    if annotations:
        lines.extend(_render_class_annotations(annotations))

    if "test_id" in generated_field_names:
        lines.append(
            "    test_id = models.CharField('test_id', max_length=255, default=None, blank=True, null=True)"
        )
        emitted_fields.add("test_id")

    unique_id_name = f"{target_class_name}_uniqueID"
    if unique_id_name in generated_field_names:
        lines.append(
            f"    {unique_id_name} = models.CharField('{unique_id_name}', max_length=255, primary_key=True)"
        )
        emitted_fields.add(unique_id_name)

    for source_class_name in source_class_names:
        source_class = ldm_module.classes[source_class_name]
        for field in source_class.fields.values():
            if field.name.endswith("_delegate"):
                continue
            if _is_folded_unique_id_field(source_class_name, target_class_name, field.name):
                continue
            if (source_class_name, field.name) in skipped_source_fields:
                continue
            output_name = source_field_names.get((source_class_name, field.name))
            if output_name is None:
                output_name = _normalize_field_name(
                    field.name,
                    target_class_name,
                    source_class_name,
                    generated_field_names,
                    ldm_module=ldm_module,
                    graph=graph,
                    target_classes=target_classes,
                )
            if output_name not in generated_field_names or output_name in emitted_fields:
                continue

            choice_statement = _choice_statement_for_field(source_class, field)
            if choice_statement is not None and field.choices_name is not None:
                choice_source = _choice_source_for_rendered_field(
                    choice_name=field.choices_name,
                    original_choice_source=choice_statement.source,
                    output_name=output_name,
                    not_applicable_choice_fields=not_applicable_choice_fields,
                    choice_values_by_field=choice_values_by_field,
                )
                lines.append(_indent_source(choice_source))
                lines.append(_indent_source(_rewrite_assignment_name(field.source, field.name, output_name)))
            elif output_name in choice_values_by_field:
                choice_name = f"{output_name}_domain"
                lines.append(_indent_source(_choice_source_from_values(choice_name, choice_values_by_field[output_name])))
                lines.append(_indent_source(_render_synthetic_choice_char_field(output_name, choice_name)))
            else:
                lines.append(_indent_source(_rewrite_assignment_name(field.source, field.name, output_name)))
            emitted_fields.add(output_name)

    for output_name, (source_class_name, source_field_name) in source_field_injections.items():
        if output_name not in generated_field_names or output_name in emitted_fields:
            continue
        source_class = ldm_module.classes.get(source_class_name)
        if source_class is None:
            continue
        source_field = source_class.fields.get(source_field_name)
        if source_field is None:
            continue
        choice_statement = _choice_statement_for_field(source_class, source_field)
        if choice_statement is not None and source_field.choices_name is not None:
            choice_source = _choice_source_for_rendered_field(
                choice_name=source_field.choices_name,
                original_choice_source=choice_statement.source,
                output_name=output_name,
                not_applicable_choice_fields=not_applicable_choice_fields,
                choice_values_by_field=choice_values_by_field,
            )
            lines.append(_indent_source(choice_source))
            lines.append(_indent_source(_rewrite_assignment_name(source_field.source, source_field_name, output_name)))
        elif output_name in choice_values_by_field:
            choice_name = f"{output_name}_domain"
            lines.append(_indent_source(_choice_source_from_values(choice_name, choice_values_by_field[output_name])))
            lines.append(_indent_source(_render_synthetic_choice_char_field(output_name, choice_name)))
        else:
            lines.append(_indent_source(_rewrite_assignment_name(source_field.source, source_field_name, output_name)))
        emitted_fields.add(output_name)

    for field_name in sorted(synthetic_char_fields):
        if field_name not in generated_field_names or field_name in emitted_fields:
            continue
        lines.append(_indent_source(_render_synthetic_char_field(field_name)))
        emitted_fields.add(field_name)

    for field_name, target_model_name in relationship_targets.items():
        if field_name not in generated_field_names or field_name in emitted_fields:
            continue
        lines.append(_indent_source(_render_relationship_field(target_class_name, field_name, target_model_name)))
        emitted_fields.add(field_name)

    if len(emitted_fields) == 0:
        lines.append("    pass")

    if ldm_class.meta_source:
        lines.append("")
        lines.append(_indent_source(ldm_class.meta_source))
    else:
        lines.extend(_default_meta_lines(target_class_name))

    return lines


def _render_class_annotations(annotations: dict) -> list[str]:
    rendered = pformat(annotations, width=120, sort_dicts=False)
    lines = rendered.splitlines()
    return [
        ("    __bird_annotations__ = " if index == 0 else "    ") + line
        for index, line in enumerate(lines)
    ]


def _render_synthetic_char_field(field_name: str) -> str:
    return f"{field_name} = models.CharField('{field_name}', max_length=255, default=None, blank=True, null=True)"


def _render_synthetic_choice_char_field(field_name: str, choice_name: str) -> str:
    return (
        f"{field_name} = models.CharField('{field_name}', max_length=255, choices={choice_name}, "
        f"default=None, blank=True, null=True, db_comment='{choice_name}')"
    )


def _choice_source_for_rendered_field(
    choice_name: str,
    original_choice_source: str,
    output_name: str,
    not_applicable_choice_fields: set[str],
    choice_values_by_field: dict[str, dict[str, str]],
) -> str:
    choice_values = choice_values_by_field.get(output_name)
    if choice_values:
        rendered_choice_values = dict(choice_values)
        if output_name in not_applicable_choice_fields and "0" not in rendered_choice_values:
            rendered_choice_values["0"] = "Not_applicable"
        return _choice_source_from_values(choice_name, rendered_choice_values)
    if output_name in not_applicable_choice_fields:
        return _choice_source_with_not_applicable(original_choice_source)
    return original_choice_source


def _choice_source_from_values(choice_name: str, choice_values: dict[str, str]) -> str:
    lines = [f"{choice_name} = {{"]
    for value, label in sorted(choice_values.items(), key=lambda item: _choice_value_sort_key(item[0])):
        lines.append(f"\t{value!r}:{label!r},")
    lines.append("}")
    return "\n".join(lines)


def _choice_value_sort_key(value: str) -> tuple[int, int | str]:
    try:
        return (0, int(value))
    except ValueError:
        return (1, value)


def _choice_source_with_not_applicable(choice_source: str) -> str:
    if "0" in _literal_choice_values(choice_source):
        return choice_source
    opening_brace_index = choice_source.find("{")
    if opening_brace_index < 0:
        return choice_source
    return (
        choice_source[: opening_brace_index + 1]
        + '\t\t"0":"Not_applicable",\n'
        + choice_source[opening_brace_index + 1 :]
    )


def _render_relationship_field(owner_class_name: str, field_name: str, target_model_name: str) -> str:
    related_name = f"{owner_class_name}_to_{field_name}s"
    return (
        f"{field_name} = models.ForeignKey('{target_model_name}', models.SET_NULL, "
        f"blank=True, null=True, related_name='{related_name}')"
    )


def _normalize_field_name(
    field_name: str,
    target_class_name: str,
    source_class_name: str,
    reference_fields: set[str],
    ldm_module: DjangoModelModule | None = None,
    graph: _ClassGraph | None = None,
    target_classes: set[str] | None = None,
) -> str:
    candidates = _field_name_candidates(field_name, target_class_name, source_class_name, target_classes or set())
    for candidate in candidates:
        if candidate in reference_fields:
            return candidate
    structural_field_name = None
    if ldm_module is not None and graph is not None and target_classes is not None:
        structural_field_name = _preferred_structural_field_name(
            field_name=field_name,
            candidates=candidates,
            target_class_name=target_class_name,
            source_class_name=source_class_name,
            ldm_module=ldm_module,
            graph=graph,
            target_classes=target_classes,
        )
        if structural_field_name in reference_fields:
            return structural_field_name
    if not reference_fields and structural_field_name is not None:
        return structural_field_name
    return candidates[0]


def _field_name_candidates(
    field_name: str,
    target_class_name: str,
    source_class_name: str,
    target_classes: set[str] | None = None,
) -> list[str]:
    candidates = [field_name]
    prefixes = [target_class_name]
    if source_class_name != target_class_name:
        prefixes.append(source_class_name)

    for prefix_name in prefixes:
        prefix = f"{prefix_name}_"
        if field_name == f"{prefix_name}_ACCNTNG_CNSLDTN_LVL":
            candidates.append("ACCNTNG_CNSLDTN_LVL")
        if field_name == f"{prefix_name}_ACCNTNG_STNDRD":
            candidates.append("ACCNTNG_STNDRD")
        if source_class_name != target_class_name and field_name == f"{prefix_name}_INCPTN_DT":
            candidates.append("DT_INCPTN")
        if field_name == f"{prefix_name}_RFRNC_DT":
            candidates.append("DT_RFRNC")
        if field_name == f"{prefix_name}_RPRTNG_AGNT_ID":
            candidates.append("RPRTNG_AGNT_ID")
        if field_name.startswith(prefix):
            candidates.append(field_name[len(prefix) :])

    if field_name.endswith("_ACCNTNG_CNSLDTN_LVL"):
        candidates.append("ACCNTNG_CNSLDTN_LVL")
    if field_name.endswith("_ACCNTNG_STNDRD") and not field_name.endswith("_BY_ACCNTNG_STNDRD"):
        candidates.append("ACCNTNG_STNDRD")
    if field_name.endswith("_ACCNTNG_CLSSFCTN"):
        candidates.append("ACCNTNG_CLSSFCTN")
    if source_class_name != target_class_name and field_name.endswith("_INCPTN_DT"):
        candidates.append("DT_INCPTN")
    if field_name.endswith("_RFRNC_DT"):
        candidates.append("DT_RFRNC")
    if field_name.endswith("_RPRTNG_AGNT_ID"):
        candidates.append("RPRTNG_AGNT_ID")

    if field_name.endswith("_ID"):
        field_prefix = field_name[: -len("_ID")]
        role_supertype_id = _role_supertype_id_field(field_prefix)
        if role_supertype_id is not None:
            candidates.append(role_supertype_id)
        if field_prefix and not field_prefix.endswith("_PRTY"):
            candidates.append(f"{field_prefix}_PRTY_ID")
        if field_name.endswith("_PRTY_ID"):
            candidates.append("PRTY_ID")
        if field_name.endswith("_INSTRMNT_ID"):
            candidates.append("INSTRMNT_ID")
        if field_name.endswith("_SCRTY_ID"):
            candidates.append("SCRTY_ID")
        if field_name.endswith("_SCRTSTN_ID"):
            candidates.append("SCRTSTN_ID")
        for target_class in sorted(target_classes or set(), key=len, reverse=True):
            if field_name.endswith(f"_{target_class}_ID"):
                candidates.append(f"{target_class}_ID")

    if field_name.endswith("_PRTY_RL_TYP"):
        candidates.append(field_name[: -len("_PRTY_RL_TYP")] + "_ENTTY_RL_TYP")
        candidates.append("ENTTY_RL_TYP")
    elif field_name.endswith("_RL_TYP"):
        candidates.append(field_name[: -len("_RL_TYP")] + "_ENTTY_RL_TYP")
        candidates.append("ENTTY_RL_TYP")

    seen: set[str] = set()
    return [candidate for candidate in candidates if not (candidate in seen or seen.add(candidate))]


def _preferred_structural_field_name(
    field_name: str,
    candidates: list[str],
    target_class_name: str,
    source_class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> str:
    if field_name.endswith("_ACCNTNG_CNSLDTN_LVL"):
        return "ACCNTNG_CNSLDTN_LVL"
    if field_name.endswith("_ACCNTNG_STNDRD") and not field_name.endswith("_BY_ACCNTNG_STNDRD"):
        return "ACCNTNG_STNDRD"
    if field_name.endswith("_PSTN_ACCNTNG_CLSSFCTN"):
        return field_name
    if field_name.endswith("_ACCNTNG_CLSSFCTN"):
        return "ACCNTNG_CLSSFCTN"
    if source_class_name != target_class_name and field_name.endswith("_INCPTN_DT"):
        return "DT_INCPTN"
    if field_name.endswith("_RFRNC_DT"):
        return "DT_RFRNC"
    if field_name.endswith("_RPRTNG_AGNT_ID"):
        return "RPRTNG_AGNT_ID"

    if field_name.endswith("_PRTY_RL_TYP"):
        field_prefix = field_name[: -len("_PRTY_RL_TYP")]
        entity_role_prefix = _entity_role_field_prefix(field_prefix, ldm_module, graph, target_classes)
        if entity_role_prefix is not None:
            return f"{entity_role_prefix}_ENTTY_RL_TYP"
    elif field_name.endswith("_RL_TYP"):
        field_prefix = field_name[: -len("_RL_TYP")]
        entity_role_prefix = _entity_role_field_prefix(field_prefix, ldm_module, graph, target_classes)
        if entity_role_prefix is not None:
            return f"{entity_role_prefix}_ENTTY_RL_TYP"

    if field_name.endswith("_PRTY_ID"):
        field_prefix = field_name[: -len("_PRTY_ID")]
        entity_role_prefix = _entity_role_field_prefix(field_prefix, ldm_module, graph, target_classes)
        if entity_role_prefix is not None:
            return f"{entity_role_prefix}_PRTY_ID"
    elif field_name.endswith("_ID"):
        if field_name == "RSK_FCTR_ID":
            return field_name
        field_prefix = field_name[: -len("_ID")]
        role_supertype_id = _role_supertype_id_field(field_prefix)
        if role_supertype_id is not None:
            return role_supertype_id
        entity_role_prefix = _entity_role_field_prefix(field_prefix, ldm_module, graph, target_classes)
        if entity_role_prefix is not None:
            return f"{entity_role_prefix}_PRTY_ID"
        instrument_prefix = _target_field_prefix(field_prefix, "INSTRMNT", ldm_module, graph, target_classes)
        if instrument_prefix is not None:
            return "INSTRMNT_ID"
        preserved_security_id = _preserved_security_id_field(field_name)
        if preserved_security_id is not None:
            return preserved_security_id
        if field_name == "SCRTY_ID":
            return "SCRTY_ID"
        if field_name.endswith("_SCRTY_ID"):
            return "SCRTY_ID"
        if field_name.endswith("_SCRTSTN_ID"):
            return "SCRTSTN_ID"

    return candidates[0]


def _role_supertype_id_field(field_prefix: str) -> str | None:
    role_id_canonical_names = {
        "CLLTRL_RL": "CLLTRL_ID",
        "INSTRMNT_RL": "INSTRMNT_ID",
        "PRTCTN_ARRNGMNT_RL": "PRTCTN_ARRNGMNT_ID",
    }
    for role_class_name, canonical_field_name in role_id_canonical_names.items():
        if field_prefix.endswith(role_class_name):
            return canonical_field_name
    return None


def _preserved_security_id_field(field_name: str) -> str | None:
    preserved_suffixes = (
        "EXCHNG_TRDBL_DRVTV_PSTN_SCRTY_ID",
        "EXCHNG_TRDBL_DRVTV_SCRTY_ID",
        "OFFCL_SCRTY_ID",
    )
    for suffix in preserved_suffixes:
        if field_name.endswith(suffix):
            return suffix
    return None


def _is_direct_entity_role_relationship(
    source_class: ModelClass,
    related_model_name: str,
    target_table_name: str,
) -> bool:
    if target_table_name != "ENTTY_RL":
        return False
    return any(_has_direct_entity_role_key(source_class, prefix) for prefix in _identifier_abbreviations(related_model_name))


def _has_multiple_direct_entity_role_relationships(
    source_class: ModelClass,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    direct_relationship_count = 0
    for field in source_class.fields.values():
        if field.field_type != "ForeignKey" or field.name.endswith("_delegate") or field.related_model is None:
            continue
        if "ENTTY_RL" not in graph.relationship_target_tables(field.related_model, target_classes):
            continue
        if not any(_has_direct_entity_role_key(source_class, prefix) for prefix in _identifier_abbreviations(field.related_model)):
            continue
        direct_relationship_count += 1
        if direct_relationship_count > 1:
            return True
    return False


def _preserves_direct_entity_role_key(
    field_name: str,
    source_class_name: str,
    target_class_name: str,
    source_class: ModelClass,
    relationship_target: str,
    graph: _ClassGraph,
    target_classes: set[str],
) -> bool:
    if relationship_target != "ENTTY_RL":
        return False
    if source_class_name != target_class_name:
        return False
    if (
        _is_assignment_like_class(target_class_name)
        and not _has_multiple_direct_entity_role_relationships(source_class, graph, target_classes)
    ):
        return False
    if field_name.endswith("_PRTY_RL_TYP"):
        field_prefix = field_name[: -len("_PRTY_RL_TYP")]
    elif field_name.endswith("_RL_TYP"):
        field_prefix = field_name[: -len("_RL_TYP")]
    elif field_name.endswith("_ID") and not field_name.endswith("_PRTY_ID"):
        field_prefix = field_name[: -len("_ID")]
    else:
        return False
    return _has_direct_entity_role_key(source_class, field_prefix)


def _has_direct_entity_role_key(source_class: ModelClass, field_prefix: str) -> bool:
    return f"{field_prefix}_ID" in source_class.fields and f"{field_prefix}_PRTY_ID" not in source_class.fields


def _is_assignment_like_class(class_name: str) -> bool:
    return class_name.endswith("_ASSGNMNT")


def _entity_role_field_prefix(
    field_prefix: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> str | None:
    return _target_field_prefix(field_prefix, "ENTTY_RL", ldm_module, graph, target_classes)


def _target_field_prefix(
    field_prefix: str,
    target_class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> str | None:
    for candidate_prefix in _class_suffixes(field_prefix):
        if candidate_prefix not in ldm_module.classes:
            matched_class_name = _class_name_matching_prefix(
                candidate_prefix,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
                target_class_name=target_class_name,
            )
            if matched_class_name is None:
                continue
        else:
            matched_class_name = candidate_prefix

        nearest_target = graph.nearest_target_ancestor(matched_class_name, target_classes)
        if nearest_target == target_class_name:
            return candidate_prefix
    return None


def _class_name_matching_prefix(
    field_prefix: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
    target_class_name: str,
) -> str | None:
    for class_name in ldm_module.class_order:
        if graph.nearest_target_ancestor(class_name, target_classes) != target_class_name:
            continue
        if field_prefix in _identifier_abbreviations(class_name):
            return class_name
    return None


def _class_suffixes(field_prefix: str) -> Iterable[str]:
    parts = field_prefix.split("_")
    for index in range(len(parts)):
        yield "_".join(parts[index:])


def _annotated_relationship_key_component(
    source_class: ModelClass,
    field_name: str,
    target_class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
    allowed_relationship_targets: set[str],
) -> AnnotatedRelationshipKeyComponent | None:
    if not allowed_relationship_targets:
        return None
    if any(
        _canonical_relationship_key_field_name(field_name, relationship_target) is not None
        for relationship_target in allowed_relationship_targets
    ):
        return None

    for foreign_key in _sql_developer_foreign_keys(source_class):
        if not _foreign_key_contains_field(foreign_key, field_name):
            continue
        for relationship_target in _foreign_key_relationship_target_tables(
            foreign_key=foreign_key,
            graph=graph,
            target_classes=target_classes,
        ):
            if relationship_target == target_class_name:
                continue
            if relationship_target not in allowed_relationship_targets:
                continue
            canonical_field_name = _annotated_relationship_key_field_name(
                source_class=source_class,
                field_name=field_name,
                target_class_name=target_class_name,
                foreign_key=foreign_key,
                relationship_target=relationship_target,
                ldm_module=ldm_module,
                graph=graph,
                target_classes=target_classes,
            )
            return AnnotatedRelationshipKeyComponent(
                relationship_target=relationship_target,
                canonical_field_name=canonical_field_name,
            )
    return None


def _foreign_key_relationship_target_tables(
    foreign_key: dict,
    graph: _ClassGraph,
    target_classes: set[str],
) -> tuple[str, ...]:
    relationship_targets: list[str] = []
    for class_name_key in ("referenced_class", "source_class", "target_class"):
        class_name = foreign_key.get(class_name_key)
        if not isinstance(class_name, str):
            continue
        for relationship_target in graph.relationship_target_tables(class_name, target_classes):
            if relationship_target not in relationship_targets:
                relationship_targets.append(relationship_target)
    return tuple(relationship_targets)


def _annotated_relationship_key_field_name(
    source_class: ModelClass,
    field_name: str,
    target_class_name: str,
    foreign_key: dict,
    relationship_target: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> str | None:
    referenced_component = _referenced_primary_key_component_for_foreign_key_field(
        foreign_key=foreign_key,
        field_name=field_name,
        relationship_target=relationship_target,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    )
    if referenced_component is None:
        return _canonical_relationship_key_field_name(field_name, relationship_target)

    referenced_class_name, referenced_field_name = referenced_component
    canonical_field_name = _canonical_relationship_key_field_name(referenced_field_name, relationship_target)
    if canonical_field_name is not None:
        return canonical_field_name
    return _normalize_field_name(
        referenced_field_name,
        target_class_name=relationship_target,
        source_class_name=referenced_class_name,
        reference_fields=set(),
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    )


def _referenced_primary_key_component_for_foreign_key_field(
    foreign_key: dict,
    field_name: str,
    relationship_target: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> tuple[str, str] | None:
    foreign_key_fields = _ordered_foreign_key_fields(foreign_key)
    try:
        component_index = foreign_key_fields.index(field_name)
    except ValueError:
        return None

    referenced_class_names = _referenced_key_candidate_classes(
        foreign_key=foreign_key,
        relationship_target=relationship_target,
        ldm_module=ldm_module,
        graph=graph,
        target_classes=target_classes,
    )
    for referenced_class_name in referenced_class_names:
        referenced_class = ldm_module.classes.get(referenced_class_name)
        if referenced_class is None:
            continue
        primary_key_fields = _ordered_sql_developer_primary_key_fields(referenced_class)
        if field_name in primary_key_fields:
            return referenced_class_name, field_name

    source_canonical_field_name = _canonical_relationship_key_field_name(field_name, relationship_target)
    if source_canonical_field_name is not None:
        for referenced_class_name in referenced_class_names:
            referenced_class = ldm_module.classes.get(referenced_class_name)
            if referenced_class is None:
                continue
            for primary_key_field in _ordered_sql_developer_primary_key_fields(referenced_class):
                if _canonical_relationship_key_field_name(primary_key_field, relationship_target) == source_canonical_field_name:
                    return referenced_class_name, primary_key_field

    for referenced_class_name in referenced_class_names:
        referenced_class = ldm_module.classes.get(referenced_class_name)
        if referenced_class is None:
            continue
        primary_key_fields = _ordered_sql_developer_primary_key_fields(referenced_class)
        if component_index < len(primary_key_fields):
            return referenced_class_name, primary_key_fields[component_index]
    return None


def _referenced_key_candidate_classes(
    foreign_key: dict,
    relationship_target: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    target_classes: set[str],
) -> tuple[str, ...]:
    candidates: list[str] = []

    def add(class_name: object) -> None:
        if not isinstance(class_name, str):
            return
        if class_name not in ldm_module.classes:
            return
        if class_name not in candidates:
            candidates.append(class_name)

    for class_name_key in ("referenced_class", "source_class"):
        class_name = foreign_key.get(class_name_key)
        add(class_name)
        if isinstance(class_name, str) and class_name in ldm_module.classes:
            for ancestor_name in reversed(graph.ancestors(class_name)):
                add(ancestor_name)

    add(relationship_target)
    return tuple(candidates)


def _ordered_foreign_key_fields(foreign_key: dict) -> list[str]:
    field_entries = foreign_key.get("field_entries", [])
    if isinstance(field_entries, list) and field_entries:
        ordered_entries = sorted(
            (
                entry
                for entry in field_entries
                if isinstance(entry, dict) and isinstance(entry.get("field"), str)
            ),
            key=lambda entry: (
                entry.get("sequence") if isinstance(entry.get("sequence"), int) else 10**9,
                entry.get("field", ""),
            ),
        )
        return [entry["field"] for entry in ordered_entries]
    fields = foreign_key.get("fields", [])
    return [field for field in fields if isinstance(field, str)] if isinstance(fields, list) else []


def _ordered_sql_developer_primary_key_fields(model_class: ModelClass) -> list[str]:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    primary_key_fields = sql_developer_annotations.get("primary_key_fields", [])
    if isinstance(primary_key_fields, list) and primary_key_fields:
        ordered_entries = sorted(
            (
                entry
                for entry in primary_key_fields
                if isinstance(entry, dict) and isinstance(entry.get("field"), str)
            ),
            key=lambda entry: (
                entry.get("sequence") if isinstance(entry.get("sequence"), int) else 10**9,
                entry.get("field", ""),
            ),
        )
        return [entry["field"] for entry in ordered_entries]
    return [field for field in _sql_developer_primary_key(model_class) if isinstance(field, str)]


def _key_field_relationship_target(
    field_name: str,
    target_class_name: str,
    target_class_names_by_length: list[str],
    source_relationship_prefixes: dict[str, str],
) -> str | None:
    for source_prefix, relationship_target in sorted(
        source_relationship_prefixes.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if relationship_target == target_class_name:
            continue
        prefix = f"{source_prefix}_"
        if field_name.startswith(prefix) and _looks_like_key_component(field_name[len(prefix) :]):
            return relationship_target

    for related_target_name in target_class_names_by_length:
        if related_target_name == target_class_name:
            continue
        prefix = f"{related_target_name}_"
        if not field_name.startswith(prefix):
            continue
        suffix = field_name[len(prefix) :]
        if _looks_like_key_component(suffix):
            return related_target_name
    return None


def _looks_like_key_component(suffix: str) -> bool:
    key_suffixes = {
        "ACCNTNG_CNSLDTN_LVL",
        "ACCNTNG_STNDRD",
        "ID",
        "INSTRMNT_ID",
        "INSTRMNT_RFRNC_DT",
        "INSTRMNT_RPRTNG_AGNT_ID",
        "PRTY_ID",
        "PRTY_RFRNC_DT",
        "PRTY_RPRTNG_AGNT_ID",
        "PRTY_RL_TYP",
        "RFRNC_DT",
        "RL_TYP",
        "RPRTNG_AGNT_ID",
    }
    return suffix in key_suffixes


def _canonical_relationship_key_field_name(field_name: str, relationship_target: str) -> str | None:
    if field_name.endswith("_ACCNTNG_CNSLDTN_LVL"):
        return "ACCNTNG_CNSLDTN_LVL"
    if field_name.endswith("_ACCNTNG_STNDRD") and not field_name.endswith("_BY_ACCNTNG_STNDRD"):
        return "ACCNTNG_STNDRD"
    if field_name.endswith("_PSTN_ACCNTNG_CLSSFCTN"):
        return field_name
    if field_name.endswith("_ACCNTNG_CLSSFCTN"):
        return "ACCNTNG_CLSSFCTN"
    if field_name.endswith("_RFRNC_DT"):
        return "DT_RFRNC"
    if field_name.endswith("_RPRTNG_AGNT_ID"):
        return "RPRTNG_AGNT_ID"

    role_supertype_id = _role_supertype_id_field(relationship_target)
    if role_supertype_id is not None and field_name.endswith("_ID"):
        return role_supertype_id

    if relationship_target == "ENTTY_RL":
        if field_name.endswith("_PRTY_ID") or field_name.endswith("_ID"):
            return "PRTY_ID"
        if field_name.endswith("_PRTY_RL_TYP") or field_name.endswith("_RL_TYP"):
            return "ENTTY_RL_TYP"
        return None

    if relationship_target == "INSTRMNT":
        if field_name.endswith("_INSTRMNT_ID") or field_name.endswith("_ID"):
            return "INSTRMNT_ID"
        if field_name.endswith("_RL_TYP"):
            return "INSTRMNT_RL_TYP"
        return None

    if relationship_target in {
        "SNTHTC_SCRTSTN",
        "TRDTNL_SCRTSTN",
    }:
        if field_name.endswith("_ID"):
            return "SCRTSTN_ID"

    preserved_security_id = _preserved_security_id_field(field_name)
    if preserved_security_id is not None:
        return preserved_security_id

    if field_name in {"RSK_FCTR_ID", "SCRTY_ID"}:
        return field_name

    if field_name.endswith("_SCRTY_ID"):
        return "SCRTY_ID"

    if field_name.endswith("_ID"):
        return f"{relationship_target}_ID"
    if field_name.endswith("_RL_TYP"):
        if relationship_target.endswith("_RL"):
            return f"{relationship_target}_TYP"
        return f"{relationship_target}_RL_TYP"

    return None


def _remove_redundant_prefixed_key_fields(field_names: set[str], reference_fields: set[str] | None = None) -> None:
    reference_fields = reference_fields or set()
    canonical_suffixes = {
        "ACCNTNG_CNSLDTN_LVL": "ACCNTNG_CNSLDTN_LVL",
        "ACCNTNG_STNDRD": "ACCNTNG_STNDRD",
        "ACCNTNG_CLSSFCTN": "ACCNTNG_CLSSFCTN",
        "CLLTRL_RL_ID": "CLLTRL_ID",
        "INSTRMNT_ID": "INSTRMNT_ID",
        "INSTRMNT_RL_ID": "INSTRMNT_ID",
        "PRTCTN_ARRNGMNT_RL_ID": "PRTCTN_ARRNGMNT_ID",
        "PRTY_ID": "PRTY_ID",
        "PRTY_RL_TYP": "ENTTY_RL_TYP",
        "RFRNC_DT": "DT_RFRNC",
        "RL_TYP": "ENTTY_RL_TYP",
        "RPRTNG_AGNT_ID": "RPRTNG_AGNT_ID",
        "SCRTSTN_ID": "SCRTSTN_ID",
        "SCRTY_ID": "SCRTY_ID",
    }
    redundant_fields: set[str] = set()
    for field_name in field_names:
        if field_name in reference_fields:
            continue
        if _preserved_security_id_field(field_name) == field_name:
            continue
        for suffix, canonical_name in canonical_suffixes.items():
            if suffix == "ACCNTNG_STNDRD" and field_name.endswith("_BY_ACCNTNG_STNDRD"):
                continue
            if field_name == canonical_name:
                continue
            if canonical_name in field_names and field_name.endswith(f"_{suffix}"):
                redundant_fields.add(field_name)
                break
    field_names.difference_update(redundant_fields)


def _is_reduced_lower_level_discriminator_field(
    field_name: str,
    source_class_name: str,
    target_class_name: str,
    graph: _ClassGraph,
    ldm_module: DjangoModelModule,
) -> bool:
    if source_class_name == target_class_name:
        return False
    if source_class_name in graph.ancestors(target_class_name):
        return False
    if not (field_name.endswith("_TYP") or field_name.endswith("_INDCTR")):
        return False
    if _is_sql_developer_discriminator_not_merged(field_name, source_class_name, ldm_module):
        return False

    for identifier in _identifier_abbreviations(source_class_name):
        if field_name in {f"{identifier}_TYP", f"{identifier}_RL_TYP", f"{identifier}_INDCTR"}:
            return True
    return False


def _is_sql_developer_discriminator_not_merged(
    field_name: str,
    source_class_name: str,
    ldm_module: DjangoModelModule,
) -> bool:
    sql_developer_policy = _editable_sqldeveloper_forward_engineering_policy()
    normalized_not_merged_names = {
        _normalize_sql_developer_entity_name(name)
        for name in sql_developer_policy.discriminator_names_not_merged
    }
    if _normalize_sql_developer_entity_name(field_name) in normalized_not_merged_names:
        return True
    source_class = ldm_module.classes.get(source_class_name)
    if source_class is None:
        return False
    return any(
        _normalize_sql_developer_entity_name(logical_name) in normalized_not_merged_names
        for logical_name in _model_class_logical_names(source_class_name, source_class)
    )


def _relationship_field_prefixes(related_model_name: str, graph: _ClassGraph) -> tuple[str, ...]:
    prefixes = [related_model_name]
    prefixes.extend(reversed(graph.ancestors(related_model_name)))
    for class_name in list(prefixes):
        prefixes.extend(_identifier_abbreviations(class_name))
    seen: set[str] = set()
    return tuple(prefix for prefix in prefixes if not (prefix in seen or seen.add(prefix)))


def _identifier_abbreviations(identifier: str) -> tuple[str, ...]:
    variants: list[str] = [identifier]
    for vowels in ("AEIOU", "AEIOUY"):
        abbreviated_tokens = []
        for token in identifier.split("_"):
            if len(token) <= 1:
                abbreviated_tokens.append(token)
                continue
            abbreviated_tokens.append(token[0] + "".join(character for character in token[1:] if character not in vowels))
        variants.append("_".join(abbreviated_tokens))
    seen: set[str] = set()
    return tuple(variant for variant in variants if not (variant in seen or seen.add(variant)))


def _rewrite_assignment_name(source: str, old_name: str, new_name: str) -> str:
    if old_name == new_name:
        return source

    rewritten = re.sub(rf"^{re.escape(old_name)}\s*=", f"{new_name} =", source, count=1)
    rewritten = rewritten.replace(f"'{old_name}'", f"'{new_name}'", 1)
    rewritten = rewritten.replace(f'"{old_name}"', f'"{new_name}"', 1)
    return rewritten


def _indent_source(source: str, spaces: int = 4) -> str:
    indentation = " " * spaces
    return "\n".join(f"{indentation}{line}" if line else line for line in source.splitlines())


def _default_meta_lines(class_name: str) -> list[str]:
    return [
        "",
        "    class Meta:",
        f"        verbose_name = '{class_name}'",
        f"        verbose_name_plural = '{class_name}s'",
    ]


def _looks_like_helper_or_domain_class(class_name: str) -> bool:
    if any(character.islower() for character in class_name):
        return True

    helper_suffixes = (
        "_domain",
        "_type",
        "_indicator",
        "_by_accounting_standard",
        "_by_identifier",
        "_by_legal_proceeding_status",
    )
    return class_name.endswith(helper_suffixes)


def _is_helper_unique_id_field(source_class_name: str, field_name: str) -> bool:
    return _looks_like_helper_or_domain_class(source_class_name) and field_name == f"{source_class_name}_uniqueID"


def _is_folded_unique_id_field(source_class_name: str, target_class_name: str, field_name: str) -> bool:
    if field_name != f"{source_class_name}_uniqueID":
        return False
    return source_class_name != target_class_name or _is_helper_unique_id_field(source_class_name, field_name)


def _sorted_in_reference_order(field_names: set[str], reference_class: ModelClass | None) -> list[str]:
    if reference_class is None:
        return sorted(field_names)
    reference_order = list(reference_class.fields)
    ordered = [field_name for field_name in reference_order if field_name in field_names]
    ordered.extend(sorted(field_names - set(ordered)))
    return ordered


def _parse_generated_source(generated_source: str) -> DjangoModelModule:
    temporary_path = Path("<generated_forward_engineering_model>")
    generated_source_lines = generated_source.splitlines(keepends=True)
    parsed = ast.parse(generated_source, filename=str(temporary_path))
    classes: dict[str, ModelClass] = {}
    class_order: list[str] = []
    from pybirdai.process_steps.forward_engineering import django_model_ast as parser

    for node in parsed.body:
        if not isinstance(node, ast.ClassDef):
            continue
        model_class = ModelClass(
            name=node.name,
            bases=[parser._base_name(base) for base in node.bases],
            line_number=node.lineno,
        )
        for statement in node.body:
            parsed_annotations = parser._parse_class_annotations(statement)
            if parsed_annotations is not None:
                model_class.annotations.update(parsed_annotations)
                continue

            parsed_statement = parser._parse_class_statement(generated_source_lines, statement)
            if parsed_statement is not None:
                model_class.statements.append(parsed_statement)
            elif isinstance(statement, ast.ClassDef) and statement.name == "Meta":
                model_class.meta_source = parser._source_segment(generated_source_lines, statement)
        classes[model_class.name] = model_class
        class_order.append(model_class.name)

    return DjangoModelModule(path=temporary_path, classes=classes, class_order=class_order)


def _build_report(
    ldm_module: DjangoModelModule,
    reference_module: DjangoModelModule | None,
    target_class_order: list[str],
    class_reports: list[ClassEngineeringReport],
    comparison: dict,
    include_reference_fallback: bool,
) -> dict:
    generated_field_count = sum(len(class_report.generated_fields) for class_report in class_reports)
    folded_field_count = sum(len(class_report.folded_fields) for class_report in class_reports)
    fallback_field_count = sum(len(class_report.reference_fallback_fields) for class_report in class_reports)
    explained_field_count = generated_field_count - fallback_field_count

    return {
        "summary": {
            "ldm_model": str(ldm_module.path),
            "reference_model": str(reference_module.path) if reference_module is not None else None,
            "ldm_class_count": len(ldm_module.classes),
            "target_class_count": len(target_class_order),
            "generated_field_count": generated_field_count,
            "explained_field_count": explained_field_count,
            "folded_field_count": folded_field_count,
            "reference_fallback_field_count": fallback_field_count,
            "reference_fallback_field_ratio": (
                fallback_field_count / generated_field_count if generated_field_count else 0.0
            ),
            "include_reference_fallback": include_reference_fallback,
            "uses_discriminator_mapping_csvs": False,
        },
        "comparison": comparison,
        "classes": {
            class_report.target_class: {
                "ldm_source_classes": class_report.ldm_source_classes,
                "generated_fields": class_report.generated_fields,
                "folded_fields": class_report.folded_fields,
                "synthetic_fields": class_report.synthetic_fields,
                "reference_fallback_fields": class_report.reference_fallback_fields,
                "missing_reference_fields": class_report.missing_reference_fields,
                "extra_generated_fields": class_report.extra_generated_fields,
            }
            for class_report in class_reports
        },
    }


def _build_column_validation_rules(
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    validation_contexts: dict[str, ClassValidationContext],
) -> list[dict]:
    rules: list[dict] = []
    seen_rules: set[str] = set()

    for context in validation_contexts.values():
        for source_class_name in context.ldm_source_classes:
            source_class = ldm_module.classes.get(source_class_name)
            if source_class is None or _direct_model_parent(source_class_name, ldm_module) is None:
                continue

            discriminators = _validation_discriminator_fields_for_source(
                source_class_name=source_class_name,
                context=context,
                ldm_module=ldm_module,
                graph=graph,
            )
            if not discriminators:
                continue

            entity_member = _validation_entity_member_info(source_class_name, ldm_module, graph)
            if entity_member is None:
                continue

            for field in source_class.fields.values():
                if field.field_type == "ForeignKey":
                    continue
                if _validation_field_is_type_attribute(source_class, field.name):
                    continue

                output_name = context.derived_field_set.source_field_names.get((source_class_name, field.name))
                if output_name is None or output_name not in context.generated_field_names:
                    continue

                original_value_name = _validation_field_attribute_name(source_class, field.name)
                is_mandatory = _validation_field_is_mandatory(
                    context=context,
                    source_class=source_class,
                    field=field,
                    output_name=output_name,
                )
                for _discriminator_source_class, _discriminator_field_name, discriminator_output_name in discriminators:
                    positive_rule = {
                        "step": 1,
                        "table": context.target_class,
                        "type": "IF",
                        "attr": discriminator_output_name,
                        "comparator": "=",
                        "originalEntityName": entity_member.entity_name,
                        "entities": [[entity_member.code, entity_member.entity_name, entity_member.class_name]],
                        "value": output_name,
                        "originalValueName": original_value_name,
                        "assertComparator": "!=",
                        "assertValue": ["NULL", "Not Applicable"],
                    }
                    negative_rule = {
                        "step": 1,
                        "table": context.target_class,
                        "type": "IF",
                        "attr": discriminator_output_name,
                        "comparator": "!=",
                        "originalEntityName": entity_member.entity_name,
                        "entities": [[entity_member.code, entity_member.entity_name, entity_member.class_name]],
                        "value": output_name,
                        "originalValueName": original_value_name,
                        "assertComparator": "=",
                        "assertValue": ["NULL"],
                    }
                    if is_mandatory:
                        _append_unique_validation_rule(rules, seen_rules, positive_rule)
                    _append_unique_validation_rule(rules, seen_rules, negative_rule)

    return rules


def _build_relationship_validation_rules(
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
    validation_contexts: dict[str, ClassValidationContext],
    target_classes: set[str],
) -> list[dict]:
    rules: list[dict] = []
    seen_rules: set[str] = set()
    excluded_tables = {"ENTTY_RL", "ABSTRCT_INSTRMNT_RL"}

    for owner_class_name in ldm_module.class_order:
        owner_class = ldm_module.classes[owner_class_name]
        for source_class_name, target_class_name in _validation_relationship_class_candidates(
            owner_class_name=owner_class_name,
            owner_class=owner_class,
            ldm_module=ldm_module,
        ):
            if source_class_name is None or target_class_name is None:
                continue
            if source_class_name not in ldm_module.classes or target_class_name not in ldm_module.classes:
                continue
            if _direct_model_parent(source_class_name, ldm_module) is None:
                continue

            source_table = _validation_engineered_table_for_class(source_class_name, graph, target_classes)
            target_table = _validation_engineered_table_for_class(target_class_name, graph, target_classes)
            if source_table is None or target_table is None:
                continue
            if source_table == target_table or source_table in excluded_tables or target_table in excluded_tables:
                continue

            source_context = validation_contexts.get(source_table)
            if source_context is None:
                continue
            source_members = _validation_condition_members(source_class_name, ldm_module, graph)
            condition_values, condition_columns = _validation_source_relationship_conditions(
                context=source_context,
                source_members=source_members,
                ldm_module=ldm_module,
            )
            if not condition_columns:
                continue

            rule = {
                "type": "RELATIONSHIP",
                "targetTable": target_table,
                "sourceEntity": _validation_entity_name(source_class_name, ldm_module),
                "sourceTable": source_table,
                "targetEntity": _validation_entity_name(target_class_name, ldm_module),
                "sourceRelationshipConditionValue": condition_values,
                "sourceRelationshipConditionColumn": condition_columns,
            }
            _append_unique_validation_rule(rules, seen_rules, rule)

    return rules


def _build_choice_comparison_summary(comparison: dict) -> dict:
    if not comparison:
        return {}

    differing_label_count = 0
    differing_zero_label_count = 0
    extra_value_count = 0
    extra_zero_count = 0
    missing_value_count = 0
    missing_zero_count = 0

    for class_report in comparison.get("classes", {}).values():
        for difference in class_report.get("choice_differences", {}).values():
            missing_values = difference.get("missing_values", [])
            extra_values = difference.get("extra_values", [])
            differing_labels = difference.get("differing_labels", {})
            if isinstance(missing_values, list):
                missing_value_count += len(missing_values)
                missing_zero_count += missing_values.count("0")
            if isinstance(extra_values, list):
                extra_value_count += len(extra_values)
                extra_zero_count += extra_values.count("0")
            if isinstance(differing_labels, dict):
                differing_label_count += len(differing_labels)
                if "0" in differing_labels:
                    differing_zero_label_count += 1

    total_value_level_difference_count = missing_value_count + extra_value_count + differing_label_count
    return {
        "choice_difference_count": comparison.get("choice_difference_count", 0),
        "choice_match_ratio": comparison.get("choice_match_ratio", 1.0),
        "differing_label_count": differing_label_count,
        "differing_zero_label_count": differing_zero_label_count,
        "extra_value_count": extra_value_count,
        "extra_zero_count": extra_zero_count,
        "generated_choice_field_count": comparison.get("generated_choice_field_count", 0),
        "matching_choice_field_count": comparison.get("matching_choice_field_count", 0),
        "missing_value_count": missing_value_count,
        "missing_zero_count": missing_zero_count,
        "reference_choice_field_count": comparison.get("reference_choice_field_count", 0),
        "total_value_level_difference_count": total_value_level_difference_count,
    }


def _validation_relationship_class_candidates(
    owner_class_name: str,
    owner_class: ModelClass,
    ldm_module: DjangoModelModule,
) -> list[tuple[str | None, str | None]]:
    candidates: list[tuple[str | None, str | None]] = []
    seen: set[tuple[str | None, str | None]] = set()

    def add(source_class_name: str | None, target_class_name: str | None) -> None:
        candidate = (source_class_name, target_class_name)
        if candidate in seen:
            return
        seen.add(candidate)
        candidates.append(candidate)

    for foreign_key in _sql_developer_foreign_keys(owner_class):
        add(*_validation_foreign_key_source_target_classes(owner_class_name, foreign_key))

    for field in owner_class.fields.values():
        if field.field_type != "ForeignKey" or field.name.endswith("_delegate") or field.related_model is None:
            continue
        related_class = ldm_module.classes.get(field.related_model)
        owner_has_parent = any(base in ldm_module.classes for base in owner_class.bases)
        related_has_parent = related_class is not None and any(base in ldm_module.classes for base in related_class.bases)
        if related_has_parent:
            add(field.related_model, owner_class_name)
        if owner_has_parent or not related_has_parent:
            add(owner_class_name, field.related_model)

    return candidates


def _append_unique_validation_rule(rules: list[dict], seen_rules: set[str], rule: dict) -> None:
    rule_key = json.dumps(rule, sort_keys=True)
    if rule_key in seen_rules:
        return
    seen_rules.add(rule_key)
    rules.append(rule)


def _without_empty_values(value: dict) -> dict:
    return {
        key: item
        for key, item in value.items()
        if item is not None and item != "" and item != [] and item != {}
    }


def _validation_discriminator_fields_for_source(
    source_class_name: str,
    context: ClassValidationContext,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> list[tuple[str, str, str]]:
    source_class = ldm_module.classes[source_class_name]
    entity_member = _entity_member_annotation(source_class)
    annotated_discriminator = entity_member.get("discriminator_field") or entity_member.get("domain_synonym")
    search_class_names = [source_class_name, *reversed(graph.ancestors(source_class_name))]
    if annotated_discriminator:
        for candidate_class_name in search_class_names:
            candidate_class = ldm_module.classes.get(candidate_class_name)
            if candidate_class is None:
                continue
            for field in candidate_class.fields.values():
                if _validation_names_match(field.name, str(annotated_discriminator)):
                    output_name = _validation_output_name_for_source_field(context, candidate_class_name, field.name)
                    if output_name is not None:
                        return [(candidate_class_name, field.name, output_name)]

    discriminator_fields: list[tuple[str, str, str]] = []
    parent_class_name = _direct_model_parent(source_class_name, ldm_module)
    if parent_class_name is None:
        return discriminator_fields
    parent_class = ldm_module.classes[parent_class_name]
    for field in parent_class.fields.values():
        if field.field_type == "ForeignKey":
            continue
        if not _validation_field_is_type_attribute(parent_class, field.name):
            continue
        output_name = _validation_output_name_for_source_field(context, parent_class_name, field.name)
        if output_name is not None:
            discriminator_fields.append((parent_class_name, field.name, output_name))
    return discriminator_fields


def _validation_output_name_for_source_field(
    context: ClassValidationContext,
    source_class_name: str,
    field_name: str,
) -> str | None:
    output_name = context.derived_field_set.source_field_names.get((source_class_name, field_name))
    if output_name is not None and output_name in context.generated_field_names:
        return output_name
    if field_name in context.generated_field_names:
        return field_name
    return None


def _validation_foreign_key_source_target_classes(
    owner_class_name: str,
    foreign_key: dict,
) -> tuple[str | None, str | None]:
    source_class = foreign_key.get("source_class")
    target_class = foreign_key.get("target_class")
    if isinstance(source_class, str) and isinstance(target_class, str):
        return source_class, target_class

    referenced_class = foreign_key.get("referenced_class")
    if not isinstance(referenced_class, str):
        return None, None

    relation_side = foreign_key.get("relation_side")
    if relation_side == "source":
        return owner_class_name, referenced_class
    if relation_side == "target":
        return referenced_class, owner_class_name
    return owner_class_name, referenced_class


def _validation_engineered_table_for_class(
    class_name: str,
    graph: _ClassGraph,
    target_classes: set[str],
) -> str | None:
    target_table = graph.nearest_target_ancestor(class_name, target_classes)
    if target_table is not None:
        return target_table
    target_tables = graph.relationship_target_tables(class_name, target_classes)
    return target_tables[0] if target_tables else None


def _validation_source_relationship_conditions(
    context: ClassValidationContext,
    source_members: list[EntityMemberInfo],
    ldm_module: DjangoModelModule,
) -> tuple[list[str], list[str]]:
    condition_values: list[str] = []
    condition_columns: list[str] = []
    normalized_member_labels = {
        _normalize_sql_developer_entity_name(member.label): member.label for member in source_members
    }
    normalized_member_labels.update(
        {
            _normalize_sql_developer_entity_name(member.entity_name): member.label
            for member in source_members
        }
    )

    for field_name in sorted(context.generated_field_names):
        choice_values = context.derived_field_set.choice_values_by_field.get(field_name, {})
        if not choice_values:
            continue
        if not _validation_output_field_is_discriminator(field_name):
            continue
        matched_any = False
        for label in choice_values.values():
            member_label = normalized_member_labels.get(_normalize_sql_developer_entity_name(label))
            if member_label is None:
                continue
            _append_unique_value(condition_values, member_label)
            matched_any = True
        if matched_any:
            _append_unique_value(condition_columns, field_name)

    for member in source_members:
        member_class = ldm_module.classes.get(member.class_name)
        if member_class is None:
            continue
        entity_member = _entity_member_annotation(member_class)
        discriminator = entity_member.get("discriminator_field") or entity_member.get("domain_synonym")
        if not discriminator:
            continue
        for output_name in context.generated_field_names:
            if _validation_names_match(output_name, str(discriminator)):
                _append_unique_value(condition_values, member.label)
                _append_unique_value(condition_columns, output_name)

    return condition_values, condition_columns


def _validation_condition_members(
    source_class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> list[EntityMemberInfo]:
    member_class_names = _hierarchy_leaf_descendants(source_class_name, graph) or [source_class_name]
    members: list[EntityMemberInfo] = []
    for member_class_name in member_class_names:
        member = _validation_entity_member_info(member_class_name, ldm_module, graph)
        if member is None:
            members.append(
                EntityMemberInfo(
                    code="",
                    label=_validation_entity_name(member_class_name, ldm_module),
                    entity_name=_validation_entity_name(member_class_name, ldm_module),
                    class_name=member_class_name,
                )
            )
        else:
            members.append(member)
    return members


def _validation_entity_member_info(
    class_name: str,
    ldm_module: DjangoModelModule,
    graph: _ClassGraph,
) -> EntityMemberInfo | None:
    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return None

    entity_member = _entity_member_annotation(model_class)
    value = entity_member.get("member_code") or entity_member.get("value")
    label = (
        entity_member.get("member_description")
        or entity_member.get("source_member_description")
        or entity_member.get("member_label")
        or entity_member.get("label")
    )
    if value is None or label is None:
        automatic_member = _entity_member_for_class(class_name, ldm_module, graph)
        if automatic_member is None:
            return None
        value, label = automatic_member

    return EntityMemberInfo(
        code=str(value),
        label=_validation_readable_label(str(label)),
        entity_name=_validation_entity_name(class_name, ldm_module),
        class_name=class_name,
    )


def _validation_entity_name(class_name: str, ldm_module: DjangoModelModule) -> str:
    model_class = ldm_module.classes.get(class_name)
    if model_class is None:
        return class_name
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    entity_member = _entity_member_annotation(model_class)
    return str(
        entity_member.get("entity_name")
        or sql_developer_annotations.get("entity_name")
        or _model_verbose_name(model_class)
        or class_name
    )


def _validation_field_attribute_name(model_class: ModelClass, field_name: str) -> str:
    field_annotations = _sql_developer_field_annotations(model_class, field_name)
    return str(field_annotations.get("attribute_name") or field_name)


def _validation_field_is_type_attribute(model_class: ModelClass, field_name: str) -> bool:
    attribute_name = _validation_field_attribute_name(model_class, field_name)
    return attribute_name.strip().lower().endswith(" type") or field_name.endswith("_TYP")


def _validation_output_field_is_discriminator(field_name: str) -> bool:
    return field_name.endswith("_TYP") or field_name.endswith("_INDCTR")


def _validation_field_is_mandatory(
    context: ClassValidationContext,
    source_class: ModelClass,
    field: ModelStatement,
    output_name: str,
) -> bool:
    field_annotations = _sql_developer_field_annotations(source_class, field.name)
    for key in ("mandatory", "is_mandatory"):
        if key in field_annotations:
            return _sql_developer_bool_annotation(field_annotations, key)
    if field_annotations.get("not_applicable_present") is True:
        return False
    if output_name in context.derived_field_set.not_applicable_choice_fields:
        return False
    choice_values = context.derived_field_set.choice_values_by_field.get(output_name, {})
    if _validation_choice_values_include_not_applicable(choice_values):
        return False
    return field.primary_key


def _validation_choice_values_include_not_applicable(choice_values: dict[str, str]) -> bool:
    return any(
        value == "0" or _normalize_sql_developer_entity_name(label) == "not_applicable"
        for value, label in choice_values.items()
    )


def _validation_readable_label(label: str) -> str:
    return label.replace("_", " ") if "_" in label and " " not in label else label


def _validation_names_match(first_name: str, second_name: str) -> bool:
    return _normalize_sql_developer_entity_name(first_name) == _normalize_sql_developer_entity_name(second_name)


def _append_unique_value(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _build_field_lineage_report(
    ldm_module: DjangoModelModule,
    reference_module: DjangoModelModule | None,
    target_class_order: list[str],
    class_reports: list[ClassEngineeringReport],
    class_field_lineage: dict[str, dict],
    include_reference_fallback: bool,
) -> dict:
    generated_field_count = sum(len(class_report.generated_fields) for class_report in class_reports)
    fields_with_ldm_sources_count = sum(
        1
        for class_lineage in class_field_lineage.values()
        for field_info in class_lineage.get("fields", {}).values()
        if field_info.get("sources")
    )
    source_link_count = sum(
        len(field_info.get("sources", []))
        for class_lineage in class_field_lineage.values()
        for field_info in class_lineage.get("fields", {}).values()
    )

    return {
        "summary": {
            "ldm_model": str(ldm_module.path),
            "reference_model": str(reference_module.path) if reference_module is not None else None,
            "target_class_count": len(target_class_order),
            "generated_field_count": generated_field_count,
            "fields_with_ldm_sources_count": fields_with_ldm_sources_count,
            "source_link_count": source_link_count,
            "include_reference_fallback": include_reference_fallback,
        },
        "classes": {
            class_report.target_class: class_field_lineage.get(
                class_report.target_class,
                {
                    "ldm_source_classes": class_report.ldm_source_classes,
                    "fields": {},
                },
            )
            for class_report in class_reports
        },
    }


def _default_repo_path(relative_path: str) -> Path:
    return Path(__file__).resolve().parents[4] / relative_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Forward-engineer a generated Django BIRD LDM model into an EIL-style Django model."
    )
    parser.add_argument(
        "--ldm-model",
        type=Path,
        default=_default_repo_path("bird_data_model.py"),
        help="Path to the generated Django LDM model.",
    )
    parser.add_argument(
        "--reference-model",
        type=Path,
        default=None,
        help="Optional EIL Django model used as a target contract. Omit for a no-reference FE run.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_default_repo_path("birds_nest/results/forward_engineering/generated_bird_data_model.py"),
        help="Path where the generated Django model should be written.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=_default_repo_path("birds_nest/results/forward_engineering/forward_engineering_report.json"),
        help="Path where the JSON comparison report should be written.",
    )
    parser.add_argument(
        "--field-lineage",
        type=Path,
        default=_default_repo_path("birds_nest/results/forward_engineering/forward_engineering_field_lineage.json"),
        help="Path where generated-field to LDM-field lineage JSON should be written.",
    )
    parser.add_argument(
        "--column-validation-rules",
        type=Path,
        default=_default_repo_path(
            "birds_nest/results/forward_engineering/column_validation_rules.json"
        ),
        help="Path where SQLDeveloper-style column validation rules JSON should be written.",
    )
    parser.add_argument(
        "--relationship-validation-rules",
        type=Path,
        default=_default_repo_path(
            "birds_nest/results/forward_engineering/relationship_validation_rules.json"
        ),
        help="Path where SQLDeveloper-style relationship validation rules JSON should be written.",
    )
    parser.add_argument(
        "--choice-comparison-summary",
        type=Path,
        default=_default_repo_path(
            "birds_nest/results/forward_engineering/generated_vs_eil_choice_comparison_summary.json"
        ),
        help="Path where generated-vs-reference choice comparison summary JSON should be written.",
    )
    parser.add_argument(
        "--no-reference-fallback",
        action="store_false",
        dest="reference_fallback",
        help="Do not emit fields that can only be sourced from the reference model. This is the default.",
    )
    parser.add_argument(
        "--reference-fallback",
        action="store_true",
        dest="reference_fallback",
        help="Development aid: fill fields from the reference model when LDM-only rules do not explain them.",
    )
    parser.set_defaults(reference_fallback=False)

    args = parser.parse_args(argv)
    result = run_forward_engineering(
        ForwardEngineeringOptions(
            ldm_model_path=args.ldm_model,
            output_model_path=args.output,
            reference_model_path=args.reference_model if args.reference_model else None,
            report_path=args.report if args.report else None,
            field_lineage_path=args.field_lineage if args.field_lineage else None,
            column_validation_rules_path=args.column_validation_rules if args.column_validation_rules else None,
            relationship_validation_rules_path=(
                args.relationship_validation_rules if args.relationship_validation_rules else None
            ),
            choice_comparison_summary_path=(
                args.choice_comparison_summary if args.choice_comparison_summary else None
            ),
            include_reference_fallback=args.reference_fallback,
        )
    )

    summary = result.report["summary"]
    comparison = result.report.get("comparison", {})
    print(f"Generated {summary['target_class_count']} classes and {summary['generated_field_count']} fields.")
    if comparison:
        match_ratio = comparison.get("field_match_ratio", 0.0)
        print(f"Reference field match ratio: {match_ratio:.2%}")
    print(f"Output written to {args.output}")
    if args.report:
        print(f"Report written to {args.report}")
    if args.field_lineage:
        print(f"Field lineage written to {args.field_lineage}")
    if args.column_validation_rules:
        print(f"Column validation rules written to {args.column_validation_rules}")
    if args.relationship_validation_rules:
        print(f"Relationship validation rules written to {args.relationship_validation_rules}")
    if args.choice_comparison_summary:
        print(f"Choice comparison summary written to {args.choice_comparison_summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
