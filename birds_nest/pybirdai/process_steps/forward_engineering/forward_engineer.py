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
    skipped_source_fields: set[tuple[str, str]] = field(default_factory=set)


@dataclass
class ForwardEngineeringResult:
    """The result of a forward-engineering run."""

    generated_source: str
    report: dict


def run_forward_engineering(options: ForwardEngineeringOptions) -> ForwardEngineeringResult:
    """Run Python forward engineering and write the requested output files."""

    ldm_module = parse_django_model(options.ldm_model_path)
    reference_module = parse_django_model(options.reference_model_path) if options.reference_model_path else None

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=reference_module,
        include_reference_fallback=options.include_reference_fallback,
    )

    options.output_model_path.parent.mkdir(parents=True, exist_ok=True)
    options.output_model_path.write_text(generated_source, encoding="utf-8")

    if options.report_path is not None:
        options.report_path.parent.mkdir(parents=True, exist_ok=True)
        options.report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return ForwardEngineeringResult(generated_source=generated_source, report=report)


def generate_forward_engineered_source(
    ldm_module: DjangoModelModule,
    reference_module: DjangoModelModule | None = None,
    include_reference_fallback: bool = False,
) -> tuple[str, dict]:
    """Generate an EIL-style Django model source file from an LDM model."""

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
        derived_fields = derived_field_set.field_names
        synthetic_fields = {"test_id", f"{target_class_name}_uniqueID"}

        if reference_class is not None:
            reference_field_names = set(reference_class.fields)
            generated_field_names = (derived_fields | synthetic_fields) & reference_field_names
            if include_reference_fallback:
                generated_field_names = set(reference_field_names)
            class_lines = _render_class_from_reference(
                reference_class=reference_class,
                included_fields=generated_field_names,
            )
        else:
            generated_field_names = derived_fields | synthetic_fields
            class_lines = _render_class_from_ldm(
                target_class_name=target_class_name,
                ldm_class=ldm_class,
                source_class_names=ldm_source_classes,
                ldm_module=ldm_module,
                generated_field_names=generated_field_names,
                relationship_targets=derived_field_set.relationship_targets,
                source_field_names=derived_field_set.source_field_names,
                skipped_source_fields=derived_field_set.skipped_source_fields,
                graph=graph,
                target_classes=target_classes,
            )

        lines.extend(class_lines)
        lines.append("")

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
    return generated_source, report


def compare_model_modules(generated_module: DjangoModelModule, reference_module: DjangoModelModule | None) -> dict:
    """Compare generated model class and field names with a reference model."""

    if reference_module is None:
        return {}

    generated_classes = set(generated_module.classes)
    reference_classes = set(reference_module.classes)
    per_class: dict[str, dict] = {}

    for class_name in reference_module.class_order:
        if class_name not in generated_module.classes:
            continue
        generated_fields = set(generated_module.classes[class_name].fields)
        reference_fields = set(reference_module.classes[class_name].fields)
        per_class[class_name] = {
            "generated_field_count": len(generated_fields),
            "reference_field_count": len(reference_fields),
            "matching_fields": len(generated_fields & reference_fields),
            "missing_fields": sorted(reference_fields - generated_fields),
            "extra_fields": sorted(generated_fields - reference_fields),
        }

    generated_field_count = sum(len(model_class.fields) for model_class in generated_module.classes.values())
    reference_field_count = sum(len(model_class.fields) for model_class in reference_module.classes.values())
    matching_field_count = sum(class_report["matching_fields"] for class_report in per_class.values())

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
        "classes": per_class,
    }


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
    }

    return SQLDeveloperForwardEngineeringPolicy(
        include_entity_names=frozenset(include_entity_names),
        include_class_names=frozenset(include_class_names),
        merge_entity_names=frozenset(merge_entity_names),
        merge_class_names=frozenset(merge_class_names),
        folded_class_names=frozenset(folded_class_names),
        suppressed_field_names_by_target=suppressed_field_names_by_target,
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
        if foreign_key.get("source_class") or foreign_key.get("referenced_class"):
            continue
        relation_name = foreign_key.get("relation_name", "")
        if relation_name.startswith("Model_Context"):
            return True
    return False


def _sql_developer_foreign_keys(model_class: ModelClass) -> list[dict]:
    sql_developer_annotations = model_class.annotations.get("sql_developer", {})
    foreign_keys = sql_developer_annotations.get("foreign_keys", [])
    return foreign_keys if isinstance(foreign_keys, list) else []


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
            add_source(root_class_name)
            for descendant_name in self.folded_descendants(root_class_name, target_classes):
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
    suppressed_field_names = sql_developer_policy.suppressed_field_names_by_target.get(target_class_name, frozenset())
    apply_sql_developer_target_cleanup = target_class_name in sql_developer_policy.suppressed_field_names_by_target
    derived_field_set = DerivedFieldSet()
    relationship_counts: dict[str, int] = {}
    relationship_fields_by_target: dict[str, str] = {}
    key_relationship_candidates: list[tuple[str, str]] = []
    target_class_names_by_length = sorted(target_classes, key=len, reverse=True)

    def add_relationship_field(target_table_name: str, allow_duplicate: bool = False) -> bool:
        if not reference_fields and not allow_duplicate and target_table_name in relationship_fields_by_target:
            return True
        relationship_index = relationship_counts.get(target_table_name, 0)
        field_name = f"the{target_table_name}{relationship_index if relationship_index else ''}"
        if reference_fields and field_name not in reference_fields:
            return False
        derived_field_set.field_names.add(field_name)
        derived_field_set.relationship_targets[field_name] = target_table_name
        relationship_fields_by_target[target_table_name] = field_name
        relationship_counts[target_table_name] = relationship_index + 1
        return True

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
                if target_table_name == target_class_name:
                    continue
                for relationship_prefix in _relationship_field_prefixes(field.related_model, graph):
                    source_relationship_prefixes.setdefault(relationship_prefix, target_table_name)
                if add_relationship_field(
                    target_table_name,
                    allow_duplicate=(
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
                    ),
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
            ):
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
            relationship_target = _key_field_relationship_target(
                field.name,
                target_class_name,
                target_class_names_by_length,
                source_relationship_prefixes,
            )
            if relationship_target is not None:
                key_relationship_candidates.append((source_class_name, relationship_target))
                add_relationship_field(relationship_target)
                canonical_key_field = _canonical_relationship_key_field_name(field.name, relationship_target)
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
                elif reference_fields and output_name in reference_fields:
                    relationship_key_field = output_name
                elif canonical_key_field is not None:
                    relationship_key_field = canonical_key_field
                else:
                    relationship_key_field = output_name
                derived_field_set.field_names.add(relationship_key_field)
                derived_field_set.source_field_names[(source_class_name, field.name)] = relationship_key_field
                continue
            if _is_folded_source_primary_key_component(
                source_class_name=source_class_name,
                target_class_name=target_class_name,
                source_class=source_class,
                field_name=field.name,
            ) and apply_sql_developer_target_cleanup:
                derived_field_set.skipped_source_fields.add((source_class_name, field.name))
                continue
            derived_field_set.field_names.add(output_name)
            derived_field_set.source_field_names[(source_class_name, field.name)] = output_name

    seen_key_relationships: set[tuple[str, str]] = set()
    for source_class_name, target_table_name in key_relationship_candidates:
        relationship_key = (source_class_name, target_table_name)
        if relationship_key in seen_key_relationships:
            continue
        seen_key_relationships.add(relationship_key)
        add_relationship_field(target_table_name)

    _remove_redundant_prefixed_key_fields(derived_field_set.field_names, reference_fields)
    derived_field_set.field_names.difference_update(suppressed_field_names)
    for field_name in suppressed_field_names:
        derived_field_set.relationship_targets.pop(field_name, None)
    return derived_field_set


def _render_class_from_reference(reference_class: ModelClass, included_fields: set[str]) -> list[str]:
    lines = [f"class {reference_class.name}(models.Model):"]
    pending_choices: list[ModelStatement] = []
    emitted_any_statement = False

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
    skipped_source_fields: set[tuple[str, str]],
    graph: _ClassGraph,
    target_classes: set[str],
) -> list[str]:
    lines = [f"class {target_class_name}(models.Model):"]
    emitted_fields: set[str] = set()

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
        choices = source_class.choices
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

            if field.choices_name and field.choices_name in choices:
                lines.append(_indent_source(choices[field.choices_name].source))
            lines.append(_indent_source(_rewrite_assignment_name(field.source, field.name, output_name)))
            emitted_fields.add(output_name)

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
    if field_name.endswith("_ACCNTNG_STNDRD"):
        return "ACCNTNG_STNDRD"
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
) -> bool:
    if source_class_name == target_class_name:
        return False
    if source_class_name in graph.ancestors(target_class_name):
        return False
    if not (field_name.endswith("_TYP") or field_name.endswith("_INDCTR")):
        return False

    for identifier in _identifier_abbreviations(source_class_name):
        if field_name in {f"{identifier}_TYP", f"{identifier}_RL_TYP", f"{identifier}_INDCTR"}:
            return True
    return False


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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
