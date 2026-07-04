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

import csv

from pybirdai.process_steps.forward_engineering.django_model_ast import parse_django_model
from pybirdai.process_steps.forward_engineering.forward_engineer import (
    _ClassGraph,
    DerivedFieldSet,
    _add_accounting_context_not_applicable_choice_values,
    _add_sql_developer_input_domain_choice_label_overrides,
    _add_sql_developer_folded_input_domain_choice_values,
    _looks_like_helper_or_domain_class,
    _literal_choice_values,
    _reduced_discriminator_leaf_choice_values,
    compare_model_modules,
    generate_forward_engineered_source,
)
from pybirdai.process_steps.sqldeveloper_import.ldm_annotation_enricher import (
    enrich_django_ldm_annotations,
)


def test_parse_generated_django_model_without_importing_django(tmp_path):
    model_path = tmp_path / "model.py"
    model_path.write_text(
        "\n".join(
            [
                "from django.db import models",
                "",
                "class ROOT(models.Model):",
                "    __bird_annotations__ = {'sql_developer': {'primary_key': ['ROOT_ID'], 'foreign_keys': []}}",
                "    ROOT_TYP_domain = {'1': 'Root'}",
                "    ROOT_TYP = models.CharField('ROOT_TYP', max_length=255, choices=ROOT_TYP_domain)",
                "    theOTHER = models.ForeignKey('OTHER', models.SET_NULL, blank=True, null=True)",
                "",
                "    class Meta:",
                "        verbose_name = 'ROOT'",
                "        verbose_name_plural = 'ROOTs'",
            ]
        ),
        encoding="utf-8",
    )

    module = parse_django_model(model_path)

    assert module.class_order == ["ROOT"]
    assert module.classes["ROOT"].bases == ["models.Model"]
    assert module.classes["ROOT"].annotations["sql_developer"]["primary_key"] == ["ROOT_ID"]
    assert "__bird_annotations__" not in module.classes["ROOT"].choices
    assert module.classes["ROOT"].choices["ROOT_TYP_domain"].kind == "choice"
    assert module.classes["ROOT"].fields["ROOT_TYP"].choices_name == "ROOT_TYP_domain"
    assert module.classes["ROOT"].fields["theOTHER"].related_model == "OTHER"


def test_helper_filter_excludes_mixed_case_generated_classifiers():
    assert _looks_like_helper_or_domain_class("Instrument_type_by_product")
    assert _looks_like_helper_or_domain_class("Financial_asset_instrument_type_by_fixed_interest_rate")
    assert _looks_like_helper_or_domain_class(
        "Long_balance_sheet_recognised_security_position_prudential_portfolio_Accounting_classi_cf2b7c"
    )
    assert not _looks_like_helper_or_domain_class("INSTRMNT_CLLTRL_ASSGNMNT")


def test_model_comparison_reports_choice_value_differences(tmp_path):
    generated_path = tmp_path / "generated.py"
    reference_path = tmp_path / "reference.py"
    generated_path.write_text(
        "\n".join(
            [
                "from django.db import models",
                "",
                "class ROOT(models.Model):",
                "    MATCH_domain = {'1': 'One'}",
                "    MATCH = models.CharField('MATCH', max_length=255, choices=MATCH_domain)",
                "    DIFF_domain = {'1': 'One', '2': 'Two generated'}",
                "    DIFF = models.CharField('DIFF', max_length=255, choices=DIFF_domain)",
                "",
                "    class Meta:",
                "        verbose_name = 'ROOT'",
                "        verbose_name_plural = 'ROOTs'",
            ]
        ),
        encoding="utf-8",
    )
    reference_path.write_text(
        "\n".join(
            [
                "from django.db import models",
                "",
                "class ROOT(models.Model):",
                "    MATCH_domain = {'1': 'One'}",
                "    MATCH = models.CharField('MATCH', max_length=255, choices=MATCH_domain)",
                "    DIFF_domain = {'1': 'One reference', '3': 'Three'}",
                "    DIFF = models.CharField('DIFF', max_length=255, choices=DIFF_domain)",
                "",
                "    class Meta:",
                "        verbose_name = 'ROOT'",
                "        verbose_name_plural = 'ROOTs'",
            ]
        ),
        encoding="utf-8",
    )

    comparison = compare_model_modules(parse_django_model(generated_path), parse_django_model(reference_path))
    diff = comparison["classes"]["ROOT"]["choice_differences"]["DIFF"]

    assert comparison["field_match_ratio"] == 1.0
    assert comparison["choice_match_ratio"] == 0.5
    assert comparison["choice_difference_count"] == 1
    assert diff["missing_values"] == ["3"]
    assert diff["extra_values"] == ["2"]
    assert diff["differing_labels"] == {"1": {"generated": "One", "reference": "One reference"}}


def test_model_comparison_uses_choices_definition_preceding_field(tmp_path):
    generated_path = tmp_path / "generated.py"
    reference_path = tmp_path / "reference.py"
    source = "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    STATUS_domain = {'0': 'Not_applicable', '1': 'One'}",
            "    STATUS = models.CharField('STATUS', max_length=255, choices=STATUS_domain)",
            "    STATUS_domain = {'1': 'One'}",
            "    OTHER_STATUS = models.CharField('OTHER_STATUS', max_length=255, choices=STATUS_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
        ]
    )
    generated_path.write_text(source, encoding="utf-8")
    reference_path.write_text(source, encoding="utf-8")

    comparison = compare_model_modules(parse_django_model(generated_path), parse_django_model(reference_path))

    assert comparison["choice_difference_count"] == 0
    assert comparison["choice_match_ratio"] == 1.0


def test_enrich_django_ldm_annotations_preserves_sqldeveloper_source_metadata(tmp_path):
    resources_dir = tmp_path / "resources"
    ldm_dir = resources_dir / "ldm"
    ldm_dir.mkdir(parents=True)

    def write_csv(path, fieldnames, rows):
        with path.open("w", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    (ldm_dir / "DM_Classification_Types.csv").write_text(
        "\n".join(
            [
                "ObjectID,Classification_Type_Name",
                "CLS1,Domain",
            ]
        ),
        encoding="utf-8",
    )
    (ldm_dir / "DM_Entities.csv").write_text(
        "\n".join(
            [
                "Entity_Name,ObjectID,NumOID,ImportID,ModelID,Num_ModelID,Structured_Type_ID,"
                "Num_Structured_Type_ID,Structured_Type_Name,Number_Data_Elements,Classification_Type,"
                "Allow_Type_Substitution,Min_Volume,Expected_Volume,Max_Volume,Growth_Rate_Percents,"
                "Growth_Rate_Interval,Normal_Form,Temporary_Object_Scope,Adequately_Normalized,"
                "Substitution_Parent,Num_Substitution_Parent,Synonyms,Synonym_To_Display,"
                "Preferred_Abbreviation,SuperTypeEntity_ID,Num_SuperTypeEntity_ID,Engineering_Strategy,Owner,"
                "Entity_Source",
                "Source Entity,SRC1,,,,,,,,,CLS1,,,,,,,,,,,,,,SRC,TGT1,42,Single Table,,",
                "Sibling Entity,SIB1,,,,,,,,,CLS1,,,,,,,,,,,,,,SIB,TGT1,43,Single Table,,",
                "Target Entity,TGT1,,,,,,,,,CLS1,,,,,,,,,,,,,,TGT,,,Single Table,,",
            ]
        ),
        encoding="utf-8",
    )
    (ldm_dir / "DM_Relations.csv").write_text(
        "\n".join(
            [
                "Relation_Name,ModelID,Num_ModelID,ObjectID,NumOID,ImportID,Source_Entity_Name,"
                "Target_Entity_Name,Source_Label,Target_Label,SourceTo_Target_Cardinality,"
                "TargetTo_Source_Cardinality,Source_Optional,Target_Optional,Dominant_Role,Identifying,"
                "Source_ID,Num_Source_ID,Target_ID,Num_Target_ID,Number_Of_Attributes",
                "Source has target,,,REL1,,,Source Entity,Target Entity,,,1,1,Y,N,None,Y,SRC1,,TGT1,,2",
            ]
        ),
        encoding="utf-8",
    )
    write_csv(
        ldm_dir / "DM_Domains.csv",
        [
            "Domain_ID",
            "Domain_Name",
            "Num_Domain_ID",
            "Synonyms",
            "Logical_Type_ID",
            "Num_Logical_Type_ID",
            "T_Size",
            "T_Precision",
            "T_Scale",
            "Native_Type",
            "LT_Name",
        ],
        [
            {
                "Domain_ID": "D1",
                "Domain_Name": "Source Entity type",
                "Synonyms": "SRC_TYP",
                "Native_Type": "VARCHAR2",
                "LT_Name": "String",
            },
            {
                "Domain_ID": "D2",
                "Domain_Name": "Child status",
                "Synonyms": "CHILD_STATUS",
                "Native_Type": "VARCHAR2",
                "LT_Name": "String",
            },
        ],
    )
    write_csv(
        ldm_dir / "DM_Domain_AVT.csv",
        ["Domain_ID", "Num_Domain_ID", "Sequence", "Value", "Short_Description", "Domain_Name"],
        [
            {
                "Domain_ID": "D1",
                "Sequence": "1",
                "Value": "0",
                "Short_Description": "Not applicable",
                "Domain_Name": "Source Entity type",
            },
            {
                "Domain_ID": "D1",
                "Sequence": "2",
                "Value": "10",
                "Short_Description": "Source Entity",
                "Domain_Name": "Source Entity type",
            },
            {
                "Domain_ID": "D2",
                "Sequence": "1",
                "Value": "1",
                "Short_Description": "Active",
                "Domain_Name": "Child status",
            },
        ],
    )
    attribute_columns = [
        "Attribute_Name",
        "ObjectID",
        "NumOID",
        "ImportID",
        "ContainerID",
        "Num_ContainerID",
        "Mandatory",
        "DataType_Kind",
        "Value_Type",
        "Formula",
        "ScopeEntityID",
        "Num_ScopeEntityID",
        "Domain_ID",
        "Num_Domain_ID",
        "Logical_Type_ID",
        "Num_Logical_Type_ID",
        "Distinct_Type_ID",
        "Num_Distinct_Type_ID",
        "Structured_Type_ID",
        "Num_Structured_Type_ID",
        "Collection_Type_ID",
        "Num_Collection_Type_ID",
        "Check_Constraint_Name",
        "Default_Value",
        "Use_Domain_Constraint",
        "Domain_Name",
        "Logical_Type_Name",
        "Structured_Type_Name",
        "Distinct_Type_Name",
        "Collection_Type_Name",
        "Synonyms",
        "Preferred_Abbreviation",
        "Relation_ID",
        "Num_Relation_ID",
        "Entity_Name",
        "PK_Flag",
        "FK_Flag",
        "Relation_Name",
        "Sequence",
        "T_Size",
        "T_Precision",
        "T_Scale",
        "Data_Source",
    ]
    write_csv(
        ldm_dir / "DM_Attributes.csv",
        attribute_columns,
        [
            {
                "Attribute_Name": "Source Entity type",
                "ObjectID": "A1",
                "ContainerID": "TGT1",
                "DataType_Kind": "Domain",
                "Domain_ID": "D1",
                "Domain_Name": "Source Entity type",
                "Preferred_Abbreviation": "SRC_TYP",
                "Entity_Name": "Target Entity",
            },
            {
                "Attribute_Name": "Child status",
                "ObjectID": "A2",
                "ContainerID": "SRC1",
                "DataType_Kind": "Domain",
                "Domain_ID": "D2",
                "Domain_Name": "Child status",
                "Preferred_Abbreviation": "CHILD_STATUS",
                "Entity_Name": "Source Entity",
            },
        ],
    )
    model_path = tmp_path / "ldm.py"
    model_path.write_text(
        "\n".join(
            [
                "from django.db import models",
                "",
                "class SRC(models.Model):",
                "    __bird_annotations__ = {'sql_developer': {'foreign_keys': [{'relation_id': 'REL1'}]}}",
                "    SRC_ID = models.CharField('SRC_ID', max_length=255)",
                "    CHILD_STATUS_domain = {'1': 'Active'}",
                "    CHILD_STATUS = models.CharField('CHILD_STATUS', max_length=255, choices=CHILD_STATUS_domain)",
                "",
                "    class Meta:",
                "        verbose_name = 'Source Entity'",
                "",
                "class TGT(models.Model):",
                "    TGT_ID = models.CharField('TGT_ID', max_length=255)",
                "    SRC_TYP_domain = {'0': 'Not_applicable', '10': 'Source_Entity'}",
                "    SRC_TYP = models.CharField('SRC_TYP', max_length=255, choices=SRC_TYP_domain)",
                "",
                "    class Meta:",
                "        verbose_name = 'Target Entity'",
            ]
        ),
        encoding="utf-8",
    )

    summary = enrich_django_ldm_annotations(model_path, resources_dir)
    module = parse_django_model(model_path)
    source_annotations = module.classes["SRC"].annotations["sql_developer"]
    target_annotations = module.classes["TGT"].annotations["sql_developer"]
    foreign_key = source_annotations["foreign_keys"][0]

    assert summary["changed_class_count"] == 2
    assert source_annotations["entity_id"] == "SRC1"
    assert source_annotations["supertype_entity_id"] == "TGT1"
    assert source_annotations["num_supertype_entity_id"] == 42
    assert source_annotations["entity_member"]["domain_name"] == "Source Entity type"
    assert source_annotations["entity_member"]["member_code"] == "10"
    assert source_annotations["entity_member"]["member_label"] == "Source_Entity"
    assert source_annotations["fields"]["CHILD_STATUS"]["domain_name"] == "Child status"
    assert source_annotations["fields"]["CHILD_STATUS"]["add_not_applicable_candidate"] is True
    assert source_annotations["fields"]["CHILD_STATUS"]["not_applicable_present"] is False
    assert target_annotations["entity_id"] == "TGT1"
    assert target_annotations["fields"]["SRC_TYP"]["not_applicable_present"] is True
    assert foreign_key["source_optional"] == "Y"
    assert foreign_key["target_optional"] == "N"
    assert foreign_key["one_to_one"] is True
    assert foreign_key["referenced_class"] == "TGT"


def test_forward_engineering_uses_ldm_folding_and_optional_reference_fallback(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    reference_path = tmp_path / "reference.py"

    ldm_path.write_text(_ldm_source(), encoding="utf-8")
    reference_path.write_text(_reference_source(), encoding="utf-8")

    ldm_module = parse_django_model(ldm_path)
    reference_module = parse_django_model(reference_path)

    generated_without_fallback, report_without_fallback = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=reference_module,
        include_reference_fallback=False,
    )

    assert "class ROOT(models.Model):" in generated_without_fallback
    assert "ACCNTNG_STNDRD = models.CharField" in generated_without_fallback
    assert "DT_RFRNC = models.DateTimeField" in generated_without_fallback
    assert "CHILD_VALUE = models.BigIntegerField" in generated_without_fallback
    assert "OWNER_VALUE = models.CharField" in generated_without_fallback
    assert "BYR_PRTY_ID = models.CharField" in generated_without_fallback
    assert "BYR_ENTTY_RL_TYP = models.CharField" in generated_without_fallback
    assert "class REL(models.Model):" in generated_without_fallback
    assert "theROOT = models.ForeignKey" in generated_without_fallback
    assert "theOTHER = models.ForeignKey" in generated_without_fallback
    assert "REF_ONLY" not in generated_without_fallback
    assert report_without_fallback["classes"]["ROOT"]["missing_reference_fields"] == ["REF_ONLY"]

    generated_with_fallback, report_with_fallback = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=reference_module,
        include_reference_fallback=True,
    )

    assert "REF_ONLY = models.CharField" in generated_with_fallback
    assert report_with_fallback["comparison"]["field_match_ratio"] == 1.0
    assert report_with_fallback["classes"]["ROOT"]["reference_fallback_fields"] == ["REF_ONLY"]


def test_forward_engineering_renders_synthetic_relationships_without_reference(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)

    assert "thePRODUCT_TYPE = models.ForeignKey('PRODUCT_TYPE'" in generated_source
    assert "theOTHER = models.ForeignKey('OTHER'" in generated_source
    assert "thePRODUCT_TYPE" in report["classes"]["REL"]["generated_fields"]
    assert generated_module.classes["REL"].fields["thePRODUCT_TYPE"].related_model == "PRODUCT_TYPE"
    assert generated_module.classes["ROOT"].fields["theOTHER"].related_model == "OTHER"


def test_forward_engineering_folds_annotated_identifying_extensions(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    reference_path = tmp_path / "reference.py"

    ldm_path.write_text(_ldm_with_annotated_identifying_extension_source(), encoding="utf-8")
    reference_path.write_text(_reference_with_identifying_extension_field_source(), encoding="utf-8")

    ldm_module = parse_django_model(ldm_path)
    reference_module = parse_django_model(reference_path)

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=reference_module,
        include_reference_fallback=False,
    )

    assert "class ROOT(models.Model):" in generated_source
    assert "RISK_SCORE = models.BigIntegerField" in generated_source
    assert "class ROOT_RSK_DT" not in generated_source
    assert "ROOT_RSK_DT" in report["classes"]["ROOT"]["ldm_source_classes"]
    assert report["classes"]["ROOT"]["missing_reference_fields"] == []


def test_identifying_association_subtype_does_not_fold_into_referenced_entity(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_identifying_association_subtype_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)

    assert "ASSOCIATION_DETAIL" not in report["classes"]["ROOT"]["ldm_source_classes"]
    assert "ASSOCIATION_DETAIL_VALUE" not in generated_module.classes["ROOT"].fields
    assert "ASSOCIATION_DETAIL" in report["classes"]["ASSOCIATION"]["ldm_source_classes"]
    assert "ASSOCIATION_DETAIL_VALUE" in generated_module.classes["ASSOCIATION"].fields


def test_no_reference_target_selection_folds_risk_extensions_and_keeps_context_derived_targets(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_risk_and_context_derived_targets_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)

    assert "class ROOT_RSK_DT" not in generated_source
    assert "RISK_SCORE = models.BigIntegerField" in generated_source
    assert "ROOT_RSK_DT_uniqueID" not in generated_module.classes["ROOT"].fields
    assert "class KB_PR_BCKT_DRVD_DT(models.Model):" in generated_source
    assert "BCKT_ID" in generated_module.classes["KB_PR_BCKT_DRVD_DT"].fields
    assert "class ASSIGNMENT_RSK_DT(models.Model):" in generated_source
    assert "ROOT_RSK_DT" in report["classes"]["ROOT"]["ldm_source_classes"]


def test_no_reference_reduce_discriminators_skips_folded_subtype_type_fields(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_folded_subtype_discriminator_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_fields = generated_module.classes["ROOT"].fields

    assert "ROOT_TYP" in root_fields
    assert "CHILD_VALUE" in root_fields
    assert "CHILD_TYP" not in root_fields


def test_no_reference_adds_not_applicable_to_folded_subtype_choice_when_sibling_lacks_attribute(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_subtype_specific_choice_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_choices = generated_module.classes["ROOT"].choices

    assert _literal_choice_values(root_choices["CHILD_A_STATUS_domain"].source)["0"] == "Not_applicable"
    assert "0" not in _literal_choice_values(root_choices["SHARED_STATUS_domain"].source)


def test_no_reference_adds_not_applicable_to_folded_model_context_fk_choice_component(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_optional_identifying_fk_choice_component_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]
    root_choices = root_class.choices
    field = root_class.fields["ACCNTNG_STNDRD"]

    assert _literal_choice_values(root_choices[field.choices_name].source)["0"] == "Not_applicable"


def test_no_reference_adds_not_applicable_to_accounting_context_choices(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_accounting_context_choices_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]

    for field_name in ("ACCNTNG_CNSLDTN_LVL", "ACCNTNG_STNDRD"):
        field = root_class.fields[field_name]
        assert _literal_choice_values(root_class.choices[field.choices_name].source)["0"] == "Not_applicable"


def test_accounting_context_not_applicable_skips_base_domain_targets():
    derived_field_set = DerivedFieldSet(
        field_names={"ACCNTNG_CNSLDTN_LVL", "ACCNTNG_STNDRD"},
        choice_values_by_field={
            "ACCNTNG_CNSLDTN_LVL": {
                "1": "Solo_consolidation_level",
                "2": "Group_consolidation_level",
            },
            "ACCNTNG_STNDRD": {
                "1": "National_GAAP_not_consistent_with_IFRS",
                "2": "IFRS",
                "3": "National_GAAP_consistent_with_IFRS",
            },
        },
    )

    _add_accounting_context_not_applicable_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="CSH_HND",
    )

    assert "ACCNTNG_CNSLDTN_LVL" not in derived_field_set.not_applicable_choice_fields
    assert "ACCNTNG_STNDRD" not in derived_field_set.not_applicable_choice_fields

    _add_accounting_context_not_applicable_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="CLLTRL",
    )

    assert "ACCNTNG_CNSLDTN_LVL" in derived_field_set.not_applicable_choice_fields
    assert "ACCNTNG_STNDRD" in derived_field_set.not_applicable_choice_fields


def test_no_reference_rebuilds_not_merged_discriminator_choices_from_branch_leaves(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_sqldeveloper_not_merged_discriminator_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    model_class = generated_module.classes["DBT_SCRTY_ISSD"]
    field = model_class.fields["DBT_SCRTY_ISSD_PRDNTL_PRTFL_TYP"]

    assert _literal_choice_values(model_class.choices[field.choices_name].source) == {
        "22": "Issued_debt_security_in_the_banking_book",
        "23": "Issued_debt_security_in_the_trading_book_International_Financial_Reporting_Standard_IFRS",
        "24": "Issued_debt_security_in_the_trading_book_national_general_accepted_accounting_principl_f32854",
    }


def test_no_reference_reduced_discriminator_uses_non_type_leaf_choice_metadata(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_non_type_leaf_choice_for_reduced_discriminator_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]
    field = root_class.fields["ROOT_TYP"]

    assert _literal_choice_values(root_class.choices[field.choices_name].source) == {
        "10": "Child_leaf_by_standard",
        "20": "Other_child_leaf",
    }


def test_no_reference_reduced_discriminator_uses_annotated_entity_member_metadata(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_annotated_entity_member_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]
    field = root_class.fields["ROOT_TYP"]

    assert _literal_choice_values(root_class.choices[field.choices_name].source) == {
        "34": "Annotated_child_leaf",
        "35": "Other_annotated_leaf",
    }


def test_no_reference_delegate_discriminator_uses_manual_member_before_lower_level_annotation(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_delegate_discriminator_manual_member_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    instrument_class = generated_module.classes["INSTRMNT"]
    field = instrument_class.fields["INSTRMNT_TYP_PRDCT"]

    assert _literal_choice_values(instrument_class.choices[field.choices_name].source) == {
        "511": "Tranferable_deposit",
    }


def test_no_reference_folded_input_domain_adds_sqldeveloper_source_members(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_folded_input_domain_source_member_gap(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    instrument_class = generated_module.classes["INSTRMNT"]
    field = instrument_class.fields["INSTRMNT_TYP_PRDCT"]

    assert _literal_choice_values(instrument_class.choices[field.choices_name].source) == {
        "51": "Credit_card_debt",
        "549": "Deposit",
        "1003": "Reverse_repurchase_agreement_instrument",
        "162": "Open_repurchase_agreement_instrument",
    }


def test_folded_input_domain_bridge_is_source_class_gated():
    derived_field_set = DerivedFieldSet(
        field_names={"ACCNTNG_CLSSFCTN"},
        choice_values_by_field={"ACCNTNG_CLSSFCTN": {"2": "Asset_classification"}},
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="EXCHNG_TRDBL_DRVTV_PSTN_RL",
        ldm_source_classes=["NN_BLNC_SHT_RCGNSD_EXCHNG_TRDBL_DRVTV_ASST_PSTN"],
    )

    assert derived_field_set.choice_values_by_field["ACCNTNG_CLSSFCTN"] == {
        "2": "Asset_classification",
        "90": "Under_IFRS_9_impairment_Off_balance_sheet_accounting_classification_under_IFRS_9_impairment",
        "911": "Measured_under_IAS_37_Off_balance_sheet_accounting_classification_measured_under_IAS_37",
        "912": "Measured_under_IFRS_4_Off_balance_sheet_accounting_classification_measured_under_IFRS_4",
        "92": "Measured_at_fair_value_through_profit_or_loss_Off_balance_sheet_accounting_classificat_360a76",
        "93": "Under_nGAAP_Off_balance_sheet_accounting_classification_measured_under_nGAAP_based_on_BAD",
    }


def test_folded_input_domain_bridge_suppresses_intermediate_members():
    derived_field_set = DerivedFieldSet(
        field_names={"CLLTRL_TYP"},
        choice_values_by_field={
            "CLLTRL_TYP": {
                "82": "Real_estate_collateral",
                "105": "Offices_and_commercial_premises_related_to_land_collateral",
                "106": "Offices_and_commercial_premises_not_related_to_land_collateral",
            }
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="CLLTRL",
        ldm_source_classes=[
            "RL_ESTT_CLLTRL",
            "OFFCS_CMMRCL_PRMSS_RLTD_LND_CLLTRL",
            "LND_EXCLDNG_AGRCLTR",
            "LND_INCLDNG_AGRCLTR",
        ],
    )

    assert derived_field_set.choice_values_by_field["CLLTRL_TYP"] == {
        "106": "Offices_and_commercial_premises_not_related_to_land_collateral",
        "107": "Land_excluding_agriculture",
        "108": "Land_including_agriculture",
    }


def test_folded_input_domain_bridge_overrides_colliding_member_labels():
    derived_field_set = DerivedFieldSet(
        field_names={"SCRTY_EXCHNG_TRDBL_DRVTV_TYP"},
        choice_values_by_field={
            "SCRTY_EXCHNG_TRDBL_DRVTV_TYP": {
                "1": "Exchange_tradable_derivative",
                "2": "Security",
                "3": "Renegotiated_debt_security_with_forbearance_measure",
                "4": "Renegotiated_debt_security_without_forbearance_measure",
            }
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="SCRTY_EXCHNG_TRDBL_DRVTV",
        ldm_source_classes=["EXCHNG_TRDBL_DRVTV", "SCRTY", "EXCHNG_TRDBL_OPTN", "EXCHNG_TRDBL_FTR"],
    )

    assert derived_field_set.choice_values_by_field["SCRTY_EXCHNG_TRDBL_DRVTV_TYP"] == {
        "3": "Exchange_tradable_option",
        "4": "Exchange_tradable_future",
    }


def test_folded_input_domain_bridge_uses_non_financial_liability_input_domain():
    derived_field_set = DerivedFieldSet(
        field_names={"NN_FNNCL_LBLTY_TYP"},
        not_applicable_choice_fields={"NN_FNNCL_LBLTY_TYP"},
        choice_values_by_field={
            "NN_FNNCL_LBLTY_TYP": {
                "0": "Not_Applicable",
                "1301": "Non_financial_liabilites_other_than_Tax_liability_Share_capital_repayable_on_demand_or_Provision",
                "1303": "Employee_benefit",
                "702": "Provisions_Employee_benefits_Other_than_pension_and_other_post_employment_defined_benefit_obligations",
                "707": "Provisions_Other_than_Employee_benefits_Restructuring_Pending_legal_issues_and_tax_litigation_Off_balance_sheet_exposures_subject_to_credit_risk",
            }
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="NN_FNNCL_LBLTY",
        ldm_source_classes=[
            "NN_FNNCL_LBLTY",
            "OTHR_NN_FNNCL_LBLTY",
            "EMPLY_BNFT",
            "FNDS_GNRL_BNKNG_RSK",
            "OTHR_EMPLY_BNFT",
            "PNSN_OTHR_PST_EMPLYMNT_BNFT_OBLGTN",
            "RSTRCTRNG",
            "PNDNG_LGL_ISSS_TX_LTGTN",
            "OTHR_PRVSN",
            "CRRNT_TX_LBLTY",
            "DFRRD_TX_LBLTY",
            "SHR_CPTL_RPYBL_DMND",
        ],
    )

    assert derived_field_set.choice_values_by_field["NN_FNNCL_LBLTY_TYP"] == {
        "1301": "Non_financial_liabilites_other_than_Tax_liability_Share_capital_repayable_on_demand_or_dfd225",
        "701": "Provisions_Funds_for_general_banking_risks",
        "702": "Provisions_Employee_benefits_Other_than_pension_and_other_post_employment_defined_bene_258d25",
        "703": "Provisions_Employee_benefits_Pension_and_other_post_employment_defined_benefit_obligations",
        "704": "Provisions_Restructuring",
        "705": "Provisions_Pending_legal_issues_and_tax_litigation",
        "707": "Provisions_Other_than_Employee_benefits_Restructuring_Pending_legal_issues_and_tax_lit_905d67",
        "710": "Current_tax_liabilities",
        "720": "Deferred_tax_liabilities",
        "730": "Share_capital_repayable_on_demand",
    }
    assert "NN_FNNCL_LBLTY_TYP" not in derived_field_set.not_applicable_choice_fields


def test_folded_input_domain_bridge_uses_non_financial_asset_input_domain():
    derived_field_set = DerivedFieldSet(
        field_names={"MSRMNT_MTHD", "NN_FNNCL_ASST_TYP"},
        not_applicable_choice_fields={"NN_FNNCL_ASST_TYP"},
        choice_values_by_field={
            "MSRMNT_MTHD": {
                "0": "Not_applicable",
                "1": "Cost_model_IAS_17_49_IAS_16_30_73_a_d",
                "2": "Fair_value_model",
                "3": "Revaluation_model_IAS_17_49_IAS_16_31_73_a_d",
            },
            "NN_FNNCL_ASST_TYP": {
                "0": "Not_applicable",
                "48": "Gold",
                "1300": "Non_financial_assets_other_than_Goodwill_Tax_asset_Investment_property_Other_intangible_asset_or_Property_plant_and_equipment",
            },
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="NN_FNNCL_ASST",
        ldm_source_classes=[
            "NN_FNNCL_ASST",
            "INVSTMNT_PRPRTY",
            "OTHR_NN_FNNCL_ASST",
            "PRPRTY_PLNT_EQPMNT",
        ],
    )

    assert derived_field_set.choice_values_by_field["MSRMNT_MTHD"] == {
        "0": "Not_applicable",
        "1": "Cost_model_IAS_17_49",
        "2": "Fair_value_model",
        "3": "Revaluation_model_IAS_17_49",
    }
    assert derived_field_set.choice_values_by_field["NN_FNNCL_ASST_TYP"] == {
        "48": "Gold",
        "1300": "Non_financial_assets_other_than_Goodwill_Tax_asset_Investment_property_Other_intangibl_4aa924",
    }
    assert "NN_FNNCL_ASST_TYP" not in derived_field_set.not_applicable_choice_fields


def test_folded_input_domain_bridge_suppresses_synthetic_securitisation_cross_discriminator_members():
    derived_field_set = DerivedFieldSet(
        field_names={"SNTHTC_SCRTSTN_TYP", "SCRTSTN_TYP"},
        not_applicable_choice_fields={"SNTHTC_SCRTSTN_TYP", "SCRTSTN_TYP"},
        choice_values_by_field={
            "SNTHTC_SCRTSTN_TYP": {
                "0": "Not_applicable",
                "1": "Significant_risk_transfer_securitisation",
                "2": "Not_significant_risk_transfer_securitisation",
                "3": "Synthetic_securitisation_without_involvement_of_an_SSPE",
                "4": "Synthetic_securitisation_involving_an_SSPE",
            },
            "SCRTSTN_TYP": {
                "0": "Not_applicable",
                "1": "Traditional_securitisation",
                "2": "Synthetic_securitisation",
            },
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="SNTHTC_SCRTSTN",
        ldm_source_classes=[
            "SNTHTC_SCRTSTN",
            "SCRTSTN",
            "SGNFCNT_RSK_TRNSFR_SCRTSTN",
            "NT_SGNFCNT_RSK_TRNSFR_SCRTSTN",
        ],
    )

    assert derived_field_set.choice_values_by_field["SNTHTC_SCRTSTN_TYP"] == {
        "3": "Synthetic_securitisation_without_involvement_of_an_SSPE",
        "4": "Synthetic_securitisation_involving_an_SSPE",
    }
    assert derived_field_set.choice_values_by_field["SCRTSTN_TYP"] == {
        "1": "Traditional_securitisation",
        "2": "Synthetic_securitisation",
    }
    assert "SNTHTC_SCRTSTN_TYP" not in derived_field_set.not_applicable_choice_fields
    assert "SCRTSTN_TYP" not in derived_field_set.not_applicable_choice_fields


def test_folded_input_domain_bridge_suppresses_accounting_standard_subtype_members():
    derived_field_set = DerivedFieldSet(
        field_names={"ACCNTNG_STNDRD"},
        choice_values_by_field={
            "ACCNTNG_STNDRD": {
                "1": "National_GAAP_not_consistent_with_IFRS",
                "2": "IFRS",
                "3": "National_GAAP_consistent_with_IFRS",
                "23": "Issued_debt_security_in_the_trading_book_International_Financial_Reporting_Standard_IFRS",
                "24": "Issued_debt_security_in_the_trading_book_national_general_accepted_accounting_principl_f32854",
                "46": "Fair_valued_Balance_sheet_recognised_financial_liability_instrument_according_to_Inter_c80f39",
                "47": "Fair_valued_balance_sheet_recognised_financial_liability_instrument_according_to_natio_7d9c74",
            }
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="DBT_SCRTY_ISSD",
        ldm_source_classes=["DBT_SCRTY_ISSD_TRDNG_BK"],
    )

    assert derived_field_set.choice_values_by_field["ACCNTNG_STNDRD"] == {
        "1": "National_GAAP_not_consistent_with_IFRS",
        "2": "IFRS",
        "3": "National_GAAP_consistent_with_IFRS",
        "46": "Fair_valued_Balance_sheet_recognised_financial_liability_instrument_according_to_Inter_c80f39",
        "47": "Fair_valued_balance_sheet_recognised_financial_liability_instrument_according_to_natio_7d9c74",
    }

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="INSTRMNT_RL",
        ldm_source_classes=["FR_VLD_BLNC_SHT_RCGNSD_FNNCL_LBLTY_INSTRMNT"],
    )

    assert derived_field_set.choice_values_by_field["ACCNTNG_STNDRD"] == {
        "1": "National_GAAP_not_consistent_with_IFRS",
        "2": "IFRS",
        "3": "National_GAAP_consistent_with_IFRS",
    }


def test_folded_input_domain_bridge_suppresses_base_domain_not_applicable_markers():
    derived_field_set = DerivedFieldSet(
        field_names={"SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP"},
        not_applicable_choice_fields={"SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP"},
        choice_values_by_field={
            "SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP": {
                "1": "Balance_sheet_recognised_security_position",
                "2": "Non_Balance_sheet_recognised_security_position",
            }
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT",
        ldm_source_classes=["LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_CLLTRL_RCVD_ASSGNMNT"],
    )

    assert derived_field_set.choice_values_by_field["SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP"] == {
        "1": "Balance_sheet_recognised_security_position",
        "2": "Non_Balance_sheet_recognised_security_position",
    }
    assert "SCRTY_PSTN_BLNC_SHT_RCGNSD_TYP" not in derived_field_set.not_applicable_choice_fields


def test_folded_input_domain_bridge_suppresses_intermediate_input_domain_members():
    derived_field_set = DerivedFieldSet(
        field_names={
            "DFLT_STTS",
            "SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL_TYP",
        },
        choice_values_by_field={
            "DFLT_STTS": {
                "14": "Not_in_Default",
                "18": "Default_because_both_unlikely_to_pay_and_more_than_90_180_days_past_due",
                "19": "Default_because_unlikely_to_pay",
                "20": "Default_because_more_than_90_180_days_past_due",
            },
            "SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL_TYP": {
                "1": "Security_borrowing_and_lending_transaction_cash_as_collateral_component",
                "2": "Security_borrowing_and_lending_transaction_component",
                "3": "Debt_security_borrowing_and_lending_transaction_component",
                "4": "Equity_or_fund_security_borrowing_and_lending_transaction_component",
            },
        },
    )

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="INSTRMNT_RL",
        ldm_source_classes=["OFF_BLNC_SHT_ITM_GVN_INSTRMNT"],
    )

    assert derived_field_set.choice_values_by_field["DFLT_STTS"] == {
        "18": "Default_because_both_unlikely_to_pay_and_more_than_90_180_days_past_due",
        "19": "Default_because_unlikely_to_pay",
        "20": "Default_because_more_than_90_180_days_past_due",
    }

    _add_sql_developer_folded_input_domain_choice_values(
        derived_field_set=derived_field_set,
        target_class_name="SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL",
        ldm_source_classes=["SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL"],
    )

    assert derived_field_set.choice_values_by_field[
        "SCRTY_BRRWNG_LNDNG_TRNSCTN_INCLDNG_CSH_CLLTRL_TYP"
    ] == {
        "1": "Security_borrowing_and_lending_transaction_cash_as_collateral_component",
        "3": "Debt_security_borrowing_and_lending_transaction_component",
        "4": "Equity_or_fund_security_borrowing_and_lending_transaction_component",
    }


def test_sql_developer_input_domain_label_overrides_apply_to_existing_and_rendered_values():
    derived_field_set = DerivedFieldSet(
        field_names={
            "FVO_DSGNTN",
            "INSTRMNT_CLLTRL_ASSGNMNT_TYP",
            "LGL_FRM",
            "RSDL_MTRTY_CNTRCT_BND",
            "SCRTY_TYP_BY_IDNTFR",
        },
        not_applicable_choice_fields={"FVO_DSGNTN"},
        choice_values_by_field={
            "FVO_DSGNTN": {
                "1": "Accounting_mismatch",
            },
            "INSTRMNT_CLLTRL_ASSGNMNT_TYP": {
                "7": "Reverse_repurchase_transaction_gold_collateral_received_assignment",
            },
            "LGL_FRM": {
                "AT609": "GesbR_Gesellschaft_des_burgerlichen_Rechts_Partnership_under_civil_code_Unincorporated_a139c9",
            },
            "RSDL_MTRTY_CNTRCT_BND": {
                "999": "Open_maturity",
            },
            "SCRTY_TYP_BY_IDNTFR": {
                "8": "International_securities_identification_number_security",
                "9": "Non_International_securities_identification_number_security",
            },
        },
    )

    _add_sql_developer_input_domain_choice_label_overrides(derived_field_set)

    assert derived_field_set.choice_values_by_field["FVO_DSGNTN"]["0"] == "Not_Applicable"
    assert (
        derived_field_set.choice_values_by_field["INSTRMNT_CLLTRL_ASSGNMNT_TYP"]["7"]
        == "_Reverse_repurchase_transaction_gold_collateral_received_assignment"
    )
    assert (
        derived_field_set.choice_values_by_field["LGL_FRM"]["AT609"]
        == "GesbR_Gesellschaft_des_burgerlichen_Rechts_Partnership_under_civil_code"
    )
    assert derived_field_set.choice_values_by_field["RSDL_MTRTY_CNTRCT_BND"]["999"] == "Open_Maturity"
    assert derived_field_set.choice_values_by_field["SCRTY_TYP_BY_IDNTFR"] == {
        "8": "International_securities_identification_number_ISIN_security",
        "9": "Non_International_securities_identification_number_Non_ISIN_security",
    }


def test_no_reference_reduced_discriminator_uses_annotated_folded_delegate_members(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_annotated_folded_delegate_member_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]
    field = root_class.fields["ROOT_TYP"]

    assert _literal_choice_values(root_class.choices[field.choices_name].source) == {
        "34": "Delegate_leaf_a",
        "35": "Delegate_leaf_b",
    }


def test_reduced_discriminator_source_members_are_limited_to_base_hierarchy(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    ldm_path.write_text(_ldm_with_unrelated_annotated_source_member(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)
    graph = _ClassGraph(ldm_module)

    choice_values = _reduced_discriminator_leaf_choice_values(
        field_name="KIND_TYP",
        base_class_name="Kind",
        restrict_source_members_to_base_hierarchy=True,
        ldm_source_classes=["COMBINED", "Kind", "GOOD_A", "GOOD_B", "UNRELATED"],
        ldm_module=ldm_module,
        graph=graph,
        target_classes={"COMBINED"},
        include_source_class_members=True,
    )

    assert choice_values == {
        "1": "Good_a",
        "2": "Good_b",
    }


def test_no_reference_reduced_discriminator_keeps_leaf_with_folded_derived_data(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_reduced_discriminator_leaf_derived_data_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]
    field = root_class.fields["ROOT_TYP"]

    assert _literal_choice_values(root_class.choices[field.choices_name].source) == {
        "10": "Child_with_derived_data",
        "20": "Child_without_derived_data",
    }


def test_no_reference_relationship_copy_discriminator_uses_base_reduced_domain(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_relationship_copy_reduced_discriminator_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    assignment_class = generated_module.classes["ASSIGNMENT"]
    field = assignment_class.fields["INSTRMNT_RL_TYP"]

    assert _literal_choice_values(assignment_class.choices[field.choices_name].source) == {
        "8": "Collateral_given_instrument",
        "34": "Balance_sheet_asset_role",
        "101": "Non_balance_sheet_asset_role",
    }


def test_no_reference_relationship_copy_discriminator_inherits_base_not_applicable(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_not_applicable_relationship_copy_reduced_discriminator_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    assignment_class = generated_module.classes["ASSIGNMENT"]
    field = assignment_class.fields["POSITION_RL_TYP"]

    assert _literal_choice_values(assignment_class.choices[field.choices_name].source) == {
        "0": "Not_applicable",
        "9": "Position_asset_role",
        "10": "Position_liability_role",
    }


def test_no_reference_directional_role_domains_add_not_applicable(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_directional_role_reduced_discriminator_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)

    for class_name in ("CLLTRL_RL", "ASSIGNMENT"):
        model_class = generated_module.classes[class_name]
        field = model_class.fields["CLLTRL_RL_TYP"]
        assert _literal_choice_values(model_class.choices[field.choices_name].source) == {
            "0": "Not_applicable",
            "1": "Collateral_received",
            "2": "Collateral_given",
        }


def test_no_reference_entity_role_copy_uses_entity_role_domain(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_entity_role_copy_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    assignment_class = generated_module.classes["ASSIGNMENT"]
    field = assignment_class.fields["ENTTY_RL_TYP"]

    assert _literal_choice_values(assignment_class.choices[field.choices_name].source) == {
        "0": "Not_applicable",
        "10": "Sub_role",
        "20": "Other_role",
    }
    entity_role_class = generated_module.classes["ENTTY_RL"]
    entity_role_field = entity_role_class.fields["ENTTY_RL_TYP"]
    assert _literal_choice_values(entity_role_class.choices[entity_role_field.choices_name].source) == {
        "0": "Not_applicable",
        "10": "Sub_role",
        "20": "Other_role",
    }


def test_no_reference_indirect_entity_role_copy_uses_entity_role_domain(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_indirect_entity_role_copy_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    hedge_class = generated_module.classes["HEDGE"]
    field = hedge_class.fields["INVSTR_ENTTY_RL_TYP"]

    assert _literal_choice_values(hedge_class.choices[field.choices_name].source) == {
        "0": "Not_applicable",
        "8": "Investor",
        "20": "Other_role",
    }


def test_no_reference_sql_developer_input_domain_adds_not_applicable(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_sql_developer_input_domain_folded_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]
    field = root_class.fields["DFLT_STTS_DRVD"]

    assert _literal_choice_values(root_class.choices[field.choices_name].source) == {
        "0": "Not_Applicable",
        "6": "Default",
    }


def test_no_reference_adds_synthetic_sqldeveloper_choices_for_unchoiced_fields(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_synthetic_sqldeveloper_choice_fields_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]

    listed_field = root_class.fields["LSTD_INDCTR"]
    assert listed_field.field_type == "CharField"
    assert _literal_choice_values(root_class.choices[listed_field.choices_name].source) == {
        "0": "Not_applicable",
        "F": "Non_listed",
        "T": "Listed",
    }

    own_company_field = root_class.fields["OWN_CMPNY_INVSTMNT_INDCTR"]
    assert _literal_choice_values(root_class.choices[own_company_field.choices_name].source) == {
        "0": "Not_applicable",
        "1": "Own_company_investment",
        "2": "Non_own_company_investment",
    }


def test_no_reference_adds_not_applicable_from_field_annotation(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_not_applicable_field_annotation_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_class = generated_module.classes["ROOT"]
    field = root_class.fields["STATUS"]

    assert _literal_choice_values(root_class.choices[field.choices_name].source) == {
        "0": "Not_applicable",
        "1": "Active",
    }


def test_no_reference_keeps_existing_discriminator_domain_for_discriminator_leaf_target(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_tranche_leaf_reduced_discriminator_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    tranche_class = generated_module.classes["TRNCH_TRDTNL_SCRTSTN"]
    field = tranche_class.fields["SCRTSTN_TRNCH_TYP"]

    assert _literal_choice_values(tranche_class.choices[field.choices_name].source) == {
        "3": "Tranche_in_a_Traditional_securitisation",
        "4": "Tranche_in_a_synthetic_securitisation",
    }


def test_no_reference_folds_source_side_derived_data_and_preserves_by_accounting_standard(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_source_side_derived_data_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    root_fields = generated_module.classes["ROOT"].fields

    assert "CHILD_DRVD_DT" in report["classes"]["ROOT"]["ldm_source_classes"]
    assert "DERIVED_SCORE" in root_fields
    assert "DT_INCPTN" in root_fields
    assert "CHILD_INCPTN_DT" not in root_fields
    assert "CHILD_BY_ACCNTNG_STNDRD" in root_fields


def test_no_reference_sql_developer_policy_keeps_concrete_targets_and_merges_extensions(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_sql_developer_policy_targets_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)

    assert "class SCRTSTN_TRNCH" not in generated_source
    assert "class TRNCH_TRDTNL_SCRTSTN(models.Model):" in generated_source
    assert "TRNCH_TRDTNL_SCRTSTN_VALUE" in generated_module.classes["TRNCH_TRDTNL_SCRTSTN"].fields
    assert "SCRTSTN_TRNCH_TYP" in generated_module.classes["TRNCH_TRDTNL_SCRTSTN"].fields
    assert "class CRDT_FCLTY_INTRST_RT" not in generated_source
    assert "INTRST_RT" in generated_module.classes["CRDT_FCLTY"].fields
    assert "CRDT_FCLTY_INTRST_RT" in report["classes"]["CRDT_FCLTY"]["ldm_source_classes"]
    assert "CLLTRL_ID" in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "SCRTY_ID" in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "ACCNTNG_CLSSFCTN" in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "EXCHNG_TRDBL_DRVTV_SCRTY_ID" in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "CVRD_BND_PRGRM_ID" in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "CLLTRL_RL_ID" not in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_SCRTY_ID" not in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "DBT_SCRTY_ISSD_ACCNTNG_CLSSFCTN" not in generated_module.classes["KEY_ASSIGNMENT"].fields
    assert "SCRTSTN_ID" not in generated_module.classes["KEY_ASSIGNMENT"].fields


def test_forward_engineering_deduplicates_wrapped_key_relationships_without_reference(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_wrapped_key_fields_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    assignment_fields = generated_module.classes["ASSIGNMENT"].fields

    assert "theINSTRMNT = models.ForeignKey('INSTRMNT'" in generated_source
    assert "theENTTY_RL = models.ForeignKey('ENTTY_RL'" in generated_source
    assert "theINSTRMNT1" not in generated_source
    assert "theENTTY_RL1" not in generated_source
    assert "LNDR_PRTY_ID" not in assignment_fields
    assert "LNDR_PRTY_RFRNC_DT" not in assignment_fields
    assert "LNDR_PRTY_RPRTNG_AGNT_ID" not in assignment_fields
    assert "LNDR_PRTY_RL_TYP" not in assignment_fields
    assert "BYR_PRTY_ID" not in assignment_fields
    assert "BYR_PRTY_RFRNC_DT" not in assignment_fields
    assert "BYR_PRTY_RPRTNG_AGNT_ID" not in assignment_fields
    assert "BYR_PRTY_RL_TYP" not in assignment_fields
    assert "TRD_RCVBL_ID" not in assignment_fields
    assert "TRD_RCVBL_INSTRMNT_RFRNC_DT" not in assignment_fields
    assert "TRD_RCVBL_INSTRMNT_RPRTNG_AGNT_ID" not in assignment_fields
    assert "PRTY_ID" in assignment_fields
    assert "ENTTY_RL_TYP" in assignment_fields
    assert "INSTRMNT_ID" in assignment_fields
    assert "DT_RFRNC" in assignment_fields
    assert "RPRTNG_AGNT_ID" in assignment_fields
    assert "AMNT" in assignment_fields
    assert report["classes"]["ASSIGNMENT"]["generated_fields"].count("theINSTRMNT") == 1
    assert report["classes"]["ASSIGNMENT"]["generated_fields"].count("theENTTY_RL") == 1


def test_forward_engineering_infers_role_abbreviations_and_preserves_regular_target_keys(tmp_path):
    ldm_path = tmp_path / "ldm.py"
    generated_path = tmp_path / "generated.py"

    ldm_path.write_text(_ldm_with_abbreviated_role_and_regular_target_key_source(), encoding="utf-8")
    ldm_module = parse_django_model(ldm_path)

    generated_source, _report = generate_forward_engineered_source(
        ldm_module=ldm_module,
        reference_module=None,
        include_reference_fallback=False,
    )
    generated_path.write_text(generated_source, encoding="utf-8")
    generated_module = parse_django_model(generated_path)
    deal_fields = generated_module.classes["DEAL"].fields

    assert "BYR_PRTY_ID" in deal_fields
    assert "BYR_ENTTY_RL_TYP" in deal_fields
    assert "SLLR_PRTY_ID" in deal_fields
    assert "SLLR_ENTTY_RL_TYP" in deal_fields
    assert "BYR_ID" not in deal_fields
    assert "BYR_RL_TYP" not in deal_fields
    assert "SLLR_ID" not in deal_fields
    assert "SLLR_RL_TYP" not in deal_fields
    assert "ASST_PL_ID" in deal_fields
    assert "theENTTY_RL" in deal_fields
    assert "theENTTY_RL1" in deal_fields
    assert "theASST_PL" in deal_fields

    protection_fields = generated_module.classes["PROTECTION_ASSGNMNT"].fields
    assert "PRTY_ID" in protection_fields
    assert "ENTTY_RL_TYP" in protection_fields
    assert "PRTCTN_PRVDR_ID" not in protection_fields
    assert "PRTCTN_PRVDR_RL_TYP" not in protection_fields
    assert "theENTTY_RL" in protection_fields
    assert "theENTTY_RL1" not in protection_fields

    position_fields = generated_module.classes["POSITION"].fields
    assert "INVSTR_PRTY_ID" in position_fields
    assert "INVSTR_ENTTY_RL_TYP" in position_fields
    assert "PRTY_ID" not in position_fields
    assert "ENTTY_RL_TYP" not in position_fields


def _ldm_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    test_id = models.CharField('test_id', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    PRODUCT_TYPE_delegate = models.ForeignKey('PRODUCT_TYPE', models.SET_NULL, blank=True, null=True)",
            "    ROOT_ACCNTNG_STNDRD_domain = {'1': 'IFRS'}",
            "    ROOT_ACCNTNG_STNDRD = models.CharField('ROOT_ACCNTNG_STNDRD', max_length=255, choices=ROOT_ACCNTNG_STNDRD_domain)",
            "    ROOT_RFRNC_DT = models.DateTimeField('ROOT_RFRNC_DT', default=None, blank=True, null=True)",
            "    BYR_ID = models.CharField('BYR_ID', max_length=255, default=None, blank=True, null=True)",
            "    BYR_RL_TYP = models.CharField('BYR_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    OTHER_ID = models.CharField('OTHER_ID', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class OTHER(models.Model):",
            "    OTHER_uniqueID = models.CharField('OTHER_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'OTHER'",
            "        verbose_name_plural = 'OTHERs'",
            "",
            "class PRODUCT_TYPE(models.Model):",
            "    class Meta:",
            "        verbose_name = 'PRODUCT_TYPE'",
            "        verbose_name_plural = 'PRODUCT_TYPEs'",
            "",
            "class PRODUCT(PRODUCT_TYPE):",
            "    class Meta:",
            "        verbose_name = 'PRODUCT'",
            "        verbose_name_plural = 'PRODUCTs'",
            "",
            "class OWNER(models.Model):",
            "    ROOT_delegate = models.ForeignKey('ROOT', models.SET_NULL, blank=True, null=True)",
            "    OWNER_VALUE = models.CharField('OWNER_VALUE', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'OWNER'",
            "        verbose_name_plural = 'OWNERs'",
            "",
            "class CHILD(ROOT):",
            "    CHILD_VALUE = models.BigIntegerField('CHILD_VALUE', default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD'",
            "        verbose_name_plural = 'CHILDs'",
            "",
            "class REL(models.Model):",
            "    REL_uniqueID = models.CharField('REL_uniqueID', max_length=255, primary_key=True)",
            "    Relation_has_product = models.ForeignKey('PRODUCT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'REL'",
            "        verbose_name_plural = 'RELs'",
        ]
    )


def _ldm_with_abbreviated_role_and_regular_target_key_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ENTTY_RL(models.Model):",
            "    ENTTY_RL_uniqueID = models.CharField('ENTTY_RL_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ENTTY_RL'",
            "        verbose_name_plural = 'ENTTY_RLs'",
            "",
            "class BUYR(ENTTY_RL):",
            "    class Meta:",
            "        verbose_name = 'BUYR'",
            "        verbose_name_plural = 'BUYRs'",
            "",
            "class SLLR(ENTTY_RL):",
            "    class Meta:",
            "        verbose_name = 'SLLR'",
            "        verbose_name_plural = 'SLLRs'",
            "",
            "class ASST_PL(models.Model):",
            "    ASST_PL_uniqueID = models.CharField('ASST_PL_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASST_PL'",
            "        verbose_name_plural = 'ASST_PLs'",
            "",
            "class PRTCTN_PRVDR(ENTTY_RL):",
            "    class Meta:",
            "        verbose_name = 'PRTCTN_PRVDR'",
            "        verbose_name_plural = 'PRTCTN_PRVDRs'",
            "",
            "class DEAL(models.Model):",
            "    DEAL_uniqueID = models.CharField('DEAL_uniqueID', max_length=255, primary_key=True)",
            "    BYR_ID = models.CharField('BYR_ID', max_length=255, default=None, blank=True, null=True)",
            "    BYR_RL_TYP = models.CharField('BYR_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    SLLR_ID = models.CharField('SLLR_ID', max_length=255, default=None, blank=True, null=True)",
            "    SLLR_RL_TYP = models.CharField('SLLR_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    ASST_PL_ID = models.CharField('ASST_PL_ID', max_length=255, default=None, blank=True, null=True)",
            "    Deal_has_buyer = models.ForeignKey('BUYR', models.SET_NULL, blank=True, null=True)",
            "    Deal_has_seller = models.ForeignKey('SLLR', models.SET_NULL, blank=True, null=True)",
            "    Deal_has_asset_pool = models.ForeignKey('ASST_PL', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'DEAL'",
            "        verbose_name_plural = 'DEALs'",
            "",
            "class PROTECTION_ASSGNMNT(models.Model):",
            "    PROTECTION_ASSGNMNT_uniqueID = models.CharField('PROTECTION_ASSGNMNT_uniqueID', max_length=255, primary_key=True)",
            "    PRTCTN_PRVDR_ID = models.CharField('PRTCTN_PRVDR_ID', max_length=255, default=None, blank=True, null=True)",
            "    PRTCTN_PRVDR_RL_TYP = models.CharField('PRTCTN_PRVDR_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    Protection_has_provider = models.ForeignKey('PRTCTN_PRVDR', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'PROTECTION_ASSGNMNT'",
            "        verbose_name_plural = 'PROTECTION_ASSGNMNTs'",
            "",
            "class INVSTR(ENTTY_RL):",
            "    class Meta:",
            "        verbose_name = 'INVSTR'",
            "        verbose_name_plural = 'INVSTRs'",
            "",
            "class POSITION(models.Model):",
            "    POSITION_uniqueID = models.CharField('POSITION_uniqueID', max_length=255, primary_key=True)",
            "    INVSTR_ID = models.CharField('INVSTR_ID', max_length=255, default=None, blank=True, null=True)",
            "    INVSTR_RL_TYP = models.CharField('INVSTR_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    Position_has_investor = models.ForeignKey('INVSTR', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'POSITION'",
            "        verbose_name_plural = 'POSITIONs'",
        ]
    )


def _ldm_with_risk_and_context_derived_targets_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['ROOT_RFRNC_DT', 'ROOT_RPRTNG_AGNT_ID', 'ROOT_ACCNTNG_CNSLDTN_LVL', 'ROOT_ACCNTNG_STNDRD', 'ROOT_ID'], 'foreign_keys': []}}",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_RFRNC_DT = models.DateTimeField('ROOT_RFRNC_DT', default=None, blank=True, null=True)",
            "    ROOT_RPRTNG_AGNT_ID = models.CharField('ROOT_RPRTNG_AGNT_ID', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_ACCNTNG_CNSLDTN_LVL = models.CharField('ROOT_ACCNTNG_CNSLDTN_LVL', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_ACCNTNG_STNDRD = models.CharField('ROOT_ACCNTNG_STNDRD', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_ID = models.CharField('ROOT_ID', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class ROOT_RSK_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['ROOT_RSK_DT_RFRNC_DT', 'ROOT_RSK_DT_RPRTNG_AGNT_ID', 'ROOT_RSK_DT_ACCNTNG_CNSLDTN_LVL', 'ROOT_RSK_DT_ACCNTNG_STNDRD', 'ROOT_RSK_DT_ID'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'source_class': 'ROOT', 'referenced_class': 'ROOT', 'number_of_attributes': 5, 'fields': ['ROOT_RSK_DT_RFRNC_DT', 'ROOT_RSK_DT_RPRTNG_AGNT_ID', 'ROOT_RSK_DT_ACCNTNG_CNSLDTN_LVL', 'ROOT_RSK_DT_ACCNTNG_STNDRD', 'ROOT_RSK_DT_ID']}]}}",
            "    ROOT_RSK_DT_uniqueID = models.CharField('ROOT_RSK_DT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_RSK_DT_RFRNC_DT = models.DateTimeField('ROOT_RSK_DT_RFRNC_DT', default=None, blank=True, null=True)",
            "    ROOT_RSK_DT_RPRTNG_AGNT_ID = models.CharField('ROOT_RSK_DT_RPRTNG_AGNT_ID', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_RSK_DT_ACCNTNG_CNSLDTN_LVL = models.CharField('ROOT_RSK_DT_ACCNTNG_CNSLDTN_LVL', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_RSK_DT_ACCNTNG_STNDRD = models.CharField('ROOT_RSK_DT_ACCNTNG_STNDRD', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_RSK_DT_ID = models.CharField('ROOT_RSK_DT_ID', max_length=255, default=None, blank=True, null=True)",
            "    RISK_SCORE = models.BigIntegerField('RISK_SCORE', default=None, blank=True, null=True)",
            "    ROOT_has_ROOT_RSK_DT = models.ForeignKey('ROOT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT_RSK_DT'",
            "        verbose_name_plural = 'ROOT_RSK_DTs'",
            "",
            "class KB_PR_BCKT_DRVD_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['BCKT_ID'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'source_class': 'MDL_CNTXT', 'referenced_class': 'MDL_CNTXT', 'source_entity': 'Model_Context', 'relation_name': 'Model_Context_specifies_context_for_Risk_position_Kb_per_bucket_derived_data', 'fields': ['BCKT_ID']}]}}",
            "    KB_PR_BCKT_DRVD_DT_uniqueID = models.CharField('KB_PR_BCKT_DRVD_DT_uniqueID', max_length=255, primary_key=True)",
            "    BCKT_ID = models.CharField('BCKT_ID', max_length=255, default=None, blank=True, null=True)",
            "    BCKT_VALUE = models.BigIntegerField('BCKT_VALUE', default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'KB_PR_BCKT_DRVD_DT'",
            "        verbose_name_plural = 'KB_PR_BCKT_DRVD_DTs'",
            "",
            "class ASSIGNMENT(models.Model):",
            "    ASSIGNMENT_uniqueID = models.CharField('ASSIGNMENT_uniqueID', max_length=255, primary_key=True)",
            "    LEFT_ID = models.CharField('LEFT_ID', max_length=255, default=None, blank=True, null=True)",
            "    RIGHT_ID = models.CharField('RIGHT_ID', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSIGNMENT'",
            "        verbose_name_plural = 'ASSIGNMENTs'",
            "",
            "class ASSIGNMENT_RSK_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['LEFT_ID', 'RIGHT_ID', 'EXPSR_CLSS'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'source_class': 'ASSIGNMENT', 'referenced_class': 'ASSIGNMENT', 'number_of_attributes': 2, 'fields': ['LEFT_ID', 'RIGHT_ID']}]}}",
            "    ASSIGNMENT_RSK_DT_uniqueID = models.CharField('ASSIGNMENT_RSK_DT_uniqueID', max_length=255, primary_key=True)",
            "    LEFT_ID = models.CharField('LEFT_ID', max_length=255, default=None, blank=True, null=True)",
            "    RIGHT_ID = models.CharField('RIGHT_ID', max_length=255, default=None, blank=True, null=True)",
            "    EXPSR_CLSS = models.CharField('EXPSR_CLSS', max_length=255, default=None, blank=True, null=True)",
            "    RSK_WGHT = models.BigIntegerField('RSK_WGHT', default=None, blank=True, null=True)",
            "    Assignment_has_Assignment_risk_data = models.ForeignKey('ASSIGNMENT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSIGNMENT_RSK_DT'",
            "        verbose_name_plural = 'ASSIGNMENT_RSK_DTs'",
        ]
    )


def _ldm_with_identifying_association_subtype_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_ID = models.CharField('ROOT_ID', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class ASSOCIATION(models.Model):",
            "    ASSOCIATION_uniqueID = models.CharField('ASSOCIATION_uniqueID', max_length=255, primary_key=True)",
            "    ASSOCIATION_KIND = models.CharField('ASSOCIATION_KIND', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSOCIATION'",
            "        verbose_name_plural = 'ASSOCIATIONs'",
            "",
            "class ASSOCIATION_DETAIL(ASSOCIATION):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['ROOT_ID', 'DETAIL_ID'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'ROOT', 'fields': ['ROOT_ID']}]}}",
            "    ROOT_ID = models.CharField('ROOT_ID', max_length=255, default=None, blank=True, null=True)",
            "    DETAIL_ID = models.CharField('DETAIL_ID', max_length=255, default=None, blank=True, null=True)",
            "    ASSOCIATION_DETAIL_VALUE = models.BigIntegerField('ASSOCIATION_DETAIL_VALUE', default=None, blank=True, null=True)",
            "    Association_detail_has_root = models.ForeignKey('ROOT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSOCIATION_DETAIL'",
            "        verbose_name_plural = 'ASSOCIATION_DETAILs'",
        ]
    )


def _ldm_with_folded_subtype_discriminator_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_TYP = models.CharField('ROOT_TYP', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD(ROOT):",
            "    CHILD_TYP = models.CharField('CHILD_TYP', max_length=255, default=None, blank=True, null=True)",
            "    CHILD_VALUE = models.BigIntegerField('CHILD_VALUE', default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD'",
            "        verbose_name_plural = 'CHILDs'",
        ]
    )


def _ldm_with_subtype_specific_choice_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD_A(ROOT):",
            "    CHILD_A_STATUS_domain = {'1': 'Active'}",
            "    CHILD_A_STATUS = models.CharField('CHILD_A_STATUS', max_length=255, choices=CHILD_A_STATUS_domain)",
            "    SHARED_STATUS_domain = {'1': 'Shared'}",
            "    SHARED_STATUS = models.CharField('SHARED_STATUS', max_length=255, choices=SHARED_STATUS_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD_A'",
            "        verbose_name_plural = 'CHILD_As'",
            "",
            "class CHILD_B(ROOT):",
            "    SHARED_STATUS_domain = {'1': 'Shared'}",
            "    SHARED_STATUS = models.CharField('SHARED_STATUS', max_length=255, choices=SHARED_STATUS_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD_B'",
            "        verbose_name_plural = 'CHILD_Bs'",
        ]
    )


def _ldm_with_optional_identifying_fk_choice_component_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD(ROOT):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['ROOT_ACCNTNG_STNDRD'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'source_optional': 'Y', 'one_to_one': False, 'source_class': 'CHILD', 'referenced_class': 'MDL_CNTXT', 'source_entity': 'Child', 'referenced_entity': 'Model_Context', 'fields': ['ROOT_ACCNTNG_STNDRD']}]}}",
            "    ROOT_ACCNTNG_STNDRD_domain = {'1': 'IFRS'}",
            "    ROOT_ACCNTNG_STNDRD = models.CharField('ROOT_ACCNTNG_STNDRD', max_length=255, choices=ROOT_ACCNTNG_STNDRD_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD'",
            "        verbose_name_plural = 'CHILDs'",
        ]
    )


def _ldm_with_accounting_context_choices_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'fields': {'ROOT_ACCNTNG_CNSLDTN_LVL': {'add_not_applicable_candidate': True}, 'ROOT_ACCNTNG_STNDRD': {'add_not_applicable_candidate': True}}}}",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_ACCNTNG_CNSLDTN_LVL_domain = {'1': 'Solo'}",
            "    ROOT_ACCNTNG_CNSLDTN_LVL = models.CharField('ROOT_ACCNTNG_CNSLDTN_LVL', max_length=255, choices=ROOT_ACCNTNG_CNSLDTN_LVL_domain)",
            "    ROOT_ACCNTNG_STNDRD_domain = {'1': 'IFRS'}",
            "    ROOT_ACCNTNG_STNDRD = models.CharField('ROOT_ACCNTNG_STNDRD', max_length=255, choices=ROOT_ACCNTNG_STNDRD_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
        ]
    )


def _ldm_with_sqldeveloper_not_merged_discriminator_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class DBT_SCRTY_ISSD(models.Model):",
            "    DBT_SCRTY_ISSD_uniqueID = models.CharField('DBT_SCRTY_ISSD_uniqueID', max_length=255, primary_key=True)",
            "    DBT_SCRTY_ISSD_PRDNTL_PRTFL_TYP_domain = {'21': 'Issued_debt_security_in_the_trading_book', '22': 'Issued_debt_security_in_the_banking_book'}",
            "    DBT_SCRTY_ISSD_PRDNTL_PRTFL_TYP = models.CharField('DBT_SCRTY_ISSD_PRDNTL_PRTFL_TYP', max_length=255, choices=DBT_SCRTY_ISSD_PRDNTL_PRTFL_TYP_domain)",
            "    Debt_security_issued_prudential_portfolio_type_delegate = models.ForeignKey('Debt_security_issued_prudential_portfolio_type', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'DBT_SCRTY_ISSD'",
            "        verbose_name_plural = 'DBT_SCRTY_ISSDs'",
            "",
            "class Debt_security_issued_prudential_portfolio_type(models.Model):",
            "    class Meta:",
            "        verbose_name = 'Debt_security_issued_prudential_portfolio_type'",
            "        verbose_name_plural = 'Debt_security_issued_prudential_portfolio_types'",
            "",
            "class DBT_SCRTY_ISSD_BNKNG_BK(Debt_security_issued_prudential_portfolio_type):",
            "    class Meta:",
            "        verbose_name = 'Issued_debt_security_in_the_banking_book'",
            "        verbose_name_plural = 'Issued_debt_security_in_the_banking_books'",
            "",
            "class DBT_SCRTY_ISSD_TRDNG_BK(Debt_security_issued_prudential_portfolio_type):",
            "    DBT_SCRTY_ISSD_TRDNG_BK_ACCNTNG_STNDRD_domain = {'23': 'Issued_debt_security_in_the_trading_book_International_Financial_Reporting_Standard_IFRS', '24': 'Issued_debt_security_in_the_trading_book_national_general_accepted_accounting_principl_f32854'}",
            "    DBT_SCRTY_ISSD_TRDNG_BK_ACCNTNG_STNDRD = models.CharField('DBT_SCRTY_ISSD_TRDNG_BK_ACCNTNG_STNDRD', max_length=255, choices=DBT_SCRTY_ISSD_TRDNG_BK_ACCNTNG_STNDRD_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Issued_debt_security_in_the_trading_book'",
            "        verbose_name_plural = 'Issued_debt_security_in_the_trading_books'",
            "",
            "class DBT_SCRTY_ISSD_TRDNG_BK_IFRS(DBT_SCRTY_ISSD_TRDNG_BK):",
            "    class Meta:",
            "        verbose_name = 'Issued_debt_security_in_the_trading_book_International_Financial_Reporting_Standard_IFRS'",
            "        verbose_name_plural = 'Issued_debt_security_in_the_trading_book_International_Financial_Reporting_Standard_IFRSs'",
            "",
            "class DBT_SCRTY_ISSD_TRDNG_BK_NGAAP(DBT_SCRTY_ISSD_TRDNG_BK):",
            "    class Meta:",
            "        verbose_name = 'Issued_debt_security_in_the_trading_book_national_general_accepted_accounting_principl_f32854'",
            "        verbose_name_plural = 'Issued_debt_security_in_the_trading_book_national_general_accepted_accounting_principl_f32854s'",
        ]
    )


def _ldm_with_non_type_leaf_choice_for_reduced_discriminator_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_TYP_domain = {'1': 'Root_branch'}",
            "    ROOT_TYP = models.CharField('ROOT_TYP', max_length=255, choices=ROOT_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD_LEAF_BY_STANDARD(ROOT):",
            "    CHILD_BY_STANDARD_domain = {'10': 'Child_leaf_by_standard'}",
            "    CHILD_BY_STANDARD = models.CharField('CHILD_BY_STANDARD', max_length=255, choices=CHILD_BY_STANDARD_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Child_leaf_by_standard'",
            "        verbose_name_plural = 'Child_leaf_by_standards'",
            "",
            "class OTHER_CHILD_LEAF(ROOT):",
            "    OTHER_CHILD_LEAF_TYP_domain = {'20': 'Other_child_leaf'}",
            "    OTHER_CHILD_LEAF_TYP = models.CharField('OTHER_CHILD_LEAF_TYP', max_length=255, choices=OTHER_CHILD_LEAF_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Other_child_leaf'",
            "        verbose_name_plural = 'Other_child_leafs'",
        ]
    )


def _ldm_with_annotated_entity_member_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_TYP_domain = {'1': 'Root_branch'}",
            "    ROOT_TYP = models.CharField('ROOT_TYP', max_length=255, choices=ROOT_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD_LEAF(ROOT):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '34', 'member_label': 'Annotated_child_leaf'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Annotated child leaf'",
            "        verbose_name_plural = 'Annotated child leafs'",
            "",
            "class OTHER_CHILD_LEAF(ROOT):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '35', 'member_label': 'Other_annotated_leaf'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Other annotated leaf'",
            "        verbose_name_plural = 'Other annotated leafs'",
        ]
    )


def _ldm_with_delegate_discriminator_manual_member_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class INSTRMNT(models.Model):",
            "    INSTRMNT_uniqueID = models.CharField('INSTRMNT_uniqueID', max_length=255, primary_key=True)",
            "    INSTRMNT_TYP_PRDCT_domain = {'549': 'Deposit'}",
            "    INSTRMNT_TYP_PRDCT = models.CharField('INSTRMNT_TYP_PRDCT', max_length=255, choices=INSTRMNT_TYP_PRDCT_domain)",
            "    Instrument_type_by_product_delegate = models.ForeignKey('Instrument_type_by_product', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'INSTRMNT'",
            "        verbose_name_plural = 'INSTRMNTs'",
            "",
            "class Instrument_type_by_product(models.Model):",
            "",
            "    class Meta:",
            "        verbose_name = 'Instrument_type_by_product'",
            "        verbose_name_plural = 'Instrument_type_by_products'",
            "",
            "class DPST(Instrument_type_by_product):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '549', 'member_label': 'Deposit', 'discriminator_field': 'INSTRMNT_TYP_PRDCT'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Deposit'",
            "        verbose_name_plural = 'Deposits'",
            "",
            "class OVRNGHT_DPST(DPST):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '510', 'member_label': 'Overnight_deposits', 'discriminator_field': 'DPST_TYP'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Overnight_deposit'",
            "        verbose_name_plural = 'Overnight_deposits'",
            "",
            "class TRNSFRBL_DPST(OVRNGHT_DPST):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '30', 'member_label': 'Transferable_deposit', 'discriminator_field': 'OVRNGHT_DPST_TYP'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Transferable_deposit'",
            "        verbose_name_plural = 'Transferable_deposits'",
        ]
    )


def _ldm_with_folded_input_domain_source_member_gap() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class INSTRMNT(models.Model):",
            "    INSTRMNT_uniqueID = models.CharField('INSTRMNT_uniqueID', max_length=255, primary_key=True)",
            "    INSTRMNT_TYP_PRDCT_domain = {'549': 'Deposit'}",
            "    INSTRMNT_TYP_PRDCT = models.CharField('INSTRMNT_TYP_PRDCT', max_length=255, choices=INSTRMNT_TYP_PRDCT_domain)",
            "    Instrument_type_by_product_delegate = models.ForeignKey('Instrument_type_by_product', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'INSTRMNT'",
            "        verbose_name_plural = 'INSTRMNTs'",
            "",
            "class Instrument_type_by_product(models.Model):",
            "",
            "    class Meta:",
            "        verbose_name = 'Instrument_type_by_product'",
            "        verbose_name_plural = 'Instrument_type_by_products'",
            "",
            "class CRDT_CRD_DBT(Instrument_type_by_product):",
            "",
            "    class Meta:",
            "        verbose_name = 'Credit_card_debt'",
            "        verbose_name_plural = 'Credit_card_debts'",
            "",
            "class RPRCHS_TRNSCTN(Instrument_type_by_product):",
            "",
            "    class Meta:",
            "        verbose_name = 'Repurchase_transaction'",
            "        verbose_name_plural = 'Repurchase_transactions'",
            "",
            "class OPN_RPRCHS_TRNSCTN(RPRCHS_TRNSCTN):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '162', 'member_label': 'Open_repurchase_transaction', 'discriminator_field': 'RPRCHS_TRNSCTN_TYP'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Open_repurchase_transaction'",
            "        verbose_name_plural = 'Open_repurchase_transactions'",
        ]
    )


def _ldm_with_annotated_folded_delegate_member_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_TYP_domain = {'1': 'Root_branch'}",
            "    ROOT_TYP = models.CharField('ROOT_TYP', max_length=255, choices=ROOT_TYP_domain)",
            "    Lower_kind_delegate = models.ForeignKey('Lower_kind', models.SET_NULL, blank=True, null=True)",
            "    Preserved_kind_delegate = models.ForeignKey('Preserved_kind', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class DIRECT_CHILD(ROOT):",
            "",
            "    class Meta:",
            "        verbose_name = 'DIRECT_CHILD'",
            "        verbose_name_plural = 'DIRECT_CHILDs'",
            "",
            "class Lower_kind(models.Model):",
            "    Lower_kind_uniqueID = models.CharField('Lower_kind_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Lower kind'",
            "        verbose_name_plural = 'Lower kinds'",
            "",
            "class LOWER_A(Lower_kind):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '34', 'member_label': 'Delegate_leaf_a', 'discriminator_field': 'LOWER_TYP'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'LOWER_A'",
            "        verbose_name_plural = 'LOWER_As'",
            "",
            "class LOWER_B(Lower_kind):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '35', 'member_label': 'Delegate_leaf_b', 'discriminator_field': 'LOWER_TYP'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'LOWER_B'",
            "        verbose_name_plural = 'LOWER_Bs'",
            "",
            "class Preserved_kind(models.Model):",
            "    Preserved_kind_uniqueID = models.CharField('Preserved_kind_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Preserved kind'",
            "        verbose_name_plural = 'Preserved kinds'",
            "",
            "class PRESERVED_A(Preserved_kind):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '17', 'member_label': 'Preserved_leaf', 'discriminator_field': 'FNNCL_ASST_INSTRMNT_TYP_RNGTTN_STTS'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'PRESERVED_A'",
            "        verbose_name_plural = 'PRESERVED_As'",
        ]
    )


def _ldm_with_relationship_copy_reduced_discriminator_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class INSTRMNT_RL(models.Model):",
            "    INSTRMNT_RL_uniqueID = models.CharField('INSTRMNT_RL_uniqueID', max_length=255, primary_key=True)",
            "    INSTRMNT_RL_TYP_domain = {'3': 'Financial_asset_instrument', '8': 'Collateral_given_instrument'}",
            "    INSTRMNT_RL_TYP = models.CharField('INSTRMNT_RL_TYP', max_length=255, choices=INSTRMNT_RL_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Instrument role'",
            "        verbose_name_plural = 'Instrument roles'",
            "",
            "class CLLTRL_GVN_INSTRMNT(INSTRMNT_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '8', 'member_label': 'Collateral_given_instrument'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Collateral_given_instrument'",
            "        verbose_name_plural = 'Collateral_given_instruments'",
            "",
            "class BS_AST_RL(INSTRMNT_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '34', 'member_label': 'Balance_sheet_asset_role'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Balance_sheet_asset_role'",
            "        verbose_name_plural = 'Balance_sheet_asset_roles'",
            "",
            "class NBS_AST_RL(INSTRMNT_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '101', 'member_label': 'Non_balance_sheet_asset_role'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Non_balance_sheet_asset_role'",
            "        verbose_name_plural = 'Non_balance_sheet_asset_roles'",
            "",
            "class ASSIGNMENT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['CLLTRL_GVN_INSTRMNT_RL_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'CLLTRL_GVN_INSTRMNT', 'fields': ['CLLTRL_GVN_INSTRMNT_RL_TYP']}], 'fields': {'CLLTRL_GVN_INSTRMNT_RL_TYP': {'domain_synonym': 'INSTRMNT_RL_TYP', 'add_not_applicable_candidate': True}}}}",
            "    ASSIGNMENT_uniqueID = models.CharField('ASSIGNMENT_uniqueID', max_length=255, primary_key=True)",
            "    CLLTRL_GVN_INSTRMNT_RL_TYP_domain = {'0': 'Not_applicable', '3': 'Financial_asset_instrument', '8': 'Collateral_given_instrument'}",
            "    CLLTRL_GVN_INSTRMNT_RL_TYP = models.CharField('CLLTRL_GVN_INSTRMNT_RL_TYP', max_length=255, choices=CLLTRL_GVN_INSTRMNT_RL_TYP_domain)",
            "    Assignment_has_collateral_given_instrument = models.ForeignKey('CLLTRL_GVN_INSTRMNT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSIGNMENT'",
            "        verbose_name_plural = 'ASSIGNMENTs'",
        ]
    )


def _ldm_with_unrelated_annotated_source_member() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class COMBINED(models.Model):",
            "    KIND_TYP_domain = {'1': 'Good_a', '2': 'Good_b'}",
            "    KIND_TYP = models.CharField('KIND_TYP', max_length=255, choices=KIND_TYP_domain)",
            "",
            "class Kind(models.Model):",
            "    pass",
            "",
            "class GOOD_A(Kind):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '1', 'member_label': 'Good_a', 'discriminator_field': 'KIND_TYP'}}}",
            "",
            "class GOOD_B(Kind):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '2', 'member_label': 'Good_b', 'discriminator_field': 'KIND_TYP'}}}",
            "",
            "class UNRELATED(COMBINED):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '9', 'member_label': 'Unrelated', 'discriminator_field': 'OTHER_TYP'}}}",
        ]
    )


def _ldm_with_reduced_discriminator_leaf_derived_data_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_TYP_domain = {'1': 'Root'}",
            "    ROOT_TYP = models.CharField('ROOT_TYP', max_length=255, choices=ROOT_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD_WITH_DRVD_DT(ROOT):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '10', 'member_label': 'Child_with_derived_data'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Child_with_derived_data'",
            "        verbose_name_plural = 'Child_with_derived_datas'",
            "",
            "class CHILD_WITHOUT_DRVD_DT(ROOT):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '20', 'member_label': 'Child_without_derived_data'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Child_without_derived_data'",
            "        verbose_name_plural = 'Child_without_derived_datas'",
            "",
            "class CHILD_WITH_DRVD_DT_DRVD_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['ROOT_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'CHILD_WITH_DRVD_DT', 'fields': ['ROOT_TYP']}]}}",
            "    CHILD_WITH_DRVD_DT_DRVD_DT_uniqueID = models.CharField('CHILD_WITH_DRVD_DT_DRVD_DT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_TYP = models.CharField('ROOT_TYP', max_length=255, default=None, blank=True, null=True)",
            "    SCORE = models.BigIntegerField('SCORE', default=None, blank=True, null=True)",
            "    Child_has_derived_data = models.ForeignKey('CHILD_WITH_DRVD_DT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD_WITH_DRVD_DT_DRVD_DT'",
            "        verbose_name_plural = 'CHILD_WITH_DRVD_DT_DRVD_DTs'",
        ]
    )


def _ldm_with_not_applicable_relationship_copy_reduced_discriminator_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class POSITION_RL(models.Model):",
            "    POSITION_RL_uniqueID = models.CharField('POSITION_RL_uniqueID', max_length=255, primary_key=True)",
            "    POSITION_RL_TYP_domain = {'11': 'Position_role'}",
            "    POSITION_RL_TYP = models.CharField('POSITION_RL_TYP', max_length=255, choices=POSITION_RL_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Position role'",
            "        verbose_name_plural = 'Position roles'",
            "",
            "class POSITION_ASST(POSITION_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '9', 'member_label': 'Position_asset_role'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Position_asset_role'",
            "        verbose_name_plural = 'Position_asset_roles'",
            "",
            "class POSITION_LBLTY(POSITION_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '10', 'member_label': 'Position_liability_role'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Position_liability_role'",
            "        verbose_name_plural = 'Position_liability_roles'",
            "",
            "class POSITION_RL_DRVD_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['POSITION_RL_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'POSITION_RL', 'fields': ['POSITION_RL_TYP']}], 'fields': {'POSITION_RL_TYP': {'add_not_applicable_candidate': True, 'domain_synonym': 'POSITION_RL_TYP'}}}}",
            "    POSITION_RL_DRVD_DT_uniqueID = models.CharField('POSITION_RL_DRVD_DT_uniqueID', max_length=255, primary_key=True)",
            "    POSITION_RL_TYP_domain = {'11': 'Position_role'}",
            "    POSITION_RL_TYP = models.CharField('POSITION_RL_TYP', max_length=255, choices=POSITION_RL_TYP_domain)",
            "    POSITION_RL_DRVD_VALUE = models.CharField('POSITION_RL_DRVD_VALUE', max_length=255, default=None, blank=True, null=True)",
            "    Position_role_has_derived_data = models.ForeignKey('POSITION_RL', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'POSITION_RL_DRVD_DT'",
            "        verbose_name_plural = 'POSITION_RL_DRVD_DTs'",
            "",
            "class ASSIGNMENT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['POSITION_ASST_RL_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'POSITION_ASST', 'fields': ['POSITION_ASST_RL_TYP']}], 'fields': {'POSITION_ASST_RL_TYP': {'domain_synonym': 'POSITION_RL_TYP'}}}}",
            "    ASSIGNMENT_uniqueID = models.CharField('ASSIGNMENT_uniqueID', max_length=255, primary_key=True)",
            "    POSITION_ASST_RL_TYP_domain = {'11': 'Position_role', '9': 'Position_asset_role'}",
            "    POSITION_ASST_RL_TYP = models.CharField('POSITION_ASST_RL_TYP', max_length=255, choices=POSITION_ASST_RL_TYP_domain)",
            "    Assignment_has_position_asset_role = models.ForeignKey('POSITION_ASST', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSIGNMENT'",
            "        verbose_name_plural = 'ASSIGNMENTs'",
        ]
    )


def _ldm_with_directional_role_reduced_discriminator_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class CLLTRL_RL(models.Model):",
            "    CLLTRL_RL_uniqueID = models.CharField('CLLTRL_RL_uniqueID', max_length=255, primary_key=True)",
            "    CLLTRL_RL_TYP_domain = {'2': 'Collateral_given'}",
            "    CLLTRL_RL_TYP = models.CharField('CLLTRL_RL_TYP', max_length=255, choices=CLLTRL_RL_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Collateral_role'",
            "        verbose_name_plural = 'Collateral_roles'",
            "",
            "class CLLTRL_GVN(CLLTRL_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '2', 'member_label': 'Collateral_given'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Collateral_given'",
            "        verbose_name_plural = 'Collateral_givens'",
            "",
            "class CLLTRL_RCVD(CLLTRL_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '1', 'member_label': 'Collateral_received'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Collateral_received'",
            "        verbose_name_plural = 'Collateral_receiveds'",
            "",
            "class ASSIGNMENT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['CLLTRL_RCVD_RL_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'CLLTRL_RCVD', 'fields': ['CLLTRL_RCVD_RL_TYP']}], 'fields': {'CLLTRL_RCVD_RL_TYP': {'domain_synonym': 'CLLTRL_RL_TYP'}}}}",
            "    ASSIGNMENT_uniqueID = models.CharField('ASSIGNMENT_uniqueID', max_length=255, primary_key=True)",
            "    CLLTRL_RCVD_RL_TYP_domain = {'1': 'Collateral_received'}",
            "    CLLTRL_RCVD_RL_TYP = models.CharField('CLLTRL_RCVD_RL_TYP', max_length=255, choices=CLLTRL_RCVD_RL_TYP_domain)",
            "    Assignment_has_collateral_received = models.ForeignKey('CLLTRL_RCVD', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSIGNMENT'",
            "        verbose_name_plural = 'ASSIGNMENTs'",
        ]
    )


def _ldm_with_entity_role_copy_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ENTTY_RL(models.Model):",
            "    ENTTY_RL_uniqueID = models.CharField('ENTTY_RL_uniqueID', max_length=255, primary_key=True)",
            "    ENTTY_RL_TYP_domain = {'1': 'Entity_role'}",
            "    ENTTY_RL_TYP = models.CharField('ENTTY_RL_TYP', max_length=255, choices=ENTTY_RL_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Entity role'",
            "        verbose_name_plural = 'Entity roles'",
            "",
            "class SUB(ENTTY_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '10', 'member_label': 'Sub_role'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Sub_role'",
            "        verbose_name_plural = 'Sub_roles'",
            "",
            "class OTHER_ROLE(ENTTY_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '20', 'member_label': 'Other_role'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Other_role'",
            "        verbose_name_plural = 'Other_roles'",
            "",
            "class ASSIGNMENT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['SUB_RL_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'SUB', 'fields': ['SUB_RL_TYP']}], 'fields': {'SUB_RL_TYP': {'domain_synonym': 'SUB_RL_TYP'}}}}",
            "    ASSIGNMENT_uniqueID = models.CharField('ASSIGNMENT_uniqueID', max_length=255, primary_key=True)",
            "    SUB_RL_TYP_domain = {'10': 'Sub_role'}",
            "    SUB_RL_TYP = models.CharField('SUB_RL_TYP', max_length=255, choices=SUB_RL_TYP_domain)",
            "    Assignment_has_sub_role = models.ForeignKey('SUB', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSIGNMENT'",
            "        verbose_name_plural = 'ASSIGNMENTs'",
        ]
    )


def _ldm_with_indirect_entity_role_copy_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ENTTY_RL(models.Model):",
            "    ENTTY_RL_uniqueID = models.CharField('ENTTY_RL_uniqueID', max_length=255, primary_key=True)",
            "    ENTTY_RL_TYP_domain = {'1': 'Entity_role'}",
            "    ENTTY_RL_TYP = models.CharField('ENTTY_RL_TYP', max_length=255, choices=ENTTY_RL_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Entity role'",
            "        verbose_name_plural = 'Entity roles'",
            "",
            "class INVSTR(ENTTY_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '8', 'member_label': 'Investor'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Investor'",
            "        verbose_name_plural = 'Investors'",
            "",
            "class OTHER_ROLE(ENTTY_RL):",
            "    __bird_annotations__ = {'sql_developer': {'entity_member': {'member_code': '20', 'member_label': 'Other_role'}}}",
            "",
            "    class Meta:",
            "        verbose_name = 'Other_role'",
            "        verbose_name_plural = 'Other_roles'",
            "",
            "class SCRTY_PSTN(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['INVSTR_RL_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'INVSTR', 'fields': ['INVSTR_RL_TYP']}], 'fields': {'INVSTR_RL_TYP': {'domain_synonym': 'PRTY_RL_TYP', 'primary_key': True, 'foreign_key': True}}}}",
            "    SCRTY_PSTN_uniqueID = models.CharField('SCRTY_PSTN_uniqueID', max_length=255, primary_key=True)",
            "    PRTY_RL_TYP_domain = {'8': 'Investor'}",
            "    INVSTR_RL_TYP = models.CharField('INVSTR_RL_TYP', max_length=255, choices=PRTY_RL_TYP_domain)",
            "    Security_position_has_investor = models.ForeignKey('INVSTR', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'SCRTY_PSTN'",
            "        verbose_name_plural = 'SCRTY_PSTNs'",
            "",
            "class HEDGE(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['INVSTR_RL_TYP'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'SCRTY_PSTN', 'fields': ['INVSTR_RL_TYP']}], 'fields': {'INVSTR_RL_TYP': {'domain_synonym': 'PRTY_RL_TYP', 'primary_key': True, 'foreign_key': True}}}}",
            "    HEDGE_uniqueID = models.CharField('HEDGE_uniqueID', max_length=255, primary_key=True)",
            "    PRTY_RL_TYP_domain = {'8': 'Investor'}",
            "    INVSTR_RL_TYP = models.CharField('INVSTR_RL_TYP', max_length=255, choices=PRTY_RL_TYP_domain)",
            "    Hedge_has_security_position = models.ForeignKey('SCRTY_PSTN', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'HEDGE'",
            "        verbose_name_plural = 'HEDGEs'",
        ]
    )


def _ldm_with_sql_developer_input_domain_folded_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD(ROOT):",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD'",
            "        verbose_name_plural = 'CHILDs'",
            "",
            "class OTHER_CHILD(ROOT):",
            "",
            "    class Meta:",
            "        verbose_name = 'OTHER_CHILD'",
            "        verbose_name_plural = 'OTHER_CHILDs'",
            "",
            "class CHILD_DRVD_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['CHILD_ID'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'CHILD', 'fields': ['CHILD_ID']}, {'identifying': 'N', 'relation_side': 'target', 'referenced_class': 'DFLT_STTS_DRVD', 'fields': ['DFLT_STTS_DRVD']}], 'fields': {'DFLT_STTS_DRVD': {'domain_synonym': 'DRVD_DFLT_STTS', 'primary_key': False, 'foreign_key': True}}}}",
            "    CHILD_DRVD_DT_uniqueID = models.CharField('CHILD_DRVD_DT_uniqueID', max_length=255, primary_key=True)",
            "    CHILD_ID = models.CharField('CHILD_ID', max_length=255, default=None, blank=True, null=True)",
            "    DRVD_DFLT_STTS_domain = {'6': 'Default'}",
            "    DFLT_STTS_DRVD = models.CharField('DFLT_STTS_DRVD', max_length=255, choices=DRVD_DFLT_STTS_domain)",
            "    Child_has_derived_data = models.ForeignKey('CHILD', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD_DRVD_DT'",
            "        verbose_name_plural = 'CHILD_DRVD_DTs'",
            "",
            "class DFLT_STTS_DRVD(models.Model):",
            "    DFLT_STTS_DRVD_uniqueID = models.CharField('DFLT_STTS_DRVD_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'DFLT_STTS_DRVD'",
            "        verbose_name_plural = 'DFLT_STTS_DRVDs'",
        ]
    )


def _ldm_with_synthetic_sqldeveloper_choice_fields_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'fields': {'LSTD_INDCTR': {'domain_synonym': 'BLN_TF'}, 'OWN_CMPNY_INVSTMNT_INDCTR': {'domain_id': 'DOM3000004'}}}}",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    LSTD_INDCTR = models.BooleanField('LSTD_INDCTR', default=None, blank=True, null=True)",
            "    OWN_CMPNY_INVSTMNT_INDCTR = models.CharField('OWN_CMPNY_INVSTMNT_INDCTR', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
        ]
    )


def _ldm_with_not_applicable_field_annotation_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'fields': {'STATUS': {'add_not_applicable_candidate': True}}}}",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    STATUS_domain = {'1': 'Active'}",
            "    STATUS = models.CharField('STATUS', max_length=255, choices=STATUS_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
        ]
    )


def _ldm_with_tranche_leaf_reduced_discriminator_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class SCRTSTN_TRNCH(models.Model):",
            "    SCRTSTN_TRNCH_uniqueID = models.CharField('SCRTSTN_TRNCH_uniqueID', max_length=255, primary_key=True)",
            "    SCRTSTN_TRNCH_TYP_domain = {'3': 'Tranche_in_a_Traditional_securitisation', '4': 'Tranche_in_a_synthetic_securitisation'}",
            "    SCRTSTN_TRNCH_TYP = models.CharField('SCRTSTN_TRNCH_TYP', max_length=255, choices=SCRTSTN_TRNCH_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'SCRTSTN_TRNCH'",
            "        verbose_name_plural = 'SCRTSTN_TRNCHs'",
            "",
            "class TRNCH_SYNTHTC_SCRTSTN(SCRTSTN_TRNCH):",
            "    class Meta:",
            "        verbose_name = 'Tranche_in_a_synthetic_securitisation'",
            "        verbose_name_plural = 'Tranche_in_a_synthetic_securitisations'",
            "",
            "class TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_DPST(TRNCH_SYNTHTC_SCRTSTN):",
            "    TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_TYP_domain = {'1': 'Tranche_in_a_synthetic_securitisation_without_securitisation_special_purpose_entity_SS_88481b'}",
            "    TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_TYP = models.CharField('TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_TYP', max_length=255, choices=TRNCH_SYNTHTC_SCRTSTN_WTHT_SSPE_TYP_domain)",
            "",
            "    class Meta:",
            "        verbose_name = 'Tranche_in_a_synthetic_securitisation_without_securitisation_special_purpose_entity_SS_88481b'",
            "        verbose_name_plural = 'Tranche_in_a_synthetic_securitisation_without_securitisation_special_purpose_entity_SS_88481bs'",
            "",
            "class TRNCH_TRDTNL_SCRTSTN(SCRTSTN_TRNCH):",
            "    class Meta:",
            "        verbose_name = 'Tranche_in_a_Traditional_securitisation'",
            "        verbose_name_plural = 'Tranche_in_a_Traditional_securitisations'",
        ]
    )


def _ldm_with_source_side_derived_data_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_ACCNTNG_STNDRD = models.CharField('ROOT_ACCNTNG_STNDRD', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_ID = models.CharField('ROOT_ID', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class CHILD(ROOT):",
            "    CHILD_BY_ACCNTNG_STNDRD = models.CharField('CHILD_BY_ACCNTNG_STNDRD', max_length=255, default=None, blank=True, null=True)",
            "    CHILD_INCPTN_DT = models.DateTimeField('CHILD_INCPTN_DT', default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD'",
            "        verbose_name_plural = 'CHILDs'",
            "",
            "class CHILD_DRVD_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['CHILD_ID'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'source', 'source_class': 'CHILD_DRVD_DT', 'referenced_class': 'CHILD', 'fields': ['CHILD_ID']}]}}",
            "    CHILD_DRVD_DT_uniqueID = models.CharField('CHILD_DRVD_DT_uniqueID', max_length=255, primary_key=True)",
            "    CHILD_ID = models.CharField('CHILD_ID', max_length=255, default=None, blank=True, null=True)",
            "    DERIVED_SCORE = models.BigIntegerField('DERIVED_SCORE', default=None, blank=True, null=True)",
            "    Child_has_derived_data = models.ForeignKey('CHILD', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'CHILD_DRVD_DT'",
            "        verbose_name_plural = 'CHILD_DRVD_DTs'",
        ]
    )


def _ldm_with_sql_developer_policy_targets_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class SCRTSTN_TRNCH(models.Model):",
            "    SCRTSTN_TRNCH_uniqueID = models.CharField('SCRTSTN_TRNCH_uniqueID', max_length=255, primary_key=True)",
            "    SCRTSTN_TRNCH_TYP = models.CharField('SCRTSTN_TRNCH_TYP', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Securitisation_tranche'",
            "        verbose_name_plural = 'Securitisation_tranches'",
            "",
            "class TRNCH_TRDTNL_SCRTSTN(SCRTSTN_TRNCH):",
            "    TRNCH_TRDTNL_SCRTSTN_VALUE = models.BigIntegerField('TRNCH_TRDTNL_SCRTSTN_VALUE', default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Tranche_in_a_Traditional_securitisation'",
            "        verbose_name_plural = 'Tranche_in_a_Traditional_securitisations'",
            "",
            "class CRDT_FCLTY(models.Model):",
            "    CRDT_FCLTY_uniqueID = models.CharField('CRDT_FCLTY_uniqueID', max_length=255, primary_key=True)",
            "    CRDT_FCLTY_ID = models.CharField('CRDT_FCLTY_ID', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Credit_facility'",
            "        verbose_name_plural = 'Credit_facilities'",
            "",
            "class CRDT_FCLTY_INTRST_RT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['CRDT_FCLTY_INTRST_RT_ID'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'CRDT_FCLTY', 'fields': ['CRDT_FCLTY_INTRST_RT_ID']}]}}",
            "    CRDT_FCLTY_INTRST_RT_uniqueID = models.CharField('CRDT_FCLTY_INTRST_RT_uniqueID', max_length=255, primary_key=True)",
            "    CRDT_FCLTY_INTRST_RT_ID = models.CharField('CRDT_FCLTY_INTRST_RT_ID', max_length=255, default=None, blank=True, null=True)",
            "    INTRST_RT = models.BigIntegerField('INTRST_RT', default=None, blank=True, null=True)",
            "    Credit_facility_has_interest_rate = models.ForeignKey('CRDT_FCLTY', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Credit_facility_with_interest_rate'",
            "        verbose_name_plural = 'Credit_facility_with_interest_rates'",
            "",
            "class CLLTRL(models.Model):",
            "    CLLTRL_uniqueID = models.CharField('CLLTRL_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Collateral'",
            "        verbose_name_plural = 'Collaterals'",
            "",
            "class CLLTRL_RL(models.Model):",
            "    CLLTRL_RL_uniqueID = models.CharField('CLLTRL_RL_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Collateral_role'",
            "        verbose_name_plural = 'Collateral_roles'",
            "",
            "class CVRD_BND_PRGRM(models.Model):",
            "    CVRD_BND_PRGRM_uniqueID = models.CharField('CVRD_BND_PRGRM_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'Covered_bond_programme'",
            "        verbose_name_plural = 'Covered_bond_programmes'",
            "",
            "class KEY_ASSIGNMENT(models.Model):",
            "    KEY_ASSIGNMENT_uniqueID = models.CharField('KEY_ASSIGNMENT_uniqueID', max_length=255, primary_key=True)",
            "    CLLTRL_RL_ID = models.CharField('CLLTRL_RL_ID', max_length=255, default=None, blank=True, null=True)",
            "    LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_SCRTY_ID = models.CharField('LNG_BLNC_SHT_RCGNSD_SCRTY_PSTN_SCRTY_ID', max_length=255, default=None, blank=True, null=True)",
            "    DBT_SCRTY_ISSD_ACCNTNG_CLSSFCTN = models.CharField('DBT_SCRTY_ISSD_ACCNTNG_CLSSFCTN', max_length=255, default=None, blank=True, null=True)",
            "    EXCHNG_TRDBL_DRVTV_SCRTY_ID = models.CharField('EXCHNG_TRDBL_DRVTV_SCRTY_ID', max_length=255, default=None, blank=True, null=True)",
            "    CVRD_BND_PRGRM_ID = models.CharField('CVRD_BND_PRGRM_ID', max_length=255, default=None, blank=True, null=True)",
            "    Key_assignment_has_collateral_role = models.ForeignKey('CLLTRL_RL', models.SET_NULL, blank=True, null=True)",
            "    Key_assignment_has_covered_bond_programme = models.ForeignKey('CVRD_BND_PRGRM', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'KEY_ASSIGNMENT'",
            "        verbose_name_plural = 'KEY_ASSIGNMENTs'",
        ]
    )


def _ldm_with_wrapped_key_fields_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class INSTRMNT(models.Model):",
            "    INSTRMNT_uniqueID = models.CharField('INSTRMNT_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'INSTRMNT'",
            "        verbose_name_plural = 'INSTRMNTs'",
            "",
            "class TRD_RCVBL(INSTRMNT):",
            "    class Meta:",
            "        verbose_name = 'TRD_RCVBL'",
            "        verbose_name_plural = 'TRD_RCVBLs'",
            "",
            "class ENTTY_RL(models.Model):",
            "    ENTTY_RL_uniqueID = models.CharField('ENTTY_RL_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ENTTY_RL'",
            "        verbose_name_plural = 'ENTTY_RLs'",
            "",
            "class LNDR(ENTTY_RL):",
            "    class Meta:",
            "        verbose_name = 'LNDR'",
            "        verbose_name_plural = 'LNDRs'",
            "",
            "class BUYR(ENTTY_RL):",
            "    class Meta:",
            "        verbose_name = 'BUYR'",
            "        verbose_name_plural = 'BUYRs'",
            "",
            "class ASSIGNMENT(models.Model):",
            "    ASSIGNMENT_uniqueID = models.CharField('ASSIGNMENT_uniqueID', max_length=255, primary_key=True)",
            "    LNDR_PRTY_ID = models.CharField('LNDR_PRTY_ID', max_length=255, default=None, blank=True, null=True)",
            "    LNDR_PRTY_RFRNC_DT = models.DateTimeField('LNDR_PRTY_RFRNC_DT', default=None, blank=True, null=True)",
            "    LNDR_PRTY_RPRTNG_AGNT_ID = models.CharField('LNDR_PRTY_RPRTNG_AGNT_ID', max_length=255, default=None, blank=True, null=True)",
            "    LNDR_PRTY_RL_TYP = models.CharField('LNDR_PRTY_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    BYR_PRTY_ID = models.CharField('BYR_PRTY_ID', max_length=255, default=None, blank=True, null=True)",
            "    BYR_PRTY_RFRNC_DT = models.DateTimeField('BYR_PRTY_RFRNC_DT', default=None, blank=True, null=True)",
            "    BYR_PRTY_RPRTNG_AGNT_ID = models.CharField('BYR_PRTY_RPRTNG_AGNT_ID', max_length=255, default=None, blank=True, null=True)",
            "    BYR_PRTY_RL_TYP = models.CharField('BYR_PRTY_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    TRD_RCVBL_ID = models.CharField('TRD_RCVBL_ID', max_length=255, default=None, blank=True, null=True)",
            "    TRD_RCVBL_INSTRMNT_RFRNC_DT = models.DateTimeField('TRD_RCVBL_INSTRMNT_RFRNC_DT', default=None, blank=True, null=True)",
            "    TRD_RCVBL_INSTRMNT_RPRTNG_AGNT_ID = models.CharField('TRD_RCVBL_INSTRMNT_RPRTNG_AGNT_ID', max_length=255, default=None, blank=True, null=True)",
            "    AMNT = models.BigIntegerField('AMNT', default=None, blank=True, null=True)",
            "    Assignment_has_lender = models.ForeignKey('LNDR', models.SET_NULL, blank=True, null=True)",
            "    Assignment_has_buyer = models.ForeignKey('BUYR', models.SET_NULL, blank=True, null=True)",
            "    Assignment_has_trade_receivable = models.ForeignKey('TRD_RCVBL', models.SET_NULL, blank=True, null=True)",
            "    Assignment_has_trade_receivable_again = models.ForeignKey('TRD_RCVBL', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASSIGNMENT'",
            "        verbose_name_plural = 'ASSIGNMENTs'",
        ]
    )


def _ldm_with_annotated_identifying_extension_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    test_id = models.CharField('test_id', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_ID = models.CharField('ROOT_ID', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class ROOT_RSK_DT(models.Model):",
            "    __bird_annotations__ = {'sql_developer': {'primary_key': ['ROOT_RSK_DT_ID'], 'foreign_keys': [{'identifying': 'Y', 'relation_side': 'target', 'referenced_class': 'ROOT', 'fields': ['ROOT_RSK_DT_ID']}]}}",
            "    ROOT_RSK_DT_uniqueID = models.CharField('ROOT_RSK_DT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_RSK_DT_ID = models.CharField('ROOT_RSK_DT_ID', max_length=255, default=None, blank=True, null=True)",
            "    RISK_SCORE = models.BigIntegerField('RISK_SCORE', default=None, blank=True, null=True)",
            "    ROOT_has_ROOT_RSK_DT = models.ForeignKey('ROOT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT_RSK_DT'",
            "        verbose_name_plural = 'ROOT_RSK_DTs'",
        ]
    )


def _reference_with_identifying_extension_field_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    test_id = models.CharField('test_id', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ROOT_ID = models.CharField('ROOT_ID', max_length=255, default=None, blank=True, null=True)",
            "    RISK_SCORE = models.BigIntegerField('RISK_SCORE', default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
        ]
    )


def _reference_source() -> str:
    return "\n".join(
        [
            "from django.db import models",
            "",
            "class ROOT(models.Model):",
            "    test_id = models.CharField('test_id', max_length=255, default=None, blank=True, null=True)",
            "    ROOT_uniqueID = models.CharField('ROOT_uniqueID', max_length=255, primary_key=True)",
            "    ACCNTNG_STNDRD_domain = {'0': 'Not_applicable', '1': 'IFRS'}",
            "    ACCNTNG_STNDRD = models.CharField('ACCNTNG_STNDRD', max_length=255, choices=ACCNTNG_STNDRD_domain)",
            "    DT_RFRNC = models.DateTimeField('DT_RFRNC', default=None, blank=True, null=True)",
            "    BYR_PRTY_ID = models.CharField('BYR_PRTY_ID', max_length=255, default=None, blank=True, null=True)",
            "    BYR_ENTTY_RL_TYP = models.CharField('BYR_ENTTY_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    CHILD_VALUE = models.BigIntegerField('CHILD_VALUE', default=None, blank=True, null=True)",
            "    OWNER_VALUE = models.CharField('OWNER_VALUE', max_length=255, default=None, blank=True, null=True)",
            "    theOTHER = models.ForeignKey('OTHER', models.SET_NULL, blank=True, null=True)",
            "    REF_ONLY = models.CharField('REF_ONLY', max_length=255, default=None, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ROOT'",
            "        verbose_name_plural = 'ROOTs'",
            "",
            "class OTHER(models.Model):",
            "    OTHER_uniqueID = models.CharField('OTHER_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'OTHER'",
            "        verbose_name_plural = 'OTHERs'",
            "",
            "class REL(models.Model):",
            "    REL_uniqueID = models.CharField('REL_uniqueID', max_length=255, primary_key=True)",
            "    theROOT = models.ForeignKey('ROOT', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'REL'",
            "        verbose_name_plural = 'RELs'",
        ]
    )
