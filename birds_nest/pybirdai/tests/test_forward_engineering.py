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

from pybirdai.process_steps.forward_engineering.django_model_ast import parse_django_model
from pybirdai.process_steps.forward_engineering.forward_engineer import (
    _looks_like_helper_or_domain_class,
    generate_forward_engineered_source,
)


def test_parse_generated_django_model_without_importing_django(tmp_path):
    model_path = tmp_path / "model.py"
    model_path.write_text(
        "\n".join(
            [
                "from django.db import models",
                "",
                "class ROOT(models.Model):",
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
    assert "BYR_ID" not in deal_fields
    assert "BYR_RL_TYP" not in deal_fields
    assert "ASST_PL_ID" in deal_fields
    assert "theASST_PL" in deal_fields


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
            "class ASST_PL(models.Model):",
            "    ASST_PL_uniqueID = models.CharField('ASST_PL_uniqueID', max_length=255, primary_key=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'ASST_PL'",
            "        verbose_name_plural = 'ASST_PLs'",
            "",
            "class DEAL(models.Model):",
            "    DEAL_uniqueID = models.CharField('DEAL_uniqueID', max_length=255, primary_key=True)",
            "    BYR_ID = models.CharField('BYR_ID', max_length=255, default=None, blank=True, null=True)",
            "    BYR_RL_TYP = models.CharField('BYR_RL_TYP', max_length=255, default=None, blank=True, null=True)",
            "    ASST_PL_ID = models.CharField('ASST_PL_ID', max_length=255, default=None, blank=True, null=True)",
            "    Deal_has_asset_pool = models.ForeignKey('ASST_PL', models.SET_NULL, blank=True, null=True)",
            "",
            "    class Meta:",
            "        verbose_name = 'DEAL'",
            "        verbose_name_plural = 'DEALs'",
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
