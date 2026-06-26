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
