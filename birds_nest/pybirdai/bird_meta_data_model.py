# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
from django.db import models


class SUBDOMAIN(models.Model):
    subdomain_id = models.CharField("subdomain_id", max_length=255, primary_key=True)
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )
    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "SUBDOMAIN"
        verbose_name_plural = "SUBDOMAINs"


class SUBDOMAIN_ENUMERATION(models.Model):
    subdomain_id = models.ForeignKey(
        "SUBDOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "SUBDOMAIN_ENUMERATION"
        verbose_name_plural = "SUBDOMAIN_ENUMERATIONs"


class DOMAIN(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    FACET_VALUE_TYPE = {
        "BigInteger": "BigInteger",
        "Boolean": "Boolean",
        "DateTime": "DateTime",
        "Day_MonthDay_Month": "DayMonthDayMonth",
        "Decimal": "Decimal",
        "Double": "Double",
        "Duration": "Duration",
        "Float": "Float",
        "GregorianDay": "GregorianDay",
        "GregorianMonth": "GregorianMonth",
        "GregorianYear": "GregorianYear",
        "Integer": "Integer",
        "Long": "Long",
        "Short": "Short",
        "String": "String",
        "Time": "Time",
        "URI": "URI",
    }
    data_type = models.CharField(
        "data_type",
        max_length=255,
        choices=FACET_VALUE_TYPE,
        default=None,
        blank=True,
        null=True,
    )

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    domain_id = models.CharField("domain_id", max_length=255, primary_key=True)

    facet_id = models.ForeignKey(
        "FACET_COLLECTION",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    is_enumerated = models.BooleanField(
        "is_enumerated", default=None, blank=True, null=True
    )

    is_reference = models.BooleanField(
        "is_reference", default=None, blank=True, null=True
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "DOMAIN"
        verbose_name_plural = "DOMAINs"


class FACET_COLLECTION(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    facet_id = models.CharField("facet_id", max_length=255, primary_key=True)

    FACET_VALUE_TYPE = {
        "BigInteger": "BigInteger",
        "Boolean": "Boolean",
        "DateTime": "DateTime",
        "Day_MonthDay_Month": "DayMonthDayMonth",
        "Decimal": "Decimal",
        "Double": "Double",
        "Duration": "Duration",
        "Float": "Float",
        "GregorianDay": "GregorianDay",
        "GregorianMonth": "GregorianMonth",
        "GregorianYear": "GregorianYear",
        "Integer": "Integer",
        "Long": "Long",
        "Short": "Short",
        "String": "String",
        "Time": "Time",
        "URI": "URI",
    }
    facet_value_type = models.CharField(
        "facet_value_type",
        max_length=255,
        choices=FACET_VALUE_TYPE,
        default=None,
        blank=True,
        null=True,
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "FACET_COLLECTION"
        verbose_name_plural = "FACET_COLLECTIONs"


class MAINTENANCE_AGENCY(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    maintenance_agency_id = models.CharField(
        "maintenance_agency_id", max_length=255, primary_key=True
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MAINTENANCE_AGENCY"
        verbose_name_plural = "MAINTENANCE_AGENCYs"


class MEMBER(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_id = models.CharField("member_id", max_length=255, primary_key=True)

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MEMBER"
        verbose_name_plural = "MEMBERs"


class MEMBER_HIERARCHY(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_hierarchy_id = models.CharField(
        "member_hierarchy_id", max_length=255, primary_key=True
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MEMBER_HIERARCHY"
        verbose_name_plural = "MEMBER_HIERARCHYs"


class MEMBER_HIERARCHY_NODE(models.Model):
    comparator = models.CharField(
        "comparator", max_length=255, default=None, blank=True, null=True
    )

    level = models.BigIntegerField("level", default=None, blank=True, null=True)

    member_hierarchy_id = models.ForeignKey(
        "MEMBER_HIERARCHY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="member_id_in_hierarchy",
    )

    operator = models.CharField(
        "operator", max_length=255, default=None, blank=True, null=True
    )

    parent_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="parent_member_id",
    )

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MEMBER_HIERARCHY_NODE"
        verbose_name_plural = "MEMBER_HIERARCHY_NODEs"


class VARIABLE(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    primary_concept = models.CharField(
        "primary_concept", max_length=255, default=None, blank=True, null=True
    )

    variable_id = models.CharField("variable_id", max_length=255, primary_key=True)

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "VARIABLE"
        verbose_name_plural = "VARIABLEs"


class VARIABLE_SET(models.Model):
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name = "variable_set_maintenance_agency"
    )

    variable_set_id = models.CharField(
        "variable_set_id", max_length=255, primary_key=True
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "VARIABLE_SET"
        verbose_name_plural = "VARIABLE_SETs"


class VARIABLE_SET_ENUMERATION(models.Model):
    variable_set_id = models.ForeignKey(
        "VARIABLE_SET",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    variable_id = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    subdomain_id = models.ForeignKey(
        "SUBDOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    is_flow = models.BooleanField("is_flow", default=None, blank=True, null=True)

    order = models.BigIntegerField("order", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "VARIABLE_SET_ENUMERATION"
        verbose_name_plural = "VARIABLE_SET_ENUMERATIONs"


class FRAMEWORK(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    framework_id = models.CharField("framework_id", max_length=255, primary_key=True)

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "FRAMEWORK"
        verbose_name_plural = "FRAMEWORKs"


class MEMBER_MAPPING(models.Model):
    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_mapping_id = models.CharField(
        "member_mapping_id", max_length=255, primary_key=True
    )

    class Meta:
        verbose_name = "MEMBER_MAPPING"
        verbose_name_plural = "MEMBER_MAPPINGs"


class MEMBER_MAPPING_ITEM(models.Model):
    is_source = models.CharField(
        "is_source", max_length=255, default=None, blank=True, null=True
    )

    member = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    variable = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    row = models.CharField("row", max_length=255, default=None, blank=True, null=True)

    member_mapping_id = models.ForeignKey(
        "MEMBER_MAPPING",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_hierarchy = models.ForeignKey(
        "MEMBER_HIERARCHY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MEMBER_MAPPING_ITEM"
        verbose_name_plural = "MEMBER_MAPPING_ITEMs"


class VARIABLE_MAPPING_ITEM(models.Model):
    variable_mapping_id = models.ForeignKey(
        "VARIABLE_MAPPING",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    is_source = models.CharField(
        "is_source", max_length=255, default=None, blank=True, null=True
    )

    variable = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "VARIABLE_MAPPING_ITEM"
        verbose_name_plural = "VARIABLE_MAPPING_ITEMs"


class VARIABLE_MAPPING(models.Model):
    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    variable_mapping_id = models.CharField(
        "variable_mapping_id", max_length=255, primary_key=True
    )

    class Meta:
        verbose_name = "VARIABLE_MAPPING"
        verbose_name_plural = "VARIABLE_MAPPINGs"


class MAPPING_TO_CUBE(models.Model):
    mapping = models.ForeignKey(
        "MAPPING_DEFINITION",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    cubeMapping = models.CharField(
        "cubeMapping", max_length=255, default=None, blank=True, null=True
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MAPPING_TO_CUBE"
        verbose_name_plural = "MAPPING_TO_CUBEs"


class MAPPING_DEFINITION(models.Model):
    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    mapping_id = models.CharField("mapping_id", max_length=255, primary_key=True)

    mapping_type = models.CharField(
        "mapping_type", max_length=255, default=None, blank=True, null=True
    )

    member_mapping_id = models.ForeignKey(
        "MEMBER_MAPPING",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    variable_mapping_id = models.ForeignKey(
        "VARIABLE_MAPPING",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "MAPPING_DEFINITION"
        verbose_name_plural = "MAPPING_DEFINITIONs"


class AXIS(models.Model):
    axis_id = models.CharField("axis_id", max_length=255, primary_key=True)

    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    is_open_axis = models.BooleanField(
        "is_open_axis", default=None, blank=True, null=True
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    order = models.BigIntegerField("order", default=None, blank=True, null=True)

    orientation = models.CharField(
        "orientation", max_length=255, default=None, blank=True, null=True
    )

    table_id = models.ForeignKey(
        "TABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "AXIS"
        verbose_name_plural = "AXISs"


class AXIS_ORDINATE(models.Model):
    axis_id = models.ForeignKey(
        "AXIS",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    axis_ordinate_id = models.CharField(
        "axis_ordinate_id", max_length=255, primary_key=True
    )

    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    is_abstract_header = models.BooleanField(
        "is_open_axis", default=None, blank=True, null=True
    )

    level = models.BigIntegerField("level", default=None, blank=True, null=True)

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    order = models.BigIntegerField("order", default=None, blank=True, null=True)

    parent_axis_ordinate_id = models.ForeignKey(
        "AXIS_ORDINATE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    path = models.CharField("path", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "AXIS_ORDINATE"
        verbose_name_plural = "AXIS_ORDINATEs"


class CELL_POSITION(models.Model):
    axis_ordinate_id = models.ForeignKey(
        "AXIS_ORDINATE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    cell_id = models.ForeignKey(
        "TABLE_CELL",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "CELL_POSITION"
        verbose_name_plural = "CELL_POSITIONs"


class ORDINATE_ITEM(models.Model):
    axis_ordinate_id = models.ForeignKey(
        "AXIS_ORDINATE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    is_starting_member_included = models.CharField(
        "is_starting_member_included",
        max_length=255,
        default=None,
        blank=True,
        null=True,
    )

    member_hierarchy_id = models.ForeignKey(
        "MEMBER_HIERARCHY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    starting_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="starting_member_id",
    )

    variable_id = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "ORDINATE_ITEM"
        verbose_name_plural = "ORDINATE_ITEMs"


class TABLE(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    table_id = models.CharField("table_id", max_length=255, primary_key=True)

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    version = models.CharField(
        "version", max_length=255, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "TABLE"
        verbose_name_plural = "TABLEs"


class TABLE_CELL(models.Model):
    cell_id = models.CharField("cell_id", max_length=255, primary_key=True)

    is_shaded = models.BooleanField("is_open_axis", default=None, blank=True, null=True)

    table_id = models.ForeignKey(
        "TABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    table_cell_combination_id = models.CharField(
        "table_cell_combination_id", max_length=255, default=None, blank=True, null=True
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "TABLE_CELL"
        verbose_name_plural = "TABLE_CELLs"


class CUBE_STRUCTURE(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    cube_structure_id = models.CharField(
        "cube_structure_id", max_length=255, primary_key=True
    )

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    version = models.CharField(
        "version", max_length=255, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "CUBE_STRUCTURE"
        verbose_name_plural = "CUBE_STRUCTUREs"


class CUBE_STRUCTURE_ITEM(models.Model):
    attribute_associated_variable = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="attribute_associated_variable",
    )

    cube_structure_id = models.ForeignKey(
        "CUBE_STRUCTURE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    TYP_DMNSN = {
        "B": "B",
        "M": "M",
        "T": "T",
        "U": "U",
    }
    dimension_type = models.CharField(
        "dimension_type",
        max_length=255,
        choices=TYP_DMNSN,
        default=None,
        blank=True,
        null=True,
    )

    is_flow = models.BooleanField("is_flow", default=None, blank=True, null=True)

    is_mandatory = models.BooleanField(
        "is_mandatory", default=None, blank=True, null=True
    )

    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    order = models.BigIntegerField("order", default=None, blank=True, null=True)

    TYP_RL = {
        "O": "O",
        "A": "A",
        "D": "D",
    }
    role = models.CharField(
        "role", max_length=255, choices=TYP_RL, default=None, blank=True, null=True
    )

    variable_id = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    is_identifier = models.BooleanField(
        "is_identifier", default=None, blank=True, null=True
    )

    cube_variable_code = models.CharField(
        "cube_variable_code", max_length=255, default=None, blank=True, null=True
    )

    subdomain_id = models.ForeignKey(
        "SUBDOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "CUBE_STRUCTURE_ITEM"
        verbose_name_plural = "CUBE_STRUCTURE_ITEMs"


class CUBE(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    cube_id = models.CharField("cube_id", max_length=255, primary_key=True)

    cube_structure_id = models.ForeignKey(
        "CUBE_STRUCTURE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    cube_type = models.CharField(
        "cube_type", max_length=255, default=None, blank=True, null=True
    )

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    framework_id = models.ForeignKey(
        "FRAMEWORK",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    is_allowed = models.BooleanField("is_allowed", default=None, blank=True, null=True)

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    published = models.BooleanField("published", default=None, blank=True, null=True)

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    version = models.CharField(
        "version", max_length=255, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "CUBE"
        verbose_name_plural = "CUBEs"


class CUBE_LINK(models.Model):
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    cube_link_id = models.CharField("cube_link_id", max_length=255, primary_key=True)

    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    description = models.CharField(
        "description", max_length=255, default=None, blank=True, null=True
    )

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    version = models.CharField(
        "version", max_length=255, default=None, blank=True, null=True
    )

    order_relevance = models.BigIntegerField(
        "order_relevance", default=None, blank=True, null=True
    )

    primary_cube_id = models.ForeignKey(
        "CUBE",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="primary_cube_in_cube_link",
    )

    foreign_cube_id = models.ForeignKey(
        "CUBE",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="foreign_cube_in_cube_link",
    )

    cube_link_type = models.CharField(
        "cube_link_type", max_length=255, default=None, blank=True, null=True
    )

    join_identifier = models.CharField(
        "join_identifier", max_length=255, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "CUBE_LINK"
        verbose_name_plural = "CUBE_LINKs"


class CUBE_STRUCTURE_ITEM_LINK(models.Model):
    cube_structure_item_link_id = models.CharField(
        "cube_structure_item_link_id", max_length=255, primary_key=True
    )

    cube_link_id = models.ForeignKey(
        "CUBE_LINK",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    foreign_cube_variable_code = models.ForeignKey(
        "CUBE_STRUCTURE_ITEM",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="foreign_cube_variable_code",
    )

    primary_cube_variable_code = models.ForeignKey(
        "CUBE_STRUCTURE_ITEM",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="primary_cube_variable_code",
    )

    class Meta:
        verbose_name = "CUBE_STRUCTURE_ITEM_LINK"
        verbose_name_plural = "CUBE_STRUCTURE_ITEM_LINKs"


class COMBINATION(models.Model):
    code = models.CharField("code", max_length=255, default=None, blank=True, null=True)

    combination_id = models.CharField(
        "combination_id", max_length=255, primary_key=True
    )

    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    name = models.CharField("name", max_length=255, default=None, blank=True, null=True)

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    version = models.CharField(
        "version", max_length=255, default=None, blank=True, null=True
    )

    metric = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "COMBINATION"
        verbose_name_plural = "COMBINATIONs"


class COMBINATION_ITEM(models.Model):
    combination_id = models.ForeignKey(
        "COMBINATION",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    variable_id = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    member_hierarchy = models.ForeignKey(
        "MEMBER_HIERARCHY",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "COMBINATION_ITEM"
        verbose_name_plural = "COMBINATION_ITEMs"


class CUBE_TO_COMBINATION(models.Model):
    combination_id = models.ForeignKey(
        "COMBINATION",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    cube_id = models.ForeignKey(
        "CUBE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "CUBE_TO_COMBINATION"
        verbose_name_plural = "CUBE_TO_COMBINATIONs"


class MEMBER_LINK(models.Model):
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    is_linked = models.BooleanField("is_linked", default=None, blank=True, null=True)

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    foreign_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="foreign_member",
    )

    primary_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="primary_member",
    )

    cube_structure_item_link_id = models.ForeignKey(
        "CUBE_STRUCTURE_ITEM_LINK",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "MEMBER_LINK"
        verbose_name_plural = "MEMBER_LINKs"
