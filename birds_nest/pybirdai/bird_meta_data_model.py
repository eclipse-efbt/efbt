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
from django.db import models, OperationalError
from django.utils import timezone

# All CSV headers are listed as comments above their respective classes

class SUBDOMAIN(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,SUBDOMAIN_ID,NAME,DOMAIN_ID,IS_LISTED,CODE,FACET_ID,DESCRIPTION,IS_NATURAL
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    subdomain_id = models.CharField("subdomain_id", max_length=1000, primary_key=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    is_listed = models.BooleanField("is_listed", default=None, blank=True, null=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    facet_id = models.ForeignKey(
        "FACET_COLLECTION",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    is_natural = models.BooleanField("is_natural", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "SUBDOMAIN"
        verbose_name_plural = "SUBDOMAINs"


class SUBDOMAIN_ENUMERATION(models.Model):
    # CSV Headers: MEMBER_ID,SUBDOMAIN_ID,VALID_FROM,VALID_TO,ORDER
    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    subdomain_id = models.ForeignKey(
        "SUBDOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)
    order = models.BigIntegerField("order", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "SUBDOMAIN_ENUMERATION"
        verbose_name_plural = "SUBDOMAIN_ENUMERATIONs"


class DOMAIN(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,DOMAIN_ID,NAME,IS_ENUMERATED,DESCRIPTION,DATA_TYPE,CODE,FACET_ID,IS_REFERENCE
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    domain_id = models.CharField("domain_id", max_length=1000, primary_key=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    is_enumerated = models.BooleanField(
        "is_enumerated", default=None, blank=True, null=True
    )
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
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
        max_length=1000,
        choices=FACET_VALUE_TYPE,
        default=None,
        blank=True,
        null=True,
    )
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    facet_id = models.ForeignKey(
        "FACET_COLLECTION",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    is_reference = models.BooleanField(
        "is_reference", default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "DOMAIN"
        verbose_name_plural = "DOMAINs"

class FACET_COLLECTION(models.Model):
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)

    facet_id = models.CharField("facet_id", max_length=1000, primary_key=True)

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
        max_length=1000,
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

    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "FACET_COLLECTION"
        verbose_name_plural = "FACET_COLLECTIONs"


class MAINTENANCE_AGENCY(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,CODE,NAME,DESCRIPTION
    maintenance_agency_id = models.CharField(
        "maintenance_agency_id", max_length=1000, primary_key=True
    )
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "MAINTENANCE_AGENCY"
        verbose_name_plural = "MAINTENANCE_AGENCYs"


class MEMBER(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,MEMBER_ID,CODE,NAME,DOMAIN_ID,DESCRIPTION
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    member_id = models.CharField("member_id", max_length=1000, primary_key=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    description = models.CharField(
        "description", max_length=3000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "MEMBER"
        verbose_name_plural = "MEMBERs"


class MEMBER_HIERARCHY(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,MEMBER_HIERARCHY_ID,CODE,DOMAIN_ID,NAME,DESCRIPTION,IS_MAIN_HIERARCHY
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    member_hierarchy_id = models.CharField(
        "member_hierarchy_id", max_length=1000, primary_key=True
    )
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    is_main_hierarchy = models.BooleanField(
        "is_main_hierarchy", default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "MEMBER_HIERARCHY"
        verbose_name_plural = "MEMBER_HIERARCHYs"


class MEMBER_HIERARCHY_NODE(models.Model):
    # CSV Headers: MEMBER_HIERARCHY_ID,MEMBER_ID,LEVEL,PARENT_MEMBER_ID,COMPARATOR,OPERATOR,VALID_FROM,VALID_TO
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
    level = models.BigIntegerField("level", default=None, blank=True, null=True)
    parent_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="parent_member_id",
    )
    comparator = models.CharField(
        "comparator", max_length=1000, default=None, blank=True, null=True
    )
    operator = models.CharField(
        "operator", max_length=1000, default=None, blank=True, null=True
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MEMBER_HIERARCHY_NODE"
        verbose_name_plural = "MEMBER_HIERARCHY_NODEs"


class VARIABLE(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,VARIABLE_ID,CODE,NAME,DOMAIN_ID,DESCRIPTION,PRIMARY_CONCEPT,IS_DECOMPOSED
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    variable_id = models.CharField("variable_id", max_length=1000, primary_key=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    domain_id = models.ForeignKey(
        "DOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    primary_concept = models.CharField(
        "primary_concept", max_length=1000, default=None, blank=True, null=True
    )
    is_decomposed = models.BooleanField(
        "is_decomposed", default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "VARIABLE"
        verbose_name_plural = "VARIABLEs"


class VARIABLE_SET(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,VARIABLE_SET_ID,NAME,CODE,DESCRIPTION
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name = "variable_set_maintenance_agency"
    )
    variable_set_id = models.CharField(
        "variable_set_id", max_length=1000, primary_key=True
    )
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "VARIABLE_SET"
        verbose_name_plural = "VARIABLE_SETs"


class VARIABLE_SET_ENUMERATION(models.Model):
    # CSV Headers: VARIABLE_SET_ID,VARIABLE_ID,VALID_FROM,VALID_TO,SUBDOMAIN_ID,IS_FLOW,ORDER
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
    # CSV Headers: MAINTENANCE_AGENCY_ID,FRAMEWORK_ID,NAME,CODE,DESCRIPTION,FRAMEWORK_TYPE,REPORTING_POPULATION,OTHER_LINKS,ORDER,FRAMEWORK_STATUS
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    framework_id = models.CharField("framework_id", max_length=1000, primary_key=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    framework_type = models.CharField(
        "framework_type", max_length=1000, default=None, blank=True, null=True
    )
    reporting_population = models.CharField(
        "reporting_population", max_length=1000, default=None, blank=True, null=True
    )
    other_links = models.CharField(
        "other_links", max_length=1000, default=None, blank=True, null=True
    )
    order = models.BigIntegerField("order", default=None, blank=True, null=True)
    framework_status = models.CharField(
        "framework_status", max_length=1000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "FRAMEWORK"
        verbose_name_plural = "FRAMEWORKs"


class MEMBER_MAPPING(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,MEMBER_MAPPING_ID,NAME,CODE
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    member_mapping_id = models.CharField(
        "member_mapping_id", max_length=1000, primary_key=True
    )
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MEMBER_MAPPING"
        verbose_name_plural = "MEMBER_MAPPINGs"


class MEMBER_MAPPING_ITEM(models.Model):
    # CSV Headers: MEMBER_MAPPING_ID,MEMBER_MAPPING_ROW,VARIABLE_ID,IS_SOURCE,MEMBER_ID,VALID_FROM,VALID_TO
    member_mapping_id = models.ForeignKey(
        "MEMBER_MAPPING",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    member_mapping_row = models.CharField("row", max_length=1000, default=None, blank=True, null=True)
    variable_id = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    is_source = models.CharField(
        "is_source", max_length=1000, default=None, blank=True, null=True
    )

    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )


    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    member_hierarchy = models.ForeignKey(
        "MEMBER_HIERARCHY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    class Meta:
        verbose_name = "MEMBER_MAPPING_ITEM"
        verbose_name_plural = "MEMBER_MAPPING_ITEMs"


class VARIABLE_MAPPING_ITEM(models.Model):
    # CSV Headers: VARIABLE_MAPPING_ID,VARIABLE_ID,IS_SOURCE,VALID_FROM,VALID_TO
    variable_mapping_id = models.ForeignKey(
        "VARIABLE_MAPPING",
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
    is_source = models.CharField(
        "is_source", max_length=1000, default=None, blank=True, null=True
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "VARIABLE_MAPPING_ITEM"
        verbose_name_plural = "VARIABLE_MAPPING_ITEMs"


class VARIABLE_MAPPING(models.Model):
    # CSV Headers: VARIABLE_MAPPING_ID,MAINTENANCE_AGENCY_ID,CODE,NAME
    variable_mapping_id = models.CharField(
        "variable_mapping_id", max_length=1000, primary_key=True
    )
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "VARIABLE_MAPPING"
        verbose_name_plural = "VARIABLE_MAPPINGs"


class MAPPING_TO_CUBE(models.Model):
    # CSV Headers: CUBE_MAPPING_ID,MAPPING_ID,VALID_FROM,VALID_TO

    cube_mapping_id = models.CharField(
        "cube_mapping_id", max_length=1000, default=None, blank=True, null=True
    )

    mapping_id = models.ForeignKey(
        "MAPPING_DEFINITION",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)

    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MAPPING_TO_CUBE"
        verbose_name_plural = "MAPPING_TO_CUBEs"


class MAPPING_DEFINITION(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,MAPPING_ID,NAME,MAPPING_TYPE,CODE,ALGORITHM,MEMBER_MAPPING_ID,VARIABLE_MAPPING_ID
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    mapping_id = models.CharField("mapping_id", max_length=1000, primary_key=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    mapping_type = models.CharField(
        "mapping_type", max_length=1000, default=None, blank=True, null=True
    )
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    algorithm = models.CharField(
        "algorithm", max_length=1000, default=None, blank=True, null=True
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
    # CSV Headers: AXIS_ID,CODE,ORIENTATION,ORDER,NAME,DESCRIPTION,TABLE_ID,IS_OPEN_AXIS
    axis_id = models.CharField("axis_id", max_length=1000, primary_key=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    orientation = models.CharField(
        "orientation", max_length=1000, default=None, blank=True, null=True
    )
    order = models.BigIntegerField("order", default=None, blank=True, null=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    table_id = models.ForeignKey(
        "TABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    is_open_axis = models.BooleanField(
        "is_open_axis", default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "AXIS"
        verbose_name_plural = "AXISs"


class AXIS_ORDINATE(models.Model):
    # CSV Headers: AXIS_ORDINATE_ID,IS_ABSTRACT_HEADER,CODE,ORDER,LEVEL,PATH,AXIS_ID,PARENT_AXIS_ORDINATE_ID,NAME,DESCRIPTION
    axis_ordinate_id = models.CharField(
        "axis_ordinate_id", max_length=1000, primary_key=True
    )
    is_abstract_header = models.BooleanField(
        "is_abstract_header", default=None, blank=True, null=True
    )
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    order = models.BigIntegerField("order", default=None, blank=True, null=True)
    level = models.BigIntegerField("level", default=None, blank=True, null=True)
    path = models.CharField("path", max_length=1000, default=None, blank=True, null=True)
    axis_id = models.ForeignKey(
        "AXIS",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    parent_axis_ordinate_id = models.ForeignKey(
        "AXIS_ORDINATE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "AXIS_ORDINATE"
        verbose_name_plural = "AXIS_ORDINATEs"


class CELL_POSITION(models.Model):
    # CSV Headers: CELL_ID,AXIS_ORDINATE_ID
    cell_id = models.ForeignKey(
        "TABLE_CELL",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    axis_ordinate_id = models.ForeignKey(
        "AXIS_ORDINATE",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "CELL_POSITION"
        verbose_name_plural = "CELL_POSITIONs"


class ORDINATE_ITEM(models.Model):
    # CSV Headers: AXIS_ORDINATE_ID,VARIABLE_ID,MEMBER_ID,MEMBER_HIERARCHY_ID,MEMBER_HIERARCHY_VALID_FROM,STARTING_MEMBER_ID,IS_STARTING_MEMBER_INCLUDED
    axis_ordinate_id = models.ForeignKey(
        "AXIS_ORDINATE",
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
    member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    member_hierarchy_id = models.ForeignKey(
        "MEMBER_HIERARCHY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    member_hierarchy_valid_from = models.DateTimeField(
        "member_hierarchy_valid_from", default=None, blank=True, null=True
    )
    starting_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="starting_member_id",
    )
    is_starting_member_included = models.CharField(
        "is_starting_member_included",
        max_length=1000,
        default=None,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "ORDINATE_ITEM"
        verbose_name_plural = "ORDINATE_ITEMs"


class TABLE(models.Model):
    # CSV Headers: TABLE_ID,NAME,CODE,DESCRIPTION,MAINTENANCE_AGENCY_ID,VERSION,VALID_FROM,VALID_TO
    table_id = models.CharField("table_id", max_length=1000, primary_key=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    version = models.CharField(
        "version", max_length=1000, default=None, blank=True, null=True
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "TABLE"
        verbose_name_plural = "TABLEs"


class TABLE_CELL(models.Model):
    # CSV Headers: CELL_ID,IS_SHADED,COMBINATION_ID,TABLE_ID,SYSTEM_DATA_CODE
    cell_id = models.CharField("cell_id", max_length=1000, primary_key=True)
    is_shaded = models.BooleanField("is_shaded", default=None, blank=True, null=True)
    #rename to combination_id
    table_cell_combination_id = models.CharField(
        "table_cell_combination_id", max_length=1000, default=None, blank=True, null=True
    )

    table_id = models.ForeignKey(
        "TABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    system_data_code = models.CharField(
        "system_data_code", max_length=1000, default=None, blank=True, null=True
    )


    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)

    class Meta:
        verbose_name = "TABLE_CELL"
        verbose_name_plural = "TABLE_CELLs"


class CUBE_STRUCTURE(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,CUBE_STRUCTURE_ID,NAME,CODE,DESCRIPTION,VALID_FROM,VALID_TO,VERSION
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    cube_structure_id = models.CharField(
        "cube_structure_id", max_length=1000, primary_key=True
    )
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)
    version = models.CharField(
        "version", max_length=1000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "CUBE_STRUCTURE"
        verbose_name_plural = "CUBE_STRUCTUREs"


class CUBE_STRUCTURE_ITEM(models.Model):
    # CSV Headers: CUBE_STRUCTURE_ID,CUBE_VARIABLE_CODE,VARIABLE_ID,ROLE,ORDER,SUBDOMAIN_ID,VARIABLE_SET_ID,MEMBER_ID,DIMENSION_TYPE,ATTRIBUTE_ASSOCIATED_VARIABLE,IS_FLOW,IS_MANDATORY,DESCRIPTION,IS_IMPLEMENTED
    cube_structure_id = models.ForeignKey(
        "CUBE_STRUCTURE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    cube_variable_code = models.CharField(
        "cube_variable_code", max_length=1000, default=None, blank=True, null=True
    )
    variable_id = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    TYP_RL = {
        "O": "O",
        "A": "A",
        "D": "D",
    }
    role = models.CharField(
        "role", max_length=1000, choices=TYP_RL, default=None, blank=True, null=True
    )
    order = models.BigIntegerField("order", default=None, blank=True, null=True)
    subdomain_id = models.ForeignKey(
        "SUBDOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    variable_set_id = models.ForeignKey(
        "VARIABLE_SET",
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
    TYP_DMNSN = {
        "B": "B",
        "M": "M",
        "T": "T",
        "U": "U",
    }
    dimension_type = models.CharField(
        "dimension_type",
        max_length=1000,
        choices=TYP_DMNSN,
        default=None,
        blank=True,
        null=True,
    )
    attribute_associated_variable = models.ForeignKey(
        "VARIABLE",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="attribute_associated_variable",
    )
    is_flow = models.BooleanField("is_flow", default=None, blank=True, null=True)
    is_mandatory = models.BooleanField(
        "is_mandatory", default=None, blank=True, null=True
    )
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    is_implemented = models.BooleanField(
        "is_implemented", default=None, blank=True, null=True
    )
    is_identifier = models.BooleanField(
        "is_identifier", default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "CUBE_STRUCTURE_ITEM"
        verbose_name_plural = "CUBE_STRUCTURE_ITEMs"


class CUBE(models.Model):
    # CSV Headers: MAINTENANCE_AGENCY_ID,CUBE_ID,NAME,CODE,FRAMEWORK_ID,CUBE_STRUCTURE_ID,CUBE_TYPE,IS_ALLOWED,VALID_FROM,VALID_TO,VERSION,DESCRIPTION,PUBLISHED,DATASET_URL,FILTERS,DI_EXPORT
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    cube_id = models.CharField("cube_id", max_length=1000, primary_key=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    framework_id = models.ForeignKey(
        "FRAMEWORK",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    cube_structure_id = models.ForeignKey(
        "CUBE_STRUCTURE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    cube_type = models.CharField(
        "cube_type", max_length=1000, default=None, blank=True, null=True
    )
    is_allowed = models.BooleanField("is_allowed", default=None, blank=True, null=True)
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)
    version = models.CharField(
        "version", max_length=1000, default=None, blank=True, null=True
    )
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    published = models.BooleanField("published", default=None, blank=True, null=True)
    dataset_url = models.CharField(
        "dataset_url", max_length=1000, default=None, blank=True, null=True
    )
    filters = models.CharField(
        "filters", max_length=1000, default=None, blank=True, null=True
    )
    di_export = models.CharField(
        "di_export", max_length=1000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "CUBE"
        verbose_name_plural = "CUBEs"


class CUBE_LINK(models.Model):
    # need to find the correct csv headers
    # CSV Headers: MAINTENANCE_AGENCY_ID,CUBE_LINK_ID,CODE,NAME,DESCRIPTION,VALID_FROM,VALID_TO,VERSION,ORDER_RELEVANCE,PRIMARY_CUBE_ID,FOREIGN_CUBE_ID,CUBE_LINK_TYPE,JOIN_IDENTIFIER
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    cube_link_id = models.CharField("cube_link_id", max_length=1000, primary_key=True)
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    description = models.CharField(
        "description", max_length=1000, default=None, blank=True, null=True
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)
    version = models.CharField(
        "version", max_length=1000, default=None, blank=True, null=True
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
        "cube_link_type", max_length=1000, default=None, blank=True, null=True
    )
    join_identifier = models.CharField(
        "join_identifier", max_length=1000, default=None, blank=True, null=True
    )

    class Meta:
        verbose_name = "CUBE_LINK"
        verbose_name_plural = "CUBE_LINKs"


class CUBE_STRUCTURE_ITEM_LINK(models.Model):
    cube_structure_item_link_id = models.CharField(
        "cube_structure_item_link_id", max_length=1000, primary_key=True
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
    # CSV Headers: COMBINATION_ID,CODE,NAME,MAINTENANCE_AGENCY_ID,VERSION,VALID_FROM,VALID_TO
    combination_id = models.CharField(
        "combination_id", max_length=1000, primary_key=True
    )
    code = models.CharField("code", max_length=1000, default=None, blank=True, null=True)
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    maintenance_agency_id = models.ForeignKey(
        "MAINTENANCE_AGENCY",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    version = models.CharField(
        "version", max_length=1000, default=None, blank=True, null=True
    )
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

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
    # CSV Headers: COMBINATION_ID,VARIABLE_ID,SUBDOMAIN_ID,VARIABLE_SET_ID,MEMBER_ID
    combination_id = models.ForeignKey(
        "COMBINATION",
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
    subdomain_id = models.ForeignKey(
        "SUBDOMAIN",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    variable_set_id = models.ForeignKey(
        "VARIABLE_SET",
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
    # CSV Headers: CUBE_ID,COMBINATION_ID
    cube_id = models.ForeignKey(
        "CUBE",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    combination_id = models.ForeignKey(
        "COMBINATION",
        models.SET_NULL,
        blank=True,
        null=True,
    )

    class Meta:
        verbose_name = "CUBE_TO_COMBINATION"
        verbose_name_plural = "CUBE_TO_COMBINATIONs"


class MEMBER_LINK(models.Model):
    # CSV Headers: CUBE_STRUCTURE_ITEM_LINK_ID,PRIMARY_MEMBER_ID,FOREIGN_MEMBER_ID,IS_LINKED,VALID_FROM,VALID_TO
    cube_structure_item_link_id = models.ForeignKey(
        "CUBE_STRUCTURE_ITEM_LINK",
        models.SET_NULL,
        blank=True,
        null=True,
    )
    primary_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="primary_member",
    )
    foreign_member_id = models.ForeignKey(
        "MEMBER",
        models.SET_NULL,
        blank=True,
        null=True,
        related_name="foreign_member",
    )
    is_linked = models.BooleanField("is_linked", default=None, blank=True, null=True)
    valid_from = models.DateTimeField("valid_from", default=None, blank=True, null=True)
    valid_to = models.DateTimeField("valid_to", default=None, blank=True, null=True)

    class Meta:
        verbose_name = "MEMBER_LINK"
        verbose_name_plural = "MEMBER_LINKs"


class AutomodeConfiguration(models.Model):
    DATA_MODEL_CHOICES = [
        ('ELDM', 'ELDM (Logical Data Model)'),
        ('EIL', 'EIL (Input Layer)'),
    ]

    TECHNICAL_EXPORT_SOURCE_CHOICES = [
        ('BIRD_WEBSITE', 'BIRD Website'),
        ('GITHUB', 'GitHub Repository'),
        ('MANUAL_UPLOAD', 'Manual Upload'),
    ]

    CONFIG_FILES_SOURCE_CHOICES = [
        ('MANUAL', 'Manual Upload'),
        ('GITHUB', 'GitHub Repository'),
    ]

    WHEN_TO_STOP_CHOICES = [
        ('RESOURCE_DOWNLOAD', 'Stop after resource download and move to step by step mode'),
        ('DATABASE_CREATION', 'Stop after database creation'),
        ('SMCUBES_RULES', 'Stop after creation of SMCubes generation rules for custom configuration before python generation'),
        ('PYTHON_CODE', 'Use previous customisation and stop after generating Python code'),
        ('FULL_EXECUTION', 'Do everything including creating Python code and running the test suite'),
    ]

    data_model_type = models.CharField(
        max_length=10,
        choices=DATA_MODEL_CHOICES,
        default='ELDM',
        help_text='Select whether to use ELDM or EIL data model'
    )

    technical_export_source = models.CharField(
        max_length=20,
        choices=TECHNICAL_EXPORT_SOURCE_CHOICES,
        default='BIRD_WEBSITE',
        help_text='Source for technical export files'
    )

    technical_export_github_url = models.URLField(
        blank=True,
        null=True,
        help_text='GitHub repository URL for technical export files (when GitHub source is selected)'
    )

    config_files_source = models.CharField(
        max_length=20,
        choices=CONFIG_FILES_SOURCE_CHOICES,
        default='MANUAL',
        help_text='Source for configuration files (joins, extra variables)'
    )

    config_files_github_url = models.URLField(
        blank=True,
        null=True,
        help_text='GitHub repository URL for configuration files (when GitHub source is selected)'
    )

    when_to_stop = models.CharField(
        max_length=20,
        choices=WHEN_TO_STOP_CHOICES,
        default='RESOURCE_DOWNLOAD',
        help_text='Defines how far to take automode processing before stopping'
    )

    # Note: github_token is intentionally NOT stored in database for security reasons
    # The token should be provided at runtime and handled in memory only

    is_active = models.BooleanField(
        default=True,
        help_text='Whether this configuration is currently active'
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Automode Configuration"
        verbose_name_plural = "Automode Configurations"
        ordering = ['-updated_at']

    def __str__(self):
        return f"Automode Config ({self.data_model_type}) - {self.created_at.strftime('%Y-%m-%d %H:%M')}"

    def clean(self):
        from django.core.exceptions import ValidationError

        # Validate GitHub URLs are provided when GitHub source is selected
        if self.technical_export_source == 'GITHUB' and not self.technical_export_github_url:
            raise ValidationError({
                'technical_export_github_url': 'GitHub URL is required when GitHub is selected as technical export source.'
            })

        if self.config_files_source == 'GITHUB' and not self.config_files_github_url:
            raise ValidationError({
                'config_files_github_url': 'GitHub URL is required when GitHub is selected as config files source.'
            })

    @classmethod
    def get_active_configuration(cls):
        """Get the currently active configuration, or create a default one if none exists."""
        try:
            return cls.objects.filter(is_active=True).first()
        except cls.DoesNotExist:
            return cls.objects.create()

    def save(self, *args, **kwargs):
        # Ensure only one configuration is active at a time
        if self.is_active:
            AutomodeConfiguration.objects.filter(is_active=True).update(is_active=False)
        super().save(*args, **kwargs)


# Workflow Models for 6-Task UI

class WorkflowTaskExecution(models.Model):
    """Track execution state of workflow tasks"""

    TASK_CHOICES = [
        (1, 'SMCubes Core Creation'),
        (2, 'SMCubes Transformation Rules Creation'),
        (3, 'Python Transformation Rules Creation'),
        (4, 'Full Execution with Test Suite'),
    ]

    OPERATION_CHOICES = [
        ('do', 'Do'),
        ('review', 'Review'),
    ]

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('invalidated', 'Invalidated'),
        ('paused', 'Paused'),
    ]

    task_number = models.IntegerField(choices=TASK_CHOICES)
    subtask_name = models.CharField(max_length=100, blank=True, null=True)
    operation_type = models.CharField(max_length=20, choices=OPERATION_CHOICES)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(blank=True, null=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    execution_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    # New fields for enhanced workflow functionality
    substep_results = models.JSONField(
        default=dict,
        blank=True,
        help_text="Detailed execution results for each substep",
        verbose_name="Substep Results"
    )

    validation_messages = models.TextField(
        blank=True,
        null=True,
        help_text="Validation results and warnings from the execution",
        verbose_name="Validation Messages"
    )

    comparison_data = models.JSONField(
        default=dict,
        blank=True,
        help_text="Data comparing this execution with previous versions",
        verbose_name="Comparison Data"
    )

    REVIEW_STATUS_CHOICES = [
        ('not_reviewed', 'Not Reviewed'),
        ('in_review', 'In Review'),
        ('approved', 'Approved'),
        ('rejected', 'Rejected'),
        ('needs_revision', 'Needs Revision'),
    ]

    review_status = models.CharField(
        max_length=20,
        choices=REVIEW_STATUS_CHOICES,
        default='not_reviewed',
        help_text="Current review status of this execution",
        verbose_name="Review Status"
    )

    review_comments = models.TextField(
        blank=True,
        null=True,
        help_text="Comments from the review process",
        verbose_name="Review Comments"
    )

    progress_percentage = models.IntegerField(
        default=0,
        help_text="Progress percentage (0-100) of the current execution",
        verbose_name="Progress Percentage"
    )

    error_details = models.TextField(
        blank=True,
        null=True,
        help_text="Detailed error information including stack traces",
        verbose_name="Error Details"
    )

    RECOVERY_ACTION_CHOICES = [
        ('none', 'No Recovery Action'),
        ('retry', 'Retry Operation'),
        ('skip', 'Skip and Continue'),
        ('manual', 'Manual Intervention Required'),
        ('rollback', 'Rollback Changes'),
    ]

    recovery_action = models.CharField(
        max_length=20,
        choices=RECOVERY_ACTION_CHOICES,
        default='none',
        help_text="Recommended recovery action if execution failed",
        verbose_name="Recovery Action"
    )

    class Meta:
        verbose_name = "WorkflowTaskExecution"
        verbose_name_plural = "WorkflowTaskExecutions"
        ordering = ['task_number', 'created_at']
        unique_together = [['task_number', 'operation_type']]

    def __str__(self):
        return f"Task {self.task_number} - {self.get_operation_type_display()}: {self.status}"

    def invalidate_downstream_tasks(self):
        """Invalidate all tasks that depend on this one"""
        WorkflowTaskExecution.objects.filter(
            task_number__gt=self.task_number
        ).update(status='invalidated')

    def can_execute(self):
        """Check if this task can be executed based on dependencies"""
        if self.task_number == 1:
            return True

        # Check if previous task is completed
        previous_tasks = WorkflowTaskExecution.objects.filter(
            task_number__lt=self.task_number,
            operation_type='do'
        )

        for task in previous_tasks:
            if task.status != 'completed':
                return False

        return True

    @classmethod
    def get_latest_execution(cls, task_number, operation_type='do'):
        """Get the latest execution for a specific task and operation"""
        try:
            return cls.objects.filter(
                task_number=task_number,
                operation_type=operation_type
            ).latest('created_at')
        except cls.DoesNotExist:
            return None

    def mark_as_reviewed(self, review_status, comments=None):
        """Mark this execution as reviewed with given status and comments"""
        self.review_status = review_status
        if comments:
            self.review_comments = comments
        self.save(update_fields=['review_status', 'review_comments'])

    def update_progress(self, percentage, substep_name=None, substep_data=None):
        """Update the progress percentage and optionally add substep data"""
        self.progress_percentage = max(0, min(100, percentage))

        if substep_name and substep_data is not None:
            if not self.substep_results:
                self.substep_results = {}
            self.substep_results[substep_name] = {
                'data': substep_data,
                'timestamp': timezone.now().isoformat(),
                'progress': percentage
            }

        self.save(update_fields=['progress_percentage', 'substep_results'])

    def add_validation_message(self, message_type, message, details=None):
        """Add a validation message to the execution"""
        timestamp = timezone.now().isoformat()
        new_message = f"[{timestamp}] [{message_type.upper()}] {message}"
        if details:
            new_message += f"\nDetails: {details}"

        if self.validation_messages:
            self.validation_messages += f"\n\n{new_message}"
        else:
            self.validation_messages = new_message

        self.save(update_fields=['validation_messages'])

    def set_comparison_data(self, comparison_type, data):
        """Set comparison data for this execution"""
        if not self.comparison_data:
            self.comparison_data = {}

        self.comparison_data[comparison_type] = {
            'data': data,
            'timestamp': timezone.now().isoformat()
        }

        self.save(update_fields=['comparison_data'])

    def handle_error(self, error_message, error_details=None, recovery_action='none'):
        """Handle execution error with detailed information and recovery action"""
        self.status = 'failed'
        self.error_message = error_message
        self.error_details = error_details
        self.recovery_action = recovery_action
        self.completed_at = timezone.now()

        self.save(update_fields=[
            'status', 'error_message', 'error_details',
            'recovery_action', 'completed_at'
        ])

    def start_execution(self):
        """Mark execution as started"""
        self.status = 'running'
        self.started_at = timezone.now()
        self.progress_percentage = 0

        self.save(update_fields=['status', 'started_at', 'progress_percentage'])

    def complete_execution(self, final_data=None):
        """Mark execution as completed"""
        self.status = 'completed'
        self.completed_at = timezone.now()
        self.progress_percentage = 100

        if final_data:
            self.execution_data.update(final_data)

        self.save(update_fields=[
            'status', 'completed_at', 'progress_percentage', 'execution_data'
        ])


class WorkflowTaskDependency(models.Model):
    """Define dependencies between workflow tasks"""

    DEPENDENCY_TYPES = [
        ('sequential', 'Sequential'),
        ('optional', 'Optional'),
        ('conditional', 'Conditional'),
    ]

    task_number = models.IntegerField()
    depends_on_task = models.IntegerField()
    dependency_type = models.CharField(max_length=50, choices=DEPENDENCY_TYPES, default='sequential')

    class Meta:
        verbose_name = "WorkflowTaskDependency"
        verbose_name_plural = "WorkflowTaskDependencies"
        unique_together = [['task_number', 'depends_on_task']]

    def __str__(self):
        return f"Task {self.task_number} depends on Task {self.depends_on_task}"


class WorkflowSession(models.Model):
    """Track overall workflow session state"""

    session_id = models.CharField(max_length=100, unique=True)
    configuration = models.JSONField(default=dict)
    current_task = models.IntegerField(default=1)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "WorkflowSession"
        verbose_name_plural = "WorkflowSessions"

    def get_task_status_grid(self):
        """Get a 4x3 grid of task statuses"""
        grid = []
        for task_num in range(1, 5):
            task_row = {
                'task_number': task_num,
                'task_name': dict(WorkflowTaskExecution.TASK_CHOICES)[task_num],
                'operations': {}
            }

            for op_type in ['do', 'review']:
                try:
                    execution = WorkflowTaskExecution.objects.get(
                        task_number=task_num,
                        operation_type=op_type
                    )
                    task_row['operations'][op_type] = {
                        'status': execution.status,
                        'started_at': execution.started_at,
                        'completed_at': execution.completed_at,
                        'error_message': execution.error_message
                    }
                except OperationalError:
                    return grid
                except WorkflowTaskExecution.DoesNotExist:
                    task_row['operations'][op_type] = {
                        'status': 'pending',
                        'started_at': None,
                        'completed_at': None,
                        'error_message': None
                    }

            grid.append(task_row)

        return grid

    def get_progress_percentage(self):
        """Calculate overall progress percentage based on completed 'do' operations only"""
        total_tasks = 4  # 4 tasks total
        completed_do_operations = WorkflowTaskExecution.objects.filter(
            operation_type='do',
            status='completed'
        ).count()

        return int((completed_do_operations / total_tasks) * 100)
