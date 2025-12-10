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
#    Benjamin Arfa - framework junction tables and mapping ordinate link

"""
Extension models for BIRD metadata.

These models are not part of the core SMCube metadata standard but are
required for application-specific functionality:
- Framework junction tables: Enable filtering of entities by regulatory framework
- MAPPING_ORDINATE_LINK: Enables reconstruction of ordinate selection when editing mappings
"""

from django.db import models


# FRAMEWORK JUNCTION TABLES
# Note: These are defined for application needs to support framework-specific
# subgraph filtering. They are not, as such, defined in the metadata standard SMCube.

class FRAMEWORK_TABLE(models.Model):
    """Junction table linking FRAMEWORK to TABLE (report templates).
    Allows filtering of report templates by framework (FINREP, COREP, ANCRDT)."""
    framework_id = models.ForeignKey(
        "FRAMEWORK",
        models.CASCADE,
        db_column="framework_id",
    )
    table_id = models.ForeignKey(
        "TABLE",
        models.CASCADE,
        db_column="table_id",
    )

    class Meta:
        verbose_name = "FRAMEWORK_TABLE"
        verbose_name_plural = "FRAMEWORK_TABLEs"
        unique_together = [["framework_id", "table_id"]]

    def __str__(self):
        return f"{self.framework_id} - {self.table_id}"


class FRAMEWORK_SUBDOMAIN(models.Model):
    """Junction table linking FRAMEWORK to SUBDOMAIN.
    Allows filtering of subdomains by framework (FINREP, COREP, ANCRDT)."""
    framework_id = models.ForeignKey(
        "FRAMEWORK",
        models.CASCADE,
        db_column="framework_id",
    )
    subdomain_id = models.ForeignKey(
        "SUBDOMAIN",
        models.CASCADE,
        db_column="subdomain_id",
    )

    class Meta:
        verbose_name = "FRAMEWORK_SUBDOMAIN"
        verbose_name_plural = "FRAMEWORK_SUBDOMAINs"
        unique_together = [["framework_id", "subdomain_id"]]

    def __str__(self):
        return f"{self.framework_id} - {self.subdomain_id}"


class FRAMEWORK_HIERARCHY(models.Model):
    """Junction table linking FRAMEWORK to MEMBER_HIERARCHY.
    Allows filtering of member hierarchies by framework (FINREP, COREP, ANCRDT)."""
    framework_id = models.ForeignKey(
        "FRAMEWORK",
        models.CASCADE,
        db_column="framework_id",
    )
    member_hierarchy_id = models.ForeignKey(
        "MEMBER_HIERARCHY",
        models.CASCADE,
        db_column="member_hierarchy_id",
    )

    class Meta:
        verbose_name = "FRAMEWORK_HIERARCHY"
        verbose_name_plural = "FRAMEWORK_HIERARCHYs"
        unique_together = [["framework_id", "member_hierarchy_id"]]

    def __str__(self):
        return f"{self.framework_id} - {self.member_hierarchy_id}"


class MAPPING_ORDINATE_LINK(models.Model):
    """
    Links MAPPING_DEFINITION to the ordinates (AXIS_ORDINATE) used to create it.
    This enables reconstruction of the original ordinate selection when editing mappings.
    """
    mapping_id = models.ForeignKey(
        "MAPPING_DEFINITION",
        on_delete=models.CASCADE,
        related_name="ordinate_links"
    )
    axis_ordinate_id = models.ForeignKey(
        "AXIS_ORDINATE",
        on_delete=models.CASCADE,
        related_name="mapping_links"
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "MAPPING_ORDINATE_LINK"
        verbose_name_plural = "MAPPING_ORDINATE_LINKs"
        unique_together = ('mapping_id', 'axis_ordinate_id')

    def __str__(self):
        return f"{self.mapping_id_id} -> {self.axis_ordinate_id_id}"
