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
from django.utils import timezone

# Requirements Text Models based on requirements_text.ecore

class RequirementType(models.Model):
    name = models.CharField("name", max_length=1000, primary_key=True)
    
    class Meta:
        verbose_name = "RequirementType"
        verbose_name_plural = "RequirementTypes"


class AllowedTypes(models.Model):
    # Container for allowed requirement types
    id = models.AutoField(primary_key=True)
    
    class Meta:
        verbose_name = "AllowedTypes"
        verbose_name_plural = "AllowedTypes"


class AllowedTypesItem(models.Model):
    allowed_types = models.ForeignKey(
        AllowedTypes,
        on_delete=models.CASCADE,
        related_name="allowed_types",
    )
    requirement_type = models.ForeignKey(
        RequirementType,
        on_delete=models.CASCADE,
    )
    
    class Meta:
        verbose_name = "AllowedTypesItem"
        verbose_name_plural = "AllowedTypesItems"
        unique_together = ('allowed_types', 'requirement_type')


class RequirementsSection(models.Model):
    name = models.CharField("name", max_length=1000, primary_key=True)
    
    # Type discriminator for concrete subclasses
    SECTION_TYPE_CHOICES = [
        ('text', 'Requirements Section Text'),
        ('image', 'Requirements Section Image'),
        ('link', 'Requirements Section Link'),
        ('titled', 'Titled Requirements Section'),
    ]
    section_type = models.CharField(
        max_length=20,
        choices=SECTION_TYPE_CHOICES,
        default='text',
    )
    
    class Meta:
        verbose_name = "RequirementsSection"
        verbose_name_plural = "RequirementsSections"


class RequirementsSectionText(models.Model):
    requirements_section = models.OneToOneField(
        RequirementsSection,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="text_section",
    )
    text = models.TextField("text", blank=True, null=True)
    
    class Meta:
        verbose_name = "RequirementsSectionText"
        verbose_name_plural = "RequirementsSectionTexts"


class RequirementsSectionImage(models.Model):
    requirements_section = models.OneToOneField(
        RequirementsSection,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="image_section",
    )
    style = models.CharField("style", max_length=1000, blank=True, null=True)
    uri = models.CharField("uri", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "RequirementsSectionImage"
        verbose_name_plural = "RequirementsSectionImages"


class TitledRequirementsSection(models.Model):
    requirements_section = models.OneToOneField(
        RequirementsSection,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="titled_section",
    )
    requirements_type = models.ForeignKey(
        RequirementType,
        on_delete=models.PROTECT,
    )
    title = models.CharField("title", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "TitledRequirementsSection"
        verbose_name_plural = "TitledRequirementsSections"


class TitledRequirementsSectionContent(models.Model):
    parent_section = models.ForeignKey(
        TitledRequirementsSection,
        on_delete=models.CASCADE,
        related_name="sections",
    )
    child_section = models.ForeignKey(
        RequirementsSection,
        on_delete=models.CASCADE,
    )
    order = models.IntegerField("order", default=0)
    
    class Meta:
        verbose_name = "TitledRequirementsSectionContent"
        verbose_name_plural = "TitledRequirementsSectionContents"
        unique_together = ('parent_section', 'child_section')
        ordering = ['order']


class RequirementsSectionLinkWithText(models.Model):
    requirements_section = models.OneToOneField(
        RequirementsSection,
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="link_section",
    )
    linked_rule_section = models.ForeignKey(
        TitledRequirementsSection,
        on_delete=models.SET_NULL,
        related_name="referencing_sections",
        blank=True,
        null=True,
    )
    link_text = models.CharField("link_text", max_length=1000, blank=True, null=True)
    subsection = models.CharField("subsection", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "RequirementsSectionLinkWithText"
        verbose_name_plural = "RequirementsSectionLinksWithText"


class Tag(models.Model):
    name = models.CharField("name", max_length=1000, primary_key=True)
    display_name = models.CharField("display_name", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "Tag"
        verbose_name_plural = "Tags"


class TagRequirement(models.Model):
    tag = models.ForeignKey(
        Tag,
        on_delete=models.CASCADE,
        related_name="requirements",
    )
    requirement = models.ForeignKey(
        TitledRequirementsSection,
        on_delete=models.CASCADE,
    )
    
    class Meta:
        verbose_name = "TagRequirement"
        verbose_name_plural = "TagRequirements"
        unique_together = ('tag', 'requirement')


class TagGroup(models.Model):
    # This inherits from Module in module_management.ecore
    # Using composition for now since module_management.ecore is not provided
    module_name = models.CharField("module_name", max_length=1000)
    module_id = models.CharField("module_id", max_length=1000, primary_key=True)
    
    class Meta:
        verbose_name = "TagGroup"
        verbose_name_plural = "TagGroups"


class TagGroupItem(models.Model):
    tag_group = models.ForeignKey(
        TagGroup,
        on_delete=models.CASCADE,
        related_name="tags",
    )
    tag = models.ForeignKey(
        Tag,
        on_delete=models.CASCADE,
    )
    
    class Meta:
        verbose_name = "TagGroupItem"
        verbose_name_plural = "TagGroupItems"
        unique_together = ('tag_group', 'tag')


class RequirementsModule(models.Model):
    # This inherits from Module in module_management.ecore
    # Using composition for now since module_management.ecore is not provided
    module_name = models.CharField("module_name", max_length=1000)
    module_id = models.CharField("module_id", max_length=1000, primary_key=True)
    allowed_types = models.OneToOneField(
        AllowedTypes,
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    
    class Meta:
        verbose_name = "RequirementsModule"
        verbose_name_plural = "RequirementsModules"


class RequirementsModuleRule(models.Model):
    requirements_module = models.ForeignKey(
        RequirementsModule,
        on_delete=models.CASCADE,
        related_name="rules",
    )
    rule_section = models.ForeignKey(
        TitledRequirementsSection,
        on_delete=models.CASCADE,
    )
    order = models.IntegerField("order", default=0)
    
    class Meta:
        verbose_name = "RequirementsModuleRule"
        verbose_name_plural = "RequirementsModuleRules"
        unique_together = ('requirements_module', 'rule_section')
        ordering = ['order']