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

# BPMN Lite Models based on bpmn_lite.ecore

class BaseElement(models.Model):
    id = models.CharField("id", max_length=1000, primary_key=True)
    description = models.TextField("description", default=None, blank=True, null=True)
    invisible = models.BooleanField("invisible", default=False, blank=True, null=True)
    
    class Meta:
        abstract = True
        verbose_name = "BaseElement"
        verbose_name_plural = "BaseElements"


class FlowElement(BaseElement):
    name = models.CharField("name", max_length=1000, default=None, blank=True, null=True)
    
    class Meta:
        abstract = True
        verbose_name = "FlowElement"
        verbose_name_plural = "FlowElements"


class FlowNode(FlowElement):
    # Relationships to SequenceFlow are defined in SequenceFlow model with related_name
    
    class Meta:
        abstract = True
        verbose_name = "FlowNode"
        verbose_name_plural = "FlowNodes"


class Activity(FlowNode):
    class Meta:
        abstract = True
        verbose_name = "Activity"
        verbose_name_plural = "Activities"


class Task(Activity):
    class Meta:
        verbose_name = "Task"
        verbose_name_plural = "Tasks"


class Gateway(FlowNode):
    class Meta:
        abstract = True
        verbose_name = "Gateway"
        verbose_name_plural = "Gateways"


class ExclusiveGateway(Gateway):
    class Meta:
        verbose_name = "ExclusiveGateway"
        verbose_name_plural = "ExclusiveGateways"


class InclusiveGateway(Gateway):
    class Meta:
        verbose_name = "InclusiveGateway"
        verbose_name_plural = "InclusiveGateways"


class ParallelGateway(Gateway):
    class Meta:
        verbose_name = "ParallelGateway"
        verbose_name_plural = "ParallelGateways"


class FlowElementsContainer(BaseElement):
    # flow_elements relationship defined in concrete implementations
    
    class Meta:
        abstract = True
        verbose_name = "FlowElementsContainer"
        verbose_name_plural = "FlowElementsContainers"


class SequenceFlow(FlowElement):
    source_ref = models.ForeignKey(
        "FlowNode",
        models.SET_NULL,
        related_name="outgoing",
        blank=True,
        null=True,
    )
    target_ref = models.ForeignKey(
        "FlowNode",
        models.SET_NULL,
        related_name="incoming",
        blank=True,
        null=True,
    )
    
    class Meta:
        verbose_name = "SequenceFlow"
        verbose_name_plural = "SequenceFlows"


class DataConstraint(models.Model):
    # Note: References to XAttribute from Xcore.ecore would need to be resolved
    # For now, using CharField to store references
    attr1_reference = models.CharField("attr1_reference", max_length=1000, blank=True, null=True)
    
    ATTR_COMPARISON_CHOICES = [
        ('equals', 'Equals'),
        ('less_than', 'Less Than'),
        ('greater_than', 'Greater Than'),
        ('not_equals', 'Not Equals'),
    ]
    
    comparison = models.CharField(
        "comparison",
        max_length=20,
        choices=ATTR_COMPARISON_CHOICES,
        default='equals',
    )
    
    member_reference = models.CharField("member_reference", max_length=1000, blank=True, null=True)
    value = models.CharField("value", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "DataConstraint"
        verbose_name_plural = "DataConstraints"


class Scenario(models.Model):
    name = models.CharField("name", max_length=1000, primary_key=True)
    invisible = models.BooleanField("invisible", default=False, blank=True, null=True)
    description = models.TextField("description", blank=True, null=True)
    # Note: required_attributes would reference XMember from Xcore
    # For now, using a TextField to store JSON or comma-separated references
    required_attributes = models.TextField("required_attributes", blank=True, null=True)
    data_constraints = models.OneToOneField(
        DataConstraint,
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    
    class Meta:
        verbose_name = "Scenario"
        verbose_name_plural = "Scenarios"


class ServiceTask(Task):
    # Note: References to XOperation/XAttribute from Xcore.ecore would need to be resolved
    enriched_attribute_reference = models.CharField("enriched_attribute_reference", max_length=1000, blank=True, null=True)
    second_attribute_reference = models.CharField("second_attribute_reference", max_length=1000, blank=True, null=True)
    required_attributes_for_scenario_choice = models.TextField("required_attributes_for_scenario_choice", blank=True, null=True)
    entity_creation_task = models.BooleanField("entity_creation_task", default=False)
    required_attributes_for_entity_creation = models.TextField("required_attributes_for_entity_creation", blank=True, null=True)
    
    class Meta:
        verbose_name = "ServiceTask"
        verbose_name_plural = "ServiceTasks"


class ServiceTaskScenario(models.Model):
    service_task = models.ForeignKey(
        ServiceTask,
        on_delete=models.CASCADE,
        related_name="scenarios",
    )
    scenario = models.ForeignKey(
        Scenario,
        on_delete=models.CASCADE,
    )
    
    class Meta:
        verbose_name = "ServiceTaskScenario"
        verbose_name_plural = "ServiceTaskScenarios"
        unique_together = ('service_task', 'scenario')


class SelectionLayer(models.Model):
    name = models.CharField("name", max_length=1000, primary_key=True)
    invisible = models.BooleanField("invisible", default=False, blank=True, null=True)
    # Note: generatedEntity would reference XClass from Xcore
    generated_entity_reference = models.CharField("generated_entity_reference", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "SelectionLayer"
        verbose_name_plural = "SelectionLayers"


class ScriptTask(Task):
    # Note: output_layer would reference XClass from Xcore
    output_layer_reference = models.CharField("output_layer_reference", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "ScriptTask"
        verbose_name_plural = "ScriptTasks"


class ScriptTaskSelectionLayer(models.Model):
    script_task = models.ForeignKey(
        ScriptTask,
        on_delete=models.CASCADE,
        related_name="selection_layers",
    )
    selection_layer = models.ForeignKey(
        SelectionLayer,
        on_delete=models.CASCADE,
    )
    
    class Meta:
        verbose_name = "ScriptTaskSelectionLayer"
        verbose_name_plural = "ScriptTaskSelectionLayers"
        unique_together = ('script_task', 'selection_layer')


class UserTask(Task):
    # Note: entity would reference XClass from Xcore
    entity_reference = models.CharField("entity_reference", max_length=1000, blank=True, null=True)
    
    class Meta:
        verbose_name = "UserTask"
        verbose_name_plural = "UserTasks"


class SubProcess(Activity, FlowElementsContainer):
    # Since Django doesn't support multiple inheritance well for concrete models,
    # we'll add the flow elements relationship here
    
    class Meta:
        verbose_name = "SubProcess"
        verbose_name_plural = "SubProcesses"


class SubProcessFlowElement(models.Model):
    sub_process = models.ForeignKey(
        SubProcess,
        on_delete=models.CASCADE,
        related_name="flow_elements",
    )
    # Using generic foreign key to support different flow element types
    flow_element_type = models.CharField(max_length=50)
    flow_element_id = models.CharField(max_length=1000)
    
    class Meta:
        verbose_name = "SubProcessFlowElement"
        verbose_name_plural = "SubProcessFlowElements"
        unique_together = ('sub_process', 'flow_element_type', 'flow_element_id')


# Note: ActivityTag, ScenarioTag, and WorkflowModule reference requirements_text.ecore and module_management.ecore
# These would need to be defined after those models are created
class ActivityTag(models.Model):
    # This inherits from Tag in requirements_text.ecore
    # Will be properly defined after requirements_text_models.py is created
    activity = models.ForeignKey(
        Activity,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    # Placeholder for Tag fields
    display_name = models.CharField("display_name", max_length=1000, blank=True, null=True)
    name = models.CharField("name", max_length=1000, primary_key=True)
    
    class Meta:
        verbose_name = "ActivityTag"
        verbose_name_plural = "ActivityTags"


class ScenarioTag(models.Model):
    # This inherits from Tag in requirements_text.ecore
    # Will be properly defined after requirements_text_models.py is created
    scenario = models.ForeignKey(
        Scenario,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    # Placeholder for Tag fields
    display_name = models.CharField("display_name", max_length=1000, blank=True, null=True)
    name = models.CharField("name", max_length=1000, primary_key=True)
    
    class Meta:
        verbose_name = "ScenarioTag"
        verbose_name_plural = "ScenarioTags"


class WorkflowModule(models.Model):
    # This inherits from Module in module_management.ecore
    # Using composition for now since module_management.ecore is not provided
    # Module fields placeholder
    module_name = models.CharField("module_name", max_length=1000)
    module_id = models.CharField("module_id", max_length=1000, primary_key=True)
    
    class Meta:
        verbose_name = "WorkflowModule"
        verbose_name_plural = "WorkflowModules"


class WorkflowModuleContent(models.Model):
    workflow_module = models.ForeignKey(
        WorkflowModule,
        on_delete=models.CASCADE,
        related_name="content",
    )
    content_type = models.CharField(
        max_length=20,
        choices=[
            ('task_tag', 'Task Tag'),
            ('scenario_tag', 'Scenario Tag'),
            ('sub_process', 'Sub Process'),
        ]
    )
    content_id = models.CharField(max_length=1000)
    
    class Meta:
        verbose_name = "WorkflowModuleContent"
        verbose_name_plural = "WorkflowModuleContents"
        unique_together = ('workflow_module', 'content_type', 'content_id')