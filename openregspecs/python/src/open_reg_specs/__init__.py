
from .open_reg_specs import getEClassifier, eClassifiers
from .open_reg_specs import name, nsURI, nsPrefix, eClass
from .open_reg_specs import BaseElement, FlowElementsContainer, FlowElement, FlowNode, SequenceFlow, Task, Activity, Gateway, ExclusiveGateway, InclusiveGateway, ParallelGateway, ServiceTask, SubProcess, ScriptTask, UserTask, Module, ModuleList, AllowedTypes, RequirementsModule, RequirementsSection, RequirementsSectionImage, RequirementsSectionLinkWithText, RequirementsSectionText, RequirementType, TitledRequirementsSection, Tag, TagGroup, View, LayerSQL, SelectClause, SelectColumn, SelectColumnMemberAs, SelectColumnAttributeAs, Comparitor, WhereClause, ViewModule, ActivityTag, Scenario, ScenarioTag, Test, TestModule, DataConstraint, AttrComparison, SelectionLayer, TestScope, UnitTestScope, E2ETestScope, InputFile, WorkflowModule, XAttribute, XClass, XClassifier, XDataType, XEnum, XEnumLiteral, XMember, XModelElement, XNamedElement, XOperation, XPackage, XReference, XStructuralFeature, XTypedElement, VTLEnrichedCube, VTLGeneratedOutputlayer, VTLForOutputLayerAndIntermediateLayerCombination, VTLGeneratedIntermediateLayer, VTLTransformation, VTLScheme, VTLForSelectionLayer, VTLModule, EntityToVTLIntermediateLayerLink


from . import open_reg_specs

__all__ = ['BaseElement', 'FlowElementsContainer', 'FlowElement', 'FlowNode', 'SequenceFlow', 'Task', 'Activity', 'Gateway', 'ExclusiveGateway', 'InclusiveGateway', 'ParallelGateway', 'ServiceTask', 'SubProcess', 'ScriptTask', 'UserTask', 'Module', 'ModuleList', 'AllowedTypes', 'RequirementsModule', 'RequirementsSection', 'RequirementsSectionImage', 'RequirementsSectionLinkWithText', 'RequirementsSectionText', 'RequirementType', 'TitledRequirementsSection', 'Tag', 'TagGroup', 'View', 'LayerSQL', 'SelectClause', 'SelectColumn', 'SelectColumnMemberAs', 'SelectColumnAttributeAs', 'Comparitor', 'WhereClause', 'ViewModule',
           'ActivityTag', 'Scenario', 'ScenarioTag', 'Test', 'TestModule', 'DataConstraint', 'AttrComparison', 'SelectionLayer', 'TestScope', 'UnitTestScope', 'E2ETestScope', 'InputFile', 'WorkflowModule', 'XAttribute', 'XClass', 'XClassifier', 'XDataType', 'XEnum', 'XEnumLiteral', 'XMember', 'XModelElement', 'XNamedElement', 'XOperation', 'XPackage', 'XReference', 'XStructuralFeature', 'XTypedElement', 'VTLEnrichedCube', 'VTLGeneratedOutputlayer', 'VTLForOutputLayerAndIntermediateLayerCombination', 'VTLGeneratedIntermediateLayer', 'VTLTransformation', 'VTLScheme', 'VTLForSelectionLayer', 'VTLModule', 'EntityToVTLIntermediateLayerLink']

eSubpackages = []
eSuperPackage = None
open_reg_specs.eSubpackages = eSubpackages
open_reg_specs.eSuperPackage = eSuperPackage

FlowElementsContainer.flowElements.eType = FlowElement
ServiceTask.enrichedAttribute.eType = XAttribute
ServiceTask.scenarios.eType = Scenario
ServiceTask.secondAttribute.eType = XAttribute
ScriptTask.selectionLayers.eType = SelectionLayer
ScriptTask.outputLayer.eType = XClass
UserTask.entity.eType = XClass
Module.dependencies.eType = Module
ModuleList.modules.eType = Module
AllowedTypes.allowedTypes.eType = RequirementType
RequirementsModule.rules.eType = TitledRequirementsSection
RequirementsModule.allowedtypes.eType = AllowedTypes
TitledRequirementsSection.sections.eType = RequirementsSection
TitledRequirementsSection.requirementsType.eType = RequirementType
Tag.requirements.eType = TitledRequirementsSection
TagGroup.tags.eType = Tag
View.outputLayer.eType = XClass
View.selectionLayerSQL.eType = LayerSQL
LayerSQL.selectionLayer.eType = SelectionLayer
LayerSQL.columns.eType = SelectColumn
LayerSQL.whereClause.eType = WhereClause
SelectColumn.as_.eType = XAttribute
SelectColumnMemberAs.memberAsConstant.eType = XEnumLiteral
SelectColumnAttributeAs.attribute.eType = XAttribute
WhereClause.attribute1.eType = XAttribute
WhereClause.member.eType = XEnumLiteral
ViewModule.views.eType = View
ActivityTag.activity.eType = Activity
Scenario.requiredAttributes.eType = XMember
Scenario.data_constraints.eType = DataConstraint
ScenarioTag.scenario.eType = Scenario
Test.inputData.eType = InputFile
Test.expectedResult.eType = InputFile
Test.scope.eType = TestScope
TestModule.tests.eType = Test
DataConstraint.attr1.eType = XAttribute
DataConstraint.member.eType = XEnumLiteral
SelectionLayer.generatedEntity.eType = XClass
UnitTestScope.scenarios.eType = Scenario
E2ETestScope.scenarios.eType = Scenario
E2ETestScope.layer.eType = SelectionLayer
E2ETestScope.scriptTask.eType = ScriptTask
InputFile.entity.eType = XClass
WorkflowModule.taskTags.eType = ActivityTag
WorkflowModule.scenarioTags.eType = ScenarioTag
WorkflowModule.subProcess.eType = SubProcess
XClass.superTypes.eType = XClass
XTypedElement.type.eType = XClassifier
VTLEnrichedCube.transformations.eType = VTLTransformation
VTLGeneratedOutputlayer.dependant_intermediate_layers.eType = VTLGeneratedIntermediateLayer
VTLGeneratedOutputlayer.VTLForOutputLayerAndIntemedateLayerCombinations.eType = VTLForOutputLayerAndIntermediateLayerCombination
VTLGeneratedOutputlayer.outputLayer.eType = XClass
VTLForOutputLayerAndIntermediateLayerCombination.transformations.eType = VTLTransformation
VTLForOutputLayerAndIntermediateLayerCombination.outputLayer.eType = VTLGeneratedOutputlayer
VTLForOutputLayerAndIntermediateLayerCombination.intermediateLayer.eType = VTLGeneratedIntermediateLayer
VTLGeneratedIntermediateLayer.dependant_enriched_cubes.eType = VTLEnrichedCube
VTLGeneratedIntermediateLayer.transformations.eType = VTLTransformation
VTLScheme.expressions.eType = VTLTransformation
VTLForSelectionLayer.selectionLayer.eType = LayerSQL
VTLForSelectionLayer.outputLayer.eType = VTLGeneratedOutputlayer
VTLForSelectionLayer.intermediateLayer.eType = VTLGeneratedIntermediateLayer
VTLModule.VTLSchemes.eType = VTLScheme
VTLModule.VTLGeneratedOutputLayers.eType = VTLGeneratedOutputlayer
VTLModule.VTLGeneratedIntermediateLayers.eType = VTLGeneratedIntermediateLayer
VTLModule.VTLEnrichedCubes.eType = VTLEnrichedCube
VTLModule.VTLForSelectionLayers.eType = VTLForSelectionLayer
VTLModule.entityToVTLIntermediateLayerLinks.eType = EntityToVTLIntermediateLayerLink
EntityToVTLIntermediateLayerLink.VTLIntermediateLayer.eType = VTLGeneratedIntermediateLayer
EntityToVTLIntermediateLayerLink.entity.eType = XClass
FlowNode.outgoing.eType = SequenceFlow
FlowNode.incoming.eType = SequenceFlow
SequenceFlow.targetRef.eType = FlowNode
SequenceFlow.targetRef.eOpposite = FlowNode.incoming
SequenceFlow.sourceRef.eType = FlowNode
SequenceFlow.sourceRef.eOpposite = FlowNode.outgoing
RequirementsSectionLinkWithText.linkedRuleSection.eType = TitledRequirementsSection
TitledRequirementsSection.referencingSections.eType = RequirementsSectionLinkWithText
TitledRequirementsSection.referencingSections.eOpposite = RequirementsSectionLinkWithText.linkedRuleSection
XClass.members.eType = XMember
XClassifier.package.eType = XPackage
XEnum.literals.eType = XEnumLiteral
XEnumLiteral.enum.eType = XEnum
XEnumLiteral.enum.eOpposite = XEnum.literals
XMember.containingClass.eType = XClass
XMember.containingClass.eOpposite = XClass.members
XPackage.classifiers.eType = XClassifier
XPackage.classifiers.eOpposite = XClassifier.package

otherClassifiers = [Comparitor, AttrComparison]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
