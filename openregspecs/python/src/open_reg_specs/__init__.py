
from .open_reg_specs import getEClassifier, eClassifiers
from .open_reg_specs import name, nsURI, nsPrefix, eClass
from .open_reg_specs import OpenRegSpecs, BaseElement, FlowElementsContainer, FlowElement, FlowNode, SequenceFlow, Task, Activity, Gateway, ExclusiveGateway, InclusiveGateway, ParallelGateway, ServiceTask, SubProcess, ScriptTask, UserTask, Module, ModuleLongName, ModuleDependencies, ModuleDependency, AllowedTypes, RequirementsModule, RequirementsSection, RequirementsSectionImage, RequirementsSectionLinkWithText, RequirementsSectionText, RequirementType, TitledRequirementsSection, Tag, TagGroup, View, SelectClause, SelectColumn, SelectColumnMemberAs, SelectColumnAttributeAs, Comparitor, WhereClause, ViewModule, ActivityTag, Scenario, ScenarioTag, Test, TestModule, DataConstraint, AttrComparison, SelectionLayer, TestScope, UnitTestScope, E2ETestScope, InputFile, PlatformCall, PlatformCallModule, WorkflowModule, DataModel, XAttribute, XClass, XClassifier, XDataType, XEnum, XEnumLiteral, XMember, XModelElement, XNamedElement, XOperation, XPackage, XParameter, XReference, XStructuralFeature, XTypedElement, XFunction


from . import open_reg_specs

__all__ = ['OpenRegSpecs', 'BaseElement', 'FlowElementsContainer', 'FlowElement', 'FlowNode', 'SequenceFlow', 'Task', 'Activity', 'Gateway', 'ExclusiveGateway', 'InclusiveGateway', 'ParallelGateway', 'ServiceTask', 'SubProcess', 'ScriptTask', 'UserTask', 'Module', 'ModuleLongName', 'ModuleDependencies', 'ModuleDependency', 'AllowedTypes', 'RequirementsModule', 'RequirementsSection', 'RequirementsSectionImage', 'RequirementsSectionLinkWithText', 'RequirementsSectionText', 'RequirementType', 'TitledRequirementsSection', 'Tag', 'TagGroup', 'View', 'SelectClause', 'SelectColumn',
           'SelectColumnMemberAs', 'SelectColumnAttributeAs', 'Comparitor', 'WhereClause', 'ViewModule', 'ActivityTag', 'Scenario', 'ScenarioTag', 'Test', 'TestModule', 'DataConstraint', 'AttrComparison', 'SelectionLayer', 'TestScope', 'UnitTestScope', 'E2ETestScope', 'InputFile', 'PlatformCall', 'PlatformCallModule', 'WorkflowModule', 'DataModel', 'XAttribute', 'XClass', 'XClassifier', 'XDataType', 'XEnum', 'XEnumLiteral', 'XMember', 'XModelElement', 'XNamedElement', 'XOperation', 'XPackage', 'XParameter', 'XReference', 'XStructuralFeature', 'XTypedElement', 'XFunction']

eSubpackages = []
eSuperPackage = None
open_reg_specs.eSubpackages = eSubpackages
open_reg_specs.eSuperPackage = eSuperPackage

OpenRegSpecs.requirements.eType = RequirementsModule
OpenRegSpecs.data_model.eType = DataModel
OpenRegSpecs.process_workflow.eType = WorkflowModule
OpenRegSpecs.report_generation.eType = ViewModule
OpenRegSpecs.tests.eType = TestModule
FlowElementsContainer.flowElements.eType = FlowElement
ServiceTask.enrichedAttribute.eType = XAttribute
ServiceTask.scenarios.eType = Scenario
ServiceTask.secondAttribute.eType = XAttribute
ScriptTask.selectionLayers.eType = SelectionLayer
ScriptTask.outputLayer.eType = XClass
UserTask.entity.eType = XClass
Module.dependencies.eType = ModuleDependencies
Module.longName.eType = ModuleLongName
ModuleDependencies.theModules.eType = ModuleDependency
ModuleDependency.theModule.eType = Module
ModuleDependency.longName.eType = ModuleLongName
AllowedTypes.allowedTypes.eType = RequirementType
RequirementsModule.rules.eType = TitledRequirementsSection
RequirementsModule.allowedtypes.eType = AllowedTypes
TitledRequirementsSection.sections.eType = RequirementsSection
TitledRequirementsSection.requirementsType.eType = RequirementType
Tag.requirements.eType = TitledRequirementsSection
TagGroup.tags.eType = Tag
View.selectClause.eType = SelectClause
View.whereClause.eType = WhereClause
View.selectionLayer.eType = SelectionLayer
SelectClause.columns.eType = SelectColumn
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
PlatformCallModule.platformCalls.eType = PlatformCall
WorkflowModule.taskTags.eType = ActivityTag
WorkflowModule.scenarioTags.eType = ScenarioTag
WorkflowModule.subProcess.eType = SubProcess
DataModel.package.eType = XPackage
XClass.superTypes.eType = XClass
XDataType.createBody.eType = XFunction
XDataType.convertBody.eType = XFunction
XOperation.body.eType = XFunction
XTypedElement.type.eType = XClassifier
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
XOperation.parameters.eType = XParameter
XPackage.classifiers.eType = XClassifier
XPackage.classifiers.eOpposite = XClassifier.package
XParameter.operation.eType = XOperation
XParameter.operation.eOpposite = XOperation.parameters

otherClassifiers = [Comparitor, AttrComparison]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
