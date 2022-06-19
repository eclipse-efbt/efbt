
from .open_reg_specs import getEClassifier, eClassifiers
from .open_reg_specs import name, nsURI, nsPrefix, eClass
from .open_reg_specs import OpenRegSpecs, BaseElement, FlowElementsContainer, FlowElement, FlowNode, SequenceFlow, Task, Activity, Gateway, ExclusiveGateway, InclusiveGateway, ParallelGateway, ServiceTask, SubProcess, ScriptTask, UserTask, Attribute, Concept, RelationshipAttribute, OneToOneRelationshipAttribute, ManyToOneRelationshipAttribute, OneToManyRelationshipAttribute, ManyToManyRelationshipAttribute, Type, Element, NamedElement, EntityModule, StructuralFeature, TypedElement, GeneratedEntity, Entity, DerivedEntity, BasicEntity, EnumMember, TypesAndConcepts, Module, ModuleLongName, ModuleDependencies, ModuleDependency, AllowedTypes, RequirementsModule, RequirementsSection, RequirementsSectionImage, RequirementsSectionLinkWithText, RequirementsSectionText, RequirementType, TitledRequirementsSection, Tag, TagGroup, View, SelectClause, SelectColumn, Column, WhereClause, ViewModule, ActivityTag, Scenario, ScenarioTag, Test, TestModule, DataConstraint, AttrComparison, SelectionLayer, TestScope, UnitTestScope, E2ETestScope, CSVFile, CSVRow, DOMAIN, FACET_COLLECTION, FACET_ENUMERATION, facet_type, FACET_VALUE_TYPE, FACET_VALUE_TYPEObject, MAINTENANCE_AGENCY, MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE, SUBDOMAIN, SUBDOMAIN_ENUMERATION, VARIABLE, VARIABLE_SET, VARIABLE_SET_ENUMERATION, COMBINATION, COMBINATION_ITEM, CUBE, CUBE_GROUP, CUBE_GROUP_ENUMERATION, CUBE_HIERARCHY, CUBE_HIERARCHY_NODE, CUBE_RELATIONSHIP, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, FRAMEWORK, FRAMEWORK_SUBDOMAIN, FRAMEWORK_VARIABLE_SET, TYP_DMNSN, TYP_DMNSNObject, TYP_RL, TYP_RLObject, DomainModule, MemberHierarchyModule, MemberModule, VariableModule, SubDomainModule, SMCubesCoreModel, PlatformCall, PlatformCallModule, ImportBIRDFromMSAccess, WorkflowModule


from . import open_reg_specs

__all__ = ['OpenRegSpecs', 'BaseElement', 'FlowElementsContainer', 'FlowElement', 'FlowNode', 'SequenceFlow', 'Task', 'Activity', 'Gateway', 'ExclusiveGateway', 'InclusiveGateway', 'ParallelGateway', 'ServiceTask', 'SubProcess', 'ScriptTask', 'UserTask', 'Attribute', 'Concept', 'RelationshipAttribute', 'OneToOneRelationshipAttribute', 'ManyToOneRelationshipAttribute', 'OneToManyRelationshipAttribute', 'ManyToManyRelationshipAttribute', 'Type', 'Element', 'NamedElement', 'EntityModule', 'StructuralFeature', 'TypedElement', 'GeneratedEntity', 'Entity', 'DerivedEntity', 'BasicEntity', 'EnumMember', 'TypesAndConcepts', 'Module', 'ModuleLongName', 'ModuleDependencies', 'ModuleDependency', 'AllowedTypes', 'RequirementsModule', 'RequirementsSection', 'RequirementsSectionImage', 'RequirementsSectionLinkWithText', 'RequirementsSectionText', 'RequirementType', 'TitledRequirementsSection', 'Tag', 'TagGroup', 'View', 'SelectClause', 'SelectColumn', 'Column', 'WhereClause', 'ViewModule',
           'ActivityTag', 'Scenario', 'ScenarioTag', 'Test', 'TestModule', 'DataConstraint', 'AttrComparison', 'SelectionLayer', 'TestScope', 'UnitTestScope', 'E2ETestScope', 'CSVFile', 'CSVRow', 'DOMAIN', 'FACET_COLLECTION', 'FACET_ENUMERATION', 'facet_type', 'FACET_VALUE_TYPE', 'FACET_VALUE_TYPEObject', 'MAINTENANCE_AGENCY', 'MEMBER', 'MEMBER_HIERARCHY', 'MEMBER_HIERARCHY_NODE', 'SUBDOMAIN', 'SUBDOMAIN_ENUMERATION', 'VARIABLE', 'VARIABLE_SET', 'VARIABLE_SET_ENUMERATION', 'COMBINATION', 'COMBINATION_ITEM', 'CUBE', 'CUBE_GROUP', 'CUBE_GROUP_ENUMERATION', 'CUBE_HIERARCHY', 'CUBE_HIERARCHY_NODE', 'CUBE_RELATIONSHIP', 'CUBE_STRUCTURE', 'CUBE_STRUCTURE_ITEM', 'CUBE_TO_COMBINATION', 'FRAMEWORK', 'FRAMEWORK_SUBDOMAIN', 'FRAMEWORK_VARIABLE_SET', 'TYP_DMNSN', 'TYP_DMNSNObject', 'TYP_RL', 'TYP_RLObject', 'DomainModule', 'MemberHierarchyModule', 'MemberModule', 'VariableModule', 'SubDomainModule', 'SMCubesCoreModel', 'PlatformCall', 'PlatformCallModule', 'ImportBIRDFromMSAccess', 'WorkflowModule']

eSubpackages = []
eSuperPackage = None
open_reg_specs.eSubpackages = eSubpackages
open_reg_specs.eSuperPackage = eSuperPackage

OpenRegSpecs.requirements.eType = RequirementsModule
OpenRegSpecs.types_and_concepts.eType = SMCubesCoreModel
OpenRegSpecs.data_model.eType = EntityModule
OpenRegSpecs.process_workflow.eType = WorkflowModule
OpenRegSpecs.report_generation.eType = ViewModule
OpenRegSpecs.tests.eType = TestModule
FlowElementsContainer.flowElements.eType = FlowElement
ServiceTask.enrichedAttribute.eType = Attribute
ServiceTask.scenarios.eType = Scenario
ServiceTask.secondAttribute.eType = Attribute
ScriptTask.selectionLayers.eType = SelectionLayer
ScriptTask.outputLayer.eType = GeneratedEntity
UserTask.entity.eType = BasicEntity
Attribute.concept.eType = Concept
RelationshipAttribute.entity.eType = Entity
EntityModule.entities.eType = Entity
TypedElement.type.eType = Type
GeneratedEntity.attributes.eType = Attribute
DerivedEntity.attributes.eType = Attribute
BasicEntity.attributes.eType = Attribute
BasicEntity.superClass.eType = BasicEntity
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
SelectColumn.as_.eType = Attribute
SelectColumn.memberAsConstant.eType = EnumMember
Column.attribute.eType = Attribute
ViewModule.views.eType = View
ActivityTag.activity.eType = Activity
Scenario.requiredAttributes.eType = Attribute
Scenario.data_constraints.eType = DataConstraint
ScenarioTag.scenario.eType = Scenario
Test.inputData.eType = CSVFile
Test.expectedResult.eType = CSVFile
Test.scope.eType = TestScope
TestModule.tests.eType = Test
DataConstraint.attr1.eType = Attribute
DataConstraint.attr2.eType = Attribute
SelectionLayer.generatedEntity.eType = GeneratedEntity
UnitTestScope.scenarios.eType = Scenario
E2ETestScope.scenarios.eType = Scenario
E2ETestScope.layer.eType = SelectionLayer
E2ETestScope.scriptTask.eType = ScriptTask
CSVFile.rows.eType = CSVRow
DOMAIN.facet_id.eType = FACET_COLLECTION
DOMAIN.maintenance_agency_id.eType = MAINTENANCE_AGENCY
FACET_COLLECTION.maintenance_agency_id.eType = MAINTENANCE_AGENCY
FACET_ENUMERATION.facet_id.eType = FACET_COLLECTION
FACET_ENUMERATION.facet_type.eType = facet_type
MEMBER.domain_id.eType = DOMAIN
MEMBER.maintenance_agency_id.eType = MAINTENANCE_AGENCY
MEMBER_HIERARCHY.domain_id.eType = DOMAIN
MEMBER_HIERARCHY.maintenance_agency_id.eType = MAINTENANCE_AGENCY
MEMBER_HIERARCHY_NODE.member_hierarchy_id.eType = MEMBER_HIERARCHY
MEMBER_HIERARCHY_NODE.member_id.eType = MEMBER
MEMBER_HIERARCHY_NODE.parent_member_id.eType = MEMBER
SUBDOMAIN.domain_id.eType = DOMAIN
SUBDOMAIN.facet_id.eType = FACET_COLLECTION
SUBDOMAIN.maintenance_agency_id.eType = MAINTENANCE_AGENCY
SUBDOMAIN.items.eType = SUBDOMAIN_ENUMERATION
SUBDOMAIN_ENUMERATION.member_id.eType = MEMBER
VARIABLE.domain_id.eType = DOMAIN
VARIABLE.maintenance_agency_id.eType = MAINTENANCE_AGENCY
VARIABLE_SET.maintenance_agency_id.eType = MAINTENANCE_AGENCY
VARIABLE_SET_ENUMERATION.subdomain_id.eType = SUBDOMAIN
VARIABLE_SET_ENUMERATION.variable_id.eType = VARIABLE
VARIABLE_SET_ENUMERATION.variable_set_id.eType = VARIABLE_SET
COMBINATION.maintenance_agency_id.eType = MAINTENANCE_AGENCY
COMBINATION.combination_items.eType = COMBINATION_ITEM
COMBINATION_ITEM.member_id.eType = MEMBER
COMBINATION_ITEM.subdomain_id.eType = SUBDOMAIN
COMBINATION_ITEM.variable_id.eType = VARIABLE
COMBINATION_ITEM.variable_set_id.eType = VARIABLE_SET
CUBE.cube_structure_id.eType = CUBE_STRUCTURE
CUBE.framework_id.eType = FRAMEWORK
CUBE.maintenance_agency_id.eType = MAINTENANCE_AGENCY
CUBE_GROUP.maintenance_agency_id.eType = MAINTENANCE_AGENCY
CUBE_GROUP_ENUMERATION.cube_group_id.eType = CUBE_GROUP
CUBE_GROUP_ENUMERATION.cube_id.eType = CUBE
CUBE_HIERARCHY.framework_id.eType = FRAMEWORK
CUBE_HIERARCHY.maintenance_agency_id.eType = MAINTENANCE_AGENCY
CUBE_HIERARCHY_NODE.cube_group_id.eType = CUBE_GROUP
CUBE_HIERARCHY_NODE.cube_hierarchy_id.eType = CUBE_HIERARCHY
CUBE_HIERARCHY_NODE.parent_node_code.eType = CUBE_HIERARCHY_NODE
CUBE_RELATIONSHIP.foreign_cube_id.eType = CUBE
CUBE_RELATIONSHIP.foreign_cube_variable_code.eType = VARIABLE
CUBE_RELATIONSHIP.maintenance_agency_id.eType = MAINTENANCE_AGENCY
CUBE_RELATIONSHIP.primary_cube_id.eType = CUBE
CUBE_RELATIONSHIP.primary_cube_variable_id.eType = VARIABLE
CUBE_STRUCTURE.maintenance_agency_id.eType = MAINTENANCE_AGENCY
CUBE_STRUCTURE_ITEM.attribute_associated_variable.eType = VARIABLE
CUBE_STRUCTURE_ITEM.cube_structure_id.eType = CUBE_STRUCTURE
CUBE_STRUCTURE_ITEM.member_id.eType = MEMBER
CUBE_STRUCTURE_ITEM.subdomain_id.eType = SUBDOMAIN
CUBE_STRUCTURE_ITEM.variable_id.eType = VARIABLE
CUBE_STRUCTURE_ITEM.variable_set_id.eType = VARIABLE_SET
CUBE_TO_COMBINATION.combination_id.eType = COMBINATION
CUBE_TO_COMBINATION.cube_id.eType = CUBE
FRAMEWORK.maintenance_agency_id.eType = MAINTENANCE_AGENCY
FRAMEWORK_SUBDOMAIN.framework_id.eType = FRAMEWORK
FRAMEWORK_SUBDOMAIN.subdomain_id.eType = SUBDOMAIN
FRAMEWORK_VARIABLE_SET.framework_id.eType = FRAMEWORK
FRAMEWORK_VARIABLE_SET.variable_set_id.eType = VARIABLE_SET
DomainModule.domains.eType = DOMAIN
MemberHierarchyModule.memberHierarchies.eType = MEMBER_HIERARCHY
MemberHierarchyModule.memberHierarchiesNodes.eType = MEMBER_HIERARCHY_NODE
MemberModule.members.eType = MEMBER
VariableModule.variables.eType = VARIABLE
SubDomainModule.subdomains.eType = SUBDOMAIN
SMCubesCoreModel.variableModules.eType = VariableModule
SMCubesCoreModel.domainModules.eType = DomainModule
SMCubesCoreModel.memberModules.eType = MemberModule
SMCubesCoreModel.subDomainModules.eType = SubDomainModule
SMCubesCoreModel.memberHierarchyModules.eType = MemberHierarchyModule
PlatformCallModule.platformCalls.eType = PlatformCall
WorkflowModule.taskTags.eType = ActivityTag
WorkflowModule.scenarioTags.eType = ScenarioTag
WorkflowModule.subProcess.eType = SubProcess
FlowNode.outgoing.eType = SequenceFlow
FlowNode.incoming.eType = SequenceFlow
SequenceFlow.targetRef.eType = FlowNode
SequenceFlow.targetRef.eOpposite = FlowNode.incoming
SequenceFlow.sourceRef.eType = FlowNode
SequenceFlow.sourceRef.eOpposite = FlowNode.outgoing
RequirementsSectionLinkWithText.linkedRuleSection.eType = TitledRequirementsSection
TitledRequirementsSection.referencingSections.eType = RequirementsSectionLinkWithText
TitledRequirementsSection.referencingSections.eOpposite = RequirementsSectionLinkWithText.linkedRuleSection

otherClassifiers = [AttrComparison, FACET_VALUE_TYPE,
                    FACET_VALUE_TYPEObject, TYP_DMNSN, TYP_DMNSNObject, TYP_RL, TYP_RLObject]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
