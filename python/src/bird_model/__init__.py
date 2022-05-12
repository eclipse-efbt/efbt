
from .bird_model import getEClassifier, eClassifiers
from .bird_model import name, nsURI, nsPrefix, eClass
from .bird_model import DomainModule, MemberHierarchyModule, MemberModule, VariableModule, SubDomainModule, SMCubesCoreModel, DOMAIN, FACET_COLLECTION, FACET_ENUMERATION, facet_type, FACET_VALUE_TYPE, FACET_VALUE_TYPEObject, MAINTENANCE_AGENCY, MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE, SUBDOMAIN, SUBDOMAIN_ENUMERATION, VARIABLE, VARIABLE_SET, VARIABLE_SET_ENUMERATION, Attribute, RelationshipAttribute, OneToOneRelationshipAttribute, ManyToOneRelationshipAttribute, OneToManyRelationshipAttribute, ManyToManyRelationshipAttribute, Classifier, Element, NamedElement, EntityModule, StructuralFeature, TypedElement, GeneratedEntity, Entity, DerivedEntity, BasicEntity, Module, ModuleLongName, ModuleDependencies, ModuleDependency, BIRDModel, Concept, EnumMember


from . import bird_model

__all__ = ['DomainModule', 'MemberHierarchyModule', 'MemberModule', 'VariableModule', 'SubDomainModule', 'SMCubesCoreModel', 'DOMAIN', 'FACET_COLLECTION', 'FACET_ENUMERATION', 'facet_type', 'FACET_VALUE_TYPE', 'FACET_VALUE_TYPEObject', 'MAINTENANCE_AGENCY', 'MEMBER', 'MEMBER_HIERARCHY', 'MEMBER_HIERARCHY_NODE', 'SUBDOMAIN', 'SUBDOMAIN_ENUMERATION', 'VARIABLE', 'VARIABLE_SET', 'VARIABLE_SET_ENUMERATION', 'Attribute',
           'RelationshipAttribute', 'OneToOneRelationshipAttribute', 'ManyToOneRelationshipAttribute', 'OneToManyRelationshipAttribute', 'ManyToManyRelationshipAttribute', 'Classifier', 'Element', 'NamedElement', 'EntityModule', 'StructuralFeature', 'TypedElement', 'GeneratedEntity', 'Entity', 'DerivedEntity', 'BasicEntity', 'Module', 'ModuleLongName', 'ModuleDependencies', 'ModuleDependency', 'BIRDModel', 'Concept', 'EnumMember']

eSubpackages = []
eSuperPackage = None
bird_model.eSubpackages = eSubpackages
bird_model.eSuperPackage = eSuperPackage

DomainModule.domains.eType = DOMAIN
MemberHierarchyModule.memberHierarchies.eType = MEMBER_HIERARCHY
MemberHierarchyModule.memberHierarchiesNodes.eType = MEMBER_HIERARCHY_NODE
MemberModule.members.eType = MEMBER
VariableModule.variables.eType = VARIABLE
SubDomainModule.subdomains.eType = SUBDOMAIN
SMCubesCoreModel.variableModules.eType = VariableModule
SMCubesCoreModel.subDomainModules.eType = SubDomainModule
SMCubesCoreModel.domainModules.eType = DomainModule
SMCubesCoreModel.memberModules.eType = MemberModule
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
Attribute.variable.eType = VARIABLE
RelationshipAttribute.entity.eType = Entity
EntityModule.entities.eType = Entity
TypedElement.classifier.eType = Classifier
GeneratedEntity.attributes.eType = Attribute
DerivedEntity.attributes.eType = Attribute
BasicEntity.attributes.eType = Attribute
BasicEntity.superClass.eType = BasicEntity
Module.dependencies.eType = ModuleDependencies
Module.longName.eType = ModuleLongName
ModuleDependencies.theModules.eType = ModuleDependency
ModuleDependency.theModule.eType = Module
ModuleDependency.longName.eType = ModuleLongName
BIRDModel.entityModule.eType = EntityModule
BIRDModel.smcubesCoreModel.eType = SMCubesCoreModel

otherClassifiers = [FACET_VALUE_TYPE, FACET_VALUE_TYPEObject]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
