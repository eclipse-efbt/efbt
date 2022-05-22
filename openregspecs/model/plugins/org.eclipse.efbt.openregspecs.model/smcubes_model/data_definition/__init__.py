
from .data_definition import getEClassifier, eClassifiers
from .data_definition import name, nsURI, nsPrefix, eClass
from .data_definition import COMBINATION, COMBINATION_ITEM, CUBE, CUBE_GROUP, CUBE_GROUP_ENUMERATION, CUBE_HIERARCHY, CUBE_HIERARCHY_NODE, CUBE_RELATIONSHIP, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, FRAMEWORK, FRAMEWORK_SUBDOMAIN, FRAMEWORK_VARIABLE_SET, TYP_DMNSN, TYP_DMNSNObject, TYP_RL, TYP_RLObject

from core import MAINTENANCE_AGENCY, SUBDOMAIN, VARIABLE_SET, VARIABLE, MEMBER

from . import data_definition

__all__ = ['COMBINATION', 'COMBINATION_ITEM', 'CUBE', 'CUBE_GROUP', 'CUBE_GROUP_ENUMERATION', 'CUBE_HIERARCHY', 'CUBE_HIERARCHY_NODE', 'CUBE_RELATIONSHIP', 'CUBE_STRUCTURE',
           'CUBE_STRUCTURE_ITEM', 'CUBE_TO_COMBINATION', 'FRAMEWORK', 'FRAMEWORK_SUBDOMAIN', 'FRAMEWORK_VARIABLE_SET', 'TYP_DMNSN', 'TYP_DMNSNObject', 'TYP_RL', 'TYP_RLObject']

eSubpackages = []
eSuperPackage = None
data_definition.eSubpackages = eSubpackages
data_definition.eSuperPackage = eSuperPackage

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

otherClassifiers = [TYP_DMNSN, TYP_DMNSNObject, TYP_RL, TYP_RLObject]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
