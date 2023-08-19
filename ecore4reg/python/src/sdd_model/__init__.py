
from .sdd_model import getEClassifier, eClassifiers
from .sdd_model import name, nsURI, nsPrefix, eClass
from .sdd_model import SDDModule, DOMAIN, FACET_COLLECTION, FACET_ENUMERATION, facet_type, FACET_VALUE_TYPE, FACET_VALUE_TYPEObject, MAINTENANCE_AGENCY, MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE, SUBDOMAIN, SUBDOMAIN_ENUMERATION, VARIABLE, VARIABLE_SET, VARIABLE_SET_ENUMERATION, COMBINATION, COMBINATION_ITEM, CUBE, CUBE_GROUP, CUBE_GROUP_ENUMERATION, CUBE_HIERARCHY, CUBE_HIERARCHY_NODE, CUBE_RELATIONSHIP, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, FRAMEWORK, FRAMEWORK_SUBDOMAIN, FRAMEWORK_VARIABLE_SET, TYP_DMNSN, TYP_DMNSNObject, TYP_RL, TYP_RLObject, CUBE_MAPPING, MEMBER_MAPPING, MEMBER_MAPPING_ITEM, VARIABLE_MAPPING_ITEM, VARIABLE_MAPPING, MAPPING_TO_CUBE, VARIABLE_SET_MAPPING, MAPPING_DEFINITION, AXIS, AXIS_ORDINATE, CELL_POSITION, ORDINATE_ITEM, TABLE, TABLE_CELL, DomainModule, MemberHierarchyModule, MemberModule, VariableModule, VariableSetModule, SubDomainModule, SMCubesCoreModel, SMCubesModel, SMCubesExtraModel, CombinationModule, CubeModule, MappingDefinitionModule, CubeMappingModule, MemberMappingModule, VariableMappingModule, AxisModule, AxisOrdinateModule, CellPositionModule, OrdinateItemModule, ReportTableModule, TableCellModule


from . import sdd_model

__all__ = ['SDDModule', 'DOMAIN', 'FACET_COLLECTION', 'FACET_ENUMERATION', 'facet_type', 'FACET_VALUE_TYPE', 'FACET_VALUE_TYPEObject', 'MAINTENANCE_AGENCY', 'MEMBER', 'MEMBER_HIERARCHY', 'MEMBER_HIERARCHY_NODE', 'SUBDOMAIN', 'SUBDOMAIN_ENUMERATION', 'VARIABLE', 'VARIABLE_SET', 'VARIABLE_SET_ENUMERATION', 'COMBINATION', 'COMBINATION_ITEM', 'CUBE', 'CUBE_GROUP', 'CUBE_GROUP_ENUMERATION', 'CUBE_HIERARCHY', 'CUBE_HIERARCHY_NODE', 'CUBE_RELATIONSHIP', 'CUBE_STRUCTURE', 'CUBE_STRUCTURE_ITEM', 'CUBE_TO_COMBINATION', 'FRAMEWORK', 'FRAMEWORK_SUBDOMAIN', 'FRAMEWORK_VARIABLE_SET', 'TYP_DMNSN', 'TYP_DMNSNObject', 'TYP_RL', 'TYP_RLObject', 'CUBE_MAPPING',
           'MEMBER_MAPPING', 'MEMBER_MAPPING_ITEM', 'VARIABLE_MAPPING_ITEM', 'VARIABLE_MAPPING', 'MAPPING_TO_CUBE', 'VARIABLE_SET_MAPPING', 'MAPPING_DEFINITION', 'AXIS', 'AXIS_ORDINATE', 'CELL_POSITION', 'ORDINATE_ITEM', 'TABLE', 'TABLE_CELL', 'DomainModule', 'MemberHierarchyModule', 'MemberModule', 'VariableModule', 'VariableSetModule', 'SubDomainModule', 'SMCubesCoreModel', 'SMCubesModel', 'SMCubesExtraModel', 'CombinationModule', 'CubeModule', 'MappingDefinitionModule', 'CubeMappingModule', 'MemberMappingModule', 'VariableMappingModule', 'AxisModule', 'AxisOrdinateModule', 'CellPositionModule', 'OrdinateItemModule', 'ReportTableModule', 'TableCellModule']

eSubpackages = []
eSuperPackage = None
sdd_model.eSubpackages = eSubpackages
sdd_model.eSuperPackage = eSuperPackage

SDDModule.dependencies.eType = SDDModule
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
VARIABLE_SET.variable_set_items.eType = VARIABLE_SET_ENUMERATION
VARIABLE_SET_ENUMERATION.subdomain_id.eType = SUBDOMAIN
VARIABLE_SET_ENUMERATION.variable_id.eType = VARIABLE
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
CUBE_MAPPING.source_cube.eType = CUBE
CUBE_MAPPING.target_cube.eType = CUBE
CUBE_MAPPING.mappingsToCube.eType = MAPPING_TO_CUBE
MEMBER_MAPPING.memberMappingItems.eType = MEMBER_MAPPING_ITEM
MEMBER_MAPPING_ITEM.member.eType = MEMBER
MEMBER_MAPPING_ITEM.variable.eType = VARIABLE
VARIABLE_MAPPING_ITEM.variable.eType = VARIABLE
VARIABLE_MAPPING.varaibleMappingItems.eType = VARIABLE_MAPPING_ITEM
MAPPING_TO_CUBE.mapping.eType = MAPPING_DEFINITION
VARIABLE_SET_MAPPING.sourceMapping.eType = MAPPING_DEFINITION
VARIABLE_SET_MAPPING.targetMapping.eType = MAPPING_DEFINITION
MAPPING_DEFINITION.memberMapping.eType = MEMBER_MAPPING
MAPPING_DEFINITION.variableMapping.eType = VARIABLE_MAPPING
AXIS.table_id.eType = TABLE
AXIS_ORDINATE.axis_id.eType = AXIS
AXIS_ORDINATE.parent_axis_ordinate_id.eType = AXIS_ORDINATE
CELL_POSITION.axis_ordinate_id.eType = AXIS_ORDINATE
CELL_POSITION.cell_id.eType = TABLE_CELL
ORDINATE_ITEM.axis_ordinate_id.eType = AXIS_ORDINATE
ORDINATE_ITEM.member_hierarchy_id.eType = MEMBER_HIERARCHY
ORDINATE_ITEM.member_id.eType = MEMBER
ORDINATE_ITEM.starting_member_id.eType = MEMBER
ORDINATE_ITEM.variable_id.eType = VARIABLE
TABLE.maintenance_agency_id.eType = MAINTENANCE_AGENCY
TABLE_CELL.combination_id.eType = COMBINATION
TABLE_CELL.table_id.eType = TABLE
DomainModule.domains.eType = DOMAIN
MemberHierarchyModule.memberHierarchies.eType = MEMBER_HIERARCHY
MemberHierarchyModule.memberHierarchiesNodes.eType = MEMBER_HIERARCHY_NODE
MemberModule.members.eType = MEMBER
VariableModule.variables.eType = VARIABLE
VariableSetModule.variableSets.eType = VARIABLE_SET
SubDomainModule.subdomains.eType = SUBDOMAIN
SMCubesCoreModel.variableModules.eType = VariableModule
SMCubesCoreModel.variableSetModules.eType = VariableSetModule
SMCubesCoreModel.domainModules.eType = DomainModule
SMCubesCoreModel.memberModules.eType = MemberModule
SMCubesCoreModel.subDomainModules.eType = SubDomainModule
SMCubesCoreModel.memberHierarchyModules.eType = MemberHierarchyModule
SMCubesModel.coreModel.eType = SMCubesCoreModel
SMCubesModel.extraModel.eType = SMCubesExtraModel
SMCubesExtraModel.combinationModules.eType = CombinationModule
SMCubesExtraModel.cubeModules.eType = CubeModule
SMCubesExtraModel.mappingDefinitionModules.eType = MappingDefinitionModule
SMCubesExtraModel.cubeMappingModules.eType = CubeMappingModule
SMCubesExtraModel.memberMappingModules.eType = MemberMappingModule
SMCubesExtraModel.variableMappingModules.eType = VariableMappingModule
SMCubesExtraModel.axisModules.eType = AxisModule
SMCubesExtraModel.axisOrdinateModules.eType = AxisOrdinateModule
SMCubesExtraModel.cellPositionModules.eType = CellPositionModule
SMCubesExtraModel.ordinateItemsModules.eType = OrdinateItemModule
SMCubesExtraModel.reportTableModules.eType = ReportTableModule
SMCubesExtraModel.tableCellModules.eType = TableCellModule
CombinationModule.combinations.eType = COMBINATION
CombinationModule.combination_items.eType = COMBINATION_ITEM
CombinationModule.cubeToCombinationLinks.eType = CUBE_TO_COMBINATION
CubeModule.cubes.eType = CUBE
CubeModule.cubeStructures.eType = CUBE_STRUCTURE
CubeModule.cubeStructureItems.eType = CUBE_STRUCTURE_ITEM
MappingDefinitionModule.mappings.eType = MAPPING_DEFINITION
CubeMappingModule.cubeMappings.eType = CUBE_MAPPING
MemberMappingModule.memberMappings.eType = MEMBER_MAPPING
VariableMappingModule.variableMappings.eType = VARIABLE_MAPPING
AxisModule.axes.eType = AXIS
AxisOrdinateModule.axis_ordinates.eType = AXIS_ORDINATE
CellPositionModule.cellPositions.eType = CELL_POSITION
OrdinateItemModule.ordinateItems.eType = ORDINATE_ITEM
ReportTableModule.reportTables.eType = TABLE
TableCellModule.tableCells.eType = TABLE_CELL

otherClassifiers = [FACET_VALUE_TYPE, FACET_VALUE_TYPEObject,
                    TYP_DMNSN, TYP_DMNSNObject, TYP_RL, TYP_RLObject]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
