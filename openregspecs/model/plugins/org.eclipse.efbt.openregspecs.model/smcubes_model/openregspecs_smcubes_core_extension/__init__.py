
from .openregspecs_smcubes_core_extension import getEClassifier, eClassifiers
from .openregspecs_smcubes_core_extension import name, nsURI, nsPrefix, eClass
from .openregspecs_smcubes_core_extension import DomainModule, MemberHierarchyModule, MemberModule, VariableModule, SubDomainModule, SMCubesCoreModel

from core import MEMBER, DOMAIN, MEMBER_HIERARCHY, VARIABLE, SUBDOMAIN, MEMBER_HIERARCHY_NODE

from . import openregspecs_smcubes_core_extension

__all__ = ['DomainModule', 'MemberHierarchyModule', 'MemberModule',
           'VariableModule', 'SubDomainModule', 'SMCubesCoreModel']

eSubpackages = []
eSuperPackage = None
openregspecs_smcubes_core_extension.eSubpackages = eSubpackages
openregspecs_smcubes_core_extension.eSuperPackage = eSuperPackage

DomainModule.domains.eType = DOMAIN
MemberHierarchyModule.memberHierarchies.eType = MEMBER_HIERARCHY
MemberHierarchyModule.memberHierarchiesNodes.eType = MEMBER_HIERARCHY_NODE
MemberModule.members.eType = MEMBER
VariableModule.variables.eType = VARIABLE
SubDomainModule.subdomains.eType = SUBDOMAIN
SMCubesCoreModel.domainModules.eType = DomainModule
SMCubesCoreModel.memberModules.eType = MemberModule
SMCubesCoreModel.variableModules.eType = VariableModule
SMCubesCoreModel.subDomainModules.eType = SubDomainModule
SMCubesCoreModel.memberHierarchyModules.eType = MemberHierarchyModule

otherClassifiers = []

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
