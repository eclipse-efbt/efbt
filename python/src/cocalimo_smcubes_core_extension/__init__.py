
from .cocalimo_smcubes_core_extension import getEClassifier, eClassifiers
from .cocalimo_smcubes_core_extension import name, nsURI, nsPrefix, eClass
from .cocalimo_smcubes_core_extension import DomainModule, MemberHierarchyModule, MemberModule, VariableModule, SubDomainModule, SMCubesCoreModel

from core import MEMBER, DOMAIN, SUBDOMAIN, MEMBER_HIERARCHY, VARIABLE, MEMBER_HIERARCHY_NODE

from . import cocalimo_smcubes_core_extension

__all__ = ['DomainModule', 'MemberHierarchyModule', 'MemberModule',
           'VariableModule', 'SubDomainModule', 'SMCubesCoreModel']

eSubpackages = []
eSuperPackage = None
cocalimo_smcubes_core_extension.eSubpackages = eSubpackages
cocalimo_smcubes_core_extension.eSuperPackage = eSuperPackage

DomainModule.domains.eType = DOMAIN
MemberHierarchyModule.memberHierarchies.eType = MEMBER_HIERARCHY
MemberHierarchyModule.memberHierarchiesNodes.eType = MEMBER_HIERARCHY_NODE
MemberModule.members.eType = MEMBER
VariableModule.variables.eType = VARIABLE
SubDomainModule.subdomains.eType = SUBDOMAIN

otherClassifiers = []

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
