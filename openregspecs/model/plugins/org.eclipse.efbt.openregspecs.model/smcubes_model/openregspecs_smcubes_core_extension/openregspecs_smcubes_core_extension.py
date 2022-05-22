"""Definition of meta model 'openregspecs_smcubes_core_extension'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *
from module_management import Module


name = 'openregspecs_smcubes_core_extension'
nsURI = 'http://www.eclipse.org/efbt/openregspecs_smcubes_core_extension'
nsPrefix = 'openregspecs_smcubes_core_extension'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)


class SMCubesCoreModel(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    domainModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    memberModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    variableModules = EReference(ordered=True, unique=True,
                                 containment=True, derived=False, upper=-1)
    subDomainModules = EReference(ordered=True, unique=True,
                                  containment=True, derived=False, upper=-1)
    memberHierarchyModules = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, name=None, domainModules=None, memberModules=None, variableModules=None, subDomainModules=None, memberHierarchyModules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if domainModules:
            self.domainModules.extend(domainModules)

        if memberModules:
            self.memberModules.extend(memberModules)

        if variableModules:
            self.variableModules.extend(variableModules)

        if subDomainModules:
            self.subDomainModules.extend(subDomainModules)

        if memberHierarchyModules:
            self.memberHierarchyModules.extend(memberHierarchyModules)


class DomainModule(Module):
    """A Module storing a list of Domains"""
    domains = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, domains=None, **kwargs):

        super().__init__(**kwargs)

        if domains:
            self.domains.extend(domains)


class MemberHierarchyModule(Module):
    """A Module storing a list of MemberHierarchies and MemberHierarchiesNodes"""
    memberHierarchies = EReference(ordered=True, unique=True,
                                   containment=True, derived=False, upper=-1)
    memberHierarchiesNodes = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, memberHierarchies=None, memberHierarchiesNodes=None, **kwargs):

        super().__init__(**kwargs)

        if memberHierarchies:
            self.memberHierarchies.extend(memberHierarchies)

        if memberHierarchiesNodes:
            self.memberHierarchiesNodes.extend(memberHierarchiesNodes)


class MemberModule(Module):
    """A Module storing a list of Members"""
    members = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, members=None, **kwargs):

        super().__init__(**kwargs)

        if members:
            self.members.extend(members)


class VariableModule(Module):
    """A Module containing a list of variables"""
    variables = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, variables=None, **kwargs):

        super().__init__(**kwargs)

        if variables:
            self.variables.extend(variables)


class SubDomainModule(Module):

    subdomains = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, subdomains=None, **kwargs):

        super().__init__(**kwargs)

        if subdomains:
            self.subdomains.extend(subdomains)
