"""Definition of meta model 'bird_model'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = 'bird_model'
nsURI = 'http://www.eclipse.org/efbt/bird_model'
nsPrefix = 'bird_model'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)
FACET_VALUE_TYPE = EEnum('FACET_VALUE_TYPE', literals=['BigInteger', 'Boolean', 'DateTime', 'DayMonthDayMonth', 'Decimal', 'Double',
                         'Duration', 'Float', 'GregorianDay', 'GregorianMonth', 'GregorianYear', 'Integer', 'Long', 'Short', 'String', 'Time', 'URI'])


FACET_VALUE_TYPEObject = EDataType(
    'FACET_VALUE_TYPEObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')


class SMCubesCoreModel(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    variableModules = EReference(ordered=True, unique=True,
                                 containment=True, derived=False, upper=-1)
    subDomainModules = EReference(ordered=True, unique=True,
                                  containment=True, derived=False, upper=-1)
    domainModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    memberModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, name=None, variableModules=None, subDomainModules=None, domainModules=None, memberModules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if variableModules:
            self.variableModules.extend(variableModules)

        if subDomainModules:
            self.subDomainModules.extend(subDomainModules)

        if domainModules:
            self.domainModules.extend(domainModules)

        if memberModules:
            self.memberModules.extend(memberModules)


class DOMAIN(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    data_type = EAttribute(eType=FACET_VALUE_TYPE, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    domain_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_enumerated = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    is_reference = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    facet_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, data_type=None, description=None, domain_id=None, facet_id=None, is_enumerated=None, is_reference=None, maintenance_agency_id=None, name=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if data_type is not None:
            self.data_type = data_type

        if description is not None:
            self.description = description

        if domain_id is not None:
            self.domain_id = domain_id

        if is_enumerated is not None:
            self.is_enumerated = is_enumerated

        if is_reference is not None:
            self.is_reference = is_reference

        if name is not None:
            self.name = name

        if displayName is not None:
            self.displayName = displayName

        if facet_id is not None:
            self.facet_id = facet_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class FACET_COLLECTION(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    facet_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    facet_value_type = EAttribute(eType=FACET_VALUE_TYPE, unique=True,
                                  derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, facet_id=None, facet_value_type=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if facet_id is not None:
            self.facet_id = facet_id

        if facet_value_type is not None:
            self.facet_value_type = facet_value_type

        if name is not None:
            self.name = name

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class FACET_ENUMERATION(EObject, metaclass=MetaEClass):

    observation_value = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    facet_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    facet_type = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, facet_id=None, facet_type=None, observation_value=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if observation_value is not None:
            self.observation_value = observation_value

        if facet_id is not None:
            self.facet_id = facet_id

        if facet_type is not None:
            self.facet_type = facet_type


class facet_type(EObject, metaclass=MetaEClass):

    decimals = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    endTime = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    endValue = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    interval = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    isSequence = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maxLength = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    maxValue = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    minLength = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    minValue = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    pattern = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    startTime = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    startValue = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    timeInterval = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, decimals=None, endTime=None, endValue=None, interval=None, isSequence=None, maxLength=None, maxValue=None, minLength=None, minValue=None, pattern=None, startTime=None, startValue=None, timeInterval=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if decimals is not None:
            self.decimals = decimals

        if endTime is not None:
            self.endTime = endTime

        if endValue is not None:
            self.endValue = endValue

        if interval is not None:
            self.interval = interval

        if isSequence is not None:
            self.isSequence = isSequence

        if maxLength is not None:
            self.maxLength = maxLength

        if maxValue is not None:
            self.maxValue = maxValue

        if minLength is not None:
            self.minLength = minLength

        if minValue is not None:
            self.minValue = minValue

        if pattern is not None:
            self.pattern = pattern

        if startTime is not None:
            self.startTime = startTime

        if startValue is not None:
            self.startValue = startValue

        if timeInterval is not None:
            self.timeInterval = timeInterval


class MAINTENANCE_AGENCY(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

    def __init__(self, *, code=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if name is not None:
            self.name = name


class MEMBER_HIERARCHY(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    member_hierarchy_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, member_hierarchy_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if member_hierarchy_id is not None:
            self.member_hierarchy_id = member_hierarchy_id

        if name is not None:
            self.name = name

        if domain_id is not None:
            self.domain_id = domain_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class MEMBER_HIERARCHY_NODE(EObject, metaclass=MetaEClass):

    comparator = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    level = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    operator = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    member_hierarchy_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    member_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    parent_member_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, comparator=None, level=None, member_hierarchy_id=None, member_id=None, operator=None, parent_member_id=None, valid_from=None, valid_to=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if comparator is not None:
            self.comparator = comparator

        if level is not None:
            self.level = level

        if operator is not None:
            self.operator = operator

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if member_hierarchy_id is not None:
            self.member_hierarchy_id = member_hierarchy_id

        if member_id is not None:
            self.member_id = member_id

        if parent_member_id is not None:
            self.parent_member_id = parent_member_id


class SUBDOMAIN_ENUMERATION(EObject, metaclass=MetaEClass):

    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    member_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, member_id=None, order=None, valid_from=None, valid_to=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if order is not None:
            self.order = order

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if member_id is not None:
            self.member_id = member_id


class VARIABLE_SET(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    variable_set_id = EAttribute(eType=EString, unique=True,
                                 derived=False, changeable=True, iD=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, maintenance_agency_id=None, name=None, variable_set_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class VARIABLE_SET_ENUMERATION(EObject, metaclass=MetaEClass):

    is_flow = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    subdomain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_set_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, is_flow=None, order=None, subdomain_id=None, valid_from=None, valid_to=None, variable_id=None, variable_set_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if is_flow is not None:
            self.is_flow = is_flow

        if order is not None:
            self.order = order

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id

        if variable_id is not None:
            self.variable_id = variable_id

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id


@abstract
class Element(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


class Module(EObject, metaclass=MetaEClass):
    """A grouping of related artifacts, persisted as a resource, with a version number, and a description, and a description of which Modules it depends upon (and importantly which version of those Modules)."""
    theDescription = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    license = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    dependencies = EReference(ordered=True, unique=True, containment=True, derived=False)
    longName = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, dependencies=None, theDescription=None, license=None, name=None, version=None, longName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if theDescription is not None:
            self.theDescription = theDescription

        if license is not None:
            self.license = license

        if name is not None:
            self.name = name

        if version is not None:
            self.version = version

        if dependencies is not None:
            self.dependencies = dependencies

        if longName is not None:
            self.longName = longName


class ModuleLongName(EObject, metaclass=MetaEClass):
    """A long name associated with a module"""
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class ModuleDependencies(EObject, metaclass=MetaEClass):
    """a  list of module dependencies"""
    theModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, theModules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if theModules:
            self.theModules.extend(theModules)


class ModuleDependency(EObject, metaclass=MetaEClass):
    """A module dependency, including its version number and its long name"""
    moduleName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    moduleVersion = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    theModule = EReference(ordered=True, unique=True, containment=False, derived=False)
    longName = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, moduleName=None, moduleVersion=None, theModule=None, longName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if moduleName is not None:
            self.moduleName = moduleName

        if moduleVersion is not None:
            self.moduleVersion = moduleVersion

        if theModule is not None:
            self.theModule = theModule

        if longName is not None:
            self.longName = longName


class BIRDModel(EObject, metaclass=MetaEClass):

    entityModule = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    smcubesCoreModel = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, entityModule=None, smcubesCoreModel=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if entityModule:
            self.entityModule.extend(entityModule)

        if smcubesCoreModel is not None:
            self.smcubesCoreModel = smcubesCoreModel


class Concept(EObject, metaclass=MetaEClass):

    conceptName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, conceptName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if conceptName is not None:
            self.conceptName = conceptName


class EnumMember(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


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


class MEMBER(EnumMember):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    member_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, member_id=None, name=None, displayName=None, **kwargs):

        super().__init__(**kwargs)

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if member_id is not None:
            self.member_id = member_id

        if name is not None:
            self.name = name

        if displayName is not None:
            self.displayName = displayName

        if domain_id is not None:
            self.domain_id = domain_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class VARIABLE(Concept):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    primary_concept = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    variable_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, primary_concept=None, variable_id=None, name=None, displayName=None, **kwargs):

        super().__init__(**kwargs)

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if primary_concept is not None:
            self.primary_concept = primary_concept

        if variable_id is not None:
            self.variable_id = variable_id

        if name is not None:
            self.name = name

        if displayName is not None:
            self.displayName = displayName

        if domain_id is not None:
            self.domain_id = domain_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


@abstract
class NamedElement(Element):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, name=None, **kwargs):

        super().__init__(**kwargs)

        if name is not None:
            self.name = name


class EntityModule(Module):

    nsURI = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    nsPrefix = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    entities = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, nsURI=None, nsPrefix=None, entities=None, **kwargs):

        super().__init__(**kwargs)

        if nsURI is not None:
            self.nsURI = nsURI

        if nsPrefix is not None:
            self.nsPrefix = nsPrefix

        if entities:
            self.entities.extend(entities)


@abstract
class Classifier(NamedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class TypedElement(NamedElement):

    classifier = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, classifier=None, **kwargs):

        super().__init__(**kwargs)

        if classifier is not None:
            self.classifier = classifier


@abstract
class Entity(NamedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class SUBDOMAIN(Classifier):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_listed = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    is_natural = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    subdomain_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    facet_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    items = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, code=None, description=None, domain_id=None, facet_id=None, is_listed=None, is_natural=None, maintenance_agency_id=None, subdomain_id=None, items=None, **kwargs):

        super().__init__(**kwargs)

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if is_listed is not None:
            self.is_listed = is_listed

        if is_natural is not None:
            self.is_natural = is_natural

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id

        if domain_id is not None:
            self.domain_id = domain_id

        if facet_id is not None:
            self.facet_id = facet_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if items:
            self.items.extend(items)


@abstract
class StructuralFeature(TypedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class GeneratedEntity(Entity):

    attributes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, attributes=None, **kwargs):

        super().__init__(**kwargs)

        if attributes:
            self.attributes.extend(attributes)


class DerivedEntity(Entity):

    attributes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, attributes=None, **kwargs):

        super().__init__(**kwargs)

        if attributes:
            self.attributes.extend(attributes)


class BasicEntity(Entity):

    attributes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    superClass = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attributes=None, superClass=None, **kwargs):

        super().__init__(**kwargs)

        if attributes:
            self.attributes.extend(attributes)

        if superClass is not None:
            self.superClass = superClass


class Attribute(StructuralFeature):

    isPK = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    ordered = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    variable = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, isPK=None, variable=None, ordered=None, **kwargs):

        super().__init__(**kwargs)

        if isPK is not None:
            self.isPK = isPK

        if ordered is not None:
            self.ordered = ordered

        if variable is not None:
            self.variable = variable


class RelationshipAttribute(Attribute):

    containment = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    mandatory = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    dominant = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    entity = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, entity=None, containment=None, mandatory=None, dominant=None, **kwargs):

        super().__init__(**kwargs)

        if containment is not None:
            self.containment = containment

        if mandatory is not None:
            self.mandatory = mandatory

        if dominant is not None:
            self.dominant = dominant

        if entity is not None:
            self.entity = entity


class OneToOneRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ManyToOneRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class OneToManyRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ManyToManyRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
