"""Definition of meta model 'core'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *
from data_meta_model import Concept, EnumMember, Classifier


name = 'core'
nsURI = 'http://www.eclipse.org/efbt/core'
nsPrefix = 'core'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)
FACET_VALUE_TYPE = EEnum('FACET_VALUE_TYPE', literals=['BigInteger', 'Boolean', 'DateTime', 'DayMonthDayMonth', 'Decimal', 'Double',
                         'Duration', 'Float', 'GregorianDay', 'GregorianMonth', 'GregorianYear', 'Integer', 'Long', 'Short', 'String', 'Time', 'URI'])


FACET_VALUE_TYPEObject = EDataType(
    'FACET_VALUE_TYPEObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')


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
