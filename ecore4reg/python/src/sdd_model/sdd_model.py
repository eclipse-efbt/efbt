"""Definition of meta model 'sdd_model'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = 'sdd_model'
nsURI = 'http://www.eclipse.org/efbt/sdd_model'
nsPrefix = 'sdd_model'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)
FACET_VALUE_TYPE = EEnum('FACET_VALUE_TYPE', literals=['BigInteger', 'Boolean', 'DateTime', 'DayMonthDayMonth', 'Decimal', 'Double',
                         'Duration', 'Float', 'GregorianDay', 'GregorianMonth', 'GregorianYear', 'Integer', 'Long', 'Short', 'String', 'Time', 'URI'])

TYP_DMNSN = EEnum('TYP_DMNSN', literals=['B', 'M', 'T', 'U'])

TYP_RL = EEnum('TYP_RL', literals=['O', 'A', 'D'])


FACET_VALUE_TYPEObject = EDataType(
    'FACET_VALUE_TYPEObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')

TYP_DMNSNObject = EDataType(
    'TYP_DMNSNObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')

TYP_RLObject = EDataType('TYP_RLObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')


class SDDModule(EObject, metaclass=MetaEClass):

    theDescription = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    license = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    dependencies = EReference(ordered=True, unique=True, containment=False, derived=False, upper=-1)

    def __init__(self, *, dependencies=None, theDescription=None, license=None, name=None, version=None):
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

        if dependencies:
            self.dependencies.extend(dependencies)


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


class MEMBER(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    member_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, member_id=None, name=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

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


class SUBDOMAIN(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_listed = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    is_natural = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    subdomain_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    facet_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    items = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, code=None, description=None, domain_id=None, facet_id=None, is_listed=None, is_natural=None, maintenance_agency_id=None, subdomain_id=None, items=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

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


class VARIABLE(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    primary_concept = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    variable_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, primary_concept=None, variable_id=None, name=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

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


class COMBINATION(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    combination_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    combination_items = EReference(ordered=True, unique=True,
                                   containment=True, derived=False, upper=-1)

    def __init__(self, *, code=None, combination_id=None, maintenance_agency_id=None, name=None, valid_from=None, valid_to=None, version=None, combination_items=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if combination_id is not None:
            self.combination_id = combination_id

        if name is not None:
            self.name = name

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if combination_items:
            self.combination_items.extend(combination_items)


class COMBINATION_ITEM(EObject, metaclass=MetaEClass):

    member_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    subdomain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_set_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, member_id=None, subdomain_id=None, variable_id=None, variable_set_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if member_id is not None:
            self.member_id = member_id

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id

        if variable_id is not None:
            self.variable_id = variable_id

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id


class CUBE(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_type = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_allowed = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    published = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_structure_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_id=None, cube_structure_id=None, cube_type=None, description=None, framework_id=None, is_allowed=None, maintenance_agency_id=None, name=None, published=None, valid_from=None, valid_to=None, version=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_id is not None:
            self.cube_id = cube_id

        if cube_type is not None:
            self.cube_type = cube_type

        if description is not None:
            self.description = description

        if is_allowed is not None:
            self.is_allowed = is_allowed

        if name is not None:
            self.name = name

        if published is not None:
            self.published = published

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if displayName is not None:
            self.displayName = displayName

        if cube_structure_id is not None:
            self.cube_structure_id = cube_structure_id

        if framework_id is not None:
            self.framework_id = framework_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_GROUP(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_group_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_group_id=None, description=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_group_id is not None:
            self.cube_group_id = cube_group_id

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_GROUP_ENUMERATION(EObject, metaclass=MetaEClass):

    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    cube_group_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, cube_group_id=None, cube_id=None, order=None, valid_from=None, valid_to=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if order is not None:
            self.order = order

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if cube_group_id is not None:
            self.cube_group_id = cube_group_id

        if cube_id is not None:
            self.cube_id = cube_id


class CUBE_HIERARCHY(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_hierarchy_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_hierarchy_type = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_hierarchy_id=None, cube_hierarchy_type=None, framework_id=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_hierarchy_id is not None:
            self.cube_hierarchy_id = cube_hierarchy_id

        if cube_hierarchy_type is not None:
            self.cube_hierarchy_type = cube_hierarchy_type

        if name is not None:
            self.name = name

        if framework_id is not None:
            self.framework_id = framework_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_HIERARCHY_NODE(EObject, metaclass=MetaEClass):

    level = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    nODE_CODE = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    node_name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    cube_group_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    cube_hierarchy_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    parent_node_code = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, cube_group_id=None, cube_hierarchy_id=None, level=None, nODE_CODE=None, node_name=None, order=None, parent_node_code=None, valid_from=None, valid_to=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if level is not None:
            self.level = level

        if nODE_CODE is not None:
            self.nODE_CODE = nODE_CODE

        if node_name is not None:
            self.node_name = node_name

        if order is not None:
            self.order = order

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if cube_group_id is not None:
            self.cube_group_id = cube_group_id

        if cube_hierarchy_id is not None:
            self.cube_hierarchy_id = cube_hierarchy_id

        if parent_node_code is not None:
            self.parent_node_code = parent_node_code


class CUBE_RELATIONSHIP(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_relationship_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    establishes_integrity = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    foreign_cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    foreign_cube_variable_code = EReference(
        ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    primary_cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    primary_cube_variable_id = EReference(
        ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_relationship_id=None, description=None, establishes_integrity=None, foreign_cube_id=None, foreign_cube_variable_code=None, maintenance_agency_id=None, name=None, primary_cube_id=None, primary_cube_variable_id=None, valid_from=None, valid_to=None, version=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_relationship_id is not None:
            self.cube_relationship_id = cube_relationship_id

        if description is not None:
            self.description = description

        if establishes_integrity is not None:
            self.establishes_integrity = establishes_integrity

        if name is not None:
            self.name = name

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if foreign_cube_id is not None:
            self.foreign_cube_id = foreign_cube_id

        if foreign_cube_variable_code is not None:
            self.foreign_cube_variable_code = foreign_cube_variable_code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if primary_cube_id is not None:
            self.primary_cube_id = primary_cube_id

        if primary_cube_variable_id is not None:
            self.primary_cube_variable_id = primary_cube_variable_id


class CUBE_STRUCTURE(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_structure_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_structure_id=None, description=None, maintenance_agency_id=None, name=None, valid_from=None, valid_to=None, version=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_structure_id is not None:
            self.cube_structure_id = cube_structure_id

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if displayName is not None:
            self.displayName = displayName

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_STRUCTURE_ITEM(EObject, metaclass=MetaEClass):

    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    dimension_type = EAttribute(eType=TYP_DMNSN, unique=True, derived=False, changeable=True)
    is_flow = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    is_mandatory = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    role = EAttribute(eType=TYP_RL, unique=True, derived=False, changeable=True)
    isIdentifier = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    cube_variable_code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    attribute_associated_variable = EReference(
        ordered=True, unique=True, containment=False, derived=False)
    cube_structure_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    member_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    subdomain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_set_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attribute_associated_variable=None, cube_structure_id=None, description=None, dimension_type=None, is_flow=None, is_mandatory=None, member_id=None, order=None, role=None, subdomain_id=None, variable_id=None, variable_set_id=None, isIdentifier=None, cube_variable_code=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if description is not None:
            self.description = description

        if dimension_type is not None:
            self.dimension_type = dimension_type

        if is_flow is not None:
            self.is_flow = is_flow

        if is_mandatory is not None:
            self.is_mandatory = is_mandatory

        if order is not None:
            self.order = order

        if role is not None:
            self.role = role

        if isIdentifier is not None:
            self.isIdentifier = isIdentifier

        if cube_variable_code is not None:
            self.cube_variable_code = cube_variable_code

        if attribute_associated_variable is not None:
            self.attribute_associated_variable = attribute_associated_variable

        if cube_structure_id is not None:
            self.cube_structure_id = cube_structure_id

        if member_id is not None:
            self.member_id = member_id

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id

        if variable_id is not None:
            self.variable_id = variable_id

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id


class CUBE_TO_COMBINATION(EObject, metaclass=MetaEClass):

    combination_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, combination_id=None, cube_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if combination_id is not None:
            self.combination_id = combination_id

        if cube_id is not None:
            self.cube_id = cube_id


class FRAMEWORK(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    framework_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, framework_id=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if framework_id is not None:
            self.framework_id = framework_id

        if name is not None:
            self.name = name

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class FRAMEWORK_SUBDOMAIN(EObject, metaclass=MetaEClass):

    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    subdomain_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, framework_id=None, subdomain_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if framework_id is not None:
            self.framework_id = framework_id

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id


class FRAMEWORK_VARIABLE_SET(EObject, metaclass=MetaEClass):

    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_set_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, framework_id=None, variable_set_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if framework_id is not None:
            self.framework_id = framework_id

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id


class CUBE_MAPPING(EObject, metaclass=MetaEClass):

    cube_mapping_id = EAttribute(eType=EString, unique=True,
                                 derived=False, changeable=True, iD=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    source_cube = EReference(ordered=True, unique=True, containment=False, derived=False)
    target_cube = EReference(ordered=True, unique=True, containment=False, derived=False)
    mappingsToCube = EReference(ordered=True, unique=True,
                                containment=True, derived=False, upper=-1)

    def __init__(self, *, cube_mapping_id=None, description=None, source_cube=None, target_cube=None, name=None, code=None, maintenance_agency_id=None, mappingsToCube=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if cube_mapping_id is not None:
            self.cube_mapping_id = cube_mapping_id

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if code is not None:
            self.code = code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if source_cube is not None:
            self.source_cube = source_cube

        if target_cube is not None:
            self.target_cube = target_cube

        if mappingsToCube:
            self.mappingsToCube.extend(mappingsToCube)


class MEMBER_MAPPING(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    member_mapping_id = EAttribute(eType=EString, unique=True,
                                   derived=False, changeable=True, iD=True)
    memberMappingItems = EReference(ordered=True, unique=True,
                                    containment=True, derived=False, upper=-1)

    def __init__(self, *, name=None, code=None, maintenance_agency_id=None, member_mapping_id=None, memberMappingItems=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if code is not None:
            self.code = code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if member_mapping_id is not None:
            self.member_mapping_id = member_mapping_id

        if memberMappingItems:
            self.memberMappingItems.extend(memberMappingItems)


class MEMBER_MAPPING_ITEM(EObject, metaclass=MetaEClass):

    isSource = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    row = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    member = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, isSource=None, member=None, variable=None, row=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if isSource is not None:
            self.isSource = isSource

        if row is not None:
            self.row = row

        if member is not None:
            self.member = member

        if variable is not None:
            self.variable = variable


class VARIABLE_MAPPING_ITEM(EObject, metaclass=MetaEClass):

    isSource = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    variable = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, isSource=None, variable=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if isSource is not None:
            self.isSource = isSource

        if variable is not None:
            self.variable = variable


class VARIABLE_MAPPING(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    variable_mapping_id = EAttribute(eType=EString, unique=True,
                                     derived=False, changeable=True, iD=True)
    varaibleMappingItems = EReference(ordered=True, unique=True,
                                      containment=True, derived=False, upper=-1)

    def __init__(self, *, varaibleMappingItems=None, name=None, code=None, maintenance_agency_id=None, variable_mapping_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if code is not None:
            self.code = code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if variable_mapping_id is not None:
            self.variable_mapping_id = variable_mapping_id

        if varaibleMappingItems:
            self.varaibleMappingItems.extend(varaibleMappingItems)


class MAPPING_TO_CUBE(EObject, metaclass=MetaEClass):

    mapping = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, mapping=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if mapping is not None:
            self.mapping = mapping


class VARIABLE_SET_MAPPING(EObject, metaclass=MetaEClass):

    sourceMapping = EReference(ordered=True, unique=True, containment=False, derived=False)
    targetMapping = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, sourceMapping=None, targetMapping=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if sourceMapping is not None:
            self.sourceMapping = sourceMapping

        if targetMapping is not None:
            self.targetMapping = targetMapping


class MAPPING_DEFINITION(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    mapping_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    mapping_type = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    memberMapping = EReference(ordered=True, unique=True, containment=False, derived=False)
    variableMapping = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, name=None, code=None, maintenance_agency_id=None, mapping_id=None, mapping_type=None, memberMapping=None, variableMapping=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if code is not None:
            self.code = code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if mapping_id is not None:
            self.mapping_id = mapping_id

        if mapping_type is not None:
            self.mapping_type = mapping_type

        if memberMapping is not None:
            self.memberMapping = memberMapping

        if variableMapping is not None:
            self.variableMapping = variableMapping


class AXIS(EObject, metaclass=MetaEClass):

    axis_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_open_axis = EAttribute(eType=EBooleanObject, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    orientation = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    table_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, axis_id=None, code=None, description=None, is_open_axis=None, name=None, order=None, orientation=None, table_id=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if axis_id is not None:
            self.axis_id = axis_id

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if is_open_axis is not None:
            self.is_open_axis = is_open_axis

        if name is not None:
            self.name = name

        if order is not None:
            self.order = order

        if orientation is not None:
            self.orientation = orientation

        if displayName is not None:
            self.displayName = displayName

        if table_id is not None:
            self.table_id = table_id


class AXIS_ORDINATE(EObject, metaclass=MetaEClass):

    axis_ordinate_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_abstract_header = EAttribute(eType=EBooleanObject, unique=True,
                                    derived=False, changeable=True)
    level = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    path = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    axis_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    parent_axis_ordinate_id = EReference(
        ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, axis_id=None, axis_ordinate_id=None, code=None, description=None, is_abstract_header=None, level=None, name=None, order=None, parent_axis_ordinate_id=None, path=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if axis_ordinate_id is not None:
            self.axis_ordinate_id = axis_ordinate_id

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if is_abstract_header is not None:
            self.is_abstract_header = is_abstract_header

        if level is not None:
            self.level = level

        if name is not None:
            self.name = name

        if order is not None:
            self.order = order

        if path is not None:
            self.path = path

        if displayName is not None:
            self.displayName = displayName

        if axis_id is not None:
            self.axis_id = axis_id

        if parent_axis_ordinate_id is not None:
            self.parent_axis_ordinate_id = parent_axis_ordinate_id


class CELL_POSITION(EObject, metaclass=MetaEClass):

    axis_ordinate_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    cell_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, axis_ordinate_id=None, cell_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if axis_ordinate_id is not None:
            self.axis_ordinate_id = axis_ordinate_id

        if cell_id is not None:
            self.cell_id = cell_id


class ORDINATE_ITEM(EObject, metaclass=MetaEClass):

    is_starting_member_included = EAttribute(
        eType=EBooleanObject, unique=True, derived=False, changeable=True)
    axis_ordinate_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    member_hierarchy_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    member_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    starting_member_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, axis_ordinate_id=None, is_starting_member_included=None, member_hierarchy_id=None, member_id=None, starting_member_id=None, variable_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if is_starting_member_included is not None:
            self.is_starting_member_included = is_starting_member_included

        if axis_ordinate_id is not None:
            self.axis_ordinate_id = axis_ordinate_id

        if member_hierarchy_id is not None:
            self.member_hierarchy_id = member_hierarchy_id

        if member_id is not None:
            self.member_id = member_id

        if starting_member_id is not None:
            self.starting_member_id = starting_member_id

        if variable_id is not None:
            self.variable_id = variable_id


class TABLE(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    table_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, maintenance_agency_id=None, name=None, table_id=None, valid_from=None, valid_to=None, version=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if table_id is not None:
            self.table_id = table_id

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if displayName is not None:
            self.displayName = displayName

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class TABLE_CELL(EObject, metaclass=MetaEClass):

    cell_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_shaded = EAttribute(eType=EBooleanObject, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    combination_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    table_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, cell_id=None, combination_id=None, is_shaded=None, table_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if cell_id is not None:
            self.cell_id = cell_id

        if is_shaded is not None:
            self.is_shaded = is_shaded

        if name is not None:
            self.name = name

        if combination_id is not None:
            self.combination_id = combination_id

        if table_id is not None:
            self.table_id = table_id


class TRANSFORMATION_SCHEME(EObject, metaclass=MetaEClass):

    transformation_scheme_id = EAttribute(
        eType=EString, unique=True, derived=False, changeable=True, iD=True)
    type = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    tranformations = EReference(ordered=True, unique=True,
                                containment=True, derived=False, upper=-1)

    def __init__(self, *, transformation_scheme_id=None, type=None, description=None, tranformations=None, name=None, code=None, maintenance_agency_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if transformation_scheme_id is not None:
            self.transformation_scheme_id = transformation_scheme_id

        if type is not None:
            self.type = type

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if code is not None:
            self.code = code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if tranformations:
            self.tranformations.extend(tranformations)


class TRANSFORMATION(EObject, metaclass=MetaEClass):

    transformation_id = EAttribute(eType=EString, unique=True,
                                   derived=False, changeable=True, iD=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    expression = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    order_in_scheme = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, transformation_id=None, description=None, expression=None, name=None, code=None, maintenance_agency_id=None, order_in_scheme=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if transformation_id is not None:
            self.transformation_id = transformation_id

        if description is not None:
            self.description = description

        if expression is not None:
            self.expression = expression

        if name is not None:
            self.name = name

        if code is not None:
            self.code = code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if order_in_scheme is not None:
            self.order_in_scheme = order_in_scheme


class SmcubesExtraModel(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    combinationModules = EReference(ordered=True, unique=True,
                                    containment=True, derived=False, upper=-1)
    cubeModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    mappingDefinitionModules = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)
    cubeMappingModules = EReference(ordered=True, unique=True,
                                    containment=True, derived=False, upper=-1)
    memberMappingModules = EReference(ordered=True, unique=True,
                                      containment=True, derived=False, upper=-1)
    variableMappingModules = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)
    axisModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    axisOrdinateModules = EReference(ordered=True, unique=True,
                                     containment=True, derived=False, upper=-1)
    cellPositionModules = EReference(ordered=True, unique=True,
                                     containment=True, derived=False, upper=-1)
    ordinateItemsModules = EReference(ordered=True, unique=True,
                                      containment=True, derived=False, upper=-1)
    reportTableModules = EReference(ordered=True, unique=True,
                                    containment=True, derived=False, upper=-1)
    tableCellModules = EReference(ordered=True, unique=True,
                                  containment=True, derived=False, upper=-1)

    def __init__(self, *, name=None, combinationModules=None, cubeModules=None, mappingDefinitionModules=None, cubeMappingModules=None, memberMappingModules=None, variableMappingModules=None, axisModules=None, axisOrdinateModules=None, cellPositionModules=None, ordinateItemsModules=None, reportTableModules=None, tableCellModules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if combinationModules:
            self.combinationModules.extend(combinationModules)

        if cubeModules:
            self.cubeModules.extend(cubeModules)

        if mappingDefinitionModules:
            self.mappingDefinitionModules.extend(mappingDefinitionModules)

        if cubeMappingModules:
            self.cubeMappingModules.extend(cubeMappingModules)

        if memberMappingModules:
            self.memberMappingModules.extend(memberMappingModules)

        if variableMappingModules:
            self.variableMappingModules.extend(variableMappingModules)

        if axisModules:
            self.axisModules.extend(axisModules)

        if axisOrdinateModules:
            self.axisOrdinateModules.extend(axisOrdinateModules)

        if cellPositionModules:
            self.cellPositionModules.extend(cellPositionModules)

        if ordinateItemsModules:
            self.ordinateItemsModules.extend(ordinateItemsModules)

        if reportTableModules:
            self.reportTableModules.extend(reportTableModules)

        if tableCellModules:
            self.tableCellModules.extend(tableCellModules)


class DomainModule(SDDModule):
    """A Module storing a list of Domains"""
    domains = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, domains=None, **kwargs):

        super().__init__(**kwargs)

        if domains:
            self.domains.extend(domains)


class MemberHierarchyModule(SDDModule):
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


class MemberModule(SDDModule):
    """A Module storing a list of Members"""
    members = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, members=None, **kwargs):

        super().__init__(**kwargs)

        if members:
            self.members.extend(members)


class VariableModule(SDDModule):
    """A Module containing a list of variables"""
    variables = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, variables=None, **kwargs):

        super().__init__(**kwargs)

        if variables:
            self.variables.extend(variables)


class SubDomainModule(SDDModule):

    subdomains = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, subdomains=None, **kwargs):

        super().__init__(**kwargs)

        if subdomains:
            self.subdomains.extend(subdomains)


class SMCubesCoreModel(SDDModule):

    variableModules = EReference(ordered=True, unique=True,
                                 containment=True, derived=False, upper=-1)
    domainModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    memberModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    subDomainModules = EReference(ordered=True, unique=True,
                                  containment=True, derived=False, upper=-1)
    memberHierarchyModules = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, variableModules=None, domainModules=None, memberModules=None, subDomainModules=None, memberHierarchyModules=None, **kwargs):

        super().__init__(**kwargs)

        if variableModules:
            self.variableModules.extend(variableModules)

        if domainModules:
            self.domainModules.extend(domainModules)

        if memberModules:
            self.memberModules.extend(memberModules)

        if subDomainModules:
            self.subDomainModules.extend(subDomainModules)

        if memberHierarchyModules:
            self.memberHierarchyModules.extend(memberHierarchyModules)


class CombinationModule(SDDModule):
    """A module storing a list of Combinations, and a list of related Combination Items, and a list of CUBE_TO_COMBINATIONs"""
    combinations = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    combination_items = EReference(ordered=True, unique=True,
                                   containment=True, derived=False, upper=-1)
    cubeToCombinationLinks = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, combinations=None, combination_items=None, cubeToCombinationLinks=None, **kwargs):

        super().__init__(**kwargs)

        if combinations:
            self.combinations.extend(combinations)

        if combination_items:
            self.combination_items.extend(combination_items)

        if cubeToCombinationLinks:
            self.cubeToCombinationLinks.extend(cubeToCombinationLinks)


class CubeModule(SDDModule):
    """A Module storing a list of CUBEs and a list of CUBE_STRUCTURES and a list of CUBE_STRUCTURE_ITEMS"""
    cubes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    cubeStructures = EReference(ordered=True, unique=True,
                                containment=True, derived=False, upper=-1)
    cubeStructureItems = EReference(ordered=True, unique=True,
                                    containment=True, derived=False, upper=-1)

    def __init__(self, *, cubes=None, cubeStructures=None, cubeStructureItems=None, **kwargs):

        super().__init__(**kwargs)

        if cubes:
            self.cubes.extend(cubes)

        if cubeStructures:
            self.cubeStructures.extend(cubeStructures)

        if cubeStructureItems:
            self.cubeStructureItems.extend(cubeStructureItems)


class MappingDefinitionModule(SDDModule):
    """A Module containing SMCubes MappingDefintitions"""
    mappings = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, mappings=None, **kwargs):

        super().__init__(**kwargs)

        if mappings:
            self.mappings.extend(mappings)


class CubeMappingModule(SDDModule):
    """A Module containing SMCubes CubeMappings"""
    cubeMappings = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, cubeMappings=None, **kwargs):

        super().__init__(**kwargs)

        if cubeMappings:
            self.cubeMappings.extend(cubeMappings)


class MemberMappingModule(SDDModule):
    """A Module containing SMCubes MemberMappings"""
    memberMappings = EReference(ordered=True, unique=True,
                                containment=True, derived=False, upper=-1)

    def __init__(self, *, memberMappings=None, **kwargs):

        super().__init__(**kwargs)

        if memberMappings:
            self.memberMappings.extend(memberMappings)


class VariableMappingModule(SDDModule):
    """A Module containing SMCubes VariableMappings"""
    variableMappings = EReference(ordered=True, unique=True,
                                  containment=True, derived=False, upper=-1)

    def __init__(self, *, variableMappings=None, **kwargs):

        super().__init__(**kwargs)

        if variableMappings:
            self.variableMappings.extend(variableMappings)


class AxisModule(SDDModule):
    """A Module containing a set of  Axis """
    axes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, axes=None, **kwargs):

        super().__init__(**kwargs)

        if axes:
            self.axes.extend(axes)


class AxisOrdinateModule(SDDModule):
    """A Module containing a set of  AxisOrdinates"""
    axis_ordinates = EReference(ordered=True, unique=True,
                                containment=True, derived=False, upper=-1)

    def __init__(self, *, axis_ordinates=None, **kwargs):

        super().__init__(**kwargs)

        if axis_ordinates:
            self.axis_ordinates.extend(axis_ordinates)


class CellPositionModule(SDDModule):
    """A Module containing a set of Cell Positions """
    reportTables = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, reportTables=None, **kwargs):

        super().__init__(**kwargs)

        if reportTables:
            self.reportTables.extend(reportTables)


class OrdinateItemModule(SDDModule):
    """A Module containing a set of OrdinateItems """
    ordinateItems = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, ordinateItems=None, **kwargs):

        super().__init__(**kwargs)

        if ordinateItems:
            self.ordinateItems.extend(ordinateItems)


class ReportTableModule(SDDModule):
    """A Module containing a set of ReportTables"""
    reportTables = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, reportTables=None, **kwargs):

        super().__init__(**kwargs)

        if reportTables:
            self.reportTables.extend(reportTables)


class TableCellModule(SDDModule):
    """A Module containing a set of TableCells """
    tableCells = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, tableCells=None, **kwargs):

        super().__init__(**kwargs)

        if tableCells:
            self.tableCells.extend(tableCells)
