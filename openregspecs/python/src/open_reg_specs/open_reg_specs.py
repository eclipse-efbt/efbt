"""Definition of meta model 'open_reg_specs'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = 'open_reg_specs'
nsURI = 'http://www.eclipse.org/efbt/open_reg_specs'
nsPrefix = 'open_reg_specs'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)
AttrComparison = EEnum('AttrComparison', literals=['equals', 'less_than', 'greater_than'])

FACET_VALUE_TYPE = EEnum('FACET_VALUE_TYPE', literals=['BigInteger', 'Boolean', 'DateTime', 'DayMonthDayMonth', 'Decimal', 'Double',
                         'Duration', 'Float', 'GregorianDay', 'GregorianMonth', 'GregorianYear', 'Integer', 'Long', 'Short', 'String', 'Time', 'URI'])

TYP_DMNSN = EEnum('TYP_DMNSN', literals=['B', 'M', 'T', 'U'])

TYP_RL = EEnum('TYP_RL', literals=['O', 'A', 'D'])


FACET_VALUE_TYPEObject = EDataType(
    'FACET_VALUE_TYPEObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')

TYP_DMNSNObject = EDataType(
    'TYP_DMNSNObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')

TYP_RLObject = EDataType('TYP_RLObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')


class OpenRegSpecs(EObject, metaclass=MetaEClass):

    requirements = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    types_and_concepts = EReference(ordered=True, unique=True, containment=True, derived=False)
    data_model = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    process_workflow = EReference(ordered=True, unique=True,
                                  containment=True, derived=False, upper=-1)
    report_generation = EReference(ordered=True, unique=True,
                                   containment=True, derived=False, upper=-1)
    tests = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, requirements=None, types_and_concepts=None, data_model=None, process_workflow=None, report_generation=None, tests=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if requirements:
            self.requirements.extend(requirements)

        if types_and_concepts is not None:
            self.types_and_concepts = types_and_concepts

        if data_model:
            self.data_model.extend(data_model)

        if process_workflow:
            self.process_workflow.extend(process_workflow)

        if report_generation:
            self.report_generation.extend(report_generation)

        if tests:
            self.tests.extend(tests)


@abstract
class BaseElement(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    invisible = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)

    def __init__(self, *, name=None, description=None, invisible=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if description is not None:
            self.description = description

        if invisible is not None:
            self.invisible = invisible


class Concept(EObject, metaclass=MetaEClass):

    conceptName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, conceptName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if conceptName is not None:
            self.conceptName = conceptName


@abstract
class Element(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


class EnumMember(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


class TypesAndConcepts(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


class Module(EObject, metaclass=MetaEClass):

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

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class ModuleDependencies(EObject, metaclass=MetaEClass):

    theModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, theModules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if theModules:
            self.theModules.extend(theModules)


class ModuleDependency(EObject, metaclass=MetaEClass):

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


class AllowedTypes(EObject, metaclass=MetaEClass):

    allowedTypes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, allowedTypes=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if allowedTypes:
            self.allowedTypes.extend(allowedTypes)


@abstract
class RequirementsSection(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class RequirementType(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class Tag(EObject, metaclass=MetaEClass):

    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    requirements = EReference(ordered=True, unique=True, containment=False, derived=False, upper=-1)

    def __init__(self, *, displayName=None, name=None, requirements=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if displayName is not None:
            self.displayName = displayName

        if name is not None:
            self.name = name

        if requirements:
            self.requirements.extend(requirements)


class View(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    selectClause = EReference(ordered=True, unique=True, containment=True, derived=False)
    whereClause = EReference(ordered=True, unique=True, containment=True, derived=False)
    selectionLayer = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, selectClause=None, whereClause=None, name=None, selectionLayer=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if selectClause is not None:
            self.selectClause = selectClause

        if whereClause is not None:
            self.whereClause = whereClause

        if selectionLayer is not None:
            self.selectionLayer = selectionLayer


class SelectClause(EObject, metaclass=MetaEClass):

    columns = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, columns=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if columns:
            self.columns.extend(columns)


class Column(EObject, metaclass=MetaEClass):

    attribute = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attribute=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if attribute is not None:
            self.attribute = attribute


class WhereClause(EObject, metaclass=MetaEClass):

    text = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, text=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if text is not None:
            self.text = text


class Scenario(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    invisible = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    requiredAttributes = EReference(ordered=True, unique=True,
                                    containment=False, derived=False, upper=-1)
    data_constraints = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, name=None, invisible=None, description=None, requiredAttributes=None, data_constraints=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if invisible is not None:
            self.invisible = invisible

        if description is not None:
            self.description = description

        if requiredAttributes:
            self.requiredAttributes.extend(requiredAttributes)

        if data_constraints is not None:
            self.data_constraints = data_constraints


class Test(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    inputData = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    expectedResult = EReference(ordered=True, unique=True,
                                containment=True, derived=False, upper=-1)
    scope = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, inputData=None, expectedResult=None, name=None, scope=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if inputData:
            self.inputData.extend(inputData)

        if expectedResult:
            self.expectedResult.extend(expectedResult)

        if scope is not None:
            self.scope = scope


class DataConstraint(EObject, metaclass=MetaEClass):

    comparison = EAttribute(eType=AttrComparison, unique=True, derived=False, changeable=True)
    attr1 = EReference(ordered=True, unique=True, containment=False, derived=False)
    attr2 = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attr1=None, attr2=None, comparison=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if comparison is not None:
            self.comparison = comparison

        if attr1 is not None:
            self.attr1 = attr1

        if attr2 is not None:
            self.attr2 = attr2


class SelectionLayer(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    invisible = EAttribute(eType=EBooleanObject, unique=True, derived=False, changeable=True)
    generatedEntity = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, name=None, invisible=None, generatedEntity=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if invisible is not None:
            self.invisible = invisible

        if generatedEntity is not None:
            self.generatedEntity = generatedEntity


class TestScope(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class CSVFile(EObject, metaclass=MetaEClass):

    fileName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    rows = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, fileName=None, rows=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if fileName is not None:
            self.fileName = fileName

        if rows:
            self.rows.extend(rows)


class CSVRow(EObject, metaclass=MetaEClass):

    value = EAttribute(eType=EString, unique=True, derived=False, changeable=True, upper=-1)
    isHeader = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)

    def __init__(self, *, value=None, isHeader=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if value:
            self.value.extend(value)

        if isHeader is not None:
            self.isHeader = isHeader


class DOMAIN(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    data_type = EAttribute(eType=FACET_VALUE_TYPE, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_enumerated = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    is_reference = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    facet_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, data_type=None, description=None, facet_id=None, is_enumerated=None, is_reference=None, maintenance_agency_id=None, name=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if data_type is not None:
            self.data_type = data_type

        if description is not None:
            self.description = description

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
    facet_value_type = EAttribute(eType=FACET_VALUE_TYPE, unique=True,
                                  derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, facet_value_type=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

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
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

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


class PlatformCall(EObject, metaclass=MetaEClass):

    errorMessage = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    returnStatus = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)

    def __init__(self, *, errorMessage=None, name=None, returnStatus=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if errorMessage is not None:
            self.errorMessage = errorMessage

        if name is not None:
            self.name = name

        if returnStatus is not None:
            self.returnStatus = returnStatus


@abstract
class FlowElementsContainer(BaseElement):

    flowElements = EReference(ordered=False, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, flowElements=None, **kwargs):

        super().__init__(**kwargs)

        if flowElements:
            self.flowElements.extend(flowElements)


@abstract
class FlowElement(BaseElement):

    display_name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, display_name=None, **kwargs):

        super().__init__(**kwargs)

        if display_name is not None:
            self.display_name = display_name


@abstract
class NamedElement(Element):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)

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


class RequirementsModule(Module):

    rules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    allowedtypes = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, rules=None, allowedtypes=None, **kwargs):

        super().__init__(**kwargs)

        if rules:
            self.rules.extend(rules)

        if allowedtypes is not None:
            self.allowedtypes = allowedtypes


class RequirementsSectionImage(RequirementsSection):

    style = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    uri = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, style=None, uri=None, **kwargs):

        super().__init__(**kwargs)

        if style is not None:
            self.style = style

        if uri is not None:
            self.uri = uri


class RequirementsSectionLinkWithText(RequirementsSection):

    linkText = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    subsection = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    linkedRuleSection = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, linkedRuleSection=None, linkText=None, subsection=None, **kwargs):

        super().__init__(**kwargs)

        if linkText is not None:
            self.linkText = linkText

        if subsection is not None:
            self.subsection = subsection

        if linkedRuleSection is not None:
            self.linkedRuleSection = linkedRuleSection


class RequirementsSectionText(RequirementsSection):

    text = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, text=None, **kwargs):

        super().__init__(**kwargs)

        if text is not None:
            self.text = text


class TitledRequirementsSection(RequirementsSection):

    title = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    sections = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    referencingSections = EReference(ordered=True, unique=True, containment=False, derived=False)
    requirementsType = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, sections=None, referencingSections=None, requirementsType=None, title=None, **kwargs):

        super().__init__(**kwargs)

        if title is not None:
            self.title = title

        if sections:
            self.sections.extend(sections)

        if referencingSections is not None:
            self.referencingSections = referencingSections

        if requirementsType is not None:
            self.requirementsType = requirementsType


class TagGroup(Module):

    tags = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, tags=None, **kwargs):

        super().__init__(**kwargs)

        if tags:
            self.tags.extend(tags)


class SelectColumn(Column):

    as_ = EReference(ordered=True, unique=True, containment=False, derived=False)
    memberAsConstant = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, as_=None, memberAsConstant=None, **kwargs):

        super().__init__(**kwargs)

        if as_ is not None:
            self.as_ = as_

        if memberAsConstant is not None:
            self.memberAsConstant = memberAsConstant


class ViewModule(Module):

    views = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, views=None, **kwargs):

        super().__init__(**kwargs)

        if views:
            self.views.extend(views)


class ActivityTag(Tag):

    activity = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, activity=None, **kwargs):

        super().__init__(**kwargs)

        if activity is not None:
            self.activity = activity


class ScenarioTag(Tag):

    scenario = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, scenario=None, **kwargs):

        super().__init__(**kwargs)

        if scenario is not None:
            self.scenario = scenario


class TestModule(Module):

    tests = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, tests=None, **kwargs):

        super().__init__(**kwargs)

        if tests:
            self.tests.extend(tests)


class UnitTestScope(TestScope):

    scenarios = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, scenarios=None, **kwargs):

        super().__init__(**kwargs)

        if scenarios is not None:
            self.scenarios = scenarios


class E2ETestScope(TestScope):

    scenarios = EReference(ordered=True, unique=True, containment=False, derived=False, upper=-1)
    layer = EReference(ordered=True, unique=True, containment=False, derived=False)
    scriptTask = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, scenarios=None, layer=None, scriptTask=None, **kwargs):

        super().__init__(**kwargs)

        if scenarios:
            self.scenarios.extend(scenarios)

        if layer is not None:
            self.layer = layer

        if scriptTask is not None:
            self.scriptTask = scriptTask


class MEMBER(EnumMember):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, name=None, displayName=None, **kwargs):

        super().__init__(**kwargs)

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

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
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, domain_id=None, maintenance_agency_id=None, primary_concept=None, name=None, displayName=None, **kwargs):

        super().__init__(**kwargs)

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if primary_concept is not None:
            self.primary_concept = primary_concept

        if name is not None:
            self.name = name

        if displayName is not None:
            self.displayName = displayName

        if domain_id is not None:
            self.domain_id = domain_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class DomainModule(Module):

    domains = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, domains=None, **kwargs):

        super().__init__(**kwargs)

        if domains:
            self.domains.extend(domains)


class MemberHierarchyModule(Module):

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

    members = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, members=None, **kwargs):

        super().__init__(**kwargs)

        if members:
            self.members.extend(members)


class VariableModule(Module):

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


class SMCubesCoreModel(TypesAndConcepts):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    variableModules = EReference(ordered=True, unique=True,
                                 containment=True, derived=False, upper=-1)
    domainModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    memberModules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    subDomainModules = EReference(ordered=True, unique=True,
                                  containment=True, derived=False, upper=-1)
    memberHierarchyModules = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, name=None, variableModules=None, domainModules=None, memberModules=None, subDomainModules=None, memberHierarchyModules=None, **kwargs):

        super().__init__(**kwargs)

        if name is not None:
            self.name = name

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


class PlatformCallModule(Module):

    platformCalls = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, platformCalls=None, **kwargs):

        super().__init__(**kwargs)

        if platformCalls:
            self.platformCalls.extend(platformCalls)


class ImportBIRDFromMSAccess(PlatformCall):

    inputDirectory = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    outputDirectory = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, inputDirectory=None, outputDirectory=None, **kwargs):

        super().__init__(**kwargs)

        if inputDirectory is not None:
            self.inputDirectory = inputDirectory

        if outputDirectory is not None:
            self.outputDirectory = outputDirectory


class WorkflowModule(Module):

    taskTags = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    scenarioTags = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    subProcess = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, taskTags=None, scenarioTags=None, subProcess=None, **kwargs):

        super().__init__(**kwargs)

        if taskTags:
            self.taskTags.extend(taskTags)

        if scenarioTags:
            self.scenarioTags.extend(scenarioTags)

        if subProcess:
            self.subProcess.extend(subProcess)


@abstract
class FlowNode(FlowElement):

    outgoing = EReference(ordered=True, unique=True, containment=False, derived=False, upper=-1)
    incoming = EReference(ordered=False, unique=True, containment=False, derived=False, upper=-1)

    def __init__(self, *, outgoing=None, incoming=None, **kwargs):

        super().__init__(**kwargs)

        if outgoing:
            self.outgoing.extend(outgoing)

        if incoming:
            self.incoming.extend(incoming)


class SequenceFlow(FlowElement):

    targetRef = EReference(ordered=False, unique=True, containment=False, derived=False)
    sourceRef = EReference(ordered=False, unique=True, containment=False, derived=False)

    def __init__(self, *, targetRef=None, sourceRef=None, **kwargs):

        super().__init__(**kwargs)

        if targetRef is not None:
            self.targetRef = targetRef

        if sourceRef is not None:
            self.sourceRef = sourceRef


@abstract
class Type(NamedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class TypedElement(NamedElement):

    type = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, type=None, **kwargs):

        super().__init__(**kwargs)

        if type is not None:
            self.type = type


@abstract
class Entity(NamedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class Activity(FlowNode):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class Gateway(FlowNode):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


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


class SUBDOMAIN(Type):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_listed = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    is_natural = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    domain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    facet_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    items = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, code=None, description=None, domain_id=None, facet_id=None, is_listed=None, is_natural=None, maintenance_agency_id=None, items=None, **kwargs):

        super().__init__(**kwargs)

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if is_listed is not None:
            self.is_listed = is_listed

        if is_natural is not None:
            self.is_natural = is_natural

        if domain_id is not None:
            self.domain_id = domain_id

        if facet_id is not None:
            self.facet_id = facet_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if items:
            self.items.extend(items)


class Task(Activity):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ExclusiveGateway(Gateway):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class InclusiveGateway(Gateway):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ParallelGateway(Gateway):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class Attribute(StructuralFeature):

    isPK = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    ordered = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    concept = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, isPK=None, concept=None, ordered=None, **kwargs):

        super().__init__(**kwargs)

        if isPK is not None:
            self.isPK = isPK

        if ordered is not None:
            self.ordered = ordered

        if concept is not None:
            self.concept = concept


class ServiceTask(Task):

    enrichedAttribute = EReference(ordered=True, unique=True, containment=False, derived=False)
    scenarios = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    secondAttribute = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, enrichedAttribute=None, scenarios=None, secondAttribute=None, **kwargs):

        super().__init__(**kwargs)

        if enrichedAttribute is not None:
            self.enrichedAttribute = enrichedAttribute

        if scenarios:
            self.scenarios.extend(scenarios)

        if secondAttribute is not None:
            self.secondAttribute = secondAttribute


class SubProcess(Activity, FlowElementsContainer):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ScriptTask(Task):

    selectionLayers = EReference(ordered=True, unique=True,
                                 containment=True, derived=False, upper=-1)
    outputLayer = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, selectionLayers=None, outputLayer=None, **kwargs):

        super().__init__(**kwargs)

        if selectionLayers:
            self.selectionLayers.extend(selectionLayers)

        if outputLayer is not None:
            self.outputLayer = outputLayer


class UserTask(Task):

    entity = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, entity=None, **kwargs):

        super().__init__(**kwargs)

        if entity is not None:
            self.entity = entity


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
