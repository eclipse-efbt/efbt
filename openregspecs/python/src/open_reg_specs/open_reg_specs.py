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
Comparitor = EEnum('Comparitor', literals=['less_than', 'equals', 'greater_than'])

AttrComparison = EEnum('AttrComparison', literals=[
                       'equals', 'less_than', 'greater_than', 'not_equals'])


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


class Module(EObject, metaclass=MetaEClass):

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


class SelectColumn(EObject, metaclass=MetaEClass):

    as_ = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, as_=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if as_ is not None:
            self.as_ = as_


class WhereClause(EObject, metaclass=MetaEClass):

    comparitor = EAttribute(eType=Comparitor, unique=True, derived=False, changeable=True)
    value = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    attribute1 = EReference(ordered=True, unique=True, containment=False, derived=False)
    member = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attribute1=None, comparitor=None, member=None, value=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if comparitor is not None:
            self.comparitor = comparitor

        if value is not None:
            self.value = value

        if attribute1 is not None:
            self.attribute1 = attribute1

        if member is not None:
            self.member = member


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
    value = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    attr1 = EReference(ordered=True, unique=True, containment=False, derived=False)
    member = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attr1=None, comparison=None, member=None, value=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if comparison is not None:
            self.comparison = comparison

        if value is not None:
            self.value = value

        if attr1 is not None:
            self.attr1 = attr1

        if member is not None:
            self.member = member


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


class InputFile(EObject, metaclass=MetaEClass):

    fileName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    entity = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, fileName=None, entity=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if fileName is not None:
            self.fileName = fileName

        if entity is not None:
            self.entity = entity


@abstract
class XModelElement(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


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


class SelectColumnMemberAs(SelectColumn):

    memberAsConstant = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, memberAsConstant=None, **kwargs):

        super().__init__(**kwargs)

        if memberAsConstant is not None:
            self.memberAsConstant = memberAsConstant


class SelectColumnAttributeAs(SelectColumn):

    attribute = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attribute=None, **kwargs):

        super().__init__(**kwargs)

        if attribute is not None:
            self.attribute = attribute


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
class XNamedElement(XModelElement):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, name=None, **kwargs):

        super().__init__(**kwargs)

        if name is not None:
            self.name = name


class XPackage(Module):

    classifiers = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, classifiers=None, **kwargs):

        super().__init__(**kwargs)

        if classifiers:
            self.classifiers.extend(classifiers)


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
class XClassifier(XNamedElement):

    package = EReference(ordered=True, unique=True, containment=False,
                         derived=False, transient=True)

    def __init__(self, *, package=None, **kwargs):

        super().__init__(**kwargs)

        if package is not None:
            self.package = package


class XEnumLiteral(XNamedElement):

    value = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    literal = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    enum = EReference(ordered=True, unique=True, containment=False, derived=False, transient=True)

    def __init__(self, *, value=None, literal=None, enum=None, **kwargs):

        super().__init__(**kwargs)

        if value is not None:
            self.value = value

        if literal is not None:
            self.literal = literal

        if enum is not None:
            self.enum = enum


@abstract
class XTypedElement(XNamedElement):

    upperBound = EAttribute(eType=EInt, unique=True, derived=False,
                            changeable=True, default_value=1)
    lowerBound = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    type = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, type=None, upperBound=None, lowerBound=None, **kwargs):

        super().__init__(**kwargs)

        if upperBound is not None:
            self.upperBound = upperBound

        if lowerBound is not None:
            self.lowerBound = lowerBound

        if type is not None:
            self.type = type


@abstract
class Activity(FlowNode):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class Gateway(FlowNode):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class XClass(XClassifier):

    abstract = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    members = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    superTypes = EReference(ordered=True, unique=True, containment=False, derived=False, upper=-1)

    def __init__(self, *, abstract=None, members=None, superTypes=None, **kwargs):

        super().__init__(**kwargs)

        if abstract is not None:
            self.abstract = abstract

        if members:
            self.members.extend(members)

        if superTypes:
            self.superTypes.extend(superTypes)


class XDataType(XClassifier):

    industryName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, industryName=None, **kwargs):

        super().__init__(**kwargs)

        if industryName is not None:
            self.industryName = industryName


@abstract
class XMember(XTypedElement):

    containingClass = EReference(ordered=True, unique=True,
                                 containment=False, derived=False, transient=True)

    def __init__(self, *, containingClass=None, **kwargs):

        super().__init__(**kwargs)

        if containingClass is not None:
            self.containingClass = containingClass


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


class XEnum(XDataType):

    literals = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, literals=None, **kwargs):

        super().__init__(**kwargs)

        if literals:
            self.literals.extend(literals)


class XOperation(XMember):

    body = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, body=None, **kwargs):

        super().__init__(**kwargs)

        if body is not None:
            self.body = body


@abstract
class XStructuralFeature(XMember):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


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


class XAttribute(XStructuralFeature):

    defaultValueLiteral = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    iD = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)

    def __init__(self, *, defaultValueLiteral=None, iD=None, **kwargs):

        super().__init__(**kwargs)

        if defaultValueLiteral is not None:
            self.defaultValueLiteral = defaultValueLiteral

        if iD is not None:
            self.iD = iD


class XReference(XStructuralFeature):

    containment = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)

    def __init__(self, *, containment=None, **kwargs):

        super().__init__(**kwargs)

        if containment is not None:
            self.containment = containment
