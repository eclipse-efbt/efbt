"""Definition of meta model 'ecore4reg'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = 'ecore4reg'
nsURI = 'http://www.eclipse.org/efbt/ecore4reg'
nsPrefix = 'ecore4reg'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)
Comparitor = EEnum('Comparitor', literals=['less_than', 'equals', 'greater_than', 'not_equals'])


class Import(EObject, metaclass=MetaEClass):

    importedNamespace = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, importedNamespace=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if importedNamespace is not None:
            self.importedNamespace = importedNamespace


class Module(EObject, metaclass=MetaEClass):

    theDescription = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    license = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    dependencies = EReference(ordered=True, unique=True, containment=False, derived=False, upper=-1)
    imports = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, dependencies=None, theDescription=None, license=None, name=None, version=None, imports=None):
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

        if imports:
            self.imports.extend(imports)


class ModuleList(EObject, metaclass=MetaEClass):

    modules = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, modules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if modules:
            self.modules.extend(modules)


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


class RulesForReport(EObject, metaclass=MetaEClass):

    outputLayerCube = EReference(ordered=True, unique=True, containment=False, derived=False)
    rulesForTable = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, outputLayerCube=None, rulesForTable=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if outputLayerCube is not None:
            self.outputLayerCube = outputLayerCube

        if rulesForTable:
            self.rulesForTable.extend(rulesForTable)


class RulesForILTable(EObject, metaclass=MetaEClass):

    rulesForTablePart = EReference(ordered=True, unique=True,
                                   containment=True, derived=False, upper=-1)
    inputLayerTable = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, rulesForTablePart=None, inputLayerTable=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if rulesForTablePart:
            self.rulesForTablePart.extend(rulesForTablePart)

        if inputLayerTable is not None:
            self.inputLayerTable = inputLayerTable


class SelectColumn(EObject, metaclass=MetaEClass):

    asAttribute = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, asAttribute=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if asAttribute is not None:
            self.asAttribute = asAttribute


class TableFilter(EObject, metaclass=MetaEClass):

    predicate = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, predicate=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if predicate is not None:
            self.predicate = predicate


class RuleForILTablePart(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    columns = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    whereClause = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, name=None, columns=None, whereClause=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if columns:
            self.columns.extend(columns)

        if whereClause is not None:
            self.whereClause = whereClause


@abstract
class Predicate(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


@abstract
class ELModelElement(EObject, metaclass=MetaEClass):

    eAnnotations = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, eAnnotations=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if eAnnotations is not None:
            self.eAnnotations = eAnnotations


class ELStringToStringMapEntry(EObject, metaclass=MetaEClass):

    key = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    value = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, key=None, value=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if key is not None:
            self.key = key

        if value is not None:
            self.value = value


class VTLEnrichedCube(EObject, metaclass=MetaEClass):

    transformations = EReference(ordered=True, unique=True,
                                 containment=False, derived=False, upper=-1)

    def __init__(self, *, transformations=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if transformations:
            self.transformations.extend(transformations)


class VTLGeneratedOutputlayer(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    dependant_intermediate_layers = EReference(
        ordered=True, unique=True, containment=False, derived=False, upper=-1)
    VTLForOutputLayerAndIntemedateLayerCombinations = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)
    outputLayer = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, name=None, dependant_intermediate_layers=None, VTLForOutputLayerAndIntemedateLayerCombinations=None, outputLayer=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if dependant_intermediate_layers:
            self.dependant_intermediate_layers.extend(dependant_intermediate_layers)

        if VTLForOutputLayerAndIntemedateLayerCombinations:
            self.VTLForOutputLayerAndIntemedateLayerCombinations.extend(
                VTLForOutputLayerAndIntemedateLayerCombinations)

        if outputLayer is not None:
            self.outputLayer = outputLayer


class VTLForOutputLayerAndIntermediateLayerCombination(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    transformations = EReference(ordered=True, unique=True,
                                 containment=False, derived=False, upper=-1)
    outputLayer = EReference(ordered=True, unique=True, containment=False, derived=False)
    intermediateLayer = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, transformations=None, outputLayer=None, intermediateLayer=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if transformations:
            self.transformations.extend(transformations)

        if outputLayer is not None:
            self.outputLayer = outputLayer

        if intermediateLayer is not None:
            self.intermediateLayer = intermediateLayer


class VTLGeneratedIntermediateLayer(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    isEnrichment = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    dependant_enriched_cubes = EReference(
        ordered=True, unique=True, containment=False, derived=False)
    transformations = EReference(ordered=True, unique=True,
                                 containment=False, derived=False, upper=-1)

    def __init__(self, *, dependant_enriched_cubes=None, transformations=None, name=None, isEnrichment=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if isEnrichment is not None:
            self.isEnrichment = isEnrichment

        if dependant_enriched_cubes is not None:
            self.dependant_enriched_cubes = dependant_enriched_cubes

        if transformations:
            self.transformations.extend(transformations)


class VTLTransformation(EObject, metaclass=MetaEClass):

    expression = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    transformation_id = EAttribute(eType=EString, unique=True,
                                   derived=False, changeable=True, iD=True)
    order_in_scheme = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, expression=None, description=None, transformation_id=None, order_in_scheme=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if expression is not None:
            self.expression = expression

        if description is not None:
            self.description = description

        if transformation_id is not None:
            self.transformation_id = transformation_id

        if order_in_scheme is not None:
            self.order_in_scheme = order_in_scheme


class VTLScheme(EObject, metaclass=MetaEClass):

    scheme_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    expressions = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, expressions=None, scheme_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if scheme_id is not None:
            self.scheme_id = scheme_id

        if expressions:
            self.expressions.extend(expressions)


class VTLForSelectionLayer(EObject, metaclass=MetaEClass):

    selectionLayer = EReference(ordered=True, unique=True, containment=False, derived=False)
    outputLayer = EReference(ordered=True, unique=True, containment=False, derived=False)
    intermediateLayer = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, selectionLayer=None, outputLayer=None, intermediateLayer=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if selectionLayer is not None:
            self.selectionLayer = selectionLayer

        if outputLayer is not None:
            self.outputLayer = outputLayer

        if intermediateLayer is not None:
            self.intermediateLayer = intermediateLayer


class EntityToVTLIntermediateLayerLink(EObject, metaclass=MetaEClass):

    theFilter = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    VTLIntermediateLayer = EReference(ordered=True, unique=True, containment=False, derived=False)
    entity = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, VTLIntermediateLayer=None, entity=None, theFilter=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if theFilter is not None:
            self.theFilter = theFilter

        if VTLIntermediateLayer is not None:
            self.VTLIntermediateLayer = VTLIntermediateLayer

        if entity is not None:
            self.entity = entity


class VTLForView(EObject, metaclass=MetaEClass):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    view = EReference(ordered=True, unique=True, containment=False, derived=False)
    vtl = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, name=None, view=None, vtl=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name

        if view is not None:
            self.view = view

        if vtl is not None:
            self.vtl = vtl


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


class SelectDerivedColumnAs(SelectColumn):

    attribute = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attribute=None, **kwargs):

        super().__init__(**kwargs)

        if attribute is not None:
            self.attribute = attribute


class SelectValueAs(SelectColumn):

    value = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, value=None, **kwargs):

        super().__init__(**kwargs)

        if value is not None:
            self.value = value


class GenerationRulesModule(Module):

    rulesForReport = EReference(ordered=True, unique=True,
                                containment=True, derived=False, upper=-1)

    def __init__(self, *, rulesForReport=None, **kwargs):

        super().__init__(**kwargs)

        if rulesForReport:
            self.rulesForReport.extend(rulesForReport)


class AndPredicate(Predicate):

    operands = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, operands=None, **kwargs):

        super().__init__(**kwargs)

        if operands:
            self.operands.extend(operands)


class OrPredicate(Predicate):

    operands = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, operands=None, **kwargs):

        super().__init__(**kwargs)

        if operands:
            self.operands.extend(operands)


class NotPredicate(Predicate):

    operand = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, operand=None, **kwargs):

        super().__init__(**kwargs)

        if operand is not None:
            self.operand = operand


class AttributePredicate(Predicate):

    comparitor = EAttribute(eType=Comparitor, unique=True, derived=False, changeable=True)
    value = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    attribute1 = EReference(ordered=True, unique=True, containment=False, derived=False)
    member = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attribute1=None, comparitor=None, member=None, value=None, **kwargs):

        super().__init__(**kwargs)

        if comparitor is not None:
            self.comparitor = comparitor

        if value is not None:
            self.value = value

        if attribute1 is not None:
            self.attribute1 = attribute1

        if member is not None:
            self.member = member


@abstract
class ELNamedElement(ELModelElement):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, name=None, **kwargs):

        super().__init__(**kwargs)

        if name is not None:
            self.name = name


class ELPackage(Module):

    nsURI = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    nsPrefix = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    eClassifiers = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, eClassifiers=None, nsURI=None, nsPrefix=None, **kwargs):

        super().__init__(**kwargs)

        if nsURI is not None:
            self.nsURI = nsURI

        if nsPrefix is not None:
            self.nsPrefix = nsPrefix

        if eClassifiers:
            self.eClassifiers.extend(eClassifiers)


class ELAnnotation(ELModelElement):

    source = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    details = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, details=None, source=None, **kwargs):

        super().__init__(**kwargs)

        if source is not None:
            self.source = source

        if details:
            self.details.extend(details)


class VTLModule(Module):

    VTLSchemes = EReference(ordered=True, unique=True, containment=True, derived=False)
    VTLGeneratedOutputLayers = EReference(
        ordered=True, unique=True, containment=True, derived=False)
    VTLGeneratedIntermediateLayers = EReference(
        ordered=True, unique=True, containment=True, derived=False)
    VTLEnrichedLayers = EReference(ordered=True, unique=True, containment=True, derived=False)
    VTLForSelectionLayers = EReference(ordered=True, unique=True, containment=True, derived=False)
    entityToVTLIntermediateLayerLinks = EReference(
        ordered=True, unique=True, containment=True, derived=False)
    VTLForViews = EReference(ordered=True, unique=True, containment=True, derived=False)

    def __init__(self, *, VTLSchemes=None, VTLGeneratedOutputLayers=None, VTLGeneratedIntermediateLayers=None, VTLEnrichedLayers=None, VTLForSelectionLayers=None, entityToVTLIntermediateLayerLinks=None, VTLForViews=None, **kwargs):

        super().__init__(**kwargs)

        if VTLSchemes is not None:
            self.VTLSchemes = VTLSchemes

        if VTLGeneratedOutputLayers is not None:
            self.VTLGeneratedOutputLayers = VTLGeneratedOutputLayers

        if VTLGeneratedIntermediateLayers is not None:
            self.VTLGeneratedIntermediateLayers = VTLGeneratedIntermediateLayers

        if VTLEnrichedLayers is not None:
            self.VTLEnrichedLayers = VTLEnrichedLayers

        if VTLForSelectionLayers is not None:
            self.VTLForSelectionLayers = VTLForSelectionLayers

        if entityToVTLIntermediateLayerLinks is not None:
            self.entityToVTLIntermediateLayerLinks = entityToVTLIntermediateLayerLinks

        if VTLForViews is not None:
            self.VTLForViews = VTLForViews


class VTLGeneratedOutputlayerModule(Module):

    vTLGeneratedOutputlayerModules = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, vTLGeneratedOutputlayerModules=None, **kwargs):

        super().__init__(**kwargs)

        if vTLGeneratedOutputlayerModules:
            self.vTLGeneratedOutputlayerModules.extend(vTLGeneratedOutputlayerModules)


class VTLForOutputLayerAndIntermediateLayerCombinationModule(Module):

    vTLForOutputLayerAndIntermediateLayerCombinations = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, vTLForOutputLayerAndIntermediateLayerCombinations=None, **kwargs):

        super().__init__(**kwargs)

        if vTLForOutputLayerAndIntermediateLayerCombinations:
            self.vTLForOutputLayerAndIntermediateLayerCombinations.extend(
                vTLForOutputLayerAndIntermediateLayerCombinations)


class VTLGeneratedIntermediateLayerModule(Module):

    vTLGeneratedIntermediateLayers = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, vTLGeneratedIntermediateLayers=None, **kwargs):

        super().__init__(**kwargs)

        if vTLGeneratedIntermediateLayers:
            self.vTLGeneratedIntermediateLayers.extend(vTLGeneratedIntermediateLayers)


class VTLSchemesModule(Module):

    vTLSchemes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, vTLSchemes=None, **kwargs):

        super().__init__(**kwargs)

        if vTLSchemes:
            self.vTLSchemes.extend(vTLSchemes)


class VTLForSelectionLayerModule(Module):

    vTLForSelectionLayers = EReference(ordered=True, unique=True,
                                       containment=True, derived=False, upper=-1)

    def __init__(self, *, vTLForSelectionLayers=None, **kwargs):

        super().__init__(**kwargs)

        if vTLForSelectionLayers:
            self.vTLForSelectionLayers.extend(vTLForSelectionLayers)


class EntityToVTLIntermediateLayerLinkModule(Module):

    entityToVTLIntermediateLayerLinks = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, entityToVTLIntermediateLayerLinks=None, **kwargs):

        super().__init__(**kwargs)

        if entityToVTLIntermediateLayerLinks:
            self.entityToVTLIntermediateLayerLinks.extend(entityToVTLIntermediateLayerLinks)


class VTLForViewModule(Module):

    vTLForViews = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, vTLForViews=None, **kwargs):

        super().__init__(**kwargs)

        if vTLForViews:
            self.vTLForViews.extend(vTLForViews)


@abstract
class ELClassifier(ELNamedElement):

    ePackage = EReference(ordered=True, unique=True, containment=False,
                          derived=False, transient=True)

    def __init__(self, *, ePackage=None, **kwargs):

        super().__init__(**kwargs)

        if ePackage is not None:
            self.ePackage = ePackage


class ELEnumLiteral(ELNamedElement):

    value = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    literal = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, value=None, literal=None, **kwargs):

        super().__init__(**kwargs)

        if value is not None:
            self.value = value

        if literal is not None:
            self.literal = literal


@abstract
class ELTypedElement(ELNamedElement):

    upperBound = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    lowerBound = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    eType = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, eType=None, upperBound=None, lowerBound=None, **kwargs):

        super().__init__(**kwargs)

        if upperBound is not None:
            self.upperBound = upperBound

        if lowerBound is not None:
            self.lowerBound = lowerBound

        if eType is not None:
            self.eType = eType


class ELClass(ELClassifier):

    eAbstract = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    eSuperTypes = EReference(ordered=True, unique=True, containment=False, derived=False, upper=-1)
    eStructuralFeatures = EReference(ordered=True, unique=True,
                                     containment=True, derived=False, upper=-1)
    eOperations = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, eAbstract=None, eSuperTypes=None, eStructuralFeatures=None, eOperations=None, **kwargs):

        super().__init__(**kwargs)

        if eAbstract is not None:
            self.eAbstract = eAbstract

        if eSuperTypes:
            self.eSuperTypes.extend(eSuperTypes)

        if eStructuralFeatures:
            self.eStructuralFeatures.extend(eStructuralFeatures)

        if eOperations:
            self.eOperations.extend(eOperations)


class ELDataType(ELClassifier):

    industryName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, industryName=None, **kwargs):

        super().__init__(**kwargs)

        if industryName is not None:
            self.industryName = industryName


class ELOperation(ELTypedElement):

    body = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    eParameters = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, body=None, eParameters=None, **kwargs):

        super().__init__(**kwargs)

        if body is not None:
            self.body = body

        if eParameters:
            self.eParameters.extend(eParameters)


class ELParameter(ELTypedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class ELStructuralFeature(ELTypedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ELAttribute(ELStructuralFeature):

    iD = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    eAttributeType = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, iD=None, eAttributeType=None, **kwargs):

        super().__init__(**kwargs)

        if iD is not None:
            self.iD = iD

        if eAttributeType is not None:
            self.eAttributeType = eAttributeType


class ELEnum(ELDataType):

    eLiterals = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, eLiterals=None, **kwargs):

        super().__init__(**kwargs)

        if eLiterals:
            self.eLiterals.extend(eLiterals)


class ELReference(ELStructuralFeature):

    containment = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    eReferenceType = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, containment=None, eReferenceType=None, **kwargs):

        super().__init__(**kwargs)

        if containment is not None:
            self.containment = containment

        if eReferenceType is not None:
            self.eReferenceType = eReferenceType
