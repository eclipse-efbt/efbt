"""Definition of meta model 'regdna'."""

from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = "regdna"
nsURI = "http://www.eclipse.org/efbt/regdna"
nsPrefix = "regdna"

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)
Comparitor = EEnum(
    "Comparitor", literals=["less_than", "equals", "greater_than", "not_equals"]
)


class Import(EObject, metaclass=MetaEClass):
    importedNamespace = EAttribute(
        eType=EString, unique=True, derived=False, changeable=True
    )

    def __init__(self, *, importedNamespace=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if importedNamespace is not None:
            self.importedNamespace = importedNamespace


class Module(EObject, metaclass=MetaEClass):
    theDescription = EAttribute(
        eType=EString, unique=True, derived=False, changeable=True
    )
    license = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(
        eType=EString, unique=True, derived=False, changeable=True, iD=True
    )
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    dependencies = EReference(
        ordered=True, unique=True, containment=False, derived=False, upper=-1
    )
    imports = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    annotationDirectives = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(
        self,
        *,
        dependencies=None,
        theDescription=None,
        license=None,
        name=None,
        version=None,
        imports=None,
        annotationDirectives=None,
    ):
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

        if annotationDirectives:
            self.annotationDirectives.extend(annotationDirectives)


class ModuleList(EObject, metaclass=MetaEClass):
    modules = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, modules=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if modules:
            self.modules.extend(modules)


class RulesForReport(EObject, metaclass=MetaEClass):
    outputLayerCube = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    rulesForTable = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, outputLayerCube=None, rulesForTable=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if outputLayerCube is not None:
            self.outputLayerCube = outputLayerCube

        if rulesForTable:
            self.rulesForTable.extend(rulesForTable)


class RulesForILTable(EObject, metaclass=MetaEClass):
    rulesForTablePart = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    inputLayerTable = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, rulesForTablePart=None, inputLayerTable=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if rulesForTablePart:
            self.rulesForTablePart.extend(rulesForTablePart)

        if inputLayerTable is not None:
            self.inputLayerTable = inputLayerTable


class SelectColumn(EObject, metaclass=MetaEClass):
    asAttribute = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

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
    columns = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
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
    eAnnotations = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, eAnnotations=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if eAnnotations:
            self.eAnnotations.extend(eAnnotations)


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


class Report(EObject, metaclass=MetaEClass):
    outputLayer = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    rows = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    columns = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, outputLayer=None, rows=None, columns=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if outputLayer is not None:
            self.outputLayer = outputLayer

        if rows:
            self.rows.extend(rows)

        if columns:
            self.columns.extend(columns)


class ReportRow(EObject, metaclass=MetaEClass):
    name = EAttribute(
        eType=EString, unique=True, derived=False, changeable=True, iD=True
    )

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class ReportColumn(EObject, metaclass=MetaEClass):
    name = EAttribute(
        eType=EString, unique=True, derived=False, changeable=True, iD=True
    )

    def __init__(self, *, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if name is not None:
            self.name = name


class ReportCell(EObject, metaclass=MetaEClass):
    datapointID = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    row = EReference(ordered=True, unique=True, containment=False, derived=False)
    column = EReference(ordered=True, unique=True, containment=False, derived=False)
    filters = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    metric = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(
        self, *, row=None, column=None, filters=None, metric=None, datapointID=None
    ):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if datapointID is not None:
            self.datapointID = datapointID

        if row is not None:
            self.row = row

        if column is not None:
            self.column = column

        if filters:
            self.filters.extend(filters)

        if metric is not None:
            self.metric = metric


class Filter(EObject, metaclass=MetaEClass):
    outputLayer = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    operation = EReference(ordered=True, unique=True, containment=False, derived=False)
    member = EReference(
        ordered=True, unique=True, containment=False, derived=False, upper=-1
    )

    def __init__(self, *, outputLayer=None, operation=None, member=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if outputLayer is not None:
            self.outputLayer = outputLayer

        if operation is not None:
            self.operation = operation

        if member:
            self.member.extend(member)


class RowFilters(EObject, metaclass=MetaEClass):
    row = EReference(ordered=True, unique=True, containment=False, derived=False)
    filters = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    metric = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, row=None, filters=None, metric=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if row is not None:
            self.row = row

        if filters:
            self.filters.extend(filters)

        if metric is not None:
            self.metric = metric


class ColumnFilters(EObject, metaclass=MetaEClass):
    column = EReference(ordered=True, unique=True, containment=False, derived=False)
    filters = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    metric = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, column=None, filters=None, metric=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if column is not None:
            self.column = column

        if filters:
            self.filters.extend(filters)

        if metric is not None:
            self.metric = metric


class WholeReportFilters(EObject, metaclass=MetaEClass):
    filters = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, filters=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if filters:
            self.filters.extend(filters)


class SelectColumnMemberAs(SelectColumn):
    memberAsConstant = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

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
    rulesForReport = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, rulesForReport=None, **kwargs):
        super().__init__(**kwargs)

        if rulesForReport:
            self.rulesForReport.extend(rulesForReport)


class AndPredicate(Predicate):
    operands = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, operands=None, **kwargs):
        super().__init__(**kwargs)

        if operands:
            self.operands.extend(operands)


class OrPredicate(Predicate):
    operands = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

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
    comparitor = EAttribute(
        eType=Comparitor, unique=True, derived=False, changeable=True
    )
    value = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    attribute1 = EReference(ordered=True, unique=True, containment=False, derived=False)
    member = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(
        self, *, attribute1=None, comparitor=None, member=None, value=None, **kwargs
    ):
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
    eClassifiers = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, eClassifiers=None, nsURI=None, nsPrefix=None, **kwargs):
        super().__init__(**kwargs)

        if nsURI is not None:
            self.nsURI = nsURI

        if nsPrefix is not None:
            self.nsPrefix = nsPrefix

        if eClassifiers:
            self.eClassifiers.extend(eClassifiers)


class ELAnnotation(ELModelElement):
    details = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    source = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, details=None, source=None, **kwargs):
        super().__init__(**kwargs)

        if details:
            self.details.extend(details)

        if source is not None:
            self.source = source


class CellBasedReport(Report):
    reportCells = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, reportCells=None, **kwargs):
        super().__init__(**kwargs)

        if reportCells:
            self.reportCells.extend(reportCells)


class ReportModule(Module):
    reports = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, reports=None, **kwargs):
        super().__init__(**kwargs)

        if reports:
            self.reports.extend(reports)


class RowColumnBasedReport(Report):
    columnFilters = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    rowFilters = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    wholeReportFilters = EReference(
        ordered=True, unique=True, containment=True, derived=False
    )

    def __init__(
        self, *, columnFilters=None, rowFilters=None, wholeReportFilters=None, **kwargs
    ):
        super().__init__(**kwargs)

        if columnFilters:
            self.columnFilters.extend(columnFilters)

        if rowFilters:
            self.rowFilters.extend(rowFilters)

        if wholeReportFilters is not None:
            self.wholeReportFilters = wholeReportFilters


@abstract
class ELClassifier(ELNamedElement):
    ePackage = EReference(
        ordered=True, unique=True, containment=False, derived=False, transient=True
    )

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


class ELAnnotationDirective(ELNamedElement):
    sourceURI = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    module = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, module=None, sourceURI=None, **kwargs):
        super().__init__(**kwargs)

        if sourceURI is not None:
            self.sourceURI = sourceURI

        if module is not None:
            self.module = module


class ELClass(ELClassifier):
    eAbstract = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    eSuperTypes = EReference(
        ordered=True, unique=True, containment=False, derived=False, upper=-1
    )
    eStructuralFeatures = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    eOperations = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(
        self,
        *,
        eAbstract=None,
        eSuperTypes=None,
        eStructuralFeatures=None,
        eOperations=None,
        **kwargs,
    ):
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
    industryName = EAttribute(
        eType=EString, unique=True, derived=False, changeable=True
    )

    def __init__(self, *, industryName=None, **kwargs):
        super().__init__(**kwargs)

        if industryName is not None:
            self.industryName = industryName


class ELOperation(ELTypedElement):
    body = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, body=None, **kwargs):
        super().__init__(**kwargs)

        if body is not None:
            self.body = body


class ELParameter(ELTypedElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


@abstract
class ELStructuralFeature(ELTypedElement):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class ELAttribute(ELStructuralFeature):
    iD = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    eAttributeType = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, iD=None, eAttributeType=None, **kwargs):
        super().__init__(**kwargs)

        if iD is not None:
            self.iD = iD

        if eAttributeType is not None:
            self.eAttributeType = eAttributeType


class ELEnum(ELDataType):
    eLiterals = EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, eLiterals=None, **kwargs):
        super().__init__(**kwargs)

        if eLiterals:
            self.eLiterals.extend(eLiterals)


class ELReference(ELStructuralFeature):
    containment = EAttribute(
        eType=EBoolean, unique=True, derived=False, changeable=True
    )
    eReferenceType = EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    eOpposite = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(
        self, *, containment=None, eReferenceType=None, eOpposite=None, **kwargs
    ):
        super().__init__(**kwargs)

        if containment is not None:
            self.containment = containment

        if eReferenceType is not None:
            self.eReferenceType = eReferenceType

        if eOpposite is not None:
            self.eOpposite = eOpposite
