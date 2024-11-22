"""Definition of meta model 'regdna'."""

from functools import partial
import pyecore.ecore as ecore

# ecore references first
name = "regdna"
nsURI = "http://www.eclipse.org/efbt/regdna"
nsPrefix = "regdna"

eClass = ecore.EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(ecore.getEClassifier, searchspace=eClassifiers)
Comparitor = ecore.EEnum(
    "Comparitor", literals=["less_than", "equals", "greater_than", "not_equals"]
)

# Map EL types to ecore types
ELStringToStringMapEntry = ecore.EStringToStringMapEntry
ELAnnotation = ecore.EAnnotation
ELEnumLiteral = ecore.EEnumLiteral
ELClass = ecore.EClass
ELAttribute = ecore.EAttribute
ELReference = ecore.EReference
ELEnum = ecore.EEnum
ELOperation = ecore.EOperation
ELParameter = ecore.EParameter
ELStructuralFeature = ecore.EStructuralFeature
ELDataType = ecore.EDataType
ELClassifier = ecore.EClassifier
ELTypedElement = ecore.ETypedElement
ELPackage = ecore.EPackage
ELNamedElement = ecore.ENamedElement

# Custom class definitions
class Import(ecore.EObject, metaclass=ecore.MetaEClass):
    importedNamespace = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )

    def __init__(self, *, importedNamespace=None):
        super().__init__()

        if importedNamespace is not None:
            self.importedNamespace = importedNamespace


class Module(ecore.EObject, metaclass=ecore.MetaEClass):
    theDescription = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )
    license = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )
    name = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True, iD=True
    )
    version = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )
    dependencies = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False, upper=-1
    )
    imports = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    annotationDirectives = ecore.EReference(
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


class ModuleList(ecore.EObject, metaclass=ecore.MetaEClass):
    modules = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, modules=None):
        super().__init__()

        if modules:
            self.modules.extend(modules)


class RulesForReport(ecore.EObject, metaclass=ecore.MetaEClass):
    outputLayerCube = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    rulesForTable = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, outputLayerCube=None, rulesForTable=None):
        super().__init__()

        if outputLayerCube is not None:
            self.outputLayerCube = outputLayerCube

        if rulesForTable:
            self.rulesForTable.extend(rulesForTable)


class RulesForILTable(ecore.EObject, metaclass=ecore.MetaEClass):
    rulesForTablePart = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    inputLayerTable = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, rulesForTablePart=None, inputLayerTable=None):
        super().__init__()

        if rulesForTablePart:
            self.rulesForTablePart.extend(rulesForTablePart)

        if inputLayerTable is not None:
            self.inputLayerTable = inputLayerTable


class SelectColumn(ecore.EObject, metaclass=ecore.MetaEClass):
    asAttribute = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, asAttribute=None):
        super().__init__()

        if asAttribute is not None:
            self.asAttribute = asAttribute


class TableFilter(ecore.EObject, metaclass=ecore.MetaEClass):
    predicate = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False
    )

    def __init__(self, *, predicate=None):
        super().__init__()

        if predicate is not None:
            self.predicate = predicate


class RuleForILTablePart(ecore.EObject, metaclass=ecore.MetaEClass):
    name = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )
    columns = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    whereClause = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False
    )

    def __init__(self, *, name=None, columns=None, whereClause=None):
        super().__init__()

        if name is not None:
            self.name = name

        if columns:
            self.columns.extend(columns)

        if whereClause is not None:
            self.whereClause = whereClause


@ecore.abstract
class Predicate(ecore.EObject, metaclass=ecore.MetaEClass):
    def __init__(self):
        super().__init__()


@ecore.abstract
class ELModelElement(ecore.EObject, metaclass=ecore.MetaEClass):
    eAnnotations = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, eAnnotations=None):
        super().__init__()

        if eAnnotations:
            self.eAnnotations.extend(eAnnotations)


class Report(ecore.EObject, metaclass=ecore.MetaEClass):
    outputLayer = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    rows = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    columns = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, outputLayer=None, rows=None, columns=None):
        super().__init__()

        if outputLayer is not None:
            self.outputLayer = outputLayer

        if rows:
            self.rows.extend(rows)

        if columns:
            self.columns.extend(columns)


class ReportRow(ecore.EObject, metaclass=ecore.MetaEClass):
    name = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True, iD=True
    )

    def __init__(self, *, name=None):
        super().__init__()

        if name is not None:
            self.name = name


class ReportColumn(ecore.EObject, metaclass=ecore.MetaEClass):
    name = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True, iD=True
    )

    def __init__(self, *, name=None):
        super().__init__()

        if name is not None:
            self.name = name


class ReportCell(ecore.EObject, metaclass=ecore.MetaEClass):
    datapointID = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )
    row = ecore.EReference(ordered=True, unique=True, containment=False, derived=False)
    column = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    filters = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    metric = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(
        self, *, row=None, column=None, filters=None, metric=None, datapointID=None
    ):
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


class Filter(ecore.EObject, metaclass=ecore.MetaEClass):
    outputLayer = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    operation = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    member = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False, upper=-1
    )

    def __init__(self, *, outputLayer=None, operation=None, member=None):
        super().__init__()

        if outputLayer is not None:
            self.outputLayer = outputLayer

        if operation is not None:
            self.operation = operation

        if member:
            self.member.extend(member)


class RowFilters(ecore.EObject, metaclass=ecore.MetaEClass):
    row = ecore.EReference(ordered=True, unique=True, containment=False, derived=False)
    filters = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    metric = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, row=None, filters=None, metric=None):
        super().__init__()

        if row is not None:
            self.row = row

        if filters:
            self.filters.extend(filters)

        if metric is not None:
            self.metric = metric


class ColumnFilters(ecore.EObject, metaclass=ecore.MetaEClass):
    column = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    filters = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    metric = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, column=None, filters=None, metric=None):
        super().__init__()

        if column is not None:
            self.column = column

        if filters:
            self.filters.extend(filters)

        if metric is not None:
            self.metric = metric


class WholeReportFilters(ecore.EObject, metaclass=ecore.MetaEClass):
    filters = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, filters=None):
        super().__init__()

        if filters:
            self.filters.extend(filters)


class SelectColumnMemberAs(SelectColumn):
    memberAsConstant = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, memberAsConstant=None, **kwargs):
        super().__init__(**kwargs)

        if memberAsConstant is not None:
            self.memberAsConstant = memberAsConstant


class SelectColumnAttributeAs(SelectColumn):
    attribute = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, attribute=None, **kwargs):
        super().__init__(**kwargs)

        if attribute is not None:
            self.attribute = attribute


class SelectDerivedColumnAs(SelectColumn):
    attribute = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, attribute=None, **kwargs):
        super().__init__(**kwargs)

        if attribute is not None:
            self.attribute = attribute


class SelectValueAs(SelectColumn):
    value = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )

    def __init__(self, *, value=None, **kwargs):
        super().__init__(**kwargs)

        if value is not None:
            self.value = value


class GenerationRulesModule(Module):
    rulesForReport = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, rulesForReport=None, **kwargs):
        super().__init__(**kwargs)

        if rulesForReport:
            self.rulesForReport.extend(rulesForReport)


class AndPredicate(Predicate):
    operands = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, operands=None, **kwargs):
        super().__init__(**kwargs)

        if operands:
            self.operands.extend(operands)


class OrPredicate(Predicate):
    operands = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, operands=None, **kwargs):
        super().__init__(**kwargs)

        if operands:
            self.operands.extend(operands)


class NotPredicate(Predicate):
    operand = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False
    )

    def __init__(self, *, operand=None, **kwargs):
        super().__init__(**kwargs)

        if operand is not None:
            self.operand = operand


class AttributePredicate(Predicate):
    comparitor = ecore.EAttribute(
        eType=Comparitor, unique=True, derived=False, changeable=True
    )
    value = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )
    attribute1 = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )
    member = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

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


class CellBasedReport(Report):
    reportCells = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, reportCells=None, **kwargs):
        super().__init__(**kwargs)

        if reportCells:
            self.reportCells.extend(reportCells)


class ReportModule(Module):
    reports = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )

    def __init__(self, *, reports=None, **kwargs):
        super().__init__(**kwargs)

        if reports:
            self.reports.extend(reports)


class RowColumnBasedReport(Report):
    columnFilters = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    rowFilters = ecore.EReference(
        ordered=True, unique=True, containment=True, derived=False, upper=-1
    )
    wholeReportFilters = ecore.EReference(
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

class ELAnnotationDirective(ELNamedElement):
    sourceURI = ecore.EAttribute(
        eType=ecore.EString, unique=True, derived=False, changeable=True
    )
    module = ecore.EReference(
        ordered=True, unique=True, containment=False, derived=False
    )

    def __init__(self, *, module=None, sourceURI=None, **kwargs):
        super().__init__(**kwargs)

        if sourceURI is not None:
            self.sourceURI = sourceURI

        if module is not None:
            self.module = module
