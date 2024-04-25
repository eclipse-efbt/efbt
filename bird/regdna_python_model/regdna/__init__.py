
from .regdna import getEClassifier, eClassifiers
from .regdna import name, nsURI, nsPrefix, eClass
from .regdna import Import, Module, ModuleList, RulesForReport, RulesForILTable, SelectColumn, SelectColumnMemberAs, SelectColumnAttributeAs, SelectDerivedColumnAs, SelectValueAs, Comparitor, TableFilter, GenerationRulesModule, RuleForILTablePart, Predicate, AndPredicate, OrPredicate, NotPredicate, AttributePredicate, ELAttribute, ELClass, ELClassifier, ELDataType, ELEnum, ELEnumLiteral, ELModelElement, ELNamedElement, ELOperation, ELParameter, ELPackage, ELReference, ELStructuralFeature, ELTypedElement, ELAnnotation, ELAnnotationDirective, ELStringToStringMapEntry, Report, CellBasedReport, ReportRow, ReportColumn, ReportCell, Filter, ReportModule, RowColumnBasedReport, RowFilters, ColumnFilters, WholeReportFilters


from . import regdna

__all__ = ['Import', 'Module', 'ModuleList', 'RulesForReport', 'RulesForILTable', 'SelectColumn', 'SelectColumnMemberAs', 'SelectColumnAttributeAs', 'SelectDerivedColumnAs', 'SelectValueAs', 'Comparitor', 'TableFilter', 'GenerationRulesModule', 'RuleForILTablePart', 'Predicate', 'AndPredicate', 'OrPredicate', 'NotPredicate', 'AttributePredicate', 'ELAttribute', 'ELClass', 'ELClassifier', 'ELDataType',
           'ELEnum', 'ELEnumLiteral', 'ELModelElement', 'ELNamedElement', 'ELOperation', 'ELParameter', 'ELPackage', 'ELReference', 'ELStructuralFeature', 'ELTypedElement', 'ELAnnotation', 'ELAnnotationDirective', 'ELStringToStringMapEntry', 'Report', 'CellBasedReport', 'ReportRow', 'ReportColumn', 'ReportCell', 'Filter', 'ReportModule', 'RowColumnBasedReport', 'RowFilters', 'ColumnFilters', 'WholeReportFilters']

eSubpackages = []
eSuperPackage = None
regdna.eSubpackages = eSubpackages
regdna.eSuperPackage = eSuperPackage

Module.dependencies.eType = Module
Module.imports.eType = Import
Module.annotationDirectives.eType = ELAnnotationDirective
ModuleList.modules.eType = Module
RulesForReport.outputLayerCube.eType = ELClass
RulesForReport.rulesForTable.eType = RulesForILTable
RulesForILTable.rulesForTablePart.eType = RuleForILTablePart
RulesForILTable.inputLayerTable.eType = ELClass
SelectColumn.asAttribute.eType = ELOperation
SelectColumnMemberAs.memberAsConstant.eType = ELEnumLiteral
SelectColumnAttributeAs.attribute.eType = ELAttribute
SelectDerivedColumnAs.attribute.eType = ELOperation
TableFilter.predicate.eType = Predicate
GenerationRulesModule.rulesForReport.eType = RulesForReport
RuleForILTablePart.columns.eType = SelectColumn
RuleForILTablePart.whereClause.eType = TableFilter
AndPredicate.operands.eType = Predicate
OrPredicate.operands.eType = Predicate
NotPredicate.operand.eType = Predicate
AttributePredicate.attribute1.eType = ELStructuralFeature
AttributePredicate.member.eType = ELEnumLiteral
ELAttribute.eAttributeType.eType = ELDataType
ELClass.eSuperTypes.eType = ELClass
ELClass.eStructuralFeatures.eType = ELStructuralFeature
ELClass.eOperations.eType = ELOperation
ELEnum.eLiterals.eType = ELEnumLiteral
ELModelElement.eAnnotations.eType = ELAnnotation
ELReference.eReferenceType.eType = ELClassifier
ELReference.eOpposite.eType = ELReference
ELTypedElement.eType.eType = ELClassifier
ELAnnotation.details.eType = ELStringToStringMapEntry
ELAnnotation.source.eType = ELAnnotationDirective
ELAnnotationDirective.module.eType = Module
Report.outputLayer.eType = ELClass
Report.rows.eType = ReportRow
Report.columns.eType = ReportColumn
CellBasedReport.reportCells.eType = ReportCell
ReportCell.row.eType = ReportRow
ReportCell.column.eType = ReportColumn
ReportCell.filters.eType = Filter
ReportCell.metric.eType = ELOperation
Filter.outputLayer.eType = ELClass
Filter.operation.eType = ELOperation
Filter.member.eType = ELEnumLiteral
ReportModule.reports.eType = Report
RowColumnBasedReport.columnFilters.eType = ColumnFilters
RowColumnBasedReport.rowFilters.eType = RowFilters
RowColumnBasedReport.wholeReportFilters.eType = WholeReportFilters
RowFilters.row.eType = ReportRow
RowFilters.filters.eType = Filter
RowFilters.metric.eType = ELOperation
ColumnFilters.column.eType = ReportColumn
ColumnFilters.filters.eType = Filter
ColumnFilters.metric.eType = ELOperation
WholeReportFilters.filters.eType = Filter
ELClassifier.ePackage.eType = ELPackage
ELPackage.eClassifiers.eType = ELClassifier
ELPackage.eClassifiers.eOpposite = ELClassifier.ePackage

otherClassifiers = [Comparitor]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
