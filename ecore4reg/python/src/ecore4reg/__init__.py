
from .ecore4reg import getEClassifier, eClassifiers
from .ecore4reg import name, nsURI, nsPrefix, eClass
from .ecore4reg import Import, Module, ModuleList, AllowedTypes, RequirementsModule, RequirementsSection, RequirementsSectionImage, RequirementsSectionLinkWithText, RequirementsSectionText, RequirementType, TitledRequirementsSection, Tag, TagGroup, RulesForReport, RulesForILTable, SelectColumn, SelectColumnMemberAs, SelectColumnAttributeAs, SelectDerivedColumnAs, SelectValueAs, Comparitor, TableFilter, GenerationRulesModule, RuleForILTablePart, Predicate, AndPredicate, OrPredicate, NotPredicate, AttributePredicate, ELAttribute, ELClass, ELClassifier, ELDataType, ELEnum, ELEnumLiteral, ELModelElement, ELNamedElement, ELOperation, ELPublicOperation, ELPrivateOperation, ELParameter, ELPackage, ELReference, ELStructuralFeature, ELTypedElement, ELAnnotation, ELStringToStringMapEntry, OperationTag


from . import ecore4reg

__all__ = ['Import', 'Module', 'ModuleList', 'AllowedTypes', 'RequirementsModule', 'RequirementsSection', 'RequirementsSectionImage', 'RequirementsSectionLinkWithText', 'RequirementsSectionText', 'RequirementType', 'TitledRequirementsSection', 'Tag', 'TagGroup', 'RulesForReport', 'RulesForILTable', 'SelectColumn', 'SelectColumnMemberAs', 'SelectColumnAttributeAs', 'SelectDerivedColumnAs', 'SelectValueAs', 'Comparitor', 'TableFilter',
           'GenerationRulesModule', 'RuleForILTablePart', 'Predicate', 'AndPredicate', 'OrPredicate', 'NotPredicate', 'AttributePredicate', 'ELAttribute', 'ELClass', 'ELClassifier', 'ELDataType', 'ELEnum', 'ELEnumLiteral', 'ELModelElement', 'ELNamedElement', 'ELOperation', 'ELPublicOperation', 'ELPrivateOperation', 'ELParameter', 'ELPackage', 'ELReference', 'ELStructuralFeature', 'ELTypedElement', 'ELAnnotation', 'ELStringToStringMapEntry', 'OperationTag']

eSubpackages = []
eSuperPackage = None
ecore4reg.eSubpackages = eSubpackages
ecore4reg.eSuperPackage = eSuperPackage

Module.dependencies.eType = Module
Module.imports.eType = Import
ModuleList.modules.eType = Module
AllowedTypes.allowedTypes.eType = RequirementType
RequirementsModule.rules.eType = TitledRequirementsSection
RequirementsModule.allowedtypes.eType = AllowedTypes
TitledRequirementsSection.sections.eType = RequirementsSection
TitledRequirementsSection.requirementsType.eType = RequirementType
Tag.requirements.eType = TitledRequirementsSection
TagGroup.tags.eType = Tag
RulesForReport.outputLayerCube.eType = ELClass
RulesForReport.rulesForTable.eType = RulesForILTable
RulesForILTable.rulesForTablePart.eType = RuleForILTablePart
RulesForILTable.inputLayerTable.eType = ELClass
SelectColumn.asAttribute.eType = ELPublicOperation
SelectColumnMemberAs.memberAsConstant.eType = ELEnumLiteral
SelectColumnAttributeAs.attribute.eType = ELAttribute
SelectDerivedColumnAs.attribute.eType = ELPublicOperation
TableFilter.predicate.eType = Predicate
GenerationRulesModule.rulesForReport.eType = RulesForReport
RuleForILTablePart.columns.eType = SelectColumn
RuleForILTablePart.whereClause.eType = TableFilter
AndPredicate.operands.eType = Predicate
OrPredicate.operands.eType = Predicate
NotPredicate.operand.eType = Predicate
AttributePredicate.attribute1.eType = ELAttribute
AttributePredicate.member.eType = ELEnumLiteral
ELAttribute.eAttributeType.eType = ELDataType
ELClass.eSuperTypes.eType = ELClass
ELClass.eStructuralFeatures.eType = ELStructuralFeature
ELClass.eOperations.eType = ELOperation
ELEnum.eLiterals.eType = ELEnumLiteral
ELModelElement.eAnnotations.eType = ELAnnotation
ELPublicOperation.calledPrivateOperations.eType = ELPrivateOperation
ELPrivateOperation.eParameters.eType = ELParameter
ELReference.eReferenceType.eType = ELClassifier
ELTypedElement.eType.eType = ELClassifier
ELAnnotation.details.eType = ELStringToStringMapEntry
OperationTag.operation.eType = ELOperation
RequirementsSectionLinkWithText.linkedRuleSection.eType = TitledRequirementsSection
TitledRequirementsSection.referencingSections.eType = RequirementsSectionLinkWithText
TitledRequirementsSection.referencingSections.eOpposite = RequirementsSectionLinkWithText.linkedRuleSection
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
