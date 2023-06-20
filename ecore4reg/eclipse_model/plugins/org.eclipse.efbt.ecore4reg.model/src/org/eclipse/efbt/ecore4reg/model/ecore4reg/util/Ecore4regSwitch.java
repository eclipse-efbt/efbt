/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.util;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELNamedElement;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELParameter;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStructuralFeature;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Import;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionText;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnAttributeAs;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnMemberAs;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectDerivedColumnAs;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectValueAs;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.TagGroup;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLEnrichedCube;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage
 * @generated
 */
public class Ecore4regSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Ecore4regPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore4regSwitch() {
		if (modelPackage == null) {
			modelPackage = Ecore4regPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case Ecore4regPackage.IMPORT: {
				Import import_ = (Import)theEObject;
				T result = caseImport(import_);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.MODULE: {
				org.eclipse.efbt.ecore4reg.model.ecore4reg.Module module = (org.eclipse.efbt.ecore4reg.model.ecore4reg.Module)theEObject;
				T result = caseModule(module);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.MODULE_LIST: {
				ModuleList moduleList = (ModuleList)theEObject;
				T result = caseModuleList(moduleList);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.ALLOWED_TYPES: {
				AllowedTypes allowedTypes = (AllowedTypes)theEObject;
				T result = caseAllowedTypes(allowedTypes);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.REQUIREMENTS_MODULE: {
				RequirementsModule requirementsModule = (RequirementsModule)theEObject;
				T result = caseRequirementsModule(requirementsModule);
				if (result == null) result = caseModule(requirementsModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.REQUIREMENTS_SECTION: {
				RequirementsSection requirementsSection = (RequirementsSection)theEObject;
				T result = caseRequirementsSection(requirementsSection);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.REQUIREMENTS_SECTION_IMAGE: {
				RequirementsSectionImage requirementsSectionImage = (RequirementsSectionImage)theEObject;
				T result = caseRequirementsSectionImage(requirementsSectionImage);
				if (result == null) result = caseRequirementsSection(requirementsSectionImage);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT: {
				RequirementsSectionLinkWithText requirementsSectionLinkWithText = (RequirementsSectionLinkWithText)theEObject;
				T result = caseRequirementsSectionLinkWithText(requirementsSectionLinkWithText);
				if (result == null) result = caseRequirementsSection(requirementsSectionLinkWithText);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.REQUIREMENTS_SECTION_TEXT: {
				RequirementsSectionText requirementsSectionText = (RequirementsSectionText)theEObject;
				T result = caseRequirementsSectionText(requirementsSectionText);
				if (result == null) result = caseRequirementsSection(requirementsSectionText);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.REQUIREMENT_TYPE: {
				RequirementType requirementType = (RequirementType)theEObject;
				T result = caseRequirementType(requirementType);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.TITLED_REQUIREMENTS_SECTION: {
				TitledRequirementsSection titledRequirementsSection = (TitledRequirementsSection)theEObject;
				T result = caseTitledRequirementsSection(titledRequirementsSection);
				if (result == null) result = caseRequirementsSection(titledRequirementsSection);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.TAG: {
				Tag tag = (Tag)theEObject;
				T result = caseTag(tag);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.TAG_GROUP: {
				TagGroup tagGroup = (TagGroup)theEObject;
				T result = caseTagGroup(tagGroup);
				if (result == null) result = caseModule(tagGroup);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.RULES_FOR_REPORT: {
				RulesForReport rulesForReport = (RulesForReport)theEObject;
				T result = caseRulesForReport(rulesForReport);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.RULES_FOR_IL_TABLE: {
				RulesForILTable rulesForILTable = (RulesForILTable)theEObject;
				T result = caseRulesForILTable(rulesForILTable);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.SELECT_COLUMN: {
				SelectColumn selectColumn = (SelectColumn)theEObject;
				T result = caseSelectColumn(selectColumn);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.SELECT_COLUMN_MEMBER_AS: {
				SelectColumnMemberAs selectColumnMemberAs = (SelectColumnMemberAs)theEObject;
				T result = caseSelectColumnMemberAs(selectColumnMemberAs);
				if (result == null) result = caseSelectColumn(selectColumnMemberAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.SELECT_COLUMN_ATTRIBUTE_AS: {
				SelectColumnAttributeAs selectColumnAttributeAs = (SelectColumnAttributeAs)theEObject;
				T result = caseSelectColumnAttributeAs(selectColumnAttributeAs);
				if (result == null) result = caseSelectColumn(selectColumnAttributeAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.SELECT_DERIVED_COLUMN_AS: {
				SelectDerivedColumnAs selectDerivedColumnAs = (SelectDerivedColumnAs)theEObject;
				T result = caseSelectDerivedColumnAs(selectDerivedColumnAs);
				if (result == null) result = caseSelectColumn(selectDerivedColumnAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.SELECT_VALUE_AS: {
				SelectValueAs selectValueAs = (SelectValueAs)theEObject;
				T result = caseSelectValueAs(selectValueAs);
				if (result == null) result = caseSelectColumn(selectValueAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.TABLE_FILTER: {
				TableFilter tableFilter = (TableFilter)theEObject;
				T result = caseTableFilter(tableFilter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.GENERATION_RULES_MODULE: {
				GenerationRulesModule generationRulesModule = (GenerationRulesModule)theEObject;
				T result = caseGenerationRulesModule(generationRulesModule);
				if (result == null) result = caseModule(generationRulesModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART: {
				RuleForILTablePart ruleForILTablePart = (RuleForILTablePart)theEObject;
				T result = caseRuleForILTablePart(ruleForILTablePart);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_ATTRIBUTE: {
				ELAttribute elAttribute = (ELAttribute)theEObject;
				T result = caseELAttribute(elAttribute);
				if (result == null) result = caseELStructuralFeature(elAttribute);
				if (result == null) result = caseELTypedElement(elAttribute);
				if (result == null) result = caseELNamedElement(elAttribute);
				if (result == null) result = caseELModelElement(elAttribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_CLASS: {
				ELClass elClass = (ELClass)theEObject;
				T result = caseELClass(elClass);
				if (result == null) result = caseELClassifier(elClass);
				if (result == null) result = caseELNamedElement(elClass);
				if (result == null) result = caseELModelElement(elClass);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_CLASSIFIER: {
				ELClassifier elClassifier = (ELClassifier)theEObject;
				T result = caseELClassifier(elClassifier);
				if (result == null) result = caseELNamedElement(elClassifier);
				if (result == null) result = caseELModelElement(elClassifier);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_DATA_TYPE: {
				ELDataType elDataType = (ELDataType)theEObject;
				T result = caseELDataType(elDataType);
				if (result == null) result = caseELClassifier(elDataType);
				if (result == null) result = caseELNamedElement(elDataType);
				if (result == null) result = caseELModelElement(elDataType);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_ENUM: {
				ELEnum elEnum = (ELEnum)theEObject;
				T result = caseELEnum(elEnum);
				if (result == null) result = caseELDataType(elEnum);
				if (result == null) result = caseELClassifier(elEnum);
				if (result == null) result = caseELNamedElement(elEnum);
				if (result == null) result = caseELModelElement(elEnum);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_ENUM_LITERAL: {
				ELEnumLiteral elEnumLiteral = (ELEnumLiteral)theEObject;
				T result = caseELEnumLiteral(elEnumLiteral);
				if (result == null) result = caseELNamedElement(elEnumLiteral);
				if (result == null) result = caseELModelElement(elEnumLiteral);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_MODEL_ELEMENT: {
				ELModelElement elModelElement = (ELModelElement)theEObject;
				T result = caseELModelElement(elModelElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_NAMED_ELEMENT: {
				ELNamedElement elNamedElement = (ELNamedElement)theEObject;
				T result = caseELNamedElement(elNamedElement);
				if (result == null) result = caseELModelElement(elNamedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_OPERATION: {
				ELOperation elOperation = (ELOperation)theEObject;
				T result = caseELOperation(elOperation);
				if (result == null) result = caseELTypedElement(elOperation);
				if (result == null) result = caseELNamedElement(elOperation);
				if (result == null) result = caseELModelElement(elOperation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_PARAMETER: {
				ELParameter elParameter = (ELParameter)theEObject;
				T result = caseELParameter(elParameter);
				if (result == null) result = caseELTypedElement(elParameter);
				if (result == null) result = caseELNamedElement(elParameter);
				if (result == null) result = caseELModelElement(elParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_PACKAGE: {
				ELPackage elPackage = (ELPackage)theEObject;
				T result = caseELPackage(elPackage);
				if (result == null) result = caseModule(elPackage);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_REFERENCE: {
				ELReference elReference = (ELReference)theEObject;
				T result = caseELReference(elReference);
				if (result == null) result = caseELStructuralFeature(elReference);
				if (result == null) result = caseELTypedElement(elReference);
				if (result == null) result = caseELNamedElement(elReference);
				if (result == null) result = caseELModelElement(elReference);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_STRUCTURAL_FEATURE: {
				ELStructuralFeature elStructuralFeature = (ELStructuralFeature)theEObject;
				T result = caseELStructuralFeature(elStructuralFeature);
				if (result == null) result = caseELTypedElement(elStructuralFeature);
				if (result == null) result = caseELNamedElement(elStructuralFeature);
				if (result == null) result = caseELModelElement(elStructuralFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_TYPED_ELEMENT: {
				ELTypedElement elTypedElement = (ELTypedElement)theEObject;
				T result = caseELTypedElement(elTypedElement);
				if (result == null) result = caseELNamedElement(elTypedElement);
				if (result == null) result = caseELModelElement(elTypedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_ANNOTATION: {
				ELAnnotation elAnnotation = (ELAnnotation)theEObject;
				T result = caseELAnnotation(elAnnotation);
				if (result == null) result = caseELModelElement(elAnnotation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.EL_STRING_TO_STRING_MAP_ENTRY: {
				ELStringToStringMapEntry elStringToStringMapEntry = (ELStringToStringMapEntry)theEObject;
				T result = caseELStringToStringMapEntry(elStringToStringMapEntry);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_ENRICHED_CUBE: {
				VTLEnrichedCube vtlEnrichedCube = (VTLEnrichedCube)theEObject;
				T result = caseVTLEnrichedCube(vtlEnrichedCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER: {
				VTLGeneratedOutputlayer vtlGeneratedOutputlayer = (VTLGeneratedOutputlayer)theEObject;
				T result = caseVTLGeneratedOutputlayer(vtlGeneratedOutputlayer);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION: {
				VTLForOutputLayerAndIntermediateLayerCombination vtlForOutputLayerAndIntermediateLayerCombination = (VTLForOutputLayerAndIntermediateLayerCombination)theEObject;
				T result = caseVTLForOutputLayerAndIntermediateLayerCombination(vtlForOutputLayerAndIntermediateLayerCombination);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER: {
				VTLGeneratedIntermediateLayer vtlGeneratedIntermediateLayer = (VTLGeneratedIntermediateLayer)theEObject;
				T result = caseVTLGeneratedIntermediateLayer(vtlGeneratedIntermediateLayer);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_TRANSFORMATION: {
				VTLTransformation vtlTransformation = (VTLTransformation)theEObject;
				T result = caseVTLTransformation(vtlTransformation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_SCHEME: {
				VTLScheme vtlScheme = (VTLScheme)theEObject;
				T result = caseVTLScheme(vtlScheme);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER: {
				VTLForSelectionLayer vtlForSelectionLayer = (VTLForSelectionLayer)theEObject;
				T result = caseVTLForSelectionLayer(vtlForSelectionLayer);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_MODULE: {
				VTLModule vtlModule = (VTLModule)theEObject;
				T result = caseVTLModule(vtlModule);
				if (result == null) result = caseModule(vtlModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK: {
				EntityToVTLIntermediateLayerLink entityToVTLIntermediateLayerLink = (EntityToVTLIntermediateLayerLink)theEObject;
				T result = caseEntityToVTLIntermediateLayerLink(entityToVTLIntermediateLayerLink);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_FOR_VIEW: {
				VTLForView vtlForView = (VTLForView)theEObject;
				T result = caseVTLForView(vtlForView);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE: {
				VTLGeneratedOutputlayerModule vtlGeneratedOutputlayerModule = (VTLGeneratedOutputlayerModule)theEObject;
				T result = caseVTLGeneratedOutputlayerModule(vtlGeneratedOutputlayerModule);
				if (result == null) result = caseModule(vtlGeneratedOutputlayerModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE: {
				VTLForOutputLayerAndIntermediateLayerCombinationModule vtlForOutputLayerAndIntermediateLayerCombinationModule = (VTLForOutputLayerAndIntermediateLayerCombinationModule)theEObject;
				T result = caseVTLForOutputLayerAndIntermediateLayerCombinationModule(vtlForOutputLayerAndIntermediateLayerCombinationModule);
				if (result == null) result = caseModule(vtlForOutputLayerAndIntermediateLayerCombinationModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE: {
				VTLGeneratedIntermediateLayerModule vtlGeneratedIntermediateLayerModule = (VTLGeneratedIntermediateLayerModule)theEObject;
				T result = caseVTLGeneratedIntermediateLayerModule(vtlGeneratedIntermediateLayerModule);
				if (result == null) result = caseModule(vtlGeneratedIntermediateLayerModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_SCHEMES_MODULE: {
				VTLSchemesModule vtlSchemesModule = (VTLSchemesModule)theEObject;
				T result = caseVTLSchemesModule(vtlSchemesModule);
				if (result == null) result = caseModule(vtlSchemesModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE: {
				VTLForSelectionLayerModule vtlForSelectionLayerModule = (VTLForSelectionLayerModule)theEObject;
				T result = caseVTLForSelectionLayerModule(vtlForSelectionLayerModule);
				if (result == null) result = caseModule(vtlForSelectionLayerModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE: {
				EntityToVTLIntermediateLayerLinkModule entityToVTLIntermediateLayerLinkModule = (EntityToVTLIntermediateLayerLinkModule)theEObject;
				T result = caseEntityToVTLIntermediateLayerLinkModule(entityToVTLIntermediateLayerLinkModule);
				if (result == null) result = caseModule(entityToVTLIntermediateLayerLinkModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore4regPackage.VTL_FOR_VIEW_MODULE: {
				VTLForViewModule vtlForViewModule = (VTLForViewModule)theEObject;
				T result = caseVTLForViewModule(vtlForViewModule);
				if (result == null) result = caseModule(vtlForViewModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Import</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Import</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseImport(Import object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.ecore4reg.model.ecore4reg.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module List</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module List</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModuleList(ModuleList object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Allowed Types</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Allowed Types</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAllowedTypes(AllowedTypes object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsModule(RequirementsModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSection(RequirementsSection object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section Image</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section Image</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSectionImage(RequirementsSectionImage object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section Link With Text</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section Link With Text</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSectionLinkWithText(RequirementsSectionLinkWithText object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirements Section Text</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirements Section Text</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementsSectionText(RequirementsSectionText object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Requirement Type</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Requirement Type</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRequirementType(RequirementType object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Titled Requirements Section</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Titled Requirements Section</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTitledRequirementsSection(TitledRequirementsSection object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTag(Tag object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Tag Group</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Tag Group</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTagGroup(TagGroup object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Rules For Report</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Rules For Report</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRulesForReport(RulesForReport object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Rules For IL Table</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Rules For IL Table</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRulesForILTable(RulesForILTable object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Column</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Column</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectColumn(SelectColumn object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Column Member As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Column Member As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectColumnMemberAs(SelectColumnMemberAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Column Attribute As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Column Attribute As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectColumnAttributeAs(SelectColumnAttributeAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Derived Column As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Derived Column As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectDerivedColumnAs(SelectDerivedColumnAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Value As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Value As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectValueAs(SelectValueAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Table Filter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Table Filter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTableFilter(TableFilter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Generation Rules Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Generation Rules Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGenerationRulesModule(GenerationRulesModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Rule For IL Table Part</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Rule For IL Table Part</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRuleForILTablePart(RuleForILTablePart object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELAttribute(ELAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Class</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Class</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELClass(ELClass object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Classifier</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Classifier</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELClassifier(ELClassifier object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Data Type</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Data Type</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELDataType(ELDataType object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Enum</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Enum</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELEnum(ELEnum object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Enum Literal</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Enum Literal</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELEnumLiteral(ELEnumLiteral object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Model Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Model Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELModelElement(ELModelElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Named Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Named Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELNamedElement(ELNamedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELOperation(ELOperation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELParameter(ELParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Package</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Package</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELPackage(ELPackage object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Reference</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Reference</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELReference(ELReference object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Structural Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Structural Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELStructuralFeature(ELStructuralFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Typed Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Typed Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELTypedElement(ELTypedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Annotation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Annotation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELAnnotation(ELAnnotation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL String To String Map Entry</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL String To String Map Entry</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELStringToStringMapEntry(ELStringToStringMapEntry object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Enriched Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Enriched Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLEnrichedCube(VTLEnrichedCube object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Generated Outputlayer</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Generated Outputlayer</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLGeneratedOutputlayer(VTLGeneratedOutputlayer object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL For Output Layer And Intermediate Layer Combination</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL For Output Layer And Intermediate Layer Combination</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLForOutputLayerAndIntermediateLayerCombination(VTLForOutputLayerAndIntermediateLayerCombination object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Generated Intermediate Layer</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Generated Intermediate Layer</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLGeneratedIntermediateLayer(VTLGeneratedIntermediateLayer object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Transformation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Transformation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLTransformation(VTLTransformation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Scheme</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Scheme</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLScheme(VTLScheme object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL For Selection Layer</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL For Selection Layer</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLForSelectionLayer(VTLForSelectionLayer object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLModule(VTLModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Entity To VTL Intermediate Layer Link</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity To VTL Intermediate Layer Link</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntityToVTLIntermediateLayerLink(EntityToVTLIntermediateLayerLink object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL For View</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL For View</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLForView(VTLForView object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Generated Outputlayer Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Generated Outputlayer Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLGeneratedOutputlayerModule(VTLGeneratedOutputlayerModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL For Output Layer And Intermediate Layer Combination Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL For Output Layer And Intermediate Layer Combination Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLForOutputLayerAndIntermediateLayerCombinationModule(VTLForOutputLayerAndIntermediateLayerCombinationModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Generated Intermediate Layer Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Generated Intermediate Layer Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLGeneratedIntermediateLayerModule(VTLGeneratedIntermediateLayerModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL Schemes Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL Schemes Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLSchemesModule(VTLSchemesModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL For Selection Layer Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL For Selection Layer Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLForSelectionLayerModule(VTLForSelectionLayerModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Entity To VTL Intermediate Layer Link Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity To VTL Intermediate Layer Link Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntityToVTLIntermediateLayerLinkModule(EntityToVTLIntermediateLayerLinkModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VTL For View Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VTL For View Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVTLForViewModule(VTLForViewModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //Ecore4regSwitch