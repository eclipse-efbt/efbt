/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.AndPredicate;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.AttributePredicate;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Comparitor;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELParameter;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regFactory;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Import;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.NotPredicate;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.OrPredicate;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule;
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

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Ecore4regFactoryImpl extends EFactoryImpl implements Ecore4regFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Ecore4regFactory init() {
		try {
			Ecore4regFactory theEcore4regFactory = (Ecore4regFactory)EPackage.Registry.INSTANCE.getEFactory(Ecore4regPackage.eNS_URI);
			if (theEcore4regFactory != null) {
				return theEcore4regFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Ecore4regFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore4regFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Ecore4regPackage.IMPORT: return createImport();
			case Ecore4regPackage.MODULE: return createModule();
			case Ecore4regPackage.MODULE_LIST: return createModuleList();
			case Ecore4regPackage.ALLOWED_TYPES: return createAllowedTypes();
			case Ecore4regPackage.REQUIREMENTS_MODULE: return createRequirementsModule();
			case Ecore4regPackage.REQUIREMENTS_SECTION_IMAGE: return createRequirementsSectionImage();
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT: return createRequirementsSectionLinkWithText();
			case Ecore4regPackage.REQUIREMENTS_SECTION_TEXT: return createRequirementsSectionText();
			case Ecore4regPackage.REQUIREMENT_TYPE: return createRequirementType();
			case Ecore4regPackage.TITLED_REQUIREMENTS_SECTION: return createTitledRequirementsSection();
			case Ecore4regPackage.TAG: return createTag();
			case Ecore4regPackage.TAG_GROUP: return createTagGroup();
			case Ecore4regPackage.RULES_FOR_REPORT: return createRulesForReport();
			case Ecore4regPackage.RULES_FOR_IL_TABLE: return createRulesForILTable();
			case Ecore4regPackage.SELECT_COLUMN: return createSelectColumn();
			case Ecore4regPackage.SELECT_COLUMN_MEMBER_AS: return createSelectColumnMemberAs();
			case Ecore4regPackage.SELECT_COLUMN_ATTRIBUTE_AS: return createSelectColumnAttributeAs();
			case Ecore4regPackage.SELECT_DERIVED_COLUMN_AS: return createSelectDerivedColumnAs();
			case Ecore4regPackage.SELECT_VALUE_AS: return createSelectValueAs();
			case Ecore4regPackage.TABLE_FILTER: return createTableFilter();
			case Ecore4regPackage.GENERATION_RULES_MODULE: return createGenerationRulesModule();
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART: return createRuleForILTablePart();
			case Ecore4regPackage.AND_PREDICATE: return createAndPredicate();
			case Ecore4regPackage.OR_PREDICATE: return createOrPredicate();
			case Ecore4regPackage.NOT_PREDICATE: return createNotPredicate();
			case Ecore4regPackage.ATTRIBUTE_PREDICATE: return createAttributePredicate();
			case Ecore4regPackage.EL_ATTRIBUTE: return createELAttribute();
			case Ecore4regPackage.EL_CLASS: return createELClass();
			case Ecore4regPackage.EL_DATA_TYPE: return createELDataType();
			case Ecore4regPackage.EL_ENUM: return createELEnum();
			case Ecore4regPackage.EL_ENUM_LITERAL: return createELEnumLiteral();
			case Ecore4regPackage.EL_OPERATION: return createELOperation();
			case Ecore4regPackage.EL_PARAMETER: return createELParameter();
			case Ecore4regPackage.EL_PACKAGE: return createELPackage();
			case Ecore4regPackage.EL_REFERENCE: return createELReference();
			case Ecore4regPackage.EL_ANNOTATION: return createELAnnotation();
			case Ecore4regPackage.EL_STRING_TO_STRING_MAP_ENTRY: return createELStringToStringMapEntry();
			case Ecore4regPackage.VTL_ENRICHED_CUBE: return createVTLEnrichedCube();
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER: return createVTLGeneratedOutputlayer();
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION: return createVTLForOutputLayerAndIntermediateLayerCombination();
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER: return createVTLGeneratedIntermediateLayer();
			case Ecore4regPackage.VTL_TRANSFORMATION: return createVTLTransformation();
			case Ecore4regPackage.VTL_SCHEME: return createVTLScheme();
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER: return createVTLForSelectionLayer();
			case Ecore4regPackage.VTL_MODULE: return createVTLModule();
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK: return createEntityToVTLIntermediateLayerLink();
			case Ecore4regPackage.VTL_FOR_VIEW: return createVTLForView();
			case Ecore4regPackage.VTL_GENERATED_OUTPUTLAYER_MODULE: return createVTLGeneratedOutputlayerModule();
			case Ecore4regPackage.VTL_FOR_OUTPUT_LAYER_AND_INTERMEDIATE_LAYER_COMBINATION_MODULE: return createVTLForOutputLayerAndIntermediateLayerCombinationModule();
			case Ecore4regPackage.VTL_GENERATED_INTERMEDIATE_LAYER_MODULE: return createVTLGeneratedIntermediateLayerModule();
			case Ecore4regPackage.VTL_SCHEMES_MODULE: return createVTLSchemesModule();
			case Ecore4regPackage.VTL_FOR_SELECTION_LAYER_MODULE: return createVTLForSelectionLayerModule();
			case Ecore4regPackage.ENTITY_TO_VTL_INTERMEDIATE_LAYER_LINK_MODULE: return createEntityToVTLIntermediateLayerLinkModule();
			case Ecore4regPackage.VTL_FOR_VIEW_MODULE: return createVTLForViewModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object createFromString(EDataType eDataType, String initialValue) {
		switch (eDataType.getClassifierID()) {
			case Ecore4regPackage.COMPARITOR:
				return createComparitorFromString(eDataType, initialValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String convertToString(EDataType eDataType, Object instanceValue) {
		switch (eDataType.getClassifierID()) {
			case Ecore4regPackage.COMPARITOR:
				return convertComparitorToString(eDataType, instanceValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Import createImport() {
		ImportImpl import_ = new ImportImpl();
		return import_;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public org.eclipse.efbt.ecore4reg.model.ecore4reg.Module createModule() {
		ModuleImpl module = new ModuleImpl();
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleList createModuleList() {
		ModuleListImpl moduleList = new ModuleListImpl();
		return moduleList;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AllowedTypes createAllowedTypes() {
		AllowedTypesImpl allowedTypes = new AllowedTypesImpl();
		return allowedTypes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsModule createRequirementsModule() {
		RequirementsModuleImpl requirementsModule = new RequirementsModuleImpl();
		return requirementsModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsSectionImage createRequirementsSectionImage() {
		RequirementsSectionImageImpl requirementsSectionImage = new RequirementsSectionImageImpl();
		return requirementsSectionImage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsSectionLinkWithText createRequirementsSectionLinkWithText() {
		RequirementsSectionLinkWithTextImpl requirementsSectionLinkWithText = new RequirementsSectionLinkWithTextImpl();
		return requirementsSectionLinkWithText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsSectionText createRequirementsSectionText() {
		RequirementsSectionTextImpl requirementsSectionText = new RequirementsSectionTextImpl();
		return requirementsSectionText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementType createRequirementType() {
		RequirementTypeImpl requirementType = new RequirementTypeImpl();
		return requirementType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TitledRequirementsSection createTitledRequirementsSection() {
		TitledRequirementsSectionImpl titledRequirementsSection = new TitledRequirementsSectionImpl();
		return titledRequirementsSection;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Tag createTag() {
		TagImpl tag = new TagImpl();
		return tag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TagGroup createTagGroup() {
		TagGroupImpl tagGroup = new TagGroupImpl();
		return tagGroup;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RulesForReport createRulesForReport() {
		RulesForReportImpl rulesForReport = new RulesForReportImpl();
		return rulesForReport;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RulesForILTable createRulesForILTable() {
		RulesForILTableImpl rulesForILTable = new RulesForILTableImpl();
		return rulesForILTable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectColumn createSelectColumn() {
		SelectColumnImpl selectColumn = new SelectColumnImpl();
		return selectColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectColumnMemberAs createSelectColumnMemberAs() {
		SelectColumnMemberAsImpl selectColumnMemberAs = new SelectColumnMemberAsImpl();
		return selectColumnMemberAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectColumnAttributeAs createSelectColumnAttributeAs() {
		SelectColumnAttributeAsImpl selectColumnAttributeAs = new SelectColumnAttributeAsImpl();
		return selectColumnAttributeAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectDerivedColumnAs createSelectDerivedColumnAs() {
		SelectDerivedColumnAsImpl selectDerivedColumnAs = new SelectDerivedColumnAsImpl();
		return selectDerivedColumnAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SelectValueAs createSelectValueAs() {
		SelectValueAsImpl selectValueAs = new SelectValueAsImpl();
		return selectValueAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TableFilter createTableFilter() {
		TableFilterImpl tableFilter = new TableFilterImpl();
		return tableFilter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GenerationRulesModule createGenerationRulesModule() {
		GenerationRulesModuleImpl generationRulesModule = new GenerationRulesModuleImpl();
		return generationRulesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RuleForILTablePart createRuleForILTablePart() {
		RuleForILTablePartImpl ruleForILTablePart = new RuleForILTablePartImpl();
		return ruleForILTablePart;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AndPredicate createAndPredicate() {
		AndPredicateImpl andPredicate = new AndPredicateImpl();
		return andPredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public OrPredicate createOrPredicate() {
		OrPredicateImpl orPredicate = new OrPredicateImpl();
		return orPredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotPredicate createNotPredicate() {
		NotPredicateImpl notPredicate = new NotPredicateImpl();
		return notPredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributePredicate createAttributePredicate() {
		AttributePredicateImpl attributePredicate = new AttributePredicateImpl();
		return attributePredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELAttribute createELAttribute() {
		ELAttributeImpl elAttribute = new ELAttributeImpl();
		return elAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClass createELClass() {
		ELClassImpl elClass = new ELClassImpl();
		return elClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELDataType createELDataType() {
		ELDataTypeImpl elDataType = new ELDataTypeImpl();
		return elDataType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELEnum createELEnum() {
		ELEnumImpl elEnum = new ELEnumImpl();
		return elEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELEnumLiteral createELEnumLiteral() {
		ELEnumLiteralImpl elEnumLiteral = new ELEnumLiteralImpl();
		return elEnumLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELOperation createELOperation() {
		ELOperationImpl elOperation = new ELOperationImpl();
		return elOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELParameter createELParameter() {
		ELParameterImpl elParameter = new ELParameterImpl();
		return elParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELPackage createELPackage() {
		ELPackageImpl elPackage = new ELPackageImpl();
		return elPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELReference createELReference() {
		ELReferenceImpl elReference = new ELReferenceImpl();
		return elReference;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELAnnotation createELAnnotation() {
		ELAnnotationImpl elAnnotation = new ELAnnotationImpl();
		return elAnnotation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELStringToStringMapEntry createELStringToStringMapEntry() {
		ELStringToStringMapEntryImpl elStringToStringMapEntry = new ELStringToStringMapEntryImpl();
		return elStringToStringMapEntry;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLEnrichedCube createVTLEnrichedCube() {
		VTLEnrichedCubeImpl vtlEnrichedCube = new VTLEnrichedCubeImpl();
		return vtlEnrichedCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedOutputlayer createVTLGeneratedOutputlayer() {
		VTLGeneratedOutputlayerImpl vtlGeneratedOutputlayer = new VTLGeneratedOutputlayerImpl();
		return vtlGeneratedOutputlayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForOutputLayerAndIntermediateLayerCombination createVTLForOutputLayerAndIntermediateLayerCombination() {
		VTLForOutputLayerAndIntermediateLayerCombinationImpl vtlForOutputLayerAndIntermediateLayerCombination = new VTLForOutputLayerAndIntermediateLayerCombinationImpl();
		return vtlForOutputLayerAndIntermediateLayerCombination;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayer createVTLGeneratedIntermediateLayer() {
		VTLGeneratedIntermediateLayerImpl vtlGeneratedIntermediateLayer = new VTLGeneratedIntermediateLayerImpl();
		return vtlGeneratedIntermediateLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLTransformation createVTLTransformation() {
		VTLTransformationImpl vtlTransformation = new VTLTransformationImpl();
		return vtlTransformation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLScheme createVTLScheme() {
		VTLSchemeImpl vtlScheme = new VTLSchemeImpl();
		return vtlScheme;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForSelectionLayer createVTLForSelectionLayer() {
		VTLForSelectionLayerImpl vtlForSelectionLayer = new VTLForSelectionLayerImpl();
		return vtlForSelectionLayer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLModule createVTLModule() {
		VTLModuleImpl vtlModule = new VTLModuleImpl();
		return vtlModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityToVTLIntermediateLayerLink createEntityToVTLIntermediateLayerLink() {
		EntityToVTLIntermediateLayerLinkImpl entityToVTLIntermediateLayerLink = new EntityToVTLIntermediateLayerLinkImpl();
		return entityToVTLIntermediateLayerLink;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForView createVTLForView() {
		VTLForViewImpl vtlForView = new VTLForViewImpl();
		return vtlForView;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedOutputlayerModule createVTLGeneratedOutputlayerModule() {
		VTLGeneratedOutputlayerModuleImpl vtlGeneratedOutputlayerModule = new VTLGeneratedOutputlayerModuleImpl();
		return vtlGeneratedOutputlayerModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForOutputLayerAndIntermediateLayerCombinationModule createVTLForOutputLayerAndIntermediateLayerCombinationModule() {
		VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl vtlForOutputLayerAndIntermediateLayerCombinationModule = new VTLForOutputLayerAndIntermediateLayerCombinationModuleImpl();
		return vtlForOutputLayerAndIntermediateLayerCombinationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedIntermediateLayerModule createVTLGeneratedIntermediateLayerModule() {
		VTLGeneratedIntermediateLayerModuleImpl vtlGeneratedIntermediateLayerModule = new VTLGeneratedIntermediateLayerModuleImpl();
		return vtlGeneratedIntermediateLayerModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLSchemesModule createVTLSchemesModule() {
		VTLSchemesModuleImpl vtlSchemesModule = new VTLSchemesModuleImpl();
		return vtlSchemesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForSelectionLayerModule createVTLForSelectionLayerModule() {
		VTLForSelectionLayerModuleImpl vtlForSelectionLayerModule = new VTLForSelectionLayerModuleImpl();
		return vtlForSelectionLayerModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityToVTLIntermediateLayerLinkModule createEntityToVTLIntermediateLayerLinkModule() {
		EntityToVTLIntermediateLayerLinkModuleImpl entityToVTLIntermediateLayerLinkModule = new EntityToVTLIntermediateLayerLinkModuleImpl();
		return entityToVTLIntermediateLayerLinkModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLForViewModule createVTLForViewModule() {
		VTLForViewModuleImpl vtlForViewModule = new VTLForViewModuleImpl();
		return vtlForViewModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Comparitor createComparitorFromString(EDataType eDataType, String initialValue) {
		Comparitor result = Comparitor.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertComparitorToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore4regPackage getEcore4regPackage() {
		return (Ecore4regPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Ecore4regPackage getPackage() {
		return Ecore4regPackage.eINSTANCE;
	}

} //Ecore4regFactoryImpl
