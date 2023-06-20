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

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage
 * @generated
 */
public class Ecore4regAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Ecore4regPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore4regAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Ecore4regPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Ecore4regSwitch<Adapter> modelSwitch =
		new Ecore4regSwitch<Adapter>() {
			@Override
			public Adapter caseImport(Import object) {
				return createImportAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.ecore4reg.model.ecore4reg.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter caseModuleList(ModuleList object) {
				return createModuleListAdapter();
			}
			@Override
			public Adapter caseAllowedTypes(AllowedTypes object) {
				return createAllowedTypesAdapter();
			}
			@Override
			public Adapter caseRequirementsModule(RequirementsModule object) {
				return createRequirementsModuleAdapter();
			}
			@Override
			public Adapter caseRequirementsSection(RequirementsSection object) {
				return createRequirementsSectionAdapter();
			}
			@Override
			public Adapter caseRequirementsSectionImage(RequirementsSectionImage object) {
				return createRequirementsSectionImageAdapter();
			}
			@Override
			public Adapter caseRequirementsSectionLinkWithText(RequirementsSectionLinkWithText object) {
				return createRequirementsSectionLinkWithTextAdapter();
			}
			@Override
			public Adapter caseRequirementsSectionText(RequirementsSectionText object) {
				return createRequirementsSectionTextAdapter();
			}
			@Override
			public Adapter caseRequirementType(RequirementType object) {
				return createRequirementTypeAdapter();
			}
			@Override
			public Adapter caseTitledRequirementsSection(TitledRequirementsSection object) {
				return createTitledRequirementsSectionAdapter();
			}
			@Override
			public Adapter caseTag(Tag object) {
				return createTagAdapter();
			}
			@Override
			public Adapter caseTagGroup(TagGroup object) {
				return createTagGroupAdapter();
			}
			@Override
			public Adapter caseRulesForReport(RulesForReport object) {
				return createRulesForReportAdapter();
			}
			@Override
			public Adapter caseRulesForILTable(RulesForILTable object) {
				return createRulesForILTableAdapter();
			}
			@Override
			public Adapter caseSelectColumn(SelectColumn object) {
				return createSelectColumnAdapter();
			}
			@Override
			public Adapter caseSelectColumnMemberAs(SelectColumnMemberAs object) {
				return createSelectColumnMemberAsAdapter();
			}
			@Override
			public Adapter caseSelectColumnAttributeAs(SelectColumnAttributeAs object) {
				return createSelectColumnAttributeAsAdapter();
			}
			@Override
			public Adapter caseSelectDerivedColumnAs(SelectDerivedColumnAs object) {
				return createSelectDerivedColumnAsAdapter();
			}
			@Override
			public Adapter caseSelectValueAs(SelectValueAs object) {
				return createSelectValueAsAdapter();
			}
			@Override
			public Adapter caseTableFilter(TableFilter object) {
				return createTableFilterAdapter();
			}
			@Override
			public Adapter caseGenerationRulesModule(GenerationRulesModule object) {
				return createGenerationRulesModuleAdapter();
			}
			@Override
			public Adapter caseRuleForILTablePart(RuleForILTablePart object) {
				return createRuleForILTablePartAdapter();
			}
			@Override
			public Adapter caseELAttribute(ELAttribute object) {
				return createELAttributeAdapter();
			}
			@Override
			public Adapter caseELClass(ELClass object) {
				return createELClassAdapter();
			}
			@Override
			public Adapter caseELClassifier(ELClassifier object) {
				return createELClassifierAdapter();
			}
			@Override
			public Adapter caseELDataType(ELDataType object) {
				return createELDataTypeAdapter();
			}
			@Override
			public Adapter caseELEnum(ELEnum object) {
				return createELEnumAdapter();
			}
			@Override
			public Adapter caseELEnumLiteral(ELEnumLiteral object) {
				return createELEnumLiteralAdapter();
			}
			@Override
			public Adapter caseELModelElement(ELModelElement object) {
				return createELModelElementAdapter();
			}
			@Override
			public Adapter caseELNamedElement(ELNamedElement object) {
				return createELNamedElementAdapter();
			}
			@Override
			public Adapter caseELOperation(ELOperation object) {
				return createELOperationAdapter();
			}
			@Override
			public Adapter caseELParameter(ELParameter object) {
				return createELParameterAdapter();
			}
			@Override
			public Adapter caseELPackage(ELPackage object) {
				return createELPackageAdapter();
			}
			@Override
			public Adapter caseELReference(ELReference object) {
				return createELReferenceAdapter();
			}
			@Override
			public Adapter caseELStructuralFeature(ELStructuralFeature object) {
				return createELStructuralFeatureAdapter();
			}
			@Override
			public Adapter caseELTypedElement(ELTypedElement object) {
				return createELTypedElementAdapter();
			}
			@Override
			public Adapter caseELAnnotation(ELAnnotation object) {
				return createELAnnotationAdapter();
			}
			@Override
			public Adapter caseELStringToStringMapEntry(ELStringToStringMapEntry object) {
				return createELStringToStringMapEntryAdapter();
			}
			@Override
			public Adapter caseVTLEnrichedCube(VTLEnrichedCube object) {
				return createVTLEnrichedCubeAdapter();
			}
			@Override
			public Adapter caseVTLGeneratedOutputlayer(VTLGeneratedOutputlayer object) {
				return createVTLGeneratedOutputlayerAdapter();
			}
			@Override
			public Adapter caseVTLForOutputLayerAndIntermediateLayerCombination(VTLForOutputLayerAndIntermediateLayerCombination object) {
				return createVTLForOutputLayerAndIntermediateLayerCombinationAdapter();
			}
			@Override
			public Adapter caseVTLGeneratedIntermediateLayer(VTLGeneratedIntermediateLayer object) {
				return createVTLGeneratedIntermediateLayerAdapter();
			}
			@Override
			public Adapter caseVTLTransformation(VTLTransformation object) {
				return createVTLTransformationAdapter();
			}
			@Override
			public Adapter caseVTLScheme(VTLScheme object) {
				return createVTLSchemeAdapter();
			}
			@Override
			public Adapter caseVTLForSelectionLayer(VTLForSelectionLayer object) {
				return createVTLForSelectionLayerAdapter();
			}
			@Override
			public Adapter caseVTLModule(VTLModule object) {
				return createVTLModuleAdapter();
			}
			@Override
			public Adapter caseEntityToVTLIntermediateLayerLink(EntityToVTLIntermediateLayerLink object) {
				return createEntityToVTLIntermediateLayerLinkAdapter();
			}
			@Override
			public Adapter caseVTLForView(VTLForView object) {
				return createVTLForViewAdapter();
			}
			@Override
			public Adapter caseVTLGeneratedOutputlayerModule(VTLGeneratedOutputlayerModule object) {
				return createVTLGeneratedOutputlayerModuleAdapter();
			}
			@Override
			public Adapter caseVTLForOutputLayerAndIntermediateLayerCombinationModule(VTLForOutputLayerAndIntermediateLayerCombinationModule object) {
				return createVTLForOutputLayerAndIntermediateLayerCombinationModuleAdapter();
			}
			@Override
			public Adapter caseVTLGeneratedIntermediateLayerModule(VTLGeneratedIntermediateLayerModule object) {
				return createVTLGeneratedIntermediateLayerModuleAdapter();
			}
			@Override
			public Adapter caseVTLSchemesModule(VTLSchemesModule object) {
				return createVTLSchemesModuleAdapter();
			}
			@Override
			public Adapter caseVTLForSelectionLayerModule(VTLForSelectionLayerModule object) {
				return createVTLForSelectionLayerModuleAdapter();
			}
			@Override
			public Adapter caseEntityToVTLIntermediateLayerLinkModule(EntityToVTLIntermediateLayerLinkModule object) {
				return createEntityToVTLIntermediateLayerLinkModuleAdapter();
			}
			@Override
			public Adapter caseVTLForViewModule(VTLForViewModule object) {
				return createVTLForViewModuleAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Import <em>Import</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Import
	 * @generated
	 */
	public Adapter createImportAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Module
	 * @generated
	 */
	public Adapter createModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList <em>Module List</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ModuleList
	 * @generated
	 */
	public Adapter createModuleListAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes <em>Allowed Types</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.AllowedTypes
	 * @generated
	 */
	public Adapter createAllowedTypesAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule <em>Requirements Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsModule
	 * @generated
	 */
	public Adapter createRequirementsModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection <em>Requirements Section</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSection
	 * @generated
	 */
	public Adapter createRequirementsSectionAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage <em>Requirements Section Image</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionImage
	 * @generated
	 */
	public Adapter createRequirementsSectionImageAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText <em>Requirements Section Link With Text</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText
	 * @generated
	 */
	public Adapter createRequirementsSectionLinkWithTextAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionText <em>Requirements Section Text</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionText
	 * @generated
	 */
	public Adapter createRequirementsSectionTextAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType <em>Requirement Type</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementType
	 * @generated
	 */
	public Adapter createRequirementTypeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection <em>Titled Requirements Section</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection
	 * @generated
	 */
	public Adapter createTitledRequirementsSectionAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag <em>Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Tag
	 * @generated
	 */
	public Adapter createTagAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TagGroup <em>Tag Group</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TagGroup
	 * @generated
	 */
	public Adapter createTagGroupAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport <em>Rules For Report</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport
	 * @generated
	 */
	public Adapter createRulesForReportAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable <em>Rules For IL Table</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForILTable
	 * @generated
	 */
	public Adapter createRulesForILTableAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn <em>Select Column</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn
	 * @generated
	 */
	public Adapter createSelectColumnAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnMemberAs <em>Select Column Member As</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnMemberAs
	 * @generated
	 */
	public Adapter createSelectColumnMemberAsAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnAttributeAs <em>Select Column Attribute As</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumnAttributeAs
	 * @generated
	 */
	public Adapter createSelectColumnAttributeAsAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectDerivedColumnAs <em>Select Derived Column As</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectDerivedColumnAs
	 * @generated
	 */
	public Adapter createSelectDerivedColumnAsAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectValueAs <em>Select Value As</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectValueAs
	 * @generated
	 */
	public Adapter createSelectValueAsAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter <em>Table Filter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter
	 * @generated
	 */
	public Adapter createTableFilterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule <em>Generation Rules Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.GenerationRulesModule
	 * @generated
	 */
	public Adapter createGenerationRulesModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart <em>Rule For IL Table Part</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart
	 * @generated
	 */
	public Adapter createRuleForILTablePartAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute <em>EL Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAttribute
	 * @generated
	 */
	public Adapter createELAttributeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass <em>EL Class</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClass
	 * @generated
	 */
	public Adapter createELClassAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier <em>EL Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier
	 * @generated
	 */
	public Adapter createELClassifierAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType <em>EL Data Type</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELDataType
	 * @generated
	 */
	public Adapter createELDataTypeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum <em>EL Enum</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnum
	 * @generated
	 */
	public Adapter createELEnumAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral <em>EL Enum Literal</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELEnumLiteral
	 * @generated
	 */
	public Adapter createELEnumLiteralAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement <em>EL Model Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELModelElement
	 * @generated
	 */
	public Adapter createELModelElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELNamedElement <em>EL Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELNamedElement
	 * @generated
	 */
	public Adapter createELNamedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation <em>EL Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELOperation
	 * @generated
	 */
	public Adapter createELOperationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELParameter <em>EL Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELParameter
	 * @generated
	 */
	public Adapter createELParameterAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage <em>EL Package</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage
	 * @generated
	 */
	public Adapter createELPackageAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference <em>EL Reference</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELReference
	 * @generated
	 */
	public Adapter createELReferenceAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStructuralFeature <em>EL Structural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStructuralFeature
	 * @generated
	 */
	public Adapter createELStructuralFeatureAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement <em>EL Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELTypedElement
	 * @generated
	 */
	public Adapter createELTypedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation <em>EL Annotation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELAnnotation
	 * @generated
	 */
	public Adapter createELAnnotationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry <em>EL String To String Map Entry</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.ELStringToStringMapEntry
	 * @generated
	 */
	public Adapter createELStringToStringMapEntryAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLEnrichedCube <em>VTL Enriched Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLEnrichedCube
	 * @generated
	 */
	public Adapter createVTLEnrichedCubeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer <em>VTL Generated Outputlayer</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer
	 * @generated
	 */
	public Adapter createVTLGeneratedOutputlayerAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination <em>VTL For Output Layer And Intermediate Layer Combination</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination
	 * @generated
	 */
	public Adapter createVTLForOutputLayerAndIntermediateLayerCombinationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer <em>VTL Generated Intermediate Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer
	 * @generated
	 */
	public Adapter createVTLGeneratedIntermediateLayerAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation <em>VTL Transformation</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation
	 * @generated
	 */
	public Adapter createVTLTransformationAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme <em>VTL Scheme</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLScheme
	 * @generated
	 */
	public Adapter createVTLSchemeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer <em>VTL For Selection Layer</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer
	 * @generated
	 */
	public Adapter createVTLForSelectionLayerAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule <em>VTL Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule
	 * @generated
	 */
	public Adapter createVTLModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink <em>Entity To VTL Intermediate Layer Link</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink
	 * @generated
	 */
	public Adapter createEntityToVTLIntermediateLayerLinkAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView <em>VTL For View</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView
	 * @generated
	 */
	public Adapter createVTLForViewAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule <em>VTL Generated Outputlayer Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule
	 * @generated
	 */
	public Adapter createVTLGeneratedOutputlayerModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule <em>VTL For Output Layer And Intermediate Layer Combination Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombinationModule
	 * @generated
	 */
	public Adapter createVTLForOutputLayerAndIntermediateLayerCombinationModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule <em>VTL Generated Intermediate Layer Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayerModule
	 * @generated
	 */
	public Adapter createVTLGeneratedIntermediateLayerModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule <em>VTL Schemes Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLSchemesModule
	 * @generated
	 */
	public Adapter createVTLSchemesModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule <em>VTL For Selection Layer Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule
	 * @generated
	 */
	public Adapter createVTLForSelectionLayerModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule <em>Entity To VTL Intermediate Layer Link Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule
	 * @generated
	 */
	public Adapter createEntityToVTLIntermediateLayerLinkModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule <em>VTL For View Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForViewModule
	 * @generated
	 */
	public Adapter createVTLForViewModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //Ecore4regAdapterFactory