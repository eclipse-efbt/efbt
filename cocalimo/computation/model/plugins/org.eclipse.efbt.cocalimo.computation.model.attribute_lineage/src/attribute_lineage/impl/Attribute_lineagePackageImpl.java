/**
 */
package attribute_lineage.impl;

import advanced_row_transformation_logic.Advanced_row_transformation_logicPackage;

import advanced_row_transformation_logic.impl.Advanced_row_transformation_logicPackageImpl;

import attribute_lineage.AttributeLineageModel;
import attribute_lineage.AttributeLineageModelModule;
import attribute_lineage.Attribute_lineageFactory;
import attribute_lineage.Attribute_lineagePackage;
import attribute_lineage.IsNotSubset;
import attribute_lineage.IsSubset;
import attribute_lineage.SubsetBoolean;

import column_transformation_logic.Column_transformation_logicPackage;

import cube_transformation_logic.Cube_transformation_logicPackage;

import cube_transformation_logic.impl.Cube_transformation_logicPackageImpl;

import entities.EntitiesPackage;

import functions.FunctionsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

import row_transformation_logic.Row_transformation_logicPackage;

import row_transformation_logic.impl.Row_transformation_logicPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Attribute_lineagePackageImpl extends EPackageImpl implements Attribute_lineagePackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributeLineageModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributeLineageModelModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass isNotSubsetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass isSubsetEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass subsetBooleanEClass = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see attribute_lineage.Attribute_lineagePackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Attribute_lineagePackageImpl() {
		super(eNS_URI, Attribute_lineageFactory.eINSTANCE);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link Attribute_lineagePackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Attribute_lineagePackage init() {
		if (isInited) return (Attribute_lineagePackage)EPackage.Registry.INSTANCE.getEPackage(Attribute_lineagePackage.eNS_URI);

		// Obtain or create and register package
		Object registeredAttribute_lineagePackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Attribute_lineagePackageImpl theAttribute_lineagePackage = registeredAttribute_lineagePackage instanceof Attribute_lineagePackageImpl ? (Attribute_lineagePackageImpl)registeredAttribute_lineagePackage : new Attribute_lineagePackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		EntitiesPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Column_transformation_logicPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Advanced_row_transformation_logicPackage.eNS_URI);
		Advanced_row_transformation_logicPackageImpl theAdvanced_row_transformation_logicPackage = (Advanced_row_transformation_logicPackageImpl)(registeredPackage instanceof Advanced_row_transformation_logicPackageImpl ? registeredPackage : Advanced_row_transformation_logicPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Cube_transformation_logicPackage.eNS_URI);
		Cube_transformation_logicPackageImpl theCube_transformation_logicPackage = (Cube_transformation_logicPackageImpl)(registeredPackage instanceof Cube_transformation_logicPackageImpl ? registeredPackage : Cube_transformation_logicPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Row_transformation_logicPackage.eNS_URI);
		Row_transformation_logicPackageImpl theRow_transformation_logicPackage = (Row_transformation_logicPackageImpl)(registeredPackage instanceof Row_transformation_logicPackageImpl ? registeredPackage : Row_transformation_logicPackage.eINSTANCE);

		// Create package meta-data objects
		theAttribute_lineagePackage.createPackageContents();
		theAdvanced_row_transformation_logicPackage.createPackageContents();
		theCube_transformation_logicPackage.createPackageContents();
		theRow_transformation_logicPackage.createPackageContents();

		// Initialize created meta-data
		theAttribute_lineagePackage.initializePackageContents();
		theAdvanced_row_transformation_logicPackage.initializePackageContents();
		theCube_transformation_logicPackage.initializePackageContents();
		theRow_transformation_logicPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theAttribute_lineagePackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Attribute_lineagePackage.eNS_URI, theAttribute_lineagePackage);
		return theAttribute_lineagePackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAttributeLineageModel() {
		return attributeLineageModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributeLineageModel_RowTransformations() {
		return (EReference)attributeLineageModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributeLineageModel_BaseSchemas() {
		return (EReference)attributeLineageModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributeLineageModel_SpecialFunctionSpecs() {
		return (EReference)attributeLineageModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeLineageModel_Name() {
		return (EAttribute)attributeLineageModelEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAttributeLineageModelModule() {
		return attributeLineageModelModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributeLineageModelModule_AttributeLineageModels() {
		return (EReference)attributeLineageModelModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getIsNotSubset() {
		return isNotSubsetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getIsSubset() {
		return isSubsetEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSubsetBoolean() {
		return subsetBooleanEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute_lineageFactory getAttribute_lineageFactory() {
		return (Attribute_lineageFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isCreated = false;

	/**
	 * Creates the meta-model objects for the package.  This method is
	 * guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void createPackageContents() {
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		attributeLineageModelEClass = createEClass(ATTRIBUTE_LINEAGE_MODEL);
		createEReference(attributeLineageModelEClass, ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS);
		createEReference(attributeLineageModelEClass, ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS);
		createEReference(attributeLineageModelEClass, ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS);
		createEAttribute(attributeLineageModelEClass, ATTRIBUTE_LINEAGE_MODEL__NAME);

		attributeLineageModelModuleEClass = createEClass(ATTRIBUTE_LINEAGE_MODEL_MODULE);
		createEReference(attributeLineageModelModuleEClass, ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS);

		isNotSubsetEClass = createEClass(IS_NOT_SUBSET);

		isSubsetEClass = createEClass(IS_SUBSET);

		subsetBooleanEClass = createEClass(SUBSET_BOOLEAN);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isInitialized = false;

	/**
	 * Complete the initialization of the package and its meta-model.  This
	 * method is guarded to have no affect on any invocation but its first.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void initializePackageContents() {
		if (isInitialized) return;
		isInitialized = true;

		// Initialize package
		setName(eNAME);
		setNsPrefix(eNS_PREFIX);
		setNsURI(eNS_URI);

		// Obtain other dependent packages
		Cube_transformation_logicPackage theCube_transformation_logicPackage = (Cube_transformation_logicPackage)EPackage.Registry.INSTANCE.getEPackage(Cube_transformation_logicPackage.eNS_URI);
		EntitiesPackage theEntitiesPackage = (EntitiesPackage)EPackage.Registry.INSTANCE.getEPackage(EntitiesPackage.eNS_URI);
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		attributeLineageModelModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		isNotSubsetEClass.getESuperTypes().add(this.getSubsetBoolean());
		isSubsetEClass.getESuperTypes().add(this.getSubsetBoolean());

		// Initialize classes, features, and operations; add parameters
		initEClass(attributeLineageModelEClass, AttributeLineageModel.class, "AttributeLineageModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAttributeLineageModel_RowTransformations(), theCube_transformation_logicPackage.getCubeTransformationLogic(), null, "rowTransformations", null, 0, -1, AttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAttributeLineageModel_BaseSchemas(), theEntitiesPackage.getBaseEntity(), null, "baseSchemas", null, 0, -1, AttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAttributeLineageModel_SpecialFunctionSpecs(), theFunctionsPackage.getFunctionSpec(), null, "specialFunctionSpecs", null, 0, -1, AttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAttributeLineageModel_Name(), ecorePackage.getEString(), "name", null, 0, 1, AttributeLineageModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(attributeLineageModelModuleEClass, AttributeLineageModelModule.class, "AttributeLineageModelModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAttributeLineageModelModule_AttributeLineageModels(), this.getAttributeLineageModel(), null, "attributeLineageModels", null, 0, -1, AttributeLineageModelModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(isNotSubsetEClass, IsNotSubset.class, "IsNotSubset", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(isSubsetEClass, IsSubset.class, "IsSubset", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(subsetBooleanEClass, SubsetBoolean.class, "SubsetBoolean", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
		// http:///org/eclipse/emf/ecore/util/ExtendedMetaData
		createExtendedMetaDataAnnotations();
	}

	/**
	 * Initializes the annotations for <b>license</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createLicenseAnnotations() {
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations() {
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (attributeLineageModelEClass,
		   source,
		   new String[] {
			   "name", "AttributeLineageModel",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAttributeLineageModel_RowTransformations(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "rowTransformations"
		   });
		addAnnotation
		  (getAttributeLineageModel_BaseSchemas(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "baseSchemas"
		   });
		addAnnotation
		  (getAttributeLineageModel_SpecialFunctionSpecs(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "specialFunctionSpecs"
		   });
		addAnnotation
		  (getAttributeLineageModel_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (attributeLineageModelModuleEClass,
		   source,
		   new String[] {
			   "name", "AttributeLineageModelModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getAttributeLineageModelModule_AttributeLineageModels(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "attributeLineageModels"
		   });
		addAnnotation
		  (isNotSubsetEClass,
		   source,
		   new String[] {
			   "name", "IsNotSubset",
			   "kind", "empty"
		   });
		addAnnotation
		  (isSubsetEClass,
		   source,
		   new String[] {
			   "name", "IsSubset",
			   "kind", "empty"
		   });
		addAnnotation
		  (subsetBooleanEClass,
		   source,
		   new String[] {
			   "name", "SubsetBoolean",
			   "kind", "empty"
		   });
	}

} //Attribute_lineagePackageImpl
