/**
 */
package entity_functions.impl;

import advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;

import advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl;

import column_transformation_logic.Column_transformation_logicPackage;

import column_transformation_logic.impl.Column_transformation_logicPackageImpl;

import data_lineage_functions.Data_lineage_functionsPackage;

import data_lineage_functions.impl.Data_lineage_functionsPackageImpl;

import entities.EntitiesPackage;

import entity_functions.Entity_functionsFactory;
import entity_functions.Entity_functionsPackage;
import entity_functions.MemberParameter;
import entity_functions.ValueParameter;

import functions.FunctionsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

import org.eclipse.emf.ecore.impl.EPackageImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Entity_functionsPackageImpl extends EPackageImpl implements Entity_functionsPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass memberParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass valueParameterEClass = null;

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
	 * @see entity_functions.Entity_functionsPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Entity_functionsPackageImpl() {
		super(eNS_URI, Entity_functionsFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Entity_functionsPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Entity_functionsPackage init() {
		if (isInited) return (Entity_functionsPackage)EPackage.Registry.INSTANCE.getEPackage(Entity_functionsPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEntity_functionsPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Entity_functionsPackageImpl theEntity_functionsPackage = registeredEntity_functionsPackage instanceof Entity_functionsPackageImpl ? (Entity_functionsPackageImpl)registeredEntity_functionsPackage : new Entity_functionsPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		EntitiesPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Advanced_variable_lineagefunctionsPackage.eNS_URI);
		Advanced_variable_lineagefunctionsPackageImpl theAdvanced_variable_lineagefunctionsPackage = (Advanced_variable_lineagefunctionsPackageImpl)(registeredPackage instanceof Advanced_variable_lineagefunctionsPackageImpl ? registeredPackage : Advanced_variable_lineagefunctionsPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Column_transformation_logicPackage.eNS_URI);
		Column_transformation_logicPackageImpl theColumn_transformation_logicPackage = (Column_transformation_logicPackageImpl)(registeredPackage instanceof Column_transformation_logicPackageImpl ? registeredPackage : Column_transformation_logicPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Data_lineage_functionsPackage.eNS_URI);
		Data_lineage_functionsPackageImpl theData_lineage_functionsPackage = (Data_lineage_functionsPackageImpl)(registeredPackage instanceof Data_lineage_functionsPackageImpl ? registeredPackage : Data_lineage_functionsPackage.eINSTANCE);

		// Create package meta-data objects
		theEntity_functionsPackage.createPackageContents();
		theAdvanced_variable_lineagefunctionsPackage.createPackageContents();
		theColumn_transformation_logicPackage.createPackageContents();
		theData_lineage_functionsPackage.createPackageContents();

		// Initialize created meta-data
		theEntity_functionsPackage.initializePackageContents();
		theAdvanced_variable_lineagefunctionsPackage.initializePackageContents();
		theColumn_transformation_logicPackage.initializePackageContents();
		theData_lineage_functionsPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theEntity_functionsPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Entity_functionsPackage.eNS_URI, theEntity_functionsPackage);
		return theEntity_functionsPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getMemberParameter() {
		return memberParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getMemberParameter_Param() {
		return (EReference)memberParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getValueParameter() {
		return valueParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getValueParameter_Value() {
		return (EAttribute)valueParameterEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getValueParameter_DataType() {
		return (EReference)valueParameterEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Entity_functionsFactory getEntity_functionsFactory() {
		return (Entity_functionsFactory)getEFactoryInstance();
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
		memberParameterEClass = createEClass(MEMBER_PARAMETER);
		createEReference(memberParameterEClass, MEMBER_PARAMETER__PARAM);

		valueParameterEClass = createEClass(VALUE_PARAMETER);
		createEAttribute(valueParameterEClass, VALUE_PARAMETER__VALUE);
		createEReference(valueParameterEClass, VALUE_PARAMETER__DATA_TYPE);
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
		FunctionsPackage theFunctionsPackage = (FunctionsPackage)EPackage.Registry.INSTANCE.getEPackage(FunctionsPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		memberParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());
		valueParameterEClass.getESuperTypes().add(theFunctionsPackage.getParameter());

		// Initialize classes, features, and operations; add parameters
		initEClass(memberParameterEClass, MemberParameter.class, "MemberParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getMemberParameter_Param(), theEcorePackage.getEEnumLiteral(), null, "param", null, 0, 1, MemberParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(valueParameterEClass, ValueParameter.class, "ValueParameter", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getValueParameter_Value(), ecorePackage.getEString(), "value", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getValueParameter_DataType(), theEcorePackage.getEDataType(), null, "dataType", null, 0, 1, ValueParameter.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (memberParameterEClass,
		   source,
		   new String[] {
			   "name", "MemberParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getMemberParameter_Param(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "param"
		   });
		addAnnotation
		  (valueParameterEClass,
		   source,
		   new String[] {
			   "name", "ValueParameter",
			   "kind", "empty"
		   });
		addAnnotation
		  (getValueParameter_Value(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "value"
		   });
	}

} //Entity_functionsPackageImpl
