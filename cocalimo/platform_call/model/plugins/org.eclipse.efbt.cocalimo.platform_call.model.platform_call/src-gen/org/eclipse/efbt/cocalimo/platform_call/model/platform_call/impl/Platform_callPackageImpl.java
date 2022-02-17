/**
 */
package org.eclipse.efbt.cocalimo.platform_call.model.platform_call.impl;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.ConvertDataMetaModelToEcore;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.ConvertSQLDeveloperModelToEcore;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.CreateLogicalTransformationViewForScope;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.ImportBIRDFromMSAccess;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.PlatformCall;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.PlatformCallModule;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callFactory;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;

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
public class Platform_callPackageImpl extends EPackageImpl implements Platform_callPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass platformCallEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass platformCallModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass convertSQLDeveloperModelToEcoreEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass importBIRDFromMSAccessEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass createLogicalTransformationViewForScopeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass convertDataMetaModelToEcoreEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Platform_callPackageImpl() {
		super(eNS_URI, Platform_callFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Platform_callPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Platform_callPackage init() {
		if (isInited) return (Platform_callPackage)EPackage.Registry.INSTANCE.getEPackage(Platform_callPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredPlatform_callPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Platform_callPackageImpl thePlatform_callPackage = registeredPlatform_callPackage instanceof Platform_callPackageImpl ? (Platform_callPackageImpl)registeredPlatform_callPackage : new Platform_callPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		Logical_transformationsPackage.eINSTANCE.eClass();
		Bpmn_litePackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_meta_modelPackage.eINSTANCE.eClass();

		// Create package meta-data objects
		thePlatform_callPackage.createPackageContents();

		// Initialize created meta-data
		thePlatform_callPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		thePlatform_callPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Platform_callPackage.eNS_URI, thePlatform_callPackage);
		return thePlatform_callPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getPlatformCall() {
		return platformCallEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPlatformCall_ErrorMessage() {
		return (EAttribute)platformCallEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPlatformCall_Name() {
		return (EAttribute)platformCallEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getPlatformCall_ReturnStatus() {
		return (EAttribute)platformCallEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getPlatformCallModule() {
		return platformCallModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getPlatformCallModule_PlatformCalls() {
		return (EReference)platformCallModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getConvertSQLDeveloperModelToEcore() {
		return convertSQLDeveloperModelToEcoreEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getConvertSQLDeveloperModelToEcore_InputDirectory() {
		return (EAttribute)convertSQLDeveloperModelToEcoreEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getConvertSQLDeveloperModelToEcore_OutputDirectory() {
		return (EAttribute)convertSQLDeveloperModelToEcoreEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getImportBIRDFromMSAccess() {
		return importBIRDFromMSAccessEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getImportBIRDFromMSAccess_InputDirectory() {
		return (EAttribute)importBIRDFromMSAccessEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getImportBIRDFromMSAccess_OutputDirectory() {
		return (EAttribute)importBIRDFromMSAccessEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCreateLogicalTransformationViewForScope() {
		return createLogicalTransformationViewForScopeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCreateLogicalTransformationViewForScope_LogicalTransformationModule() {
		return (EReference)createLogicalTransformationViewForScopeEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCreateLogicalTransformationViewForScope_AttributeLineage() {
		return (EReference)createLogicalTransformationViewForScopeEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCreateLogicalTransformationViewForScope_TestScope() {
		return (EReference)createLogicalTransformationViewForScopeEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getConvertDataMetaModelToEcore() {
		return convertDataMetaModelToEcoreEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getConvertDataMetaModelToEcore_OutputDirectory() {
		return (EAttribute)convertDataMetaModelToEcoreEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getConvertDataMetaModelToEcore_EntityModule() {
		return (EReference)convertDataMetaModelToEcoreEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Platform_callFactory getPlatform_callFactory() {
		return (Platform_callFactory)getEFactoryInstance();
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
		platformCallEClass = createEClass(PLATFORM_CALL);
		createEAttribute(platformCallEClass, PLATFORM_CALL__ERROR_MESSAGE);
		createEAttribute(platformCallEClass, PLATFORM_CALL__NAME);
		createEAttribute(platformCallEClass, PLATFORM_CALL__RETURN_STATUS);

		platformCallModuleEClass = createEClass(PLATFORM_CALL_MODULE);
		createEReference(platformCallModuleEClass, PLATFORM_CALL_MODULE__PLATFORM_CALLS);

		convertSQLDeveloperModelToEcoreEClass = createEClass(CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE);
		createEAttribute(convertSQLDeveloperModelToEcoreEClass, CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__INPUT_DIRECTORY);
		createEAttribute(convertSQLDeveloperModelToEcoreEClass, CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__OUTPUT_DIRECTORY);

		importBIRDFromMSAccessEClass = createEClass(IMPORT_BIRD_FROM_MS_ACCESS);
		createEAttribute(importBIRDFromMSAccessEClass, IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY);
		createEAttribute(importBIRDFromMSAccessEClass, IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY);

		createLogicalTransformationViewForScopeEClass = createEClass(CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE);
		createEReference(createLogicalTransformationViewForScopeEClass, CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE);
		createEReference(createLogicalTransformationViewForScopeEClass, CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE);
		createEReference(createLogicalTransformationViewForScopeEClass, CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE);

		convertDataMetaModelToEcoreEClass = createEClass(CONVERT_DATA_META_MODEL_TO_ECORE);
		createEAttribute(convertDataMetaModelToEcoreEClass, CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY);
		createEReference(convertDataMetaModelToEcoreEClass, CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE);
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
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		Logical_transformationsPackage theLogical_transformationsPackage = (Logical_transformationsPackage)EPackage.Registry.INSTANCE.getEPackage(Logical_transformationsPackage.eNS_URI);
		EcorePackage theEcorePackage = (EcorePackage)EPackage.Registry.INSTANCE.getEPackage(EcorePackage.eNS_URI);
		Data_meta_modelPackage theData_meta_modelPackage = (Data_meta_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Data_meta_modelPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		platformCallModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		convertSQLDeveloperModelToEcoreEClass.getESuperTypes().add(this.getPlatformCall());
		importBIRDFromMSAccessEClass.getESuperTypes().add(this.getPlatformCall());
		createLogicalTransformationViewForScopeEClass.getESuperTypes().add(this.getPlatformCall());
		convertDataMetaModelToEcoreEClass.getESuperTypes().add(this.getPlatformCall());

		// Initialize classes, features, and operations; add parameters
		initEClass(platformCallEClass, PlatformCall.class, "PlatformCall", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getPlatformCall_ErrorMessage(), ecorePackage.getEString(), "errorMessage", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPlatformCall_Name(), ecorePackage.getEString(), "name", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPlatformCall_ReturnStatus(), ecorePackage.getEInt(), "returnStatus", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(platformCallModuleEClass, PlatformCallModule.class, "PlatformCallModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getPlatformCallModule_PlatformCalls(), this.getPlatformCall(), null, "platformCalls", null, 0, -1, PlatformCallModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(convertSQLDeveloperModelToEcoreEClass, ConvertSQLDeveloperModelToEcore.class, "ConvertSQLDeveloperModelToEcore", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getConvertSQLDeveloperModelToEcore_InputDirectory(), ecorePackage.getEString(), "inputDirectory", null, 0, 1, ConvertSQLDeveloperModelToEcore.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getConvertSQLDeveloperModelToEcore_OutputDirectory(), ecorePackage.getEString(), "outputDirectory", null, 0, 1, ConvertSQLDeveloperModelToEcore.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(importBIRDFromMSAccessEClass, ImportBIRDFromMSAccess.class, "ImportBIRDFromMSAccess", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getImportBIRDFromMSAccess_InputDirectory(), ecorePackage.getEString(), "inputDirectory", null, 0, 1, ImportBIRDFromMSAccess.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getImportBIRDFromMSAccess_OutputDirectory(), ecorePackage.getEString(), "outputDirectory", null, 0, 1, ImportBIRDFromMSAccess.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(createLogicalTransformationViewForScopeEClass, CreateLogicalTransformationViewForScope.class, "CreateLogicalTransformationViewForScope", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getCreateLogicalTransformationViewForScope_LogicalTransformationModule(), theLogical_transformationsPackage.getLogicalTransformationModule(), null, "logicalTransformationModule", null, 0, 1, CreateLogicalTransformationViewForScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCreateLogicalTransformationViewForScope_AttributeLineage(), theEcorePackage.getEStructuralFeature(), null, "attributeLineage", null, 0, -1, CreateLogicalTransformationViewForScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getCreateLogicalTransformationViewForScope_TestScope(), theLogical_transformationsPackage.getE2ETestScope(), null, "testScope", null, 0, -1, CreateLogicalTransformationViewForScope.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(convertDataMetaModelToEcoreEClass, ConvertDataMetaModelToEcore.class, "ConvertDataMetaModelToEcore", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getConvertDataMetaModelToEcore_OutputDirectory(), ecorePackage.getEString(), "outputDirectory", null, 0, 1, ConvertDataMetaModelToEcore.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getConvertDataMetaModelToEcore_EntityModule(), theData_meta_modelPackage.getEntityModule(), null, "entityModule", null, 0, 1, ConvertDataMetaModelToEcore.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
		  (platformCallEClass,
		   source,
		   new String[] {
			   "name", "PlatformCall",
			   "kind", "empty"
		   });
		addAnnotation
		  (getPlatformCall_ErrorMessage(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "errorMessage"
		   });
		addAnnotation
		  (getPlatformCall_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
		addAnnotation
		  (getPlatformCall_ReturnStatus(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "returnStatus"
		   });
		addAnnotation
		  (platformCallModuleEClass,
		   source,
		   new String[] {
			   "name", "PlatformCallModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getPlatformCallModule_PlatformCalls(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "platformCalls"
		   });
	}

} //Platform_callPackageImpl
