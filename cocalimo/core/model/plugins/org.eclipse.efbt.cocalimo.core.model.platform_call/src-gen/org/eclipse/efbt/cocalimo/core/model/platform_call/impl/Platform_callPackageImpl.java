/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore;
import org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCallModule;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callFactory;
import org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.cocalimo.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason;
import org.eclipse.efbt.cocalimo.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

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
	private EClass translateBIRDWithOldTestFormatToCocasonEClass = null;

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
	private EClass translateBIRDWithNewTestFormatToCocasonEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callPackage#eNS_URI
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
		Module_managementPackage.eINSTANCE.eClass();

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
	public EClass getTranslateBIRDWithOldTestFormatToCocason() {
		return translateBIRDWithOldTestFormatToCocasonEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTranslateBIRDWithOldTestFormatToCocason_InputDirectory() {
		return (EAttribute)translateBIRDWithOldTestFormatToCocasonEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTranslateBIRDWithOldTestFormatToCocason_OutputDirectory() {
		return (EAttribute)translateBIRDWithOldTestFormatToCocasonEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTranslateBIRDWithOldTestFormatToCocason_TestDirectory() {
		return (EAttribute)translateBIRDWithOldTestFormatToCocasonEClass.getEStructuralFeatures().get(2);
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
	public EClass getTranslateBIRDWithNewTestFormatToCocason() {
		return translateBIRDWithNewTestFormatToCocasonEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTranslateBIRDWithNewTestFormatToCocason_InputDirectory() {
		return (EAttribute)translateBIRDWithNewTestFormatToCocasonEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTranslateBIRDWithNewTestFormatToCocason_OutputDirectory() {
		return (EAttribute)translateBIRDWithNewTestFormatToCocasonEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTranslateBIRDWithNewTestFormatToCocason_TestDirectory() {
		return (EAttribute)translateBIRDWithNewTestFormatToCocasonEClass.getEStructuralFeatures().get(2);
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

		translateBIRDWithOldTestFormatToCocasonEClass = createEClass(TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON);
		createEAttribute(translateBIRDWithOldTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY);
		createEAttribute(translateBIRDWithOldTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY);
		createEAttribute(translateBIRDWithOldTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY);

		convertSQLDeveloperModelToEcoreEClass = createEClass(CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE);
		createEAttribute(convertSQLDeveloperModelToEcoreEClass, CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__INPUT_DIRECTORY);
		createEAttribute(convertSQLDeveloperModelToEcoreEClass, CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__OUTPUT_DIRECTORY);

		translateBIRDWithNewTestFormatToCocasonEClass = createEClass(TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON);
		createEAttribute(translateBIRDWithNewTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY);
		createEAttribute(translateBIRDWithNewTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY);
		createEAttribute(translateBIRDWithNewTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY);
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

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		platformCallModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		translateBIRDWithOldTestFormatToCocasonEClass.getESuperTypes().add(this.getPlatformCall());
		convertSQLDeveloperModelToEcoreEClass.getESuperTypes().add(this.getPlatformCall());
		translateBIRDWithNewTestFormatToCocasonEClass.getESuperTypes().add(this.getPlatformCall());

		// Initialize classes, features, and operations; add parameters
		initEClass(platformCallEClass, PlatformCall.class, "PlatformCall", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getPlatformCall_ErrorMessage(), ecorePackage.getEString(), "errorMessage", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPlatformCall_Name(), ecorePackage.getEString(), "name", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getPlatformCall_ReturnStatus(), ecorePackage.getEInt(), "returnStatus", null, 0, 1, PlatformCall.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(platformCallModuleEClass, PlatformCallModule.class, "PlatformCallModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getPlatformCallModule_PlatformCalls(), this.getPlatformCall(), null, "platformCalls", null, 0, -1, PlatformCallModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(translateBIRDWithOldTestFormatToCocasonEClass, TranslateBIRDWithOldTestFormatToCocason.class, "TranslateBIRDWithOldTestFormatToCocason", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTranslateBIRDWithOldTestFormatToCocason_InputDirectory(), ecorePackage.getEString(), "inputDirectory", null, 0, 1, TranslateBIRDWithOldTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTranslateBIRDWithOldTestFormatToCocason_OutputDirectory(), ecorePackage.getEString(), "outputDirectory", null, 0, 1, TranslateBIRDWithOldTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTranslateBIRDWithOldTestFormatToCocason_TestDirectory(), ecorePackage.getEString(), "testDirectory", null, 0, 1, TranslateBIRDWithOldTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(convertSQLDeveloperModelToEcoreEClass, ConvertSQLDeveloperModelToEcore.class, "ConvertSQLDeveloperModelToEcore", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getConvertSQLDeveloperModelToEcore_InputDirectory(), ecorePackage.getEString(), "inputDirectory", null, 0, 1, ConvertSQLDeveloperModelToEcore.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getConvertSQLDeveloperModelToEcore_OutputDirectory(), ecorePackage.getEString(), "outputDirectory", null, 0, 1, ConvertSQLDeveloperModelToEcore.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(translateBIRDWithNewTestFormatToCocasonEClass, TranslateBIRDWithNewTestFormatToCocason.class, "TranslateBIRDWithNewTestFormatToCocason", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTranslateBIRDWithNewTestFormatToCocason_InputDirectory(), ecorePackage.getEString(), "inputDirectory", null, 0, 1, TranslateBIRDWithNewTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTranslateBIRDWithNewTestFormatToCocason_OutputDirectory(), ecorePackage.getEString(), "outputDirectory", null, 0, 1, TranslateBIRDWithNewTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTranslateBIRDWithNewTestFormatToCocason_TestDirectory(), ecorePackage.getEString(), "testDirectory", null, 0, 1, TranslateBIRDWithNewTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
