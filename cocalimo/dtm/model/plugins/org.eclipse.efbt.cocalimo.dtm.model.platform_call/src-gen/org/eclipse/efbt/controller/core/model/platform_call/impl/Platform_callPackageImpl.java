/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

import org.eclipse.efbt.controller.core.model.platform_call.ConvertSQLDeveloperModelToEcore;
import org.eclipse.efbt.controller.core.model.platform_call.PlatformCall;
import org.eclipse.efbt.controller.core.model.platform_call.PlatformCallModule;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callFactory;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls;
import org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason;
import org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsPackage;

import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass programAndCallsEClass = null;

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
	 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage#eNS_URI
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
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		FunctionsPackage.eINSTANCE.eClass();
		Cube_schemaPackage.eINSTANCE.eClass();
		CubesPackage.eINSTANCE.eClass();
		Efbt_advanced_data_definitionPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		Base_column_structured_dataPackage.eINSTANCE.eClass();
		Column_structured_dataPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		Efbt_data_definitionPackage.eINSTANCE.eClass();
		Efbt_mappingPackage.eINSTANCE.eClass();
		Efbt_vtl_transformationPackage.eINSTANCE.eClass();
		MappingPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();
		Smcubes_modelPackage.eINSTANCE.eClass();
		Vtl_transformationPackage.eINSTANCE.eClass();
		Test_definitionPackage.eINSTANCE.eClass();
		Test_input_dataPackage.eINSTANCE.eClass();
		TestPackage.eINSTANCE.eClass();
		TransformationPackage.eINSTANCE.eClass();
		dtm_report_cell_viewsPackage.eINSTANCE.eClass();
		dtm_viewsPackage.eINSTANCE.eClass();
		ProgramPackage.eINSTANCE.eClass();

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
	public EClass getProgramAndCalls() {
		return programAndCallsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getProgramAndCalls_PlatformCalls() {
		return (EReference)programAndCallsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getProgramAndCalls_Program() {
		return (EReference)programAndCallsEClass.getEStructuralFeatures().get(1);
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

		translateBIRDWithNewTestFormatToCocasonEClass = createEClass(TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON);
		createEAttribute(translateBIRDWithNewTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY);
		createEAttribute(translateBIRDWithNewTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY);
		createEAttribute(translateBIRDWithNewTestFormatToCocasonEClass, TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY);

		programAndCallsEClass = createEClass(PROGRAM_AND_CALLS);
		createEReference(programAndCallsEClass, PROGRAM_AND_CALLS__PLATFORM_CALLS);
		createEReference(programAndCallsEClass, PROGRAM_AND_CALLS__PROGRAM);
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
		TransformationPackage theTransformationPackage = (TransformationPackage)EPackage.Registry.INSTANCE.getEPackage(TransformationPackage.eNS_URI);

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

		initEClass(translateBIRDWithNewTestFormatToCocasonEClass, TranslateBIRDWithNewTestFormatToCocason.class, "TranslateBIRDWithNewTestFormatToCocason", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTranslateBIRDWithNewTestFormatToCocason_InputDirectory(), ecorePackage.getEString(), "inputDirectory", null, 0, 1, TranslateBIRDWithNewTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTranslateBIRDWithNewTestFormatToCocason_OutputDirectory(), ecorePackage.getEString(), "outputDirectory", null, 0, 1, TranslateBIRDWithNewTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTranslateBIRDWithNewTestFormatToCocason_TestDirectory(), ecorePackage.getEString(), "testDirectory", null, 0, 1, TranslateBIRDWithNewTestFormatToCocason.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(programAndCallsEClass, ProgramAndCalls.class, "ProgramAndCalls", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getProgramAndCalls_PlatformCalls(), this.getPlatformCallModule(), null, "platformCalls", null, 0, 1, ProgramAndCalls.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getProgramAndCalls_Program(), theTransformationPackage.getdtmProgram(), null, "program", null, 0, 1, ProgramAndCalls.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
