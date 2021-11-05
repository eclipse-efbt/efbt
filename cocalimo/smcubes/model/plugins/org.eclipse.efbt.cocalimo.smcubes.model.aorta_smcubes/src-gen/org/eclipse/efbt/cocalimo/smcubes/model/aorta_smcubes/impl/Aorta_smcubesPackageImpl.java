/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.ReportResults;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestModule;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.input_data.Input_dataPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.Logical_transformationsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelPackage;
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
public class Aorta_smcubesPackageImpl extends EPackageImpl implements Aorta_smcubesPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass aortaSMCubesModelEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportResultsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testDefinitionModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testDefinitionEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Aorta_smcubesPackageImpl() {
		super(eNS_URI, Aorta_smcubesFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Aorta_smcubesPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Aorta_smcubesPackage init() {
		if (isInited) return (Aorta_smcubesPackage)EPackage.Registry.INSTANCE.getEPackage(Aorta_smcubesPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredAorta_smcubesPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Aorta_smcubesPackageImpl theAorta_smcubesPackage = registeredAorta_smcubesPackage instanceof Aorta_smcubesPackageImpl ? (Aorta_smcubesPackageImpl)registeredAorta_smcubesPackage : new Aorta_smcubesPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		EcorePackage.eINSTANCE.eClass();
		Input_dataPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		CorePackage.eINSTANCE.eClass();
		Data_definitionPackage.eINSTANCE.eClass();
		Efbt_data_definitionPackage.eINSTANCE.eClass();
		Efbt_mappingPackage.eINSTANCE.eClass();
		MappingPackage.eINSTANCE.eClass();
		RenderingPackage.eINSTANCE.eClass();
		ReportsPackage.eINSTANCE.eClass();
		Smcubes_modelPackage.eINSTANCE.eClass();
		Logical_transformationsPackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		Bpmn_litePackage.eINSTANCE.eClass();

		// Create package meta-data objects
		theAorta_smcubesPackage.createPackageContents();

		// Initialize created meta-data
		theAorta_smcubesPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theAorta_smcubesPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Aorta_smcubesPackage.eNS_URI, theAorta_smcubesPackage);
		return theAorta_smcubesPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAortaSMCubesModel() {
		return aortaSMCubesModelEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAortaSMCubesModel_SmCubesModel() {
		return (EReference)aortaSMCubesModelEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAortaSMCubesModel_Requirements() {
		return (EReference)aortaSMCubesModelEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAortaSMCubesModel_Tests() {
		return (EReference)aortaSMCubesModelEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAortaSMCubesModel_TaskModules() {
		return (EReference)aortaSMCubesModelEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAortaSMCubesModel_TestDefinitions() {
		return (EReference)aortaSMCubesModelEClass.getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getAortaSMCubesModel_Name() {
		return (EAttribute)aortaSMCubesModelEClass.getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportResults() {
		return reportResultsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportResults_ReportCells() {
		return (EReference)reportResultsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTest() {
		return testEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTest_InputData() {
		return (EReference)testEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTest_ExpectedResults() {
		return (EReference)testEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTest_TestDefinition() {
		return (EReference)testEClass.getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTest_Name() {
		return (EAttribute)testEClass.getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTestModule() {
		return testModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTestModule_Tests() {
		return (EReference)testModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTestDefinitionModule() {
		return testDefinitionModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTestDefinitionModule_TestDefinitions() {
		return (EReference)testDefinitionModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTestDefinition() {
		return testDefinitionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTestDefinition_Scenario() {
		return (EReference)testDefinitionEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTestDefinition_Name() {
		return (EAttribute)testDefinitionEClass.getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Aorta_smcubesFactory getAorta_smcubesFactory() {
		return (Aorta_smcubesFactory)getEFactoryInstance();
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
		aortaSMCubesModelEClass = createEClass(AORTA_SM_CUBES_MODEL);
		createEReference(aortaSMCubesModelEClass, AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL);
		createEReference(aortaSMCubesModelEClass, AORTA_SM_CUBES_MODEL__REQUIREMENTS);
		createEReference(aortaSMCubesModelEClass, AORTA_SM_CUBES_MODEL__TESTS);
		createEReference(aortaSMCubesModelEClass, AORTA_SM_CUBES_MODEL__TASK_MODULES);
		createEReference(aortaSMCubesModelEClass, AORTA_SM_CUBES_MODEL__TEST_DEFINITIONS);
		createEAttribute(aortaSMCubesModelEClass, AORTA_SM_CUBES_MODEL__NAME);

		reportResultsEClass = createEClass(REPORT_RESULTS);
		createEReference(reportResultsEClass, REPORT_RESULTS__REPORT_CELLS);

		testEClass = createEClass(TEST);
		createEReference(testEClass, TEST__INPUT_DATA);
		createEReference(testEClass, TEST__EXPECTED_RESULTS);
		createEReference(testEClass, TEST__TEST_DEFINITION);
		createEAttribute(testEClass, TEST__NAME);

		testModuleEClass = createEClass(TEST_MODULE);
		createEReference(testModuleEClass, TEST_MODULE__TESTS);

		testDefinitionModuleEClass = createEClass(TEST_DEFINITION_MODULE);
		createEReference(testDefinitionModuleEClass, TEST_DEFINITION_MODULE__TEST_DEFINITIONS);

		testDefinitionEClass = createEClass(TEST_DEFINITION);
		createEReference(testDefinitionEClass, TEST_DEFINITION__SCENARIO);
		createEAttribute(testDefinitionEClass, TEST_DEFINITION__NAME);
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
		Smcubes_modelPackage theSmcubes_modelPackage = (Smcubes_modelPackage)EPackage.Registry.INSTANCE.getEPackage(Smcubes_modelPackage.eNS_URI);
		Requirements_textPackage theRequirements_textPackage = (Requirements_textPackage)EPackage.Registry.INSTANCE.getEPackage(Requirements_textPackage.eNS_URI);
		Logical_transformationsPackage theLogical_transformationsPackage = (Logical_transformationsPackage)EPackage.Registry.INSTANCE.getEPackage(Logical_transformationsPackage.eNS_URI);
		ReportsPackage theReportsPackage = (ReportsPackage)EPackage.Registry.INSTANCE.getEPackage(ReportsPackage.eNS_URI);
		Input_dataPackage theInput_dataPackage = (Input_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Input_dataPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);
		ScenariosPackage theScenariosPackage = (ScenariosPackage)EPackage.Registry.INSTANCE.getEPackage(ScenariosPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		testModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		testDefinitionModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(aortaSMCubesModelEClass, AortaSMCubesModel.class, "AortaSMCubesModel", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getAortaSMCubesModel_SmCubesModel(), theSmcubes_modelPackage.getSmcubesModel(), null, "smCubesModel", null, 0, 1, AortaSMCubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAortaSMCubesModel_Requirements(), theRequirements_textPackage.getRequirementsModule(), null, "requirements", null, 0, 1, AortaSMCubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAortaSMCubesModel_Tests(), this.getTestModule(), null, "tests", null, 0, 1, AortaSMCubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAortaSMCubesModel_TaskModules(), theLogical_transformationsPackage.getLogicalTransformationModule(), null, "taskModules", null, 0, 1, AortaSMCubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getAortaSMCubesModel_TestDefinitions(), this.getTestDefinitionModule(), null, "testDefinitions", null, 0, 1, AortaSMCubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getAortaSMCubesModel_Name(), ecorePackage.getEString(), "name", null, 0, 1, AortaSMCubesModel.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(reportResultsEClass, ReportResults.class, "ReportResults", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getReportResults_ReportCells(), theReportsPackage.getReportCellValue(), null, "reportCells", null, 0, -1, ReportResults.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testEClass, Test.class, "Test", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTest_InputData(), theInput_dataPackage.getInputData(), null, "inputData", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTest_ExpectedResults(), this.getReportResults(), null, "expectedResults", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEReference(getTest_TestDefinition(), this.getTestDefinition(), null, "testDefinition", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTest_Name(), ecorePackage.getEString(), "name", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testModuleEClass, TestModule.class, "TestModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTestModule_Tests(), this.getTest(), null, "tests", null, 0, -1, TestModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testDefinitionModuleEClass, TestDefinitionModule.class, "TestDefinitionModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTestDefinitionModule_TestDefinitions(), this.getTestDefinition(), null, "testDefinitions", null, 0, -1, TestDefinitionModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testDefinitionEClass, TestDefinition.class, "TestDefinition", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getTestDefinition_Scenario(), theScenariosPackage.getScenario(), null, "scenario", null, 0, 1, TestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);
		initEAttribute(getTestDefinition_Name(), ecorePackage.getEString(), "name", null, 0, 1, TestDefinition.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		// Create resource
		createResource(eNS_URI);

		// Create annotations
		// license
		createLicenseAnnotations();
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

} //Aorta_smcubesPackageImpl
