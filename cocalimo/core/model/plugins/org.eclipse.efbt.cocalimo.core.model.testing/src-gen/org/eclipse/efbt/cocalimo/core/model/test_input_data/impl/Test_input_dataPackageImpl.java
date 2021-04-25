/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.ExternalTestData;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.InputDataModule;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataFactory;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippetsModule;

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
public class Test_input_dataPackageImpl extends EPackageImpl implements Test_input_dataPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass inputDataModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass usefulInputDataSnippetsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass testInputDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass externalTestDataEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass usefulInputDataSnippetsModuleEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Test_input_dataPackageImpl()
	{
		super(eNS_URI, Test_input_dataFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Test_input_dataPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static Test_input_dataPackage init()
	{
		if (isInited) return (Test_input_dataPackage)EPackage.Registry.INSTANCE.getEPackage(Test_input_dataPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTest_input_dataPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Test_input_dataPackageImpl theTest_input_dataPackage = registeredTest_input_dataPackage instanceof Test_input_dataPackageImpl ? (Test_input_dataPackageImpl)registeredTest_input_dataPackage : new Test_input_dataPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		Functionality_modulePackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Test_definitionPackage.eNS_URI);
		Test_definitionPackageImpl theTest_definitionPackage = (Test_definitionPackageImpl)(registeredPackage instanceof Test_definitionPackageImpl ? registeredPackage : Test_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(TestPackage.eNS_URI);
		TestPackageImpl theTestPackage = (TestPackageImpl)(registeredPackage instanceof TestPackageImpl ? registeredPackage : TestPackage.eINSTANCE);

		// Create package meta-data objects
		theTest_input_dataPackage.createPackageContents();
		theTest_definitionPackage.createPackageContents();
		theTestPackage.createPackageContents();

		// Initialize created meta-data
		theTest_input_dataPackage.initializePackageContents();
		theTest_definitionPackage.initializePackageContents();
		theTestPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTest_input_dataPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Test_input_dataPackage.eNS_URI, theTest_input_dataPackage);
		return theTest_input_dataPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getInputDataModule()
	{
		return inputDataModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getInputDataModule_InputData()
	{
		return (EReference)inputDataModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getUsefulInputDataSnippets()
	{
		return usefulInputDataSnippetsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getUsefulInputDataSnippets_TestInputData()
	{
		return (EReference)usefulInputDataSnippetsEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTestInputData()
	{
		return testInputDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTestInputData_Name()
	{
		return (EAttribute)testInputDataEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getExternalTestData()
	{
		return externalTestDataEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getUsefulInputDataSnippetsModule()
	{
		return usefulInputDataSnippetsModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getUsefulInputDataSnippetsModule_Snippets()
	{
		return (EReference)usefulInputDataSnippetsModuleEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Test_input_dataFactory getTest_input_dataFactory()
	{
		return (Test_input_dataFactory)getEFactoryInstance();
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
	public void createPackageContents()
	{
		if (isCreated) return;
		isCreated = true;

		// Create classes and their features
		inputDataModuleEClass = createEClass(INPUT_DATA_MODULE);
		createEReference(inputDataModuleEClass, INPUT_DATA_MODULE__INPUT_DATA);

		usefulInputDataSnippetsEClass = createEClass(USEFUL_INPUT_DATA_SNIPPETS);
		createEReference(usefulInputDataSnippetsEClass, USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA);

		testInputDataEClass = createEClass(TEST_INPUT_DATA);
		createEAttribute(testInputDataEClass, TEST_INPUT_DATA__NAME);

		externalTestDataEClass = createEClass(EXTERNAL_TEST_DATA);

		usefulInputDataSnippetsModuleEClass = createEClass(USEFUL_INPUT_DATA_SNIPPETS_MODULE);
		createEReference(usefulInputDataSnippetsModuleEClass, USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS);
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
	public void initializePackageContents()
	{
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
		inputDataModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());
		usefulInputDataSnippetsModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(inputDataModuleEClass, InputDataModule.class, "InputDataModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getInputDataModule_InputData(), this.getTestInputData(), null, "inputData", null, 0, -1, InputDataModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(usefulInputDataSnippetsEClass, UsefulInputDataSnippets.class, "UsefulInputDataSnippets", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getUsefulInputDataSnippets_TestInputData(), this.getTestInputData(), null, "testInputData", null, 0, 1, UsefulInputDataSnippets.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testInputDataEClass, TestInputData.class, "TestInputData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTestInputData_Name(), ecorePackage.getEString(), "name", null, 0, 1, TestInputData.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, !IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(externalTestDataEClass, ExternalTestData.class, "ExternalTestData", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(usefulInputDataSnippetsModuleEClass, UsefulInputDataSnippetsModule.class, "UsefulInputDataSnippetsModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getUsefulInputDataSnippetsModule_Snippets(), this.getUsefulInputDataSnippets(), null, "snippets", null, 0, -1, UsefulInputDataSnippetsModule.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, IS_COMPOSITE, !IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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
	protected void createLicenseAnnotations()
	{
		String source = "license";
		addAnnotation
		  (this,
		   source,
		   new String[] {
			   "license", "Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r"
		   });
	}

	/**
	 * Initializes the annotations for <b>http:///org/eclipse/emf/ecore/util/ExtendedMetaData</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void createExtendedMetaDataAnnotations()
	{
		String source = "http:///org/eclipse/emf/ecore/util/ExtendedMetaData";
		addAnnotation
		  (inputDataModuleEClass,
		   source,
		   new String[] {
			   "name", "InputDataModule",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getInputDataModule_InputData(),
		   source,
		   new String[] {
			   "kind", "element",
			   "name", "inputData"
		   });
		addAnnotation
		  (testInputDataEClass,
		   source,
		   new String[] {
			   "name", "InputData",
			   "kind", "elementOnly"
		   });
		addAnnotation
		  (getTestInputData_Name(),
		   source,
		   new String[] {
			   "kind", "attribute",
			   "name", "name"
		   });
	}

} //Test_input_dataPackageImpl
