/**
 */
package org.eclipse.efbt.cocalimo.core.model.test.impl;

import org.eclipse.efbt.cocalimo.core.model.task.TaskPackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;

import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage;

import org.eclipse.efbt.cocalimo.core.model.test.BDDTest;
import org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest;
import org.eclipse.efbt.cocalimo.core.model.test.Result;
import org.eclipse.efbt.cocalimo.core.model.test.Test;
import org.eclipse.efbt.cocalimo.core.model.test.TestFactory;
import org.eclipse.efbt.cocalimo.core.model.test.TestModule;
import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.impl.Test_definitionPackageImpl;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl;

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
public class TestPackageImpl extends EPackageImpl implements TestPackage
{
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass resultEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass bddTestEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass regFunctionalityTestEClass = null;

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
	private EClass testEClass = null;

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
	 * @see org.eclipse.efbt.cocalimo.core.model.test.TestPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private TestPackageImpl()
	{
		super(eNS_URI, TestFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link TestPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @see #createPackageContents()
	 * @see #initializePackageContents()
	 * @generated
	 */
	public static TestPackage init()
	{
		if (isInited) return (TestPackage)EPackage.Registry.INSTANCE.getEPackage(TestPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredTestPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		TestPackageImpl theTestPackage = registeredTestPackage instanceof TestPackageImpl ? (TestPackageImpl)registeredTestPackage : new TestPackageImpl();

		isInited = true;

		// Initialize simple dependencies
		TaskPackage.eINSTANCE.eClass();
		ScenariosPackage.eINSTANCE.eClass();
		Module_managementPackage.eINSTANCE.eClass();
		Requirements_textPackage.eINSTANCE.eClass();
		EcorePackage.eINSTANCE.eClass();

		// Obtain or create and register interdependencies
		Object registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Test_definitionPackage.eNS_URI);
		Test_definitionPackageImpl theTest_definitionPackage = (Test_definitionPackageImpl)(registeredPackage instanceof Test_definitionPackageImpl ? registeredPackage : Test_definitionPackage.eINSTANCE);
		registeredPackage = EPackage.Registry.INSTANCE.getEPackage(Test_input_dataPackage.eNS_URI);
		Test_input_dataPackageImpl theTest_input_dataPackage = (Test_input_dataPackageImpl)(registeredPackage instanceof Test_input_dataPackageImpl ? registeredPackage : Test_input_dataPackage.eINSTANCE);

		// Create package meta-data objects
		theTestPackage.createPackageContents();
		theTest_definitionPackage.createPackageContents();
		theTest_input_dataPackage.createPackageContents();

		// Initialize created meta-data
		theTestPackage.initializePackageContents();
		theTest_definitionPackage.initializePackageContents();
		theTest_input_dataPackage.initializePackageContents();

		// Mark meta-data to indicate it can't be changed
		theTestPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(TestPackage.eNS_URI, theTestPackage);
		return theTestPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getResult()
	{
		return resultEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getBDDTest()
	{
		return bddTestEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getBDDTest_TestDefinition()
	{
		return (EReference)bddTestEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRegFunctionalityTest()
	{
		return regFunctionalityTestEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRegFunctionalityTest_TestDefinition()
	{
		return (EReference)regFunctionalityTestEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTestModule()
	{
		return testModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTest()
	{
		return testEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getTest_Name()
	{
		return (EAttribute)testEClass.getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestFactory getTestFactory()
	{
		return (TestFactory)getEFactoryInstance();
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
		resultEClass = createEClass(RESULT);

		bddTestEClass = createEClass(BDD_TEST);
		createEReference(bddTestEClass, BDD_TEST__TEST_DEFINITION);

		regFunctionalityTestEClass = createEClass(REG_FUNCTIONALITY_TEST);
		createEReference(regFunctionalityTestEClass, REG_FUNCTIONALITY_TEST__TEST_DEFINITION);

		testModuleEClass = createEClass(TEST_MODULE);

		testEClass = createEClass(TEST);
		createEAttribute(testEClass, TEST__NAME);
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
		Test_definitionPackage theTest_definitionPackage = (Test_definitionPackage)EPackage.Registry.INSTANCE.getEPackage(Test_definitionPackage.eNS_URI);
		Module_managementPackage theModule_managementPackage = (Module_managementPackage)EPackage.Registry.INSTANCE.getEPackage(Module_managementPackage.eNS_URI);

		// Create type parameters

		// Set bounds for type parameters

		// Add supertypes to classes
		bddTestEClass.getESuperTypes().add(this.getTest());
		regFunctionalityTestEClass.getESuperTypes().add(this.getTest());
		testModuleEClass.getESuperTypes().add(theModule_managementPackage.getModule());

		// Initialize classes, features, and operations; add parameters
		initEClass(resultEClass, Result.class, "Result", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(bddTestEClass, BDDTest.class, "BDDTest", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getBDDTest_TestDefinition(), theTest_definitionPackage.getBDDTestDefinition(), null, "testDefinition", null, 0, 1, BDDTest.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(regFunctionalityTestEClass, RegFunctionalityTest.class, "RegFunctionalityTest", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEReference(getRegFunctionalityTest_TestDefinition(), theTest_definitionPackage.getRegFunctionalityTestDefinition(), null, "testDefinition", null, 0, 1, RegFunctionalityTest.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_COMPOSITE, IS_RESOLVE_PROXIES, !IS_UNSETTABLE, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

		initEClass(testModuleEClass, TestModule.class, "TestModule", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);

		initEClass(testEClass, Test.class, "Test", !IS_ABSTRACT, !IS_INTERFACE, IS_GENERATED_INSTANCE_CLASS);
		initEAttribute(getTest_Name(), ecorePackage.getEString(), "name", null, 0, 1, Test.class, !IS_TRANSIENT, !IS_VOLATILE, IS_CHANGEABLE, !IS_UNSETTABLE, IS_ID, IS_UNIQUE, !IS_DERIVED, IS_ORDERED);

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

} //TestPackageImpl
