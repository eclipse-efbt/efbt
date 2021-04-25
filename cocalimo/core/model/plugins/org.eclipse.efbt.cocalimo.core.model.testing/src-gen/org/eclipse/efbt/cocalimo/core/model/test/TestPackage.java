/**
 */
package org.eclipse.efbt.cocalimo.core.model.test;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * <!-- begin-model-doc -->
 *  This package describes the test package of FreeBIRDTools
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.test.TestFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface TestPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "test";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/test";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "test";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.ResultImpl <em>Result</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.ResultImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getResult()
	 * @generated
	 */
	int RESULT = 0;

	/**
	 * The number of structural features of the '<em>Result</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESULT_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Result</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESULT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.TestImpl <em>Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getTest()
	 * @generated
	 */
	int TEST = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__NAME = 0;

	/**
	 * The number of structural features of the '<em>Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.BDDTestImpl <em>BDD Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.BDDTestImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getBDDTest()
	 * @generated
	 */
	int BDD_TEST = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST__NAME = TEST__NAME;

	/**
	 * The feature id for the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST__TEST_DEFINITION = TEST_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>BDD Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_FEATURE_COUNT = TEST_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>BDD Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BDD_TEST_OPERATION_COUNT = TEST_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.RegFunctionalityTestImpl <em>Reg Functionality Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.RegFunctionalityTestImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getRegFunctionalityTest()
	 * @generated
	 */
	int REG_FUNCTIONALITY_TEST = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST__NAME = TEST__NAME;

	/**
	 * The feature id for the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST__TEST_DEFINITION = TEST_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Reg Functionality Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_FEATURE_COUNT = TEST_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Reg Functionality Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REG_FUNCTIONALITY_TEST_OPERATION_COUNT = TEST_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.TestModuleImpl <em>Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getTestModule()
	 * @generated
	 */
	int TEST_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The number of structural features of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test.Result <em>Result</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Result</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.Result
	 * @generated
	 */
	EClass getResult();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test.BDDTest <em>BDD Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BDD Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.BDDTest
	 * @generated
	 */
	EClass getBDDTest();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.test.BDDTest#getTestDefinition <em>Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.BDDTest#getTestDefinition()
	 * @see #getBDDTest()
	 * @generated
	 */
	EReference getBDDTest_TestDefinition();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest <em>Reg Functionality Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Reg Functionality Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest
	 * @generated
	 */
	EClass getRegFunctionalityTest();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest#getTestDefinition <em>Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest#getTestDefinition()
	 * @see #getRegFunctionalityTest()
	 * @generated
	 */
	EReference getRegFunctionalityTest_TestDefinition();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test.TestModule <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.TestModule
	 * @generated
	 */
	EClass getTestModule();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test.Test <em>Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.Test
	 * @generated
	 */
	EClass getTest();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test.Test#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test.Test#getName()
	 * @see #getTest()
	 * @generated
	 */
	EAttribute getTest_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TestFactory getTestFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals
	{
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.ResultImpl <em>Result</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.ResultImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getResult()
		 * @generated
		 */
		EClass RESULT = eINSTANCE.getResult();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.BDDTestImpl <em>BDD Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.BDDTestImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getBDDTest()
		 * @generated
		 */
		EClass BDD_TEST = eINSTANCE.getBDDTest();

		/**
		 * The meta object literal for the '<em><b>Test Definition</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BDD_TEST__TEST_DEFINITION = eINSTANCE.getBDDTest_TestDefinition();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.RegFunctionalityTestImpl <em>Reg Functionality Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.RegFunctionalityTestImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getRegFunctionalityTest()
		 * @generated
		 */
		EClass REG_FUNCTIONALITY_TEST = eINSTANCE.getRegFunctionalityTest();

		/**
		 * The meta object literal for the '<em><b>Test Definition</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REG_FUNCTIONALITY_TEST__TEST_DEFINITION = eINSTANCE.getRegFunctionalityTest_TestDefinition();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.TestModuleImpl <em>Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getTestModule()
		 * @generated
		 */
		EClass TEST_MODULE = eINSTANCE.getTestModule();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test.impl.TestImpl <em>Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test.impl.TestPackageImpl#getTest()
		 * @generated
		 */
		EClass TEST = eINSTANCE.getTest();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST__NAME = eINSTANCE.getTest_Name();

	}

} //TestPackage
