/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data;

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
 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2019 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Test_input_dataPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "test_input_data";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/test_input_data";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "test_input_data";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Test_input_dataPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.InputDataModuleImpl <em>Input Data Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.InputDataModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getInputDataModule()
	 * @generated
	 */
	int INPUT_DATA_MODULE = 0;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE__INPUT_DATA = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Input Data Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Input Data Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsImpl <em>Useful Input Data Snippets</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getUsefulInputDataSnippets()
	 * @generated
	 */
	int USEFUL_INPUT_DATA_SNIPPETS = 1;

	/**
	 * The feature id for the '<em><b>Test Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA = 0;

	/**
	 * The number of structural features of the '<em>Useful Input Data Snippets</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Useful Input Data Snippets</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.TestInputDataImpl <em>Test Input Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.TestInputDataImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getTestInputData()
	 * @generated
	 */
	int TEST_INPUT_DATA = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_INPUT_DATA__NAME = 0;

	/**
	 * The number of structural features of the '<em>Test Input Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_INPUT_DATA_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Test Input Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_INPUT_DATA_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.ExternalTestDataImpl <em>External Test Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.ExternalTestDataImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getExternalTestData()
	 * @generated
	 */
	int EXTERNAL_TEST_DATA = 3;

	/**
	 * The number of structural features of the '<em>External Test Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXTERNAL_TEST_DATA_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>External Test Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXTERNAL_TEST_DATA_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsModuleImpl <em>Useful Input Data Snippets Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getUsefulInputDataSnippetsModule()
	 * @generated
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Snippets</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Useful Input Data Snippets Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Useful Input Data Snippets Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int USEFUL_INPUT_DATA_SNIPPETS_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.InputDataModule <em>Input Data Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Input Data Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.InputDataModule
	 * @generated
	 */
	EClass getInputDataModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.InputDataModule#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.InputDataModule#getInputData()
	 * @see #getInputDataModule()
	 * @generated
	 */
	EReference getInputDataModule_InputData();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets <em>Useful Input Data Snippets</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Useful Input Data Snippets</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets
	 * @generated
	 */
	EClass getUsefulInputDataSnippets();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets#getTestInputData <em>Test Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Test Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets#getTestInputData()
	 * @see #getUsefulInputDataSnippets()
	 * @generated
	 */
	EReference getUsefulInputDataSnippets_TestInputData();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData <em>Test Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData
	 * @generated
	 */
	EClass getTestInputData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData#getName()
	 * @see #getTestInputData()
	 * @generated
	 */
	EAttribute getTestInputData_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.ExternalTestData <em>External Test Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>External Test Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.ExternalTestData
	 * @generated
	 */
	EClass getExternalTestData();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippetsModule <em>Useful Input Data Snippets Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Useful Input Data Snippets Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippetsModule
	 * @generated
	 */
	EClass getUsefulInputDataSnippetsModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippetsModule#getSnippets <em>Snippets</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Snippets</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippetsModule#getSnippets()
	 * @see #getUsefulInputDataSnippetsModule()
	 * @generated
	 */
	EReference getUsefulInputDataSnippetsModule_Snippets();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Test_input_dataFactory getTest_input_dataFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.InputDataModuleImpl <em>Input Data Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.InputDataModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getInputDataModule()
		 * @generated
		 */
		EClass INPUT_DATA_MODULE = eINSTANCE.getInputDataModule();

		/**
		 * The meta object literal for the '<em><b>Input Data</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INPUT_DATA_MODULE__INPUT_DATA = eINSTANCE.getInputDataModule_InputData();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsImpl <em>Useful Input Data Snippets</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getUsefulInputDataSnippets()
		 * @generated
		 */
		EClass USEFUL_INPUT_DATA_SNIPPETS = eINSTANCE.getUsefulInputDataSnippets();

		/**
		 * The meta object literal for the '<em><b>Test Input Data</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA = eINSTANCE.getUsefulInputDataSnippets_TestInputData();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.TestInputDataImpl <em>Test Input Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.TestInputDataImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getTestInputData()
		 * @generated
		 */
		EClass TEST_INPUT_DATA = eINSTANCE.getTestInputData();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST_INPUT_DATA__NAME = eINSTANCE.getTestInputData_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.ExternalTestDataImpl <em>External Test Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.ExternalTestDataImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getExternalTestData()
		 * @generated
		 */
		EClass EXTERNAL_TEST_DATA = eINSTANCE.getExternalTestData();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsModuleImpl <em>Useful Input Data Snippets Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.Test_input_dataPackageImpl#getUsefulInputDataSnippetsModule()
		 * @generated
		 */
		EClass USEFUL_INPUT_DATA_SNIPPETS_MODULE = eINSTANCE.getUsefulInputDataSnippetsModule();

		/**
		 * The meta object literal for the '<em><b>Snippets</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference USEFUL_INPUT_DATA_SNIPPETS_MODULE__SNIPPETS = eINSTANCE.getUsefulInputDataSnippetsModule_Snippets();

	}

} //Test_input_dataPackage
