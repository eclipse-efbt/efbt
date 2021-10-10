/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes;

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
 * This Package describes the grouping of artifacts for CoCaMo for SMCubes into a Program. 
 * A program relates to the group of artifacts, and we allow the contents of  Programs to reference other programs.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Aorta_smcubesPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "aorta_smcubes";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/aorta_smcubes";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "aorta_smcubes";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Aorta_smcubesPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl <em>Aorta SM Cubes Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getAortaSMCubesModel()
	 * @generated
	 */
	int AORTA_SM_CUBES_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Sm Cubes Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL__REQUIREMENTS = 1;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL__TESTS = 2;

	/**
	 * The feature id for the '<em><b>Task Modules</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL__TASK_MODULES = 3;

	/**
	 * The feature id for the '<em><b>Reg Functionality Test Definitions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL__NAME = 5;

	/**
	 * The number of structural features of the '<em>Aorta SM Cubes Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>Aorta SM Cubes Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AORTA_SM_CUBES_MODEL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.ReportResultsImpl <em>Report Results</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.ReportResultsImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getReportResults()
	 * @generated
	 */
	int REPORT_RESULTS = 1;

	/**
	 * The feature id for the '<em><b>Report Cells</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_RESULTS__REPORT_CELLS = 0;

	/**
	 * The number of structural features of the '<em>Report Results</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_RESULTS_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Report Results</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_RESULTS_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl <em>Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTest()
	 * @generated
	 */
	int TEST = 2;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__INPUT_DATA = 0;

	/**
	 * The feature id for the '<em><b>Expected Results</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__EXPECTED_RESULTS = 1;

	/**
	 * The feature id for the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__TEST_DEFINITION = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__NAME = 3;

	/**
	 * The number of structural features of the '<em>Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestModuleImpl <em>Test Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTestModule()
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
	 * The feature id for the '<em><b>Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__TESTS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionModuleImpl <em>Test Definition Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTestDefinitionModule()
	 * @generated
	 */
	int TEST_DEFINITION_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Test Definitions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE__TEST_DEFINITIONS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Test Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Test Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl <em>Test Definition</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTestDefinition()
	 * @generated
	 */
	int TEST_DEFINITION = 5;

	/**
	 * The feature id for the '<em><b>Read Tasks</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION__READ_TASKS = 0;

	/**
	 * The feature id for the '<em><b>Processing Tasks</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION__PROCESSING_TASKS = 1;

	/**
	 * The feature id for the '<em><b>Report Cell Creation Tasks</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION__REPORT_CELL_CREATION_TASKS = 2;

	/**
	 * The feature id for the '<em><b>Scenarios</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION__SCENARIOS = 3;

	/**
	 * The feature id for the '<em><b>Picture</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION__PICTURE = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION__NAME = 5;

	/**
	 * The number of structural features of the '<em>Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>Test Definition</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_DEFINITION_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel <em>Aorta SM Cubes Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aorta SM Cubes Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel
	 * @generated
	 */
	EClass getAortaSMCubesModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getSmCubesModel <em>Sm Cubes Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Sm Cubes Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getSmCubesModel()
	 * @see #getAortaSMCubesModel()
	 * @generated
	 */
	EReference getAortaSMCubesModel_SmCubesModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getRequirements()
	 * @see #getAortaSMCubesModel()
	 * @generated
	 */
	EReference getAortaSMCubesModel_Requirements();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTests()
	 * @see #getAortaSMCubesModel()
	 * @generated
	 */
	EReference getAortaSMCubesModel_Tests();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTaskModules <em>Task Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Task Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTaskModules()
	 * @see #getAortaSMCubesModel()
	 * @generated
	 */
	EReference getAortaSMCubesModel_TaskModules();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Reg Functionality Test Definitions</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getRegFunctionalityTestDefinitions()
	 * @see #getAortaSMCubesModel()
	 * @generated
	 */
	EReference getAortaSMCubesModel_RegFunctionalityTestDefinitions();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getName()
	 * @see #getAortaSMCubesModel()
	 * @generated
	 */
	EAttribute getAortaSMCubesModel_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.ReportResults <em>Report Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.ReportResults
	 * @generated
	 */
	EClass getReportResults();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.ReportResults#getReportCells <em>Report Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.ReportResults#getReportCells()
	 * @see #getReportResults()
	 * @generated
	 */
	EReference getReportResults_ReportCells();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test <em>Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test
	 * @generated
	 */
	EClass getTest();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getInputData()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_InputData();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getExpectedResults <em>Expected Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expected Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getExpectedResults()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_ExpectedResults();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getTestDefinition <em>Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getTestDefinition()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_TestDefinition();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Test#getName()
	 * @see #getTest()
	 * @generated
	 */
	EAttribute getTest_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestModule <em>Test Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestModule
	 * @generated
	 */
	EClass getTestModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestModule#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestModule#getTests()
	 * @see #getTestModule()
	 * @generated
	 */
	EReference getTestModule_Tests();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule <em>Test Definition Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Definition Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule
	 * @generated
	 */
	EClass getTestDefinitionModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule#getTestDefinitions <em>Test Definitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Test Definitions</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule#getTestDefinitions()
	 * @see #getTestDefinitionModule()
	 * @generated
	 */
	EReference getTestDefinitionModule_TestDefinitions();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition <em>Test Definition</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Definition</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition
	 * @generated
	 */
	EClass getTestDefinition();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getReadTasks <em>Read Tasks</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Read Tasks</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getReadTasks()
	 * @see #getTestDefinition()
	 * @generated
	 */
	EReference getTestDefinition_ReadTasks();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getProcessingTasks <em>Processing Tasks</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Processing Tasks</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getProcessingTasks()
	 * @see #getTestDefinition()
	 * @generated
	 */
	EReference getTestDefinition_ProcessingTasks();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getReportCellCreationTasks <em>Report Cell Creation Tasks</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Report Cell Creation Tasks</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getReportCellCreationTasks()
	 * @see #getTestDefinition()
	 * @generated
	 */
	EReference getTestDefinition_ReportCellCreationTasks();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getScenarios <em>Scenarios</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Scenarios</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getScenarios()
	 * @see #getTestDefinition()
	 * @generated
	 */
	EReference getTestDefinition_Scenarios();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getPicture <em>Picture</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Picture</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getPicture()
	 * @see #getTestDefinition()
	 * @generated
	 */
	EReference getTestDefinition_Picture();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getName()
	 * @see #getTestDefinition()
	 * @generated
	 */
	EAttribute getTestDefinition_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Aorta_smcubesFactory getAorta_smcubesFactory();

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
	interface Literals {
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl <em>Aorta SM Cubes Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.AortaSMCubesModelImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getAortaSMCubesModel()
		 * @generated
		 */
		EClass AORTA_SM_CUBES_MODEL = eINSTANCE.getAortaSMCubesModel();

		/**
		 * The meta object literal for the '<em><b>Sm Cubes Model</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AORTA_SM_CUBES_MODEL__SM_CUBES_MODEL = eINSTANCE.getAortaSMCubesModel_SmCubesModel();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AORTA_SM_CUBES_MODEL__REQUIREMENTS = eINSTANCE.getAortaSMCubesModel_Requirements();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AORTA_SM_CUBES_MODEL__TESTS = eINSTANCE.getAortaSMCubesModel_Tests();

		/**
		 * The meta object literal for the '<em><b>Task Modules</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AORTA_SM_CUBES_MODEL__TASK_MODULES = eINSTANCE.getAortaSMCubesModel_TaskModules();

		/**
		 * The meta object literal for the '<em><b>Reg Functionality Test Definitions</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AORTA_SM_CUBES_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS = eINSTANCE.getAortaSMCubesModel_RegFunctionalityTestDefinitions();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute AORTA_SM_CUBES_MODEL__NAME = eINSTANCE.getAortaSMCubesModel_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.ReportResultsImpl <em>Report Results</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.ReportResultsImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getReportResults()
		 * @generated
		 */
		EClass REPORT_RESULTS = eINSTANCE.getReportResults();

		/**
		 * The meta object literal for the '<em><b>Report Cells</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REPORT_RESULTS__REPORT_CELLS = eINSTANCE.getReportResults_ReportCells();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl <em>Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTest()
		 * @generated
		 */
		EClass TEST = eINSTANCE.getTest();

		/**
		 * The meta object literal for the '<em><b>Input Data</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__INPUT_DATA = eINSTANCE.getTest_InputData();

		/**
		 * The meta object literal for the '<em><b>Expected Results</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__EXPECTED_RESULTS = eINSTANCE.getTest_ExpectedResults();

		/**
		 * The meta object literal for the '<em><b>Test Definition</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__TEST_DEFINITION = eINSTANCE.getTest_TestDefinition();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST__NAME = eINSTANCE.getTest_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestModuleImpl <em>Test Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTestModule()
		 * @generated
		 */
		EClass TEST_MODULE = eINSTANCE.getTestModule();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODULE__TESTS = eINSTANCE.getTestModule_Tests();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionModuleImpl <em>Test Definition Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTestDefinitionModule()
		 * @generated
		 */
		EClass TEST_DEFINITION_MODULE = eINSTANCE.getTestDefinitionModule();

		/**
		 * The meta object literal for the '<em><b>Test Definitions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_DEFINITION_MODULE__TEST_DEFINITIONS = eINSTANCE.getTestDefinitionModule_TestDefinitions();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl <em>Test Definition</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.Aorta_smcubesPackageImpl#getTestDefinition()
		 * @generated
		 */
		EClass TEST_DEFINITION = eINSTANCE.getTestDefinition();

		/**
		 * The meta object literal for the '<em><b>Read Tasks</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_DEFINITION__READ_TASKS = eINSTANCE.getTestDefinition_ReadTasks();

		/**
		 * The meta object literal for the '<em><b>Processing Tasks</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_DEFINITION__PROCESSING_TASKS = eINSTANCE.getTestDefinition_ProcessingTasks();

		/**
		 * The meta object literal for the '<em><b>Report Cell Creation Tasks</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_DEFINITION__REPORT_CELL_CREATION_TASKS = eINSTANCE.getTestDefinition_ReportCellCreationTasks();

		/**
		 * The meta object literal for the '<em><b>Scenarios</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_DEFINITION__SCENARIOS = eINSTANCE.getTestDefinition_Scenarios();

		/**
		 * The meta object literal for the '<em><b>Picture</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_DEFINITION__PICTURE = eINSTANCE.getTestDefinition_Picture();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST_DEFINITION__NAME = eINSTANCE.getTestDefinition_Name();

	}

} //Aorta_smcubesPackage
