/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.cocamo;

import org.eclipse.efbt.cocamo.core.model.test.TestPackage;

import org.eclipse.efbt.cocamo.core.model.test_input_data.Test_input_dataPackage;

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
 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.CocamoFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface CocamoPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "cocamo";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/cocamo";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "cocamo";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	CocamoPackage eINSTANCE = org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.StaticModelImpl <em>Static Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.StaticModelImpl
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getStaticModel()
	 * @generated
	 */
	int STATIC_MODEL = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATIC_MODEL__NAME = 0;

	/**
	 * The number of structural features of the '<em>Static Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATIC_MODEL_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Static Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STATIC_MODEL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl <em>SM Cubes Static Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesStaticModel()
	 * @generated
	 */
	int SM_CUBES_STATIC_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__NAME = STATIC_MODEL__NAME;

	/**
	 * The feature id for the '<em><b>Sm Cubes Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL = STATIC_MODEL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__REQUIREMENTS = STATIC_MODEL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Test Definitions</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__TEST_DEFINITIONS = STATIC_MODEL_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__TESTS = STATIC_MODEL_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Test Templates</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__TEST_TEMPLATES = STATIC_MODEL_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Test Constriants</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS = STATIC_MODEL_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>Functionality Modules</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES = STATIC_MODEL_FEATURE_COUNT + 6;

	/**
	 * The number of structural features of the '<em>SM Cubes Static Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL_FEATURE_COUNT = STATIC_MODEL_FEATURE_COUNT + 7;

	/**
	 * The number of operations of the '<em>SM Cubes Static Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL_OPERATION_COUNT = STATIC_MODEL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestInputDataImpl <em>SM Cubes Test Input Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestInputDataImpl
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesTestInputData()
	 * @generated
	 */
	int SM_CUBES_TEST_INPUT_DATA = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_INPUT_DATA__NAME = Test_input_dataPackage.TEST_INPUT_DATA__NAME;

	/**
	 * The feature id for the '<em><b>Smcubes inputdata</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA = Test_input_dataPackage.TEST_INPUT_DATA_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>SM Cubes Test Input Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_INPUT_DATA_FEATURE_COUNT = Test_input_dataPackage.TEST_INPUT_DATA_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>SM Cubes Test Input Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_INPUT_DATA_OPERATION_COUNT = Test_input_dataPackage.TEST_INPUT_DATA_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesReportResultsImpl <em>SM Cubes Report Results</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesReportResultsImpl
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesReportResults()
	 * @generated
	 */
	int SM_CUBES_REPORT_RESULTS = 2;

	/**
	 * The feature id for the '<em><b>Report Cells</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REPORT_RESULTS__REPORT_CELLS = TestPackage.RESULT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>SM Cubes Report Results</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REPORT_RESULTS_FEATURE_COUNT = TestPackage.RESULT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>SM Cubes Report Results</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REPORT_RESULTS_OPERATION_COUNT = TestPackage.RESULT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestImpl <em>SM Cubes Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestImpl
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesTest()
	 * @generated
	 */
	int SM_CUBES_TEST = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST__NAME = TestPackage.E2E_TEST__NAME;

	/**
	 * The feature id for the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST__TEST_DEFINITION = TestPackage.E2E_TEST__TEST_DEFINITION;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST__INPUT_DATA = TestPackage.E2E_TEST_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expected Results</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST__EXPECTED_RESULTS = TestPackage.E2E_TEST_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>SM Cubes Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_FEATURE_COUNT = TestPackage.E2E_TEST_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>SM Cubes Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_OPERATION_COUNT = TestPackage.E2E_TEST_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestModuleImpl <em>SM Cubes Test Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestModuleImpl
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesTestModule()
	 * @generated
	 */
	int SM_CUBES_TEST_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE__DEPENDENCIES = TestPackage.TEST_MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE__THE_DESCRIPTION = TestPackage.TEST_MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE__LICENSE = TestPackage.TEST_MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE__NAME = TestPackage.TEST_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE__VERSION = TestPackage.TEST_MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE__LONG_NAME = TestPackage.TEST_MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE__TESTS = TestPackage.TEST_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>SM Cubes Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE_FEATURE_COUNT = TestPackage.TEST_MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>SM Cubes Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_TEST_MODULE_OPERATION_COUNT = TestPackage.TEST_MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel <em>SM Cubes Static Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Static Model</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel
	 * @generated
	 */
	EClass getSMCubesStaticModel();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getSmCubesModel <em>Sm Cubes Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Sm Cubes Model</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getSmCubesModel()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_SmCubesModel();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getRequirements()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_Requirements();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTestDefinitions <em>Test Definitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Test Definitions</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTestDefinitions()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_TestDefinitions();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTests()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_Tests();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTestTemplates <em>Test Templates</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Test Templates</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTestTemplates()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_TestTemplates();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTestConstriants <em>Test Constriants</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Test Constriants</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getTestConstriants()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_TestConstriants();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getFunctionalityModules <em>Functionality Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Functionality Modules</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel#getFunctionalityModules()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_FunctionalityModules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestInputData <em>SM Cubes Test Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Test Input Data</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestInputData
	 * @generated
	 */
	EClass getSMCubesTestInputData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestInputData#getSmcubes_inputdata <em>Smcubes inputdata</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Smcubes inputdata</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestInputData#getSmcubes_inputdata()
	 * @see #getSMCubesTestInputData()
	 * @generated
	 */
	EReference getSMCubesTestInputData_Smcubes_inputdata();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesReportResults <em>SM Cubes Report Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Report Results</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesReportResults
	 * @generated
	 */
	EClass getSMCubesReportResults();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesReportResults#getReportCells <em>Report Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Cells</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesReportResults#getReportCells()
	 * @see #getSMCubesReportResults()
	 * @generated
	 */
	EReference getSMCubesReportResults_ReportCells();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTest <em>SM Cubes Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Test</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTest
	 * @generated
	 */
	EClass getSMCubesTest();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTest#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTest#getInputData()
	 * @see #getSMCubesTest()
	 * @generated
	 */
	EReference getSMCubesTest_InputData();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTest#getExpectedResults <em>Expected Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Expected Results</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTest#getExpectedResults()
	 * @see #getSMCubesTest()
	 * @generated
	 */
	EReference getSMCubesTest_ExpectedResults();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestModule <em>SM Cubes Test Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Test Module</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestModule
	 * @generated
	 */
	EClass getSMCubesTestModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestModule#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesTestModule#getTests()
	 * @see #getSMCubesTestModule()
	 * @generated
	 */
	EReference getSMCubesTestModule_Tests();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.StaticModel <em>Static Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Static Model</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.StaticModel
	 * @generated
	 */
	EClass getStaticModel();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.StaticModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.StaticModel#getName()
	 * @see #getStaticModel()
	 * @generated
	 */
	EAttribute getStaticModel_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	CocamoFactory getCocamoFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl <em>SM Cubes Static Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesStaticModelImpl
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesStaticModel()
		 * @generated
		 */
		EClass SM_CUBES_STATIC_MODEL = eINSTANCE.getSMCubesStaticModel();

		/**
		 * The meta object literal for the '<em><b>Sm Cubes Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL = eINSTANCE.getSMCubesStaticModel_SmCubesModel();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__REQUIREMENTS = eINSTANCE.getSMCubesStaticModel_Requirements();

		/**
		 * The meta object literal for the '<em><b>Test Definitions</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__TEST_DEFINITIONS = eINSTANCE.getSMCubesStaticModel_TestDefinitions();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__TESTS = eINSTANCE.getSMCubesStaticModel_Tests();

		/**
		 * The meta object literal for the '<em><b>Test Templates</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__TEST_TEMPLATES = eINSTANCE.getSMCubesStaticModel_TestTemplates();

		/**
		 * The meta object literal for the '<em><b>Test Constriants</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS = eINSTANCE.getSMCubesStaticModel_TestConstriants();

		/**
		 * The meta object literal for the '<em><b>Functionality Modules</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES = eINSTANCE.getSMCubesStaticModel_FunctionalityModules();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestInputDataImpl <em>SM Cubes Test Input Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestInputDataImpl
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesTestInputData()
		 * @generated
		 */
		EClass SM_CUBES_TEST_INPUT_DATA = eINSTANCE.getSMCubesTestInputData();

		/**
		 * The meta object literal for the '<em><b>Smcubes inputdata</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA = eINSTANCE.getSMCubesTestInputData_Smcubes_inputdata();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesReportResultsImpl <em>SM Cubes Report Results</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesReportResultsImpl
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesReportResults()
		 * @generated
		 */
		EClass SM_CUBES_REPORT_RESULTS = eINSTANCE.getSMCubesReportResults();

		/**
		 * The meta object literal for the '<em><b>Report Cells</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_REPORT_RESULTS__REPORT_CELLS = eINSTANCE.getSMCubesReportResults_ReportCells();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestImpl <em>SM Cubes Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestImpl
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesTest()
		 * @generated
		 */
		EClass SM_CUBES_TEST = eINSTANCE.getSMCubesTest();

		/**
		 * The meta object literal for the '<em><b>Input Data</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_TEST__INPUT_DATA = eINSTANCE.getSMCubesTest_InputData();

		/**
		 * The meta object literal for the '<em><b>Expected Results</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_TEST__EXPECTED_RESULTS = eINSTANCE.getSMCubesTest_ExpectedResults();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestModuleImpl <em>SM Cubes Test Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.SMCubesTestModuleImpl
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getSMCubesTestModule()
		 * @generated
		 */
		EClass SM_CUBES_TEST_MODULE = eINSTANCE.getSMCubesTestModule();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_TEST_MODULE__TESTS = eINSTANCE.getSMCubesTestModule_Tests();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.StaticModelImpl <em>Static Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.StaticModelImpl
		 * @see org.eclipse.efbt.cocamo.smcubes.model.cocamo.impl.CocamoPackageImpl#getStaticModel()
		 * @generated
		 */
		EClass STATIC_MODEL = eINSTANCE.getStaticModel();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute STATIC_MODEL__NAME = eINSTANCE.getStaticModel_Name();

	}

} //CocamoPackage
