/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program;

import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;

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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface ProgramPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "program";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/program";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "program";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	ProgramPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.StaticModelImpl <em>Static Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.StaticModelImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getStaticModel()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl <em>SM Cubes Static Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesStaticModel()
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
	 * The feature id for the '<em><b>Sm Cubes Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL = STATIC_MODEL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Requirements</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__REQUIREMENTS = STATIC_MODEL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Bdd Test Definitions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS = STATIC_MODEL_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__TESTS = STATIC_MODEL_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Test Templates</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__TEST_TEMPLATES = STATIC_MODEL_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Test Constriants</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS = STATIC_MODEL_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>Functionality Modules</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES = STATIC_MODEL_FEATURE_COUNT + 6;

	/**
	 * The feature id for the '<em><b>Reg Functionality Tests</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS = STATIC_MODEL_FEATURE_COUNT + 7;

	/**
	 * The feature id for the '<em><b>Reg Functionality Test Definitions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS = STATIC_MODEL_FEATURE_COUNT + 8;

	/**
	 * The number of structural features of the '<em>SM Cubes Static Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL_FEATURE_COUNT = STATIC_MODEL_FEATURE_COUNT + 9;

	/**
	 * The number of operations of the '<em>SM Cubes Static Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_STATIC_MODEL_OPERATION_COUNT = STATIC_MODEL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesTestInputDataImpl <em>SM Cubes Test Input Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesTestInputDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesTestInputData()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesReportResultsImpl <em>SM Cubes Report Results</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesReportResultsImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesReportResults()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestImpl <em>SM Cubes BDD Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesBDDTest()
	 * @generated
	 */
	int SM_CUBES_BDD_TEST = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST__NAME = TestPackage.BDD_TEST__NAME;

	/**
	 * The feature id for the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST__TEST_DEFINITION = TestPackage.BDD_TEST__TEST_DEFINITION;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST__INPUT_DATA = TestPackage.BDD_TEST_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expected Results</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST__EXPECTED_RESULTS = TestPackage.BDD_TEST_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>SM Cubes BDD Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_FEATURE_COUNT = TestPackage.BDD_TEST_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>SM Cubes BDD Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_OPERATION_COUNT = TestPackage.BDD_TEST_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestModuleImpl <em>SM Cubes BDD Test Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesBDDTestModule()
	 * @generated
	 */
	int SM_CUBES_BDD_TEST_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE__DEPENDENCIES = TestPackage.TEST_MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE__THE_DESCRIPTION = TestPackage.TEST_MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE__LICENSE = TestPackage.TEST_MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE__NAME = TestPackage.TEST_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE__VERSION = TestPackage.TEST_MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE__LONG_NAME = TestPackage.TEST_MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE__TESTS = TestPackage.TEST_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>SM Cubes BDD Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE_FEATURE_COUNT = TestPackage.TEST_MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>SM Cubes BDD Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_BDD_TEST_MODULE_OPERATION_COUNT = TestPackage.TEST_MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestImpl <em>SM Cubes Reg Functionality Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesRegFunctionalityTest()
	 * @generated
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST__NAME = TestPackage.REG_FUNCTIONALITY_TEST__NAME;

	/**
	 * The feature id for the '<em><b>Test Definition</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST__TEST_DEFINITION = TestPackage.REG_FUNCTIONALITY_TEST__TEST_DEFINITION;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA = TestPackage.REG_FUNCTIONALITY_TEST_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Expected Results</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS = TestPackage.REG_FUNCTIONALITY_TEST_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>SM Cubes Reg Functionality Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_FEATURE_COUNT = TestPackage.REG_FUNCTIONALITY_TEST_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>SM Cubes Reg Functionality Test</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_OPERATION_COUNT = TestPackage.REG_FUNCTIONALITY_TEST_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestModuleImpl <em>SM Cubes Reg Functionality Test Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesRegFunctionalityTestModule()
	 * @generated
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE = 7;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__DEPENDENCIES = TestPackage.TEST_MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__THE_DESCRIPTION = TestPackage.TEST_MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__LICENSE = TestPackage.TEST_MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__NAME = TestPackage.TEST_MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__VERSION = TestPackage.TEST_MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__LONG_NAME = TestPackage.TEST_MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Reg Functionality Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS = TestPackage.TEST_MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>SM Cubes Reg Functionality Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE_FEATURE_COUNT = TestPackage.TEST_MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>SM Cubes Reg Functionality Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE_OPERATION_COUNT = TestPackage.TEST_MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel <em>SM Cubes Static Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Static Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel
	 * @generated
	 */
	EClass getSMCubesStaticModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getSmCubesModel <em>Sm Cubes Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Sm Cubes Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getSmCubesModel()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_SmCubesModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRequirements <em>Requirements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Requirements</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRequirements()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_Requirements();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getBddTestDefinitions <em>Bdd Test Definitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Bdd Test Definitions</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getBddTestDefinitions()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_BddTestDefinitions();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTests()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_Tests();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestTemplates <em>Test Templates</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Test Templates</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestTemplates()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_TestTemplates();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestConstriants <em>Test Constriants</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Test Constriants</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestConstriants()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_TestConstriants();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getFunctionalityModules <em>Functionality Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Functionality Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getFunctionalityModules()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_FunctionalityModules();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTests <em>Reg Functionality Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Reg Functionality Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTests()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_RegFunctionalityTests();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Reg Functionality Test Definitions</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTestDefinitions()
	 * @see #getSMCubesStaticModel()
	 * @generated
	 */
	EReference getSMCubesStaticModel_RegFunctionalityTestDefinitions();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesTestInputData <em>SM Cubes Test Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Test Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesTestInputData
	 * @generated
	 */
	EClass getSMCubesTestInputData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesTestInputData#getSmcubes_inputdata <em>Smcubes inputdata</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Smcubes inputdata</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesTestInputData#getSmcubes_inputdata()
	 * @see #getSMCubesTestInputData()
	 * @generated
	 */
	EReference getSMCubesTestInputData_Smcubes_inputdata();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults <em>SM Cubes Report Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Report Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults
	 * @generated
	 */
	EClass getSMCubesReportResults();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults#getReportCells <em>Report Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults#getReportCells()
	 * @see #getSMCubesReportResults()
	 * @generated
	 */
	EReference getSMCubesReportResults_ReportCells();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTest <em>SM Cubes BDD Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes BDD Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTest
	 * @generated
	 */
	EClass getSMCubesBDDTest();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTest#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTest#getInputData()
	 * @see #getSMCubesBDDTest()
	 * @generated
	 */
	EReference getSMCubesBDDTest_InputData();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTest#getExpectedResults <em>Expected Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expected Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTest#getExpectedResults()
	 * @see #getSMCubesBDDTest()
	 * @generated
	 */
	EReference getSMCubesBDDTest_ExpectedResults();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTestModule <em>SM Cubes BDD Test Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes BDD Test Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTestModule
	 * @generated
	 */
	EClass getSMCubesBDDTestModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTestModule#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesBDDTestModule#getTests()
	 * @see #getSMCubesBDDTestModule()
	 * @generated
	 */
	EReference getSMCubesBDDTestModule_Tests();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.StaticModel <em>Static Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Static Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.StaticModel
	 * @generated
	 */
	EClass getStaticModel();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.StaticModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.StaticModel#getName()
	 * @see #getStaticModel()
	 * @generated
	 */
	EAttribute getStaticModel_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest <em>SM Cubes Reg Functionality Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Reg Functionality Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest
	 * @generated
	 */
	EClass getSMCubesRegFunctionalityTest();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest#getInputData()
	 * @see #getSMCubesRegFunctionalityTest()
	 * @generated
	 */
	EReference getSMCubesRegFunctionalityTest_InputData();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest#getExpectedResults <em>Expected Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Expected Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest#getExpectedResults()
	 * @see #getSMCubesRegFunctionalityTest()
	 * @generated
	 */
	EReference getSMCubesRegFunctionalityTest_ExpectedResults();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTestModule <em>SM Cubes Reg Functionality Test Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Reg Functionality Test Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTestModule
	 * @generated
	 */
	EClass getSMCubesRegFunctionalityTestModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTestModule#getRegFunctionalityTests <em>Reg Functionality Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Reg Functionality Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTestModule#getRegFunctionalityTests()
	 * @see #getSMCubesRegFunctionalityTestModule()
	 * @generated
	 */
	EReference getSMCubesRegFunctionalityTestModule_RegFunctionalityTests();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	ProgramFactory getProgramFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl <em>SM Cubes Static Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesStaticModelImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesStaticModel()
		 * @generated
		 */
		EClass SM_CUBES_STATIC_MODEL = eINSTANCE.getSMCubesStaticModel();

		/**
		 * The meta object literal for the '<em><b>Sm Cubes Model</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__SM_CUBES_MODEL = eINSTANCE.getSMCubesStaticModel_SmCubesModel();

		/**
		 * The meta object literal for the '<em><b>Requirements</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__REQUIREMENTS = eINSTANCE.getSMCubesStaticModel_Requirements();

		/**
		 * The meta object literal for the '<em><b>Bdd Test Definitions</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__BDD_TEST_DEFINITIONS = eINSTANCE.getSMCubesStaticModel_BddTestDefinitions();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__TESTS = eINSTANCE.getSMCubesStaticModel_Tests();

		/**
		 * The meta object literal for the '<em><b>Test Templates</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__TEST_TEMPLATES = eINSTANCE.getSMCubesStaticModel_TestTemplates();

		/**
		 * The meta object literal for the '<em><b>Test Constriants</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__TEST_CONSTRIANTS = eINSTANCE.getSMCubesStaticModel_TestConstriants();

		/**
		 * The meta object literal for the '<em><b>Functionality Modules</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__FUNCTIONALITY_MODULES = eINSTANCE.getSMCubesStaticModel_FunctionalityModules();

		/**
		 * The meta object literal for the '<em><b>Reg Functionality Tests</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TESTS = eINSTANCE.getSMCubesStaticModel_RegFunctionalityTests();

		/**
		 * The meta object literal for the '<em><b>Reg Functionality Test Definitions</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_STATIC_MODEL__REG_FUNCTIONALITY_TEST_DEFINITIONS = eINSTANCE.getSMCubesStaticModel_RegFunctionalityTestDefinitions();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesTestInputDataImpl <em>SM Cubes Test Input Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesTestInputDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesTestInputData()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesReportResultsImpl <em>SM Cubes Report Results</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesReportResultsImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesReportResults()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestImpl <em>SM Cubes BDD Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesBDDTest()
		 * @generated
		 */
		EClass SM_CUBES_BDD_TEST = eINSTANCE.getSMCubesBDDTest();

		/**
		 * The meta object literal for the '<em><b>Input Data</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_BDD_TEST__INPUT_DATA = eINSTANCE.getSMCubesBDDTest_InputData();

		/**
		 * The meta object literal for the '<em><b>Expected Results</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_BDD_TEST__EXPECTED_RESULTS = eINSTANCE.getSMCubesBDDTest_ExpectedResults();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestModuleImpl <em>SM Cubes BDD Test Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesBDDTestModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesBDDTestModule()
		 * @generated
		 */
		EClass SM_CUBES_BDD_TEST_MODULE = eINSTANCE.getSMCubesBDDTestModule();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_BDD_TEST_MODULE__TESTS = eINSTANCE.getSMCubesBDDTestModule_Tests();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.StaticModelImpl <em>Static Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.StaticModelImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getStaticModel()
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

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestImpl <em>SM Cubes Reg Functionality Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesRegFunctionalityTest()
		 * @generated
		 */
		EClass SM_CUBES_REG_FUNCTIONALITY_TEST = eINSTANCE.getSMCubesRegFunctionalityTest();

		/**
		 * The meta object literal for the '<em><b>Input Data</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA = eINSTANCE.getSMCubesRegFunctionalityTest_InputData();

		/**
		 * The meta object literal for the '<em><b>Expected Results</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS = eINSTANCE.getSMCubesRegFunctionalityTest_ExpectedResults();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestModuleImpl <em>SM Cubes Reg Functionality Test Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.impl.ProgramPackageImpl#getSMCubesRegFunctionalityTestModule()
		 * @generated
		 */
		EClass SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE = eINSTANCE.getSMCubesRegFunctionalityTestModule();

		/**
		 * The meta object literal for the '<em><b>Reg Functionality Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_REG_FUNCTIONALITY_TEST_MODULE__REG_FUNCTIONALITY_TESTS = eINSTANCE.getSMCubesRegFunctionalityTestModule_RegFunctionalityTests();

	}

} //ProgramPackage
