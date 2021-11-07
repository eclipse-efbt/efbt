/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing;

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
 *  This package describes the storage of base cube data (as opposed to derived cube data) 
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface TestingPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "testing";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/testing";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "testing";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestingPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellImpl <em>Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCell()
	 * @generated
	 */
	int CELL = 3;

	/**
	 * The feature id for the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL__CELL_ID = 0;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL__COLUMN = 1;

	/**
	 * The number of structural features of the '<em>Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithEnumeratedValueImpl <em>Cell With Enumerated Value</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithEnumeratedValueImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCellWithEnumeratedValue()
	 * @generated
	 */
	int CELL_WITH_ENUMERATED_VALUE = 0;

	/**
	 * The feature id for the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_ENUMERATED_VALUE__CELL_ID = CELL__CELL_ID;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_ENUMERATED_VALUE__COLUMN = CELL__COLUMN;

	/**
	 * The feature id for the '<em><b>Value</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_ENUMERATED_VALUE__VALUE = CELL_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Cell With Enumerated Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_ENUMERATED_VALUE_FEATURE_COUNT = CELL_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Cell With Enumerated Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_ENUMERATED_VALUE_OPERATION_COUNT = CELL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithValueImpl <em>Cell With Value</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithValueImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCellWithValue()
	 * @generated
	 */
	int CELL_WITH_VALUE = 1;

	/**
	 * The feature id for the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_VALUE__CELL_ID = CELL__CELL_ID;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_VALUE__COLUMN = CELL__COLUMN;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_VALUE__VALUE = CELL_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Cell With Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_VALUE_FEATURE_COUNT = CELL_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Cell With Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_WITH_VALUE_OPERATION_COUNT = CELL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.InputDataImpl <em>Input Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.InputDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getInputData()
	 * @generated
	 */
	int INPUT_DATA = 2;

	/**
	 * The feature id for the '<em><b>Source Cube Data</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA__SOURCE_CUBE_DATA = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA__NAME = 1;

	/**
	 * The number of structural features of the '<em>Input Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Input Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CubeDataImpl <em>Cube Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CubeDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCubeData()
	 * @generated
	 */
	int CUBE_DATA = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_DATA__NAME = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_DATA__CUBE = 1;

	/**
	 * The feature id for the '<em><b>Rows</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_DATA__ROWS = 2;

	/**
	 * The number of structural features of the '<em>Cube Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_DATA_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Cube Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_DATA_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.RowDataImpl <em>Row Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.RowDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getRowData()
	 * @generated
	 */
	int ROW_DATA = 5;

	/**
	 * The feature id for the '<em><b>Row ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_DATA__ROW_ID = 0;

	/**
	 * The feature id for the '<em><b>Cells</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_DATA__CELLS = 1;

	/**
	 * The number of structural features of the '<em>Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_DATA_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_DATA_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestModuleImpl <em>Test Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getTestModule()
	 * @generated
	 */
	int TEST_MODULE = 6;

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
	 * The feature id for the '<em><b>SM Cubes Model</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__SM_CUBES_MODEL = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Tests</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__TESTS = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Logical Transformations</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE__LOGICAL_TRANSFORMATIONS = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Test Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.ReportResultsImpl <em>Report Results</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.ReportResultsImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getReportResults()
	 * @generated
	 */
	int REPORT_RESULTS = 7;

	/**
	 * The feature id for the '<em><b>Report Results</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_RESULTS__REPORT_RESULTS = 0;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl <em>Test</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getTest()
	 * @generated
	 */
	int TEST = 8;

	/**
	 * The feature id for the '<em><b>Input Data</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__INPUT_DATA = 0;

	/**
	 * The feature id for the '<em><b>Report Results</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__REPORT_RESULTS = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__NAME = 2;

	/**
	 * The feature id for the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TEST__SCENARIO = 3;

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithEnumeratedValue <em>Cell With Enumerated Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell With Enumerated Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithEnumeratedValue
	 * @generated
	 */
	EClass getCellWithEnumeratedValue();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithEnumeratedValue#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithEnumeratedValue#getValue()
	 * @see #getCellWithEnumeratedValue()
	 * @generated
	 */
	EReference getCellWithEnumeratedValue_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithValue <em>Cell With Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell With Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithValue
	 * @generated
	 */
	EClass getCellWithValue();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithValue#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CellWithValue#getValue()
	 * @see #getCellWithValue()
	 * @generated
	 */
	EAttribute getCellWithValue_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData
	 * @generated
	 */
	EClass getInputData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData#getSourceCubeData <em>Source Cube Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Source Cube Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData#getSourceCubeData()
	 * @see #getInputData()
	 * @generated
	 */
	EReference getInputData_SourceCubeData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.InputData#getName()
	 * @see #getInputData()
	 * @generated
	 */
	EAttribute getInputData_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell <em>Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell
	 * @generated
	 */
	EClass getCell();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getCellID <em>Cell ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cell ID</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getCellID()
	 * @see #getCell()
	 * @generated
	 */
	EAttribute getCell_CellID();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getColumn <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Cell#getColumn()
	 * @see #getCell()
	 * @generated
	 */
	EReference getCell_Column();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData <em>Cube Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData
	 * @generated
	 */
	EClass getCubeData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData#getName()
	 * @see #getCubeData()
	 * @generated
	 */
	EAttribute getCubeData_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData#getCube()
	 * @see #getCubeData()
	 * @generated
	 */
	EReference getCubeData_Cube();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData#getRows <em>Rows</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rows</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.CubeData#getRows()
	 * @see #getCubeData()
	 * @generated
	 */
	EReference getCubeData_Rows();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData <em>Row Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Row Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData
	 * @generated
	 */
	EClass getRowData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData#getRowID <em>Row ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Row ID</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData#getRowID()
	 * @see #getRowData()
	 * @generated
	 */
	EAttribute getRowData_RowID();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData#getCells <em>Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.RowData#getCells()
	 * @see #getRowData()
	 * @generated
	 */
	EReference getRowData_Cells();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule <em>Test Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule
	 * @generated
	 */
	EClass getTestModule();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getSMCubesModel <em>SM Cubes Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>SM Cubes Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getSMCubesModel()
	 * @see #getTestModule()
	 * @generated
	 */
	EReference getTestModule_SMCubesModel();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getTests <em>Tests</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Tests</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getTests()
	 * @see #getTestModule()
	 * @generated
	 */
	EReference getTestModule_Tests();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getLogicalTransformations <em>Logical Transformations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Logical Transformations</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getLogicalTransformations()
	 * @see #getTestModule()
	 * @generated
	 */
	EReference getTestModule_LogicalTransformations();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.ReportResults <em>Report Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.ReportResults
	 * @generated
	 */
	EClass getReportResults();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.ReportResults#getReportResults <em>Report Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.ReportResults#getReportResults()
	 * @see #getReportResults()
	 * @generated
	 */
	EReference getReportResults_ReportResults();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test <em>Test</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Test</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Test
	 * @generated
	 */
	EClass getTest();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getInputData()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_InputData();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getReportResults <em>Report Results</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Report Results</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getReportResults()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_ReportResults();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getName()
	 * @see #getTest()
	 * @generated
	 */
	EAttribute getTest_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getScenario <em>Scenario</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Scenario</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.Test#getScenario()
	 * @see #getTest()
	 * @generated
	 */
	EReference getTest_Scenario();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	TestingFactory getTestingFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithEnumeratedValueImpl <em>Cell With Enumerated Value</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithEnumeratedValueImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCellWithEnumeratedValue()
		 * @generated
		 */
		EClass CELL_WITH_ENUMERATED_VALUE = eINSTANCE.getCellWithEnumeratedValue();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CELL_WITH_ENUMERATED_VALUE__VALUE = eINSTANCE.getCellWithEnumeratedValue_Value();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithValueImpl <em>Cell With Value</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellWithValueImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCellWithValue()
		 * @generated
		 */
		EClass CELL_WITH_VALUE = eINSTANCE.getCellWithValue();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CELL_WITH_VALUE__VALUE = eINSTANCE.getCellWithValue_Value();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.InputDataImpl <em>Input Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.InputDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getInputData()
		 * @generated
		 */
		EClass INPUT_DATA = eINSTANCE.getInputData();

		/**
		 * The meta object literal for the '<em><b>Source Cube Data</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INPUT_DATA__SOURCE_CUBE_DATA = eINSTANCE.getInputData_SourceCubeData();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute INPUT_DATA__NAME = eINSTANCE.getInputData_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellImpl <em>Cell</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CellImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCell()
		 * @generated
		 */
		EClass CELL = eINSTANCE.getCell();

		/**
		 * The meta object literal for the '<em><b>Cell ID</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CELL__CELL_ID = eINSTANCE.getCell_CellID();

		/**
		 * The meta object literal for the '<em><b>Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CELL__COLUMN = eINSTANCE.getCell_Column();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CubeDataImpl <em>Cube Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.CubeDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getCubeData()
		 * @generated
		 */
		EClass CUBE_DATA = eINSTANCE.getCubeData();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_DATA__NAME = eINSTANCE.getCubeData_Name();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_DATA__CUBE = eINSTANCE.getCubeData_Cube();

		/**
		 * The meta object literal for the '<em><b>Rows</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_DATA__ROWS = eINSTANCE.getCubeData_Rows();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.RowDataImpl <em>Row Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.RowDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getRowData()
		 * @generated
		 */
		EClass ROW_DATA = eINSTANCE.getRowData();

		/**
		 * The meta object literal for the '<em><b>Row ID</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ROW_DATA__ROW_ID = eINSTANCE.getRowData_RowID();

		/**
		 * The meta object literal for the '<em><b>Cells</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_DATA__CELLS = eINSTANCE.getRowData_Cells();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestModuleImpl <em>Test Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getTestModule()
		 * @generated
		 */
		EClass TEST_MODULE = eINSTANCE.getTestModule();

		/**
		 * The meta object literal for the '<em><b>SM Cubes Model</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODULE__SM_CUBES_MODEL = eINSTANCE.getTestModule_SMCubesModel();

		/**
		 * The meta object literal for the '<em><b>Tests</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODULE__TESTS = eINSTANCE.getTestModule_Tests();

		/**
		 * The meta object literal for the '<em><b>Logical Transformations</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST_MODULE__LOGICAL_TRANSFORMATIONS = eINSTANCE.getTestModule_LogicalTransformations();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.ReportResultsImpl <em>Report Results</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.ReportResultsImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getReportResults()
		 * @generated
		 */
		EClass REPORT_RESULTS = eINSTANCE.getReportResults();

		/**
		 * The meta object literal for the '<em><b>Report Results</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REPORT_RESULTS__REPORT_RESULTS = eINSTANCE.getReportResults_ReportResults();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl <em>Test</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.impl.TestingPackageImpl#getTest()
		 * @generated
		 */
		EClass TEST = eINSTANCE.getTest();

		/**
		 * The meta object literal for the '<em><b>Input Data</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__INPUT_DATA = eINSTANCE.getTest_InputData();

		/**
		 * The meta object literal for the '<em><b>Report Results</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__REPORT_RESULTS = eINSTANCE.getTest_ReportResults();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TEST__NAME = eINSTANCE.getTest_Name();

		/**
		 * The meta object literal for the '<em><b>Scenario</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TEST__SCENARIO = eINSTANCE.getTest_Scenario();

	}

} //TestingPackage
