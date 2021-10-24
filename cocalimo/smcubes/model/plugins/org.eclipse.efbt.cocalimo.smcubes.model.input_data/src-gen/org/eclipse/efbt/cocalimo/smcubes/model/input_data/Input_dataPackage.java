/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.input_data;

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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.Input_dataFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Input_dataPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "input_data";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/input_data";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "input_data";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Input_dataPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellImpl <em>Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCell()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithEnumeratedValueImpl <em>Cell With Enumerated Value</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithEnumeratedValueImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCellWithEnumeratedValue()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithValueImpl <em>Cell With Value</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithValueImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCellWithValue()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.InputDataImpl <em>Input Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.InputDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getInputData()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CubeDataImpl <em>Cube Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CubeDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCubeData()
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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.RowDataImpl <em>Row Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.RowDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getRowData()
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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithEnumeratedValue <em>Cell With Enumerated Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell With Enumerated Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithEnumeratedValue
	 * @generated
	 */
	EClass getCellWithEnumeratedValue();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithEnumeratedValue#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithEnumeratedValue#getValue()
	 * @see #getCellWithEnumeratedValue()
	 * @generated
	 */
	EReference getCellWithEnumeratedValue_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithValue <em>Cell With Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell With Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithValue
	 * @generated
	 */
	EClass getCellWithValue();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithValue#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CellWithValue#getValue()
	 * @see #getCellWithValue()
	 * @generated
	 */
	EAttribute getCellWithValue_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData
	 * @generated
	 */
	EClass getInputData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData#getSourceCubeData <em>Source Cube Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Source Cube Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData#getSourceCubeData()
	 * @see #getInputData()
	 * @generated
	 */
	EReference getInputData_SourceCubeData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.InputData#getName()
	 * @see #getInputData()
	 * @generated
	 */
	EAttribute getInputData_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.Cell <em>Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.Cell
	 * @generated
	 */
	EClass getCell();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.Cell#getCellID <em>Cell ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cell ID</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.Cell#getCellID()
	 * @see #getCell()
	 * @generated
	 */
	EAttribute getCell_CellID();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.Cell#getColumn <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.Cell#getColumn()
	 * @see #getCell()
	 * @generated
	 */
	EReference getCell_Column();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData <em>Cube Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData
	 * @generated
	 */
	EClass getCubeData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData#getName()
	 * @see #getCubeData()
	 * @generated
	 */
	EAttribute getCubeData_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData#getCube()
	 * @see #getCubeData()
	 * @generated
	 */
	EReference getCubeData_Cube();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData#getRows <em>Rows</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rows</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.CubeData#getRows()
	 * @see #getCubeData()
	 * @generated
	 */
	EReference getCubeData_Rows();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.RowData <em>Row Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Row Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.RowData
	 * @generated
	 */
	EClass getRowData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.RowData#getRowID <em>Row ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Row ID</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.RowData#getRowID()
	 * @see #getRowData()
	 * @generated
	 */
	EAttribute getRowData_RowID();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.RowData#getCells <em>Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.RowData#getCells()
	 * @see #getRowData()
	 * @generated
	 */
	EReference getRowData_Cells();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Input_dataFactory getInput_dataFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithEnumeratedValueImpl <em>Cell With Enumerated Value</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithEnumeratedValueImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCellWithEnumeratedValue()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithValueImpl <em>Cell With Value</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellWithValueImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCellWithValue()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.InputDataImpl <em>Input Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.InputDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getInputData()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellImpl <em>Cell</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CellImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCell()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CubeDataImpl <em>Cube Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.CubeDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getCubeData()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.RowDataImpl <em>Row Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.RowDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.input_data.impl.Input_dataPackageImpl#getRowData()
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

	}

} //Input_dataPackage
