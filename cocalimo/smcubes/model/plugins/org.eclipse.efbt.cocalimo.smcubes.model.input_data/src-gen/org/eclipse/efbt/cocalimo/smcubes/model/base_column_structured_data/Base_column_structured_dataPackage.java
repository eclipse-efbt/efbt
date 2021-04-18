/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataPackage;

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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Base_column_structured_dataPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "base_column_structured_data";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/base_column_structured_data";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "base_column_structured_data";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Base_column_structured_dataPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellImpl <em>Base Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseCell()
	 * @generated
	 */
	int BASE_CELL = 0;

	/**
	 * The feature id for the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL__CELL_ID = Column_structured_dataPackage.CELL__CELL_ID;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL__COLUMN = Column_structured_dataPackage.CELL__COLUMN;

	/**
	 * The number of structural features of the '<em>Base Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_FEATURE_COUNT = Column_structured_dataPackage.CELL_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Base Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_OPERATION_COUNT = Column_structured_dataPackage.CELL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithEnumeratedValueImpl <em>Base Cell With Enumerated Value</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithEnumeratedValueImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseCellWithEnumeratedValue()
	 * @generated
	 */
	int BASE_CELL_WITH_ENUMERATED_VALUE = 1;

	/**
	 * The feature id for the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_ENUMERATED_VALUE__CELL_ID = BASE_CELL__CELL_ID;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_ENUMERATED_VALUE__COLUMN = BASE_CELL__COLUMN;

	/**
	 * The feature id for the '<em><b>Value</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_ENUMERATED_VALUE__VALUE = BASE_CELL_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base Cell With Enumerated Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_ENUMERATED_VALUE_FEATURE_COUNT = BASE_CELL_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base Cell With Enumerated Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_ENUMERATED_VALUE_OPERATION_COUNT = BASE_CELL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithValueImpl <em>Base Cell With Value</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithValueImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseCellWithValue()
	 * @generated
	 */
	int BASE_CELL_WITH_VALUE = 2;

	/**
	 * The feature id for the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_VALUE__CELL_ID = BASE_CELL__CELL_ID;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_VALUE__COLUMN = BASE_CELL__COLUMN;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_VALUE__VALUE = BASE_CELL_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base Cell With Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_VALUE_FEATURE_COUNT = BASE_CELL_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base Cell With Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_CELL_WITH_VALUE_OPERATION_COUNT = BASE_CELL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseColumnStructuredDataImpl <em>Base Column Structured Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseColumnStructuredDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseColumnStructuredData()
	 * @generated
	 */
	int BASE_COLUMN_STRUCTURED_DATA = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_COLUMN_STRUCTURED_DATA__NAME = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA__NAME;

	/**
	 * The feature id for the '<em><b>Rows</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_COLUMN_STRUCTURED_DATA__ROWS = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_COLUMN_STRUCTURED_DATA__CUBE = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Base Column Structured Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_COLUMN_STRUCTURED_DATA_FEATURE_COUNT = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Base Column Structured Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_COLUMN_STRUCTURED_DATA_OPERATION_COUNT = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseRowDataImpl <em>Base Row Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseRowDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseRowData()
	 * @generated
	 */
	int BASE_ROW_DATA = 4;

	/**
	 * The feature id for the '<em><b>Row ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ROW_DATA__ROW_ID = Column_structured_dataPackage.ROW_DATA__ROW_ID;

	/**
	 * The feature id for the '<em><b>Cells</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ROW_DATA__CELLS = Column_structured_dataPackage.ROW_DATA_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ROW_DATA_FEATURE_COUNT = Column_structured_dataPackage.ROW_DATA_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ROW_DATA_OPERATION_COUNT = Column_structured_dataPackage.ROW_DATA_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataImpl <em>Input Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getInputData()
	 * @generated
	 */
	int INPUT_DATA = 5;

	/**
	 * The feature id for the '<em><b>Source Table Data</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INPUT_DATA__SOURCE_TABLE_DATA = 0;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataModuleImpl <em>Input Data Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getInputDataModule()
	 * @generated
	 */
	int INPUT_DATA_MODULE = 6;

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCell <em>Base Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCell
	 * @generated
	 */
	EClass getBaseCell();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue <em>Base Cell With Enumerated Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Cell With Enumerated Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue
	 * @generated
	 */
	EClass getBaseCellWithEnumeratedValue();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue#getValue()
	 * @see #getBaseCellWithEnumeratedValue()
	 * @generated
	 */
	EReference getBaseCellWithEnumeratedValue_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithValue <em>Base Cell With Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Cell With Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithValue
	 * @generated
	 */
	EClass getBaseCellWithValue();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithValue#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithValue#getValue()
	 * @see #getBaseCellWithValue()
	 * @generated
	 */
	EAttribute getBaseCellWithValue_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData <em>Base Column Structured Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Column Structured Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData
	 * @generated
	 */
	EClass getBaseColumnStructuredData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData#getRows <em>Rows</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rows</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData#getRows()
	 * @see #getBaseColumnStructuredData()
	 * @generated
	 */
	EReference getBaseColumnStructuredData_Rows();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData#getCube()
	 * @see #getBaseColumnStructuredData()
	 * @generated
	 */
	EReference getBaseColumnStructuredData_Cube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseRowData <em>Base Row Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Row Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseRowData
	 * @generated
	 */
	EClass getBaseRowData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseRowData#getCells <em>Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseRowData#getCells()
	 * @see #getBaseRowData()
	 * @generated
	 */
	EReference getBaseRowData_Cells();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputData
	 * @generated
	 */
	EClass getInputData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputData#getSourceTableData <em>Source Table Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Source Table Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputData#getSourceTableData()
	 * @see #getInputData()
	 * @generated
	 */
	EReference getInputData_SourceTableData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputData#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputData#getName()
	 * @see #getInputData()
	 * @generated
	 */
	EAttribute getInputData_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputDataModule <em>Input Data Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Input Data Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputDataModule
	 * @generated
	 */
	EClass getInputDataModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputDataModule#getInputData <em>Input Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Input Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.InputDataModule#getInputData()
	 * @see #getInputDataModule()
	 * @generated
	 */
	EReference getInputDataModule_InputData();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Base_column_structured_dataFactory getBase_column_structured_dataFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellImpl <em>Base Cell</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseCell()
		 * @generated
		 */
		EClass BASE_CELL = eINSTANCE.getBaseCell();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithEnumeratedValueImpl <em>Base Cell With Enumerated Value</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithEnumeratedValueImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseCellWithEnumeratedValue()
		 * @generated
		 */
		EClass BASE_CELL_WITH_ENUMERATED_VALUE = eINSTANCE.getBaseCellWithEnumeratedValue();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_CELL_WITH_ENUMERATED_VALUE__VALUE = eINSTANCE.getBaseCellWithEnumeratedValue_Value();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithValueImpl <em>Base Cell With Value</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseCellWithValueImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseCellWithValue()
		 * @generated
		 */
		EClass BASE_CELL_WITH_VALUE = eINSTANCE.getBaseCellWithValue();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute BASE_CELL_WITH_VALUE__VALUE = eINSTANCE.getBaseCellWithValue_Value();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseColumnStructuredDataImpl <em>Base Column Structured Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseColumnStructuredDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseColumnStructuredData()
		 * @generated
		 */
		EClass BASE_COLUMN_STRUCTURED_DATA = eINSTANCE.getBaseColumnStructuredData();

		/**
		 * The meta object literal for the '<em><b>Rows</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_COLUMN_STRUCTURED_DATA__ROWS = eINSTANCE.getBaseColumnStructuredData_Rows();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_COLUMN_STRUCTURED_DATA__CUBE = eINSTANCE.getBaseColumnStructuredData_Cube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseRowDataImpl <em>Base Row Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.BaseRowDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getBaseRowData()
		 * @generated
		 */
		EClass BASE_ROW_DATA = eINSTANCE.getBaseRowData();

		/**
		 * The meta object literal for the '<em><b>Cells</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_ROW_DATA__CELLS = eINSTANCE.getBaseRowData_Cells();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataImpl <em>Input Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getInputData()
		 * @generated
		 */
		EClass INPUT_DATA = eINSTANCE.getInputData();

		/**
		 * The meta object literal for the '<em><b>Source Table Data</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INPUT_DATA__SOURCE_TABLE_DATA = eINSTANCE.getInputData_SourceTableData();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute INPUT_DATA__NAME = eINSTANCE.getInputData_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataModuleImpl <em>Input Data Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.InputDataModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.impl.Base_column_structured_dataPackageImpl#getInputDataModule()
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

	}

} //Base_column_structured_dataPackage
