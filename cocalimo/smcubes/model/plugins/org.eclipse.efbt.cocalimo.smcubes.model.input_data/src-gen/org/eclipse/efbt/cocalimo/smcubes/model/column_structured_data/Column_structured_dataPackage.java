/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data;

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
 *  This package describes the storage of  data which can be represented in a column structure,  This includes dat for cubes and smcubes since we can represent cube structures as a set of cubse structure items which can be represesnted as columns
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Column_structured_dataFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Column_structured_dataPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "column_structured_data";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/column_structured_data";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "column_structured_data";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Column_structured_dataPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.CellImpl <em>Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.CellImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getCell()
	 * @generated
	 */
	int CELL = 0;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataImpl <em>Column Structured Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getColumnStructuredData()
	 * @generated
	 */
	int COLUMN_STRUCTURED_DATA = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA__NAME = 0;

	/**
	 * The number of structural features of the '<em>Column Structured Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Column Structured Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataModuleImpl <em>Column Structured Data Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getColumnStructuredDataModule()
	 * @generated
	 */
	int COLUMN_STRUCTURED_DATA_MODULE = 2;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Cube Data</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Column Structured Data Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Column Structured Data Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_STRUCTURED_DATA_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.RowDataImpl <em>Row Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.RowDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getRowData()
	 * @generated
	 */
	int ROW_DATA = 3;

	/**
	 * The feature id for the '<em><b>Row ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_DATA__ROW_ID = 0;

	/**
	 * The number of structural features of the '<em>Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_DATA_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_DATA_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell <em>Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell
	 * @generated
	 */
	EClass getCell();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell#getCellID <em>Cell ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cell ID</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell#getCellID()
	 * @see #getCell()
	 * @generated
	 */
	EAttribute getCell_CellID();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell#getColumn <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.Cell#getColumn()
	 * @see #getCell()
	 * @generated
	 */
	EReference getCell_Column();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData <em>Column Structured Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Structured Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData
	 * @generated
	 */
	EClass getColumnStructuredData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData#getName()
	 * @see #getColumnStructuredData()
	 * @generated
	 */
	EAttribute getColumnStructuredData_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredDataModule <em>Column Structured Data Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Structured Data Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredDataModule
	 * @generated
	 */
	EClass getColumnStructuredDataModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredDataModule#getCubeData <em>Cube Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredDataModule#getCubeData()
	 * @see #getColumnStructuredDataModule()
	 * @generated
	 */
	EReference getColumnStructuredDataModule_CubeData();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData <em>Row Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Row Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData
	 * @generated
	 */
	EClass getRowData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData#getRowID <em>Row ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Row ID</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.RowData#getRowID()
	 * @see #getRowData()
	 * @generated
	 */
	EAttribute getRowData_RowID();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Column_structured_dataFactory getColumn_structured_dataFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.CellImpl <em>Cell</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.CellImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getCell()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataImpl <em>Column Structured Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getColumnStructuredData()
		 * @generated
		 */
		EClass COLUMN_STRUCTURED_DATA = eINSTANCE.getColumnStructuredData();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute COLUMN_STRUCTURED_DATA__NAME = eINSTANCE.getColumnStructuredData_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataModuleImpl <em>Column Structured Data Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.ColumnStructuredDataModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getColumnStructuredDataModule()
		 * @generated
		 */
		EClass COLUMN_STRUCTURED_DATA_MODULE = eINSTANCE.getColumnStructuredDataModule();

		/**
		 * The meta object literal for the '<em><b>Cube Data</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COLUMN_STRUCTURED_DATA_MODULE__CUBE_DATA = eINSTANCE.getColumnStructuredDataModule_CubeData();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.RowDataImpl <em>Row Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.RowDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.impl.Column_structured_dataPackageImpl#getRowData()
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

	}

} //Column_structured_dataPackage
