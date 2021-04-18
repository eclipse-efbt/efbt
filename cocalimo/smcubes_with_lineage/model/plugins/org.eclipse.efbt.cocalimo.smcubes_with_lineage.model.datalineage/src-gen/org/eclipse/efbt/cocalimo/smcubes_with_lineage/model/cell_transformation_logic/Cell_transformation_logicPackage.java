/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic;

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
 *  This package describes the DataLineageModel, DataLineageModels store the actaul lineage between calculated results, and input data, with lineage recorded between all the intermediate datasets which may have been calculated to get to the final results.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Cell_transformation_logicPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "cell_transformation_logic";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/cell_transformation_logic";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "cell_transformation_logic";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Cell_transformation_logicPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.FormulaCellImpl <em>Formula Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.FormulaCellImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl#getFormulaCell()
	 * @generated
	 */
	int FORMULA_CELL = 0;

	/**
	 * The feature id for the '<em><b>Cell ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FORMULA_CELL__CELL_ID = Column_structured_dataPackage.CELL__CELL_ID;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FORMULA_CELL__COLUMN = Column_structured_dataPackage.CELL__COLUMN;

	/**
	 * The feature id for the '<em><b>Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FORMULA_CELL__FUNCTION = Column_structured_dataPackage.CELL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FORMULA_CELL__VALUE = Column_structured_dataPackage.CELL_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Formula Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FORMULA_CELL_FEATURE_COUNT = Column_structured_dataPackage.CELL_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Formula Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FORMULA_CELL_OPERATION_COUNT = Column_structured_dataPackage.CELL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedCubeDataImpl <em>Derived Cube Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedCubeDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl#getDerivedCubeData()
	 * @generated
	 */
	int DERIVED_CUBE_DATA = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_DATA__NAME = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA__NAME;

	/**
	 * The feature id for the '<em><b>Rows</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_DATA__ROWS = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_DATA__CUBE = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Source Cube Data</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_DATA__SOURCE_CUBE_DATA = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Cube Creation Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_DATA__CUBE_CREATION_DESCRIPTION = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Derived Cube Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_DATA_FEATURE_COUNT = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Derived Cube Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_CUBE_DATA_OPERATION_COUNT = Column_structured_dataPackage.COLUMN_STRUCTURED_DATA_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedRowDataImpl <em>Derived Row Data</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedRowDataImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl#getDerivedRowData()
	 * @generated
	 */
	int DERIVED_ROW_DATA = 2;

	/**
	 * The feature id for the '<em><b>Row ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ROW_DATA__ROW_ID = Column_structured_dataPackage.ROW_DATA__ROW_ID;

	/**
	 * The feature id for the '<em><b>Formula Cells</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ROW_DATA__FORMULA_CELLS = Column_structured_dataPackage.ROW_DATA_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Special Derived Cells</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS = Column_structured_dataPackage.ROW_DATA_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Source Rows</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ROW_DATA__SOURCE_ROWS = Column_structured_dataPackage.ROW_DATA_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Derived Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ROW_DATA_FEATURE_COUNT = Column_structured_dataPackage.ROW_DATA_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Derived Row Data</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ROW_DATA_OPERATION_COUNT = Column_structured_dataPackage.ROW_DATA_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell <em>Formula Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Formula Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell
	 * @generated
	 */
	EClass getFormulaCell();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell#getFunction <em>Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell#getFunction()
	 * @see #getFormulaCell()
	 * @generated
	 */
	EReference getFormulaCell_Function();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.FormulaCell#getValue()
	 * @see #getFormulaCell()
	 * @generated
	 */
	EAttribute getFormulaCell_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData <em>Derived Cube Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Cube Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData
	 * @generated
	 */
	EClass getDerivedCubeData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getRows <em>Rows</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Rows</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getRows()
	 * @see #getDerivedCubeData()
	 * @generated
	 */
	EReference getDerivedCubeData_Rows();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCube()
	 * @see #getDerivedCubeData()
	 * @generated
	 */
	EReference getDerivedCubeData_Cube();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getSourceCubeData <em>Source Cube Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Source Cube Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getSourceCubeData()
	 * @see #getDerivedCubeData()
	 * @generated
	 */
	EReference getDerivedCubeData_SourceCubeData();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCubeCreationDescription <em>Cube Creation Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Cube Creation Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCubeCreationDescription()
	 * @see #getDerivedCubeData()
	 * @generated
	 */
	EAttribute getDerivedCubeData_CubeCreationDescription();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData <em>Derived Row Data</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Row Data</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData
	 * @generated
	 */
	EClass getDerivedRowData();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getFormulaCells <em>Formula Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Formula Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getFormulaCells()
	 * @see #getDerivedRowData()
	 * @generated
	 */
	EReference getDerivedRowData_FormulaCells();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getSpecialDerivedCells <em>Special Derived Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Special Derived Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getSpecialDerivedCells()
	 * @see #getDerivedRowData()
	 * @generated
	 */
	EReference getDerivedRowData_SpecialDerivedCells();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getSourceRows <em>Source Rows</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Source Rows</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData#getSourceRows()
	 * @see #getDerivedRowData()
	 * @generated
	 */
	EReference getDerivedRowData_SourceRows();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Cell_transformation_logicFactory getCell_transformation_logicFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.FormulaCellImpl <em>Formula Cell</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.FormulaCellImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl#getFormulaCell()
		 * @generated
		 */
		EClass FORMULA_CELL = eINSTANCE.getFormulaCell();

		/**
		 * The meta object literal for the '<em><b>Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FORMULA_CELL__FUNCTION = eINSTANCE.getFormulaCell_Function();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FORMULA_CELL__VALUE = eINSTANCE.getFormulaCell_Value();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedCubeDataImpl <em>Derived Cube Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedCubeDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl#getDerivedCubeData()
		 * @generated
		 */
		EClass DERIVED_CUBE_DATA = eINSTANCE.getDerivedCubeData();

		/**
		 * The meta object literal for the '<em><b>Rows</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_CUBE_DATA__ROWS = eINSTANCE.getDerivedCubeData_Rows();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_CUBE_DATA__CUBE = eINSTANCE.getDerivedCubeData_Cube();

		/**
		 * The meta object literal for the '<em><b>Source Cube Data</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_CUBE_DATA__SOURCE_CUBE_DATA = eINSTANCE.getDerivedCubeData_SourceCubeData();

		/**
		 * The meta object literal for the '<em><b>Cube Creation Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DERIVED_CUBE_DATA__CUBE_CREATION_DESCRIPTION = eINSTANCE.getDerivedCubeData_CubeCreationDescription();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedRowDataImpl <em>Derived Row Data</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.DerivedRowDataImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicPackageImpl#getDerivedRowData()
		 * @generated
		 */
		EClass DERIVED_ROW_DATA = eINSTANCE.getDerivedRowData();

		/**
		 * The meta object literal for the '<em><b>Formula Cells</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ROW_DATA__FORMULA_CELLS = eINSTANCE.getDerivedRowData_FormulaCells();

		/**
		 * The meta object literal for the '<em><b>Special Derived Cells</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ROW_DATA__SPECIAL_DERIVED_CELLS = eINSTANCE.getDerivedRowData_SpecialDerivedCells();

		/**
		 * The meta object literal for the '<em><b>Source Rows</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ROW_DATA__SOURCE_ROWS = eINSTANCE.getDerivedRowData_SourceRows();

	}

} //Cell_transformation_logicPackage
