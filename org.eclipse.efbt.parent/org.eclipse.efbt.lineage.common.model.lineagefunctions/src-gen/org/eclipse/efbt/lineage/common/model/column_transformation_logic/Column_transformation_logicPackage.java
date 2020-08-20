/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage;

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
 * This package describes the logic to calculate individual columns 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Column_transformation_logicPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "column_transformation_logic";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/column_transformation_logic";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "column_transformation_logic";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Column_transformation_logicPackage eINSTANCE = org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionImpl
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunction()
	 * @generated
	 */
	int COLUMN_FUNCTION = 2;

	/**
	 * The feature id for the '<em><b>Column ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__COLUMN_ID = LineagefunctionsPackage.CUBE_COLUMN__COLUMN_ID;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__CUBE = LineagefunctionsPackage.CUBE_COLUMN__CUBE;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__USED_IN_SUBSETS = LineagefunctionsPackage.CUBE_COLUMN__USED_IN_SUBSETS;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__VARIABLE = LineagefunctionsPackage.CUBE_COLUMN__VARIABLE;

	/**
	 * The number of structural features of the '<em>Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_FEATURE_COUNT = LineagefunctionsPackage.CUBE_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_OPERATION_COUNT = LineagefunctionsPackage.CUBE_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.AggregateColumnFunctionImpl
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getAggregateColumnFunction()
	 * @generated
	 */
	int AGGREGATE_COLUMN_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>Column ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__COLUMN_ID = COLUMN_FUNCTION__COLUMN_ID;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__CUBE = COLUMN_FUNCTION__CUBE;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__USED_IN_SUBSETS = COLUMN_FUNCTION__USED_IN_SUBSETS;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__VARIABLE = COLUMN_FUNCTION__VARIABLE;

	/**
	 * The feature id for the '<em><b>Aggregate Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION = COLUMN_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Aggregate Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION_FEATURE_COUNT = COLUMN_FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Aggregate Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION_OPERATION_COUNT = COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.BasicColumnFunctionImpl
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getBasicColumnFunction()
	 * @generated
	 */
	int BASIC_COLUMN_FUNCTION = 1;

	/**
	 * The feature id for the '<em><b>Column ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__COLUMN_ID = COLUMN_FUNCTION__COLUMN_ID;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__CUBE = COLUMN_FUNCTION__CUBE;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__USED_IN_SUBSETS = COLUMN_FUNCTION__USED_IN_SUBSETS;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__VARIABLE = COLUMN_FUNCTION__VARIABLE;

	/**
	 * The number of structural features of the '<em>Basic Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION_FEATURE_COUNT = COLUMN_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Basic Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION_OPERATION_COUNT = COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.StandardBasicColumnFunctionImpl <em>Standard Basic Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.StandardBasicColumnFunctionImpl
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getStandardBasicColumnFunction()
	 * @generated
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION = 3;

	/**
	 * The feature id for the '<em><b>Column ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION__COLUMN_ID = BASIC_COLUMN_FUNCTION__COLUMN_ID;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION__CUBE = BASIC_COLUMN_FUNCTION__CUBE;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION__USED_IN_SUBSETS = BASIC_COLUMN_FUNCTION__USED_IN_SUBSETS;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION__VARIABLE = BASIC_COLUMN_FUNCTION__VARIABLE;

	/**
	 * The feature id for the '<em><b>Basic Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION__BASIC_FUNCTION = BASIC_COLUMN_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Standard Basic Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION_FEATURE_COUNT = BASIC_COLUMN_FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Standard Basic Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STANDARD_BASIC_COLUMN_FUNCTION_OPERATION_COUNT = BASIC_COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionGroupImpl
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunctionGroup()
	 * @generated
	 */
	int COLUMN_FUNCTION_GROUP = 4;

	/**
	 * The feature id for the '<em><b>Column Functions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS = 0;

	/**
	 * The number of structural features of the '<em>Column Function Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_GROUP_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Column Function Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_GROUP_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction <em>Aggregate Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Column Function</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction
	 * @generated
	 */
	EClass getAggregateColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction#getAggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Aggregate Function</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction#getAggregateFunction()
	 * @see #getAggregateColumnFunction()
	 * @generated
	 */
	EReference getAggregateColumnFunction_AggregateFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction <em>Basic Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Column Function</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction
	 * @generated
	 */
	EClass getBasicColumnFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunction <em>Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunction
	 * @generated
	 */
	EClass getColumnFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.StandardBasicColumnFunction <em>Standard Basic Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Standard Basic Column Function</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.StandardBasicColumnFunction
	 * @generated
	 */
	EClass getStandardBasicColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.StandardBasicColumnFunction#getBasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Basic Function</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.StandardBasicColumnFunction#getBasicFunction()
	 * @see #getStandardBasicColumnFunction()
	 * @generated
	 */
	EReference getStandardBasicColumnFunction_BasicFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunctionGroup <em>Column Function Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function Group</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunctionGroup
	 * @generated
	 */
	EClass getColumnFunctionGroup();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunctionGroup#getColumnFunctions <em>Column Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Column Functions</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunctionGroup#getColumnFunctions()
	 * @see #getColumnFunctionGroup()
	 * @generated
	 */
	EReference getColumnFunctionGroup_ColumnFunctions();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Column_transformation_logicFactory getColumn_transformation_logicFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.AggregateColumnFunctionImpl
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getAggregateColumnFunction()
		 * @generated
		 */
		EClass AGGREGATE_COLUMN_FUNCTION = eINSTANCE.getAggregateColumnFunction();

		/**
		 * The meta object literal for the '<em><b>Aggregate Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_COLUMN_FUNCTION__AGGREGATE_FUNCTION = eINSTANCE.getAggregateColumnFunction_AggregateFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.BasicColumnFunctionImpl
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getBasicColumnFunction()
		 * @generated
		 */
		EClass BASIC_COLUMN_FUNCTION = eINSTANCE.getBasicColumnFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionImpl
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunction()
		 * @generated
		 */
		EClass COLUMN_FUNCTION = eINSTANCE.getColumnFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.StandardBasicColumnFunctionImpl <em>Standard Basic Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.StandardBasicColumnFunctionImpl
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getStandardBasicColumnFunction()
		 * @generated
		 */
		EClass STANDARD_BASIC_COLUMN_FUNCTION = eINSTANCE.getStandardBasicColumnFunction();

		/**
		 * The meta object literal for the '<em><b>Basic Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference STANDARD_BASIC_COLUMN_FUNCTION__BASIC_FUNCTION = eINSTANCE.getStandardBasicColumnFunction_BasicFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.ColumnFunctionGroupImpl
		 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunctionGroup()
		 * @generated
		 */
		EClass COLUMN_FUNCTION_GROUP = eINSTANCE.getColumnFunctionGroup();

		/**
		 * The meta object literal for the '<em><b>Column Functions</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COLUMN_FUNCTION_GROUP__COLUMN_FUNCTIONS = eINSTANCE.getColumnFunctionGroup_ColumnFunctions();

	}

} //Column_transformation_logicPackage
