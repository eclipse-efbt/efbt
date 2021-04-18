/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

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
 * This package describes the logic to calculate individual variables associated with DerivedCubes. 
 * As DerivedCubes represetn the result of a transformation, the transformation and the source cubes of the transformation will determine the varaibles that are associated with the DerivedCube.
 * If we use an analogy to tables we can consider these variables as columns on the DerivedCube.
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicFactory
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
	String eNS_URI = "http://www.eclipse.org/efbt/column_transformation_logic";

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
	Column_transformation_logicPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.CubeColumnImpl <em>Cube Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.CubeColumnImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getCubeColumn()
	 * @generated
	 */
	int CUBE_COLUMN = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__NAME = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__CUBE = 1;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__USED_IN_SUBSETS = 2;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__VARIABLE = 3;

	/**
	 * The number of structural features of the '<em>Cube Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Cube Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunction()
	 * @generated
	 */
	int COLUMN_FUNCTION = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__NAME = CUBE_COLUMN__NAME;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__CUBE = CUBE_COLUMN__CUBE;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__USED_IN_SUBSETS = CUBE_COLUMN__USED_IN_SUBSETS;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__VARIABLE = CUBE_COLUMN__VARIABLE;

	/**
	 * The number of structural features of the '<em>Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_FEATURE_COUNT = CUBE_COLUMN_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_OPERATION_COUNT = CUBE_COLUMN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.AggregateColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getAggregateColumnFunction()
	 * @generated
	 */
	int AGGREGATE_COLUMN_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__NAME = COLUMN_FUNCTION__NAME;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.BasicColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getBasicColumnFunction()
	 * @generated
	 */
	int BASIC_COLUMN_FUNCTION = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__NAME = COLUMN_FUNCTION__NAME;

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
	 * The feature id for the '<em><b>Basic Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__BASIC_FUNCTION = COLUMN_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION_FEATURE_COUNT = COLUMN_FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Basic Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION_OPERATION_COUNT = COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionGroupImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunctionGroup()
	 * @generated
	 */
	int COLUMN_FUNCTION_GROUP = 3;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ResolvedCubeColumnParameterImpl <em>Resolved Cube Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ResolvedCubeColumnParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getResolvedCubeColumnParameter()
	 * @generated
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER = 4;

	/**
	 * The feature id for the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Resolved Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Resolved Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER = 6;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Speculative Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Speculative Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.AggregateColumnFunction <em>Aggregate Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.AggregateColumnFunction
	 * @generated
	 */
	EClass getAggregateColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.AggregateColumnFunction#getAggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Aggregate Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.AggregateColumnFunction#getAggregateFunction()
	 * @see #getAggregateColumnFunction()
	 * @generated
	 */
	EReference getAggregateColumnFunction_AggregateFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.BasicColumnFunction <em>Basic Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.BasicColumnFunction
	 * @generated
	 */
	EClass getBasicColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.BasicColumnFunction#getBasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Basic Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.BasicColumnFunction#getBasicFunction()
	 * @see #getBasicColumnFunction()
	 * @generated
	 */
	EReference getBasicColumnFunction_BasicFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunction <em>Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunction
	 * @generated
	 */
	EClass getColumnFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunctionGroup <em>Column Function Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function Group</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunctionGroup
	 * @generated
	 */
	EClass getColumnFunctionGroup();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunctionGroup#getColumnFunctions <em>Column Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Column Functions</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunctionGroup#getColumnFunctions()
	 * @see #getColumnFunctionGroup()
	 * @generated
	 */
	EReference getColumnFunctionGroup_ColumnFunctions();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ResolvedCubeColumnParameter <em>Resolved Cube Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Resolved Cube Column Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ResolvedCubeColumnParameter
	 * @generated
	 */
	EClass getResolvedCubeColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ResolvedCubeColumnParameter#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ResolvedCubeColumnParameter#getCubeColumn()
	 * @see #getResolvedCubeColumnParameter()
	 * @generated
	 */
	EReference getResolvedCubeColumnParameter_CubeColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn
	 * @generated
	 */
	EClass getCubeColumn();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#getName()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EAttribute getCubeColumn_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#getCube()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EReference getCubeColumn_Cube();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#isUsedInSubsets <em>Used In Subsets</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Used In Subsets</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#isUsedInSubsets()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EAttribute getCubeColumn_UsedInSubsets();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn#getVariable()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EReference getCubeColumn_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.SpeculativeCubeColumnParameter <em>Speculative Cube Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Speculative Cube Column Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.SpeculativeCubeColumnParameter
	 * @generated
	 */
	EClass getSpeculativeCubeColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.SpeculativeCubeColumnParameter#getColumn <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.SpeculativeCubeColumnParameter#getColumn()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Column();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.SpeculativeCubeColumnParameter#getCube()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Cube();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.AggregateColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getAggregateColumnFunction()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.BasicColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getBasicColumnFunction()
		 * @generated
		 */
		EClass BASIC_COLUMN_FUNCTION = eINSTANCE.getBasicColumnFunction();

		/**
		 * The meta object literal for the '<em><b>Basic Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_COLUMN_FUNCTION__BASIC_FUNCTION = eINSTANCE.getBasicColumnFunction_BasicFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunction()
		 * @generated
		 */
		EClass COLUMN_FUNCTION = eINSTANCE.getColumnFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ColumnFunctionGroupImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunctionGroup()
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

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ResolvedCubeColumnParameterImpl <em>Resolved Cube Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.ResolvedCubeColumnParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getResolvedCubeColumnParameter()
		 * @generated
		 */
		EClass RESOLVED_CUBE_COLUMN_PARAMETER = eINSTANCE.getResolvedCubeColumnParameter();

		/**
		 * The meta object literal for the '<em><b>Cube Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN = eINSTANCE.getResolvedCubeColumnParameter_CubeColumn();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.CubeColumnImpl <em>Cube Column</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.CubeColumnImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getCubeColumn()
		 * @generated
		 */
		EClass CUBE_COLUMN = eINSTANCE.getCubeColumn();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_COLUMN__NAME = eINSTANCE.getCubeColumn_Name();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_COLUMN__CUBE = eINSTANCE.getCubeColumn_Cube();

		/**
		 * The meta object literal for the '<em><b>Used In Subsets</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_COLUMN__USED_IN_SUBSETS = eINSTANCE.getCubeColumn_UsedInSubsets();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_COLUMN__VARIABLE = eINSTANCE.getCubeColumn_Variable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicPackageImpl#getSpeculativeCubeColumnParameter()
		 * @generated
		 */
		EClass SPECULATIVE_CUBE_COLUMN_PARAMETER = eINSTANCE.getSpeculativeCubeColumnParameter();

		/**
		 * The meta object literal for the '<em><b>Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN = eINSTANCE.getSpeculativeCubeColumnParameter_Column();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE = eINSTANCE.getSpeculativeCubeColumnParameter_Cube();

	}

} //Column_transformation_logicPackage
