/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicPackage;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsPackage;

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
 * This Package describes how the advanced Varaibles are used in column transformation logic.
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Advanced_variable_lineagefunctionsPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "advanced_variable_lineagefunctions";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/advanced_variable_lineagefunctions";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "advanced_variable_lineagefunctions";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Advanced_variable_lineagefunctionsPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl <em>Get Struct Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getGetStructColumnFunction()
	 * @generated
	 */
	int GET_STRUCT_COLUMN_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__NAME = Column_transformation_logicPackage.COLUMN_FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__CUBE = Column_transformation_logicPackage.COLUMN_FUNCTION__CUBE;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__USED_IN_SUBSETS = Column_transformation_logicPackage.COLUMN_FUNCTION__USED_IN_SUBSETS;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__VARIABLE = Column_transformation_logicPackage.COLUMN_FUNCTION__VARIABLE;

	/**
	 * The feature id for the '<em><b>Column In Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT = Column_transformation_logicPackage.COLUMN_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN = Column_transformation_logicPackage.COLUMN_FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Get Struct Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION_FEATURE_COUNT = Column_transformation_logicPackage.COLUMN_FUNCTION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Get Struct Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION_OPERATION_COUNT = Column_transformation_logicPackage.COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl <em>Resolved Struct Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getResolvedStructColumnParameter()
	 * @generated
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER = 1;

	/**
	 * The feature id for the '<em><b>Column Inside Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Resolved Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Resolved Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl <em>Speculative Struct Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getSpeculativeStructColumnParameter()
	 * @generated
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER = 2;

	/**
	 * The feature id for the '<em><b>Column Inside Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN = FunctionsPackage.PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Speculative Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Speculative Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction <em>Get Struct Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Get Struct Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction
	 * @generated
	 */
	EClass getGetStructColumnFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction#getColumnInStruct <em>Column In Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column In Struct</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction#getColumnInStruct()
	 * @see #getGetStructColumnFunction()
	 * @generated
	 */
	EReference getGetStructColumnFunction_ColumnInStruct();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.GetStructColumnFunction#getCubeColumn()
	 * @see #getGetStructColumnFunction()
	 * @generated
	 */
	EReference getGetStructColumnFunction_CubeColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter <em>Resolved Struct Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Resolved Struct Column Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter
	 * @generated
	 */
	EClass getResolvedStructColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column Inside Struct</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct()
	 * @see #getResolvedStructColumnParameter()
	 * @generated
	 */
	EReference getResolvedStructColumnParameter_ColumnInsideStruct();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn()
	 * @see #getResolvedStructColumnParameter()
	 * @generated
	 */
	EReference getResolvedStructColumnParameter_CubeColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter <em>Speculative Struct Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Speculative Struct Column Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter
	 * @generated
	 */
	EClass getSpeculativeStructColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column Inside Struct</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getColumnInsideStruct()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_ColumnInsideStruct();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getCube()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_Cube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getStructColumn <em>Struct Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Struct Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getStructColumn()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_StructColumn();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Advanced_variable_lineagefunctionsFactory getAdvanced_variable_lineagefunctionsFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl <em>Get Struct Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getGetStructColumnFunction()
		 * @generated
		 */
		EClass GET_STRUCT_COLUMN_FUNCTION = eINSTANCE.getGetStructColumnFunction();

		/**
		 * The meta object literal for the '<em><b>Column In Struct</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT = eINSTANCE.getGetStructColumnFunction_ColumnInStruct();

		/**
		 * The meta object literal for the '<em><b>Cube Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN = eINSTANCE.getGetStructColumnFunction_CubeColumn();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl <em>Resolved Struct Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getResolvedStructColumnParameter()
		 * @generated
		 */
		EClass RESOLVED_STRUCT_COLUMN_PARAMETER = eINSTANCE.getResolvedStructColumnParameter();

		/**
		 * The meta object literal for the '<em><b>Column Inside Struct</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT = eINSTANCE.getResolvedStructColumnParameter_ColumnInsideStruct();

		/**
		 * The meta object literal for the '<em><b>Cube Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN = eINSTANCE.getResolvedStructColumnParameter_CubeColumn();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl <em>Speculative Struct Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getSpeculativeStructColumnParameter()
		 * @generated
		 */
		EClass SPECULATIVE_STRUCT_COLUMN_PARAMETER = eINSTANCE.getSpeculativeStructColumnParameter();

		/**
		 * The meta object literal for the '<em><b>Column Inside Struct</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT = eINSTANCE.getSpeculativeStructColumnParameter_ColumnInsideStruct();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE = eINSTANCE.getSpeculativeStructColumnParameter_Cube();

		/**
		 * The meta object literal for the '<em><b>Struct Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN = eINSTANCE.getSpeculativeStructColumnParameter_StructColumn();

	}

} //Advanced_variable_lineagefunctionsPackage
