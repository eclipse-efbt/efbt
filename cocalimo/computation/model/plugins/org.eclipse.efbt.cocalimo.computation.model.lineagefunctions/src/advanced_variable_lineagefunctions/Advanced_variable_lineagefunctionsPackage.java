/**
 */
package advanced_variable_lineagefunctions;

import column_transformation_logic.Column_transformation_logicPackage;

import functions.FunctionsPackage;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

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
 * @see advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Advanced_variable_lineagefunctionsPackage extends EPackage {
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
	Advanced_variable_lineagefunctionsPackage eINSTANCE = advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl.init();

	/**
	 * The meta object id for the '{@link advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl <em>Get Struct Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl
	 * @see advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getGetStructColumnFunction()
	 * @generated
	 */
	int GET_STRUCT_COLUMN_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__EANNOTATIONS = Column_transformation_logicPackage.COLUMN_FUNCTION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__NAME = Column_transformation_logicPackage.COLUMN_FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__ORDERED = Column_transformation_logicPackage.COLUMN_FUNCTION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__UNIQUE = Column_transformation_logicPackage.COLUMN_FUNCTION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__LOWER_BOUND = Column_transformation_logicPackage.COLUMN_FUNCTION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__UPPER_BOUND = Column_transformation_logicPackage.COLUMN_FUNCTION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__MANY = Column_transformation_logicPackage.COLUMN_FUNCTION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__REQUIRED = Column_transformation_logicPackage.COLUMN_FUNCTION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__ETYPE = Column_transformation_logicPackage.COLUMN_FUNCTION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__EGENERIC_TYPE = Column_transformation_logicPackage.COLUMN_FUNCTION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__CHANGEABLE = Column_transformation_logicPackage.COLUMN_FUNCTION__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__VOLATILE = Column_transformation_logicPackage.COLUMN_FUNCTION__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__TRANSIENT = Column_transformation_logicPackage.COLUMN_FUNCTION__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__DEFAULT_VALUE_LITERAL = Column_transformation_logicPackage.COLUMN_FUNCTION__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__DEFAULT_VALUE = Column_transformation_logicPackage.COLUMN_FUNCTION__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__UNSETTABLE = Column_transformation_logicPackage.COLUMN_FUNCTION__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__DERIVED = Column_transformation_logicPackage.COLUMN_FUNCTION__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__ECONTAINING_CLASS = Column_transformation_logicPackage.COLUMN_FUNCTION__ECONTAINING_CLASS;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION___GET_EANNOTATION__STRING = Column_transformation_logicPackage.COLUMN_FUNCTION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION___GET_FEATURE_ID = Column_transformation_logicPackage.COLUMN_FUNCTION___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION___GET_CONTAINER_CLASS = Column_transformation_logicPackage.COLUMN_FUNCTION___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Get Struct Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION_OPERATION_COUNT = Column_transformation_logicPackage.COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl <em>Resolved Struct Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl
	 * @see advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getResolvedStructColumnParameter()
	 * @generated
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER = 1;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__EANNOTATIONS = EcorePackage.EPARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__NAME = EcorePackage.EPARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__ORDERED = EcorePackage.EPARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__UNIQUE = EcorePackage.EPARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__LOWER_BOUND = EcorePackage.EPARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__UPPER_BOUND = EcorePackage.EPARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__MANY = EcorePackage.EPARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__REQUIRED = EcorePackage.EPARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__ETYPE = EcorePackage.EPARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__EGENERIC_TYPE = EcorePackage.EPARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__EOPERATION = EcorePackage.EPARAMETER__EOPERATION;

	/**
	 * The feature id for the '<em><b>Column Inside Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT = EcorePackage.EPARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN = EcorePackage.EPARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Resolved Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER_FEATURE_COUNT = EcorePackage.EPARAMETER_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER___GET_EANNOTATION__STRING = EcorePackage.EPARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Resolved Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_STRUCT_COLUMN_PARAMETER_OPERATION_COUNT = EcorePackage.EPARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl <em>Speculative Struct Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl
	 * @see advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getSpeculativeStructColumnParameter()
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
	 * Returns the meta object for class '{@link advanced_variable_lineagefunctions.GetStructColumnFunction <em>Get Struct Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Get Struct Column Function</em>'.
	 * @see advanced_variable_lineagefunctions.GetStructColumnFunction
	 * @generated
	 */
	EClass getGetStructColumnFunction();

	/**
	 * Returns the meta object for the reference '{@link advanced_variable_lineagefunctions.GetStructColumnFunction#getColumnInStruct <em>Column In Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column In Struct</em>'.
	 * @see advanced_variable_lineagefunctions.GetStructColumnFunction#getColumnInStruct()
	 * @see #getGetStructColumnFunction()
	 * @generated
	 */
	EReference getGetStructColumnFunction_ColumnInStruct();

	/**
	 * Returns the meta object for the reference '{@link advanced_variable_lineagefunctions.GetStructColumnFunction#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see advanced_variable_lineagefunctions.GetStructColumnFunction#getCubeColumn()
	 * @see #getGetStructColumnFunction()
	 * @generated
	 */
	EReference getGetStructColumnFunction_CubeColumn();

	/**
	 * Returns the meta object for class '{@link advanced_variable_lineagefunctions.ResolvedStructColumnParameter <em>Resolved Struct Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Resolved Struct Column Parameter</em>'.
	 * @see advanced_variable_lineagefunctions.ResolvedStructColumnParameter
	 * @generated
	 */
	EClass getResolvedStructColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column Inside Struct</em>'.
	 * @see advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getColumnInsideStruct()
	 * @see #getResolvedStructColumnParameter()
	 * @generated
	 */
	EReference getResolvedStructColumnParameter_ColumnInsideStruct();

	/**
	 * Returns the meta object for the reference '{@link advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see advanced_variable_lineagefunctions.ResolvedStructColumnParameter#getCubeColumn()
	 * @see #getResolvedStructColumnParameter()
	 * @generated
	 */
	EReference getResolvedStructColumnParameter_CubeColumn();

	/**
	 * Returns the meta object for class '{@link advanced_variable_lineagefunctions.SpeculativeStructColumnParameter <em>Speculative Struct Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Speculative Struct Column Parameter</em>'.
	 * @see advanced_variable_lineagefunctions.SpeculativeStructColumnParameter
	 * @generated
	 */
	EClass getSpeculativeStructColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column Inside Struct</em>'.
	 * @see advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getColumnInsideStruct()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_ColumnInsideStruct();

	/**
	 * Returns the meta object for the reference '{@link advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getCube()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_Cube();

	/**
	 * Returns the meta object for the reference '{@link advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getStructColumn <em>Struct Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Struct Column</em>'.
	 * @see advanced_variable_lineagefunctions.SpeculativeStructColumnParameter#getStructColumn()
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
	interface Literals {
		/**
		 * The meta object literal for the '{@link advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl <em>Get Struct Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see advanced_variable_lineagefunctions.impl.GetStructColumnFunctionImpl
		 * @see advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getGetStructColumnFunction()
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
		 * The meta object literal for the '{@link advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl <em>Resolved Struct Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl
		 * @see advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getResolvedStructColumnParameter()
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
		 * The meta object literal for the '{@link advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl <em>Speculative Struct Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see advanced_variable_lineagefunctions.impl.SpeculativeStructColumnParameterImpl
		 * @see advanced_variable_lineagefunctions.impl.Advanced_variable_lineagefunctionsPackageImpl#getSpeculativeStructColumnParameter()
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
