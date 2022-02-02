/**
 */
package column_transformation_logic;

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
 * This package describes the logic to calculate individual variables associated with DerivedCubes. 
 * As DerivedCubes represetn the result of a transformation, the transformation and the source cubes of the transformation will determine the varaibles that are associated with the DerivedCube.
 * If we use an analogy to tables we can consider these variables as columns on the DerivedCube.
 * <!-- end-model-doc -->
 * @see column_transformation_logic.Column_transformation_logicFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Column_transformation_logicPackage extends EPackage {
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
	Column_transformation_logicPackage eINSTANCE = column_transformation_logic.impl.Column_transformation_logicPackageImpl.init();

	/**
	 * The meta object id for the '{@link column_transformation_logic.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see column_transformation_logic.impl.ColumnFunctionImpl
	 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunction()
	 * @generated
	 */
	int COLUMN_FUNCTION = 2;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__EANNOTATIONS = EcorePackage.ESTRUCTURAL_FEATURE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__NAME = EcorePackage.ESTRUCTURAL_FEATURE__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__ORDERED = EcorePackage.ESTRUCTURAL_FEATURE__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__UNIQUE = EcorePackage.ESTRUCTURAL_FEATURE__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__LOWER_BOUND = EcorePackage.ESTRUCTURAL_FEATURE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__UPPER_BOUND = EcorePackage.ESTRUCTURAL_FEATURE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__MANY = EcorePackage.ESTRUCTURAL_FEATURE__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__REQUIRED = EcorePackage.ESTRUCTURAL_FEATURE__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__ETYPE = EcorePackage.ESTRUCTURAL_FEATURE__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__EGENERIC_TYPE = EcorePackage.ESTRUCTURAL_FEATURE__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__CHANGEABLE = EcorePackage.ESTRUCTURAL_FEATURE__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__VOLATILE = EcorePackage.ESTRUCTURAL_FEATURE__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__TRANSIENT = EcorePackage.ESTRUCTURAL_FEATURE__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__DEFAULT_VALUE_LITERAL = EcorePackage.ESTRUCTURAL_FEATURE__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__DEFAULT_VALUE = EcorePackage.ESTRUCTURAL_FEATURE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__UNSETTABLE = EcorePackage.ESTRUCTURAL_FEATURE__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__DERIVED = EcorePackage.ESTRUCTURAL_FEATURE__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION__ECONTAINING_CLASS = EcorePackage.ESTRUCTURAL_FEATURE__ECONTAINING_CLASS;

	/**
	 * The number of structural features of the '<em>Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_FEATURE_COUNT = EcorePackage.ESTRUCTURAL_FEATURE_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION___GET_EANNOTATION__STRING = EcorePackage.ESTRUCTURAL_FEATURE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION___GET_FEATURE_ID = EcorePackage.ESTRUCTURAL_FEATURE___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION___GET_CONTAINER_CLASS = EcorePackage.ESTRUCTURAL_FEATURE___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COLUMN_FUNCTION_OPERATION_COUNT = EcorePackage.ESTRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link column_transformation_logic.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see column_transformation_logic.impl.AggregateColumnFunctionImpl
	 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getAggregateColumnFunction()
	 * @generated
	 */
	int AGGREGATE_COLUMN_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__EANNOTATIONS = COLUMN_FUNCTION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__NAME = COLUMN_FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__ORDERED = COLUMN_FUNCTION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__UNIQUE = COLUMN_FUNCTION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__LOWER_BOUND = COLUMN_FUNCTION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__UPPER_BOUND = COLUMN_FUNCTION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__MANY = COLUMN_FUNCTION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__REQUIRED = COLUMN_FUNCTION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__ETYPE = COLUMN_FUNCTION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__EGENERIC_TYPE = COLUMN_FUNCTION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__CHANGEABLE = COLUMN_FUNCTION__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__VOLATILE = COLUMN_FUNCTION__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__TRANSIENT = COLUMN_FUNCTION__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__DEFAULT_VALUE_LITERAL = COLUMN_FUNCTION__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__DEFAULT_VALUE = COLUMN_FUNCTION__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__UNSETTABLE = COLUMN_FUNCTION__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__DERIVED = COLUMN_FUNCTION__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION__ECONTAINING_CLASS = COLUMN_FUNCTION__ECONTAINING_CLASS;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION___GET_EANNOTATION__STRING = COLUMN_FUNCTION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION___GET_FEATURE_ID = COLUMN_FUNCTION___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION___GET_CONTAINER_CLASS = COLUMN_FUNCTION___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Aggregate Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_COLUMN_FUNCTION_OPERATION_COUNT = COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link column_transformation_logic.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see column_transformation_logic.impl.BasicColumnFunctionImpl
	 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getBasicColumnFunction()
	 * @generated
	 */
	int BASIC_COLUMN_FUNCTION = 1;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__EANNOTATIONS = COLUMN_FUNCTION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__NAME = COLUMN_FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__ORDERED = COLUMN_FUNCTION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__UNIQUE = COLUMN_FUNCTION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__LOWER_BOUND = COLUMN_FUNCTION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__UPPER_BOUND = COLUMN_FUNCTION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__MANY = COLUMN_FUNCTION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__REQUIRED = COLUMN_FUNCTION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__ETYPE = COLUMN_FUNCTION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__EGENERIC_TYPE = COLUMN_FUNCTION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__CHANGEABLE = COLUMN_FUNCTION__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__VOLATILE = COLUMN_FUNCTION__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__TRANSIENT = COLUMN_FUNCTION__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__DEFAULT_VALUE_LITERAL = COLUMN_FUNCTION__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__DEFAULT_VALUE = COLUMN_FUNCTION__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__UNSETTABLE = COLUMN_FUNCTION__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__DERIVED = COLUMN_FUNCTION__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION__ECONTAINING_CLASS = COLUMN_FUNCTION__ECONTAINING_CLASS;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION___GET_EANNOTATION__STRING = COLUMN_FUNCTION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION___GET_FEATURE_ID = COLUMN_FUNCTION___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION___GET_CONTAINER_CLASS = COLUMN_FUNCTION___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Basic Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_COLUMN_FUNCTION_OPERATION_COUNT = COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link column_transformation_logic.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see column_transformation_logic.impl.ColumnFunctionGroupImpl
	 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunctionGroup()
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
	 * The meta object id for the '{@link column_transformation_logic.impl.ResolvedCubeColumnParameterImpl <em>Resolved Cube Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see column_transformation_logic.impl.ResolvedCubeColumnParameterImpl
	 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getResolvedCubeColumnParameter()
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
	 * The meta object id for the '{@link column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl
	 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER = 5;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

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
	 * Returns the meta object for class '{@link column_transformation_logic.AggregateColumnFunction <em>Aggregate Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Column Function</em>'.
	 * @see column_transformation_logic.AggregateColumnFunction
	 * @generated
	 */
	EClass getAggregateColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link column_transformation_logic.AggregateColumnFunction#getAggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Aggregate Function</em>'.
	 * @see column_transformation_logic.AggregateColumnFunction#getAggregateFunction()
	 * @see #getAggregateColumnFunction()
	 * @generated
	 */
	EReference getAggregateColumnFunction_AggregateFunction();

	/**
	 * Returns the meta object for class '{@link column_transformation_logic.BasicColumnFunction <em>Basic Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Column Function</em>'.
	 * @see column_transformation_logic.BasicColumnFunction
	 * @generated
	 */
	EClass getBasicColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link column_transformation_logic.BasicColumnFunction#getBasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Basic Function</em>'.
	 * @see column_transformation_logic.BasicColumnFunction#getBasicFunction()
	 * @see #getBasicColumnFunction()
	 * @generated
	 */
	EReference getBasicColumnFunction_BasicFunction();

	/**
	 * Returns the meta object for class '{@link column_transformation_logic.ColumnFunction <em>Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function</em>'.
	 * @see column_transformation_logic.ColumnFunction
	 * @generated
	 */
	EClass getColumnFunction();

	/**
	 * Returns the meta object for class '{@link column_transformation_logic.ColumnFunctionGroup <em>Column Function Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function Group</em>'.
	 * @see column_transformation_logic.ColumnFunctionGroup
	 * @generated
	 */
	EClass getColumnFunctionGroup();

	/**
	 * Returns the meta object for the containment reference list '{@link column_transformation_logic.ColumnFunctionGroup#getColumnFunctions <em>Column Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Column Functions</em>'.
	 * @see column_transformation_logic.ColumnFunctionGroup#getColumnFunctions()
	 * @see #getColumnFunctionGroup()
	 * @generated
	 */
	EReference getColumnFunctionGroup_ColumnFunctions();

	/**
	 * Returns the meta object for class '{@link column_transformation_logic.ResolvedCubeColumnParameter <em>Resolved Cube Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Resolved Cube Column Parameter</em>'.
	 * @see column_transformation_logic.ResolvedCubeColumnParameter
	 * @generated
	 */
	EClass getResolvedCubeColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link column_transformation_logic.ResolvedCubeColumnParameter#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see column_transformation_logic.ResolvedCubeColumnParameter#getCubeColumn()
	 * @see #getResolvedCubeColumnParameter()
	 * @generated
	 */
	EReference getResolvedCubeColumnParameter_CubeColumn();

	/**
	 * Returns the meta object for class '{@link column_transformation_logic.SpeculativeCubeColumnParameter <em>Speculative Cube Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Speculative Cube Column Parameter</em>'.
	 * @see column_transformation_logic.SpeculativeCubeColumnParameter
	 * @generated
	 */
	EClass getSpeculativeCubeColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link column_transformation_logic.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see column_transformation_logic.SpeculativeCubeColumnParameter#getCube()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Cube();

	/**
	 * Returns the meta object for the reference '{@link column_transformation_logic.SpeculativeCubeColumnParameter#getAttribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute</em>'.
	 * @see column_transformation_logic.SpeculativeCubeColumnParameter#getAttribute()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Attribute();

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
	interface Literals {
		/**
		 * The meta object literal for the '{@link column_transformation_logic.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see column_transformation_logic.impl.AggregateColumnFunctionImpl
		 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getAggregateColumnFunction()
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
		 * The meta object literal for the '{@link column_transformation_logic.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see column_transformation_logic.impl.BasicColumnFunctionImpl
		 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getBasicColumnFunction()
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
		 * The meta object literal for the '{@link column_transformation_logic.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see column_transformation_logic.impl.ColumnFunctionImpl
		 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunction()
		 * @generated
		 */
		EClass COLUMN_FUNCTION = eINSTANCE.getColumnFunction();

		/**
		 * The meta object literal for the '{@link column_transformation_logic.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see column_transformation_logic.impl.ColumnFunctionGroupImpl
		 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getColumnFunctionGroup()
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
		 * The meta object literal for the '{@link column_transformation_logic.impl.ResolvedCubeColumnParameterImpl <em>Resolved Cube Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see column_transformation_logic.impl.ResolvedCubeColumnParameterImpl
		 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getResolvedCubeColumnParameter()
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
		 * The meta object literal for the '{@link column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl
		 * @see column_transformation_logic.impl.Column_transformation_logicPackageImpl#getSpeculativeCubeColumnParameter()
		 * @generated
		 */
		EClass SPECULATIVE_CUBE_COLUMN_PARAMETER = eINSTANCE.getSpeculativeCubeColumnParameter();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE = eINSTANCE.getSpeculativeCubeColumnParameter_Cube();

		/**
		 * The meta object literal for the '<em><b>Attribute</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE = eINSTANCE.getSpeculativeCubeColumnParameter_Attribute();

	}

} //Column_transformation_logicPackage
