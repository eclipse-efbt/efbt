/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions;

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
 * The functions package contains information about functions and their specifications. 
 * Many languages use functions of this form,  and also attribute lineage and data lineage.
 * 
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface FunctionsPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "functions";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/functions";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "functions";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	FunctionsPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl <em>Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getParameter()
	 * @generated
	 */
	int PARAMETER = 9;

	/**
	 * The number of structural features of the '<em>Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionImpl <em>Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getFunction()
	 * @generated
	 */
	int FUNCTION = 5;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION__PARAMETERS = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION__NAME = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionImpl <em>Aggregate Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getAggregateFunction()
	 * @generated
	 */
	int AGGREGATE_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION__PARAMETERS = FUNCTION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION__NAME = FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION__FUNCTION_SPEC = FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Aggregate Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_FEATURE_COUNT = FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Aggregate Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_OPERATION_COUNT = FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecImpl <em>Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getFunctionSpec()
	 * @generated
	 */
	int FUNCTION_SPEC = 6;

	/**
	 * The feature id for the '<em><b>Parameter Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__PARAMETER_SPECS = 0;

	/**
	 * The feature id for the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__FUNCTIONAL_NAME = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__NAME = 2;

	/**
	 * The feature id for the '<em><b>Relational Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__RELATIONAL_NAME = 3;

	/**
	 * The number of structural features of the '<em>Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionSpecImpl <em>Aggregate Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getAggregateFunctionSpec()
	 * @generated
	 */
	int AGGREGATE_FUNCTION_SPEC = 1;

	/**
	 * The feature id for the '<em><b>Parameter Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__PARAMETER_SPECS = FUNCTION_SPEC__PARAMETER_SPECS;

	/**
	 * The feature id for the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__FUNCTIONAL_NAME = FUNCTION_SPEC__FUNCTIONAL_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__NAME = FUNCTION_SPEC__NAME;

	/**
	 * The feature id for the '<em><b>Relational Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__RELATIONAL_NAME = FUNCTION_SPEC__RELATIONAL_NAME;

	/**
	 * The number of structural features of the '<em>Aggregate Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC_FEATURE_COUNT = FUNCTION_SPEC_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Aggregate Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC_OPERATION_COUNT = FUNCTION_SPEC_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionImpl <em>Basic Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBasicFunction()
	 * @generated
	 */
	int BASIC_FUNCTION = 2;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION__PARAMETERS = FUNCTION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION__NAME = FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION__FUNCTION_SPEC = FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_FEATURE_COUNT = FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Basic Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_OPERATION_COUNT = FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionSpecImpl <em>Basic Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBasicFunctionSpec()
	 * @generated
	 */
	int BASIC_FUNCTION_SPEC = 3;

	/**
	 * The feature id for the '<em><b>Parameter Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__PARAMETER_SPECS = FUNCTION_SPEC__PARAMETER_SPECS;

	/**
	 * The feature id for the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__FUNCTIONAL_NAME = FUNCTION_SPEC__FUNCTIONAL_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__NAME = FUNCTION_SPEC__NAME;

	/**
	 * The feature id for the '<em><b>Relational Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__RELATIONAL_NAME = FUNCTION_SPEC__RELATIONAL_NAME;

	/**
	 * The feature id for the '<em><b>Is Infix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__IS_INFIX = FUNCTION_SPEC_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC_FEATURE_COUNT = FUNCTION_SPEC_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Basic Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC_OPERATION_COUNT = FUNCTION_SPEC_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BooleanFunctionImpl <em>Boolean Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BooleanFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBooleanFunction()
	 * @generated
	 */
	int BOOLEAN_FUNCTION = 4;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION__PARAMETERS = BASIC_FUNCTION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION__NAME = BASIC_FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION__FUNCTION_SPEC = BASIC_FUNCTION__FUNCTION_SPEC;

	/**
	 * The number of structural features of the '<em>Boolean Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION_FEATURE_COUNT = BASIC_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Boolean Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION_OPERATION_COUNT = BASIC_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecModuleImpl <em>Function Spec Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getFunctionSpecModule()
	 * @generated
	 */
	int FUNCTION_SPEC_MODULE = 7;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Function Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__FUNCTION_SPECS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Function Spec Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Function Spec Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParamaterSpecImpl <em>Paramater Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParamaterSpecImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getParamaterSpec()
	 * @generated
	 */
	int PARAMATER_SPEC = 8;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMATER_SPEC__NAME = 0;

	/**
	 * The number of structural features of the '<em>Paramater Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMATER_SPEC_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Paramater Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMATER_SPEC_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.CubeColumnImpl <em>Cube Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.CubeColumnImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getCubeColumn()
	 * @generated
	 */
	int CUBE_COLUMN = 16;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getColumnFunction()
	 * @generated
	 */
	int COLUMN_FUNCTION = 14;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.GetStructColumnFunctionImpl <em>Get Struct Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.GetStructColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getGetStructColumnFunction()
	 * @generated
	 */
	int GET_STRUCT_COLUMN_FUNCTION = 10;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__NAME = COLUMN_FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__CUBE = COLUMN_FUNCTION__CUBE;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__USED_IN_SUBSETS = COLUMN_FUNCTION__USED_IN_SUBSETS;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__VARIABLE = COLUMN_FUNCTION__VARIABLE;

	/**
	 * The feature id for the '<em><b>Column In Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__COLUMN_IN_STRUCT = COLUMN_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION__CUBE_COLUMN = COLUMN_FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Get Struct Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION_FEATURE_COUNT = COLUMN_FUNCTION_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Get Struct Column Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_STRUCT_COLUMN_FUNCTION_OPERATION_COUNT = COLUMN_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeStructColumnParameterImpl <em>Speculative Struct Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeStructColumnParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getSpeculativeStructColumnParameter()
	 * @generated
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER = 11;

	/**
	 * The feature id for the '<em><b>Column Inside Struct</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER__CUBE = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Struct Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER__STRUCT_COLUMN = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Speculative Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Speculative Struct Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_STRUCT_COLUMN_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getAggregateColumnFunction()
	 * @generated
	 */
	int AGGREGATE_COLUMN_FUNCTION = 12;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicColumnFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBasicColumnFunction()
	 * @generated
	 */
	int BASIC_COLUMN_FUNCTION = 13;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionGroupImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getColumnFunctionGroup()
	 * @generated
	 */
	int COLUMN_FUNCTION_GROUP = 15;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeCubeColumnParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER = 17;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Speculative Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Speculative Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.MemberParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getMemberParameter()
	 * @generated
	 */
	int MEMBER_PARAMETER = 18;

	/**
	 * The feature id for the '<em><b>Param</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__PARAM = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ValueParameterImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getValueParameter()
	 * @generated
	 */
	int VALUE_PARAMETER = 19;

	/**
	 * The feature id for the '<em><b>Data Type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__DATA_TYPE = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__VALUE = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction
	 * @generated
	 */
	EClass getAggregateFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction#getFunctionSpec()
	 * @see #getAggregateFunction()
	 * @generated
	 */
	EReference getAggregateFunction_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunctionSpec <em>Aggregate Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunctionSpec
	 * @generated
	 */
	EClass getAggregateFunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction
	 * @generated
	 */
	EClass getBasicFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction#getFunctionSpec()
	 * @see #getBasicFunction()
	 * @generated
	 */
	EReference getBasicFunction_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunctionSpec <em>Basic Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunctionSpec
	 * @generated
	 */
	EClass getBasicFunctionSpec();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunctionSpec#isIsInfix <em>Is Infix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Infix</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunctionSpec#isIsInfix()
	 * @see #getBasicFunctionSpec()
	 * @generated
	 */
	EAttribute getBasicFunctionSpec_IsInfix();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BooleanFunction <em>Boolean Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Boolean Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BooleanFunction
	 * @generated
	 */
	EClass getBooleanFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function <em>Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function
	 * @generated
	 */
	EClass getFunction();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function#getParameters <em>Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameters</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function#getParameters()
	 * @see #getFunction()
	 * @generated
	 */
	EReference getFunction_Parameters();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function#getName()
	 * @see #getFunction()
	 * @generated
	 */
	EAttribute getFunction_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec
	 * @generated
	 */
	EClass getFunctionSpec();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getParameterSpecs <em>Parameter Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameter Specs</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getParameterSpecs()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EReference getFunctionSpec_ParameterSpecs();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getFunctionalName <em>Functional Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Functional Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getFunctionalName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_FunctionalName();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getRelationalName <em>Relational Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Relational Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpec#getRelationalName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_RelationalName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpecModule <em>Function Spec Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function Spec Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpecModule
	 * @generated
	 */
	EClass getFunctionSpecModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpecModule#getFunctionSpecs <em>Function Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Function Specs</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpecModule#getFunctionSpecs()
	 * @see #getFunctionSpecModule()
	 * @generated
	 */
	EReference getFunctionSpecModule_FunctionSpecs();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ParamaterSpec <em>Paramater Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Paramater Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ParamaterSpec
	 * @generated
	 */
	EClass getParamaterSpec();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ParamaterSpec#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ParamaterSpec#getName()
	 * @see #getParamaterSpec()
	 * @generated
	 */
	EAttribute getParamaterSpec_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Parameter <em>Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Parameter
	 * @generated
	 */
	EClass getParameter();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction <em>Get Struct Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Get Struct Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction
	 * @generated
	 */
	EClass getGetStructColumnFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getColumnInStruct <em>Column In Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column In Struct</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getColumnInStruct()
	 * @see #getGetStructColumnFunction()
	 * @generated
	 */
	EReference getGetStructColumnFunction_ColumnInStruct();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.GetStructColumnFunction#getCubeColumn()
	 * @see #getGetStructColumnFunction()
	 * @generated
	 */
	EReference getGetStructColumnFunction_CubeColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter <em>Speculative Struct Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Speculative Struct Column Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter
	 * @generated
	 */
	EClass getSpeculativeStructColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter#getColumnInsideStruct <em>Column Inside Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column Inside Struct</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter#getColumnInsideStruct()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_ColumnInsideStruct();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter#getCube()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_Cube();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter#getStructColumn <em>Struct Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Struct Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeStructColumnParameter#getStructColumn()
	 * @see #getSpeculativeStructColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeStructColumnParameter_StructColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateColumnFunction <em>Aggregate Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateColumnFunction
	 * @generated
	 */
	EClass getAggregateColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateColumnFunction#getAggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Aggregate Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateColumnFunction#getAggregateFunction()
	 * @see #getAggregateColumnFunction()
	 * @generated
	 */
	EReference getAggregateColumnFunction_AggregateFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction <em>Basic Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction
	 * @generated
	 */
	EClass getBasicColumnFunction();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction#getBasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Basic Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction#getBasicFunction()
	 * @see #getBasicColumnFunction()
	 * @generated
	 */
	EReference getBasicColumnFunction_BasicFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunction <em>Column Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunction
	 * @generated
	 */
	EClass getColumnFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunctionGroup <em>Column Function Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Column Function Group</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunctionGroup
	 * @generated
	 */
	EClass getColumnFunctionGroup();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunctionGroup#getColumnFunctions <em>Column Functions</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Column Functions</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ColumnFunctionGroup#getColumnFunctions()
	 * @see #getColumnFunctionGroup()
	 * @generated
	 */
	EReference getColumnFunctionGroup_ColumnFunctions();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn
	 * @generated
	 */
	EClass getCubeColumn();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#getName()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EAttribute getCubeColumn_Name();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#getCube()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EReference getCubeColumn_Cube();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#isUsedInSubsets <em>Used In Subsets</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Used In Subsets</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#isUsedInSubsets()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EAttribute getCubeColumn_UsedInSubsets();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.CubeColumn#getVariable()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EReference getCubeColumn_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter <em>Speculative Cube Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Speculative Cube Column Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter
	 * @generated
	 */
	EClass getSpeculativeCubeColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getColumn <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getColumn()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Column();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.SpeculativeCubeColumnParameter#getCube()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Cube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.MemberParameter <em>Member Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.MemberParameter
	 * @generated
	 */
	EClass getMemberParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.MemberParameter#getParam <em>Param</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Param</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.MemberParameter#getParam()
	 * @see #getMemberParameter()
	 * @generated
	 */
	EReference getMemberParameter_Param();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ValueParameter <em>Value Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Value Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ValueParameter
	 * @generated
	 */
	EClass getValueParameter();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ValueParameter#getDataType <em>Data Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Data Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ValueParameter#getDataType()
	 * @see #getValueParameter()
	 * @generated
	 */
	EAttribute getValueParameter_DataType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ValueParameter#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.ValueParameter#getValue()
	 * @see #getValueParameter()
	 * @generated
	 */
	EAttribute getValueParameter_Value();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	FunctionsFactory getFunctionsFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionImpl <em>Aggregate Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getAggregateFunction()
		 * @generated
		 */
		EClass AGGREGATE_FUNCTION = eINSTANCE.getAggregateFunction();

		/**
		 * The meta object literal for the '<em><b>Function Spec</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_FUNCTION__FUNCTION_SPEC = eINSTANCE.getAggregateFunction_FunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionSpecImpl <em>Aggregate Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateFunctionSpecImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getAggregateFunctionSpec()
		 * @generated
		 */
		EClass AGGREGATE_FUNCTION_SPEC = eINSTANCE.getAggregateFunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionImpl <em>Basic Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBasicFunction()
		 * @generated
		 */
		EClass BASIC_FUNCTION = eINSTANCE.getBasicFunction();

		/**
		 * The meta object literal for the '<em><b>Function Spec</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_FUNCTION__FUNCTION_SPEC = eINSTANCE.getBasicFunction_FunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionSpecImpl <em>Basic Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicFunctionSpecImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBasicFunctionSpec()
		 * @generated
		 */
		EClass BASIC_FUNCTION_SPEC = eINSTANCE.getBasicFunctionSpec();

		/**
		 * The meta object literal for the '<em><b>Is Infix</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute BASIC_FUNCTION_SPEC__IS_INFIX = eINSTANCE.getBasicFunctionSpec_IsInfix();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BooleanFunctionImpl <em>Boolean Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BooleanFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBooleanFunction()
		 * @generated
		 */
		EClass BOOLEAN_FUNCTION = eINSTANCE.getBooleanFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionImpl <em>Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getFunction()
		 * @generated
		 */
		EClass FUNCTION = eINSTANCE.getFunction();

		/**
		 * The meta object literal for the '<em><b>Parameters</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTION__PARAMETERS = eINSTANCE.getFunction_Parameters();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION__NAME = eINSTANCE.getFunction_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecImpl <em>Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getFunctionSpec()
		 * @generated
		 */
		EClass FUNCTION_SPEC = eINSTANCE.getFunctionSpec();

		/**
		 * The meta object literal for the '<em><b>Parameter Specs</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTION_SPEC__PARAMETER_SPECS = eINSTANCE.getFunctionSpec_ParameterSpecs();

		/**
		 * The meta object literal for the '<em><b>Functional Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION_SPEC__FUNCTIONAL_NAME = eINSTANCE.getFunctionSpec_FunctionalName();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION_SPEC__NAME = eINSTANCE.getFunctionSpec_Name();

		/**
		 * The meta object literal for the '<em><b>Relational Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION_SPEC__RELATIONAL_NAME = eINSTANCE.getFunctionSpec_RelationalName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecModuleImpl <em>Function Spec Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionSpecModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getFunctionSpecModule()
		 * @generated
		 */
		EClass FUNCTION_SPEC_MODULE = eINSTANCE.getFunctionSpecModule();

		/**
		 * The meta object literal for the '<em><b>Function Specs</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTION_SPEC_MODULE__FUNCTION_SPECS = eINSTANCE.getFunctionSpecModule_FunctionSpecs();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParamaterSpecImpl <em>Paramater Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParamaterSpecImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getParamaterSpec()
		 * @generated
		 */
		EClass PARAMATER_SPEC = eINSTANCE.getParamaterSpec();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PARAMATER_SPEC__NAME = eINSTANCE.getParamaterSpec_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl <em>Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getParameter()
		 * @generated
		 */
		EClass PARAMETER = eINSTANCE.getParameter();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.GetStructColumnFunctionImpl <em>Get Struct Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.GetStructColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getGetStructColumnFunction()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeStructColumnParameterImpl <em>Speculative Struct Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeStructColumnParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getSpeculativeStructColumnParameter()
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

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateColumnFunctionImpl <em>Aggregate Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.AggregateColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getAggregateColumnFunction()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicColumnFunctionImpl <em>Basic Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.BasicColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getBasicColumnFunction()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionImpl <em>Column Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getColumnFunction()
		 * @generated
		 */
		EClass COLUMN_FUNCTION = eINSTANCE.getColumnFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionGroupImpl <em>Column Function Group</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ColumnFunctionGroupImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getColumnFunctionGroup()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.CubeColumnImpl <em>Cube Column</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.CubeColumnImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getCubeColumn()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.SpeculativeCubeColumnParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getSpeculativeCubeColumnParameter()
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

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.MemberParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getMemberParameter()
		 * @generated
		 */
		EClass MEMBER_PARAMETER = eINSTANCE.getMemberParameter();

		/**
		 * The meta object literal for the '<em><b>Param</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_PARAMETER__PARAM = eINSTANCE.getMemberParameter_Param();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ValueParameterImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.FunctionsPackageImpl#getValueParameter()
		 * @generated
		 */
		EClass VALUE_PARAMETER = eINSTANCE.getValueParameter();

		/**
		 * The meta object literal for the '<em><b>Data Type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VALUE_PARAMETER__DATA_TYPE = eINSTANCE.getValueParameter_DataType();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VALUE_PARAMETER__VALUE = eINSTANCE.getValueParameter_Value();

	}

} //FunctionsPackage
