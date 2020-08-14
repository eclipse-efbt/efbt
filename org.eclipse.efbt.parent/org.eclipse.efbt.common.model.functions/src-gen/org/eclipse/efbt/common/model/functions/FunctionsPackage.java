/**
 */
package org.eclipse.efbt.common.model.functions;

import org.eclipse.efbt.common.model.module_management.Module_managementPackage;

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
 * TheFunctions pckage cotain information about functions and their specifications. 
 * Functions ae used in DataSetTransformations such as the ones for TRL and VTL. 
 * Functions are also used in AttributeLineageModels an in SpreadsheetLineageModels.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.common.model.functions.FunctionsFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface FunctionsPackage extends EPackage
{
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
	String eNS_URI = "http://www.eclipse.org/functions";

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
	FunctionsPackage eINSTANCE = org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.ParameterImpl <em>Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.ParameterImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getParameter()
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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.FunctionImpl <em>Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getFunction()
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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.AggregateFunctionImpl <em>Aggregate Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.AggregateFunctionImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getAggregateFunction()
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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.FunctionSpecImpl <em>Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionSpecImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getFunctionSpec()
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
	 * The feature id for the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__SQL_NAME = 3;

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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.AggregateFunctionSpecImpl <em>Aggregate Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.AggregateFunctionSpecImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getAggregateFunctionSpec()
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
	 * The feature id for the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__SQL_NAME = FUNCTION_SPEC__SQL_NAME;

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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.BasicFunctionImpl <em>Basic Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.BasicFunctionImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getBasicFunction()
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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.BasicFunctionSpecImpl <em>Basic Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.BasicFunctionSpecImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getBasicFunctionSpec()
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
	 * The feature id for the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__SQL_NAME = FUNCTION_SPEC__SQL_NAME;

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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.BooleanFunctionImpl <em>Boolean Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.BooleanFunctionImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getBooleanFunction()
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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.FunctionSpecModuleImpl <em>Function Spec Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionSpecModuleImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getFunctionSpecModule()
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
	 * The meta object id for the '{@link org.eclipse.efbt.common.model.functions.impl.ParamaterSpecImpl <em>Paramater Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.common.model.functions.impl.ParamaterSpecImpl
	 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getParamaterSpec()
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
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.AggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Function</em>'.
	 * @see org.eclipse.efbt.common.model.functions.AggregateFunction
	 * @generated
	 */
	EClass getAggregateFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.common.model.functions.AggregateFunction#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.common.model.functions.AggregateFunction#getFunctionSpec()
	 * @see #getAggregateFunction()
	 * @generated
	 */
	EReference getAggregateFunction_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.AggregateFunctionSpec <em>Aggregate Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Function Spec</em>'.
	 * @see org.eclipse.efbt.common.model.functions.AggregateFunctionSpec
	 * @generated
	 */
	EClass getAggregateFunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.BasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Function</em>'.
	 * @see org.eclipse.efbt.common.model.functions.BasicFunction
	 * @generated
	 */
	EClass getBasicFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.common.model.functions.BasicFunction#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.common.model.functions.BasicFunction#getFunctionSpec()
	 * @see #getBasicFunction()
	 * @generated
	 */
	EReference getBasicFunction_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.BasicFunctionSpec <em>Basic Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Function Spec</em>'.
	 * @see org.eclipse.efbt.common.model.functions.BasicFunctionSpec
	 * @generated
	 */
	EClass getBasicFunctionSpec();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.common.model.functions.BasicFunctionSpec#isIsInfix <em>Is Infix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Infix</em>'.
	 * @see org.eclipse.efbt.common.model.functions.BasicFunctionSpec#isIsInfix()
	 * @see #getBasicFunctionSpec()
	 * @generated
	 */
	EAttribute getBasicFunctionSpec_IsInfix();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.BooleanFunction <em>Boolean Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Boolean Function</em>'.
	 * @see org.eclipse.efbt.common.model.functions.BooleanFunction
	 * @generated
	 */
	EClass getBooleanFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.Function <em>Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function</em>'.
	 * @see org.eclipse.efbt.common.model.functions.Function
	 * @generated
	 */
	EClass getFunction();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.common.model.functions.Function#getParameters <em>Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameters</em>'.
	 * @see org.eclipse.efbt.common.model.functions.Function#getParameters()
	 * @see #getFunction()
	 * @generated
	 */
	EReference getFunction_Parameters();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.common.model.functions.Function#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.common.model.functions.Function#getName()
	 * @see #getFunction()
	 * @generated
	 */
	EAttribute getFunction_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.FunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.common.model.functions.FunctionSpec
	 * @generated
	 */
	EClass getFunctionSpec();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.common.model.functions.FunctionSpec#getParameterSpecs <em>Parameter Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameter Specs</em>'.
	 * @see org.eclipse.efbt.common.model.functions.FunctionSpec#getParameterSpecs()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EReference getFunctionSpec_ParameterSpecs();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.common.model.functions.FunctionSpec#getFunctionalName <em>Functional Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Functional Name</em>'.
	 * @see org.eclipse.efbt.common.model.functions.FunctionSpec#getFunctionalName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_FunctionalName();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.common.model.functions.FunctionSpec#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.common.model.functions.FunctionSpec#getName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.common.model.functions.FunctionSpec#getSqlName <em>Sql Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Sql Name</em>'.
	 * @see org.eclipse.efbt.common.model.functions.FunctionSpec#getSqlName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_SqlName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.FunctionSpecModule <em>Function Spec Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function Spec Module</em>'.
	 * @see org.eclipse.efbt.common.model.functions.FunctionSpecModule
	 * @generated
	 */
	EClass getFunctionSpecModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.common.model.functions.FunctionSpecModule#getFunctionSpecs <em>Function Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Function Specs</em>'.
	 * @see org.eclipse.efbt.common.model.functions.FunctionSpecModule#getFunctionSpecs()
	 * @see #getFunctionSpecModule()
	 * @generated
	 */
	EReference getFunctionSpecModule_FunctionSpecs();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.ParamaterSpec <em>Paramater Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Paramater Spec</em>'.
	 * @see org.eclipse.efbt.common.model.functions.ParamaterSpec
	 * @generated
	 */
	EClass getParamaterSpec();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.common.model.functions.ParamaterSpec#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.common.model.functions.ParamaterSpec#getName()
	 * @see #getParamaterSpec()
	 * @generated
	 */
	EAttribute getParamaterSpec_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.common.model.functions.Parameter <em>Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parameter</em>'.
	 * @see org.eclipse.efbt.common.model.functions.Parameter
	 * @generated
	 */
	EClass getParameter();

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
	interface Literals
	{
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.AggregateFunctionImpl <em>Aggregate Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.AggregateFunctionImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getAggregateFunction()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.AggregateFunctionSpecImpl <em>Aggregate Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.AggregateFunctionSpecImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getAggregateFunctionSpec()
		 * @generated
		 */
		EClass AGGREGATE_FUNCTION_SPEC = eINSTANCE.getAggregateFunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.BasicFunctionImpl <em>Basic Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.BasicFunctionImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getBasicFunction()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.BasicFunctionSpecImpl <em>Basic Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.BasicFunctionSpecImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getBasicFunctionSpec()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.BooleanFunctionImpl <em>Boolean Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.BooleanFunctionImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getBooleanFunction()
		 * @generated
		 */
		EClass BOOLEAN_FUNCTION = eINSTANCE.getBooleanFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.FunctionImpl <em>Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getFunction()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.FunctionSpecImpl <em>Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionSpecImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getFunctionSpec()
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
		 * The meta object literal for the '<em><b>Sql Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION_SPEC__SQL_NAME = eINSTANCE.getFunctionSpec_SqlName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.FunctionSpecModuleImpl <em>Function Spec Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionSpecModuleImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getFunctionSpecModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.ParamaterSpecImpl <em>Paramater Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.ParamaterSpecImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getParamaterSpec()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.common.model.functions.impl.ParameterImpl <em>Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.common.model.functions.impl.ParameterImpl
		 * @see org.eclipse.efbt.common.model.functions.impl.FunctionsPackageImpl#getParameter()
		 * @generated
		 */
		EClass PARAMETER = eINSTANCE.getParameter();

	}

} //FunctionsPackage
