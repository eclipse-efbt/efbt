/**
 */
package org.eclipse.efbt.controller.core.model.platform_call;

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
 *  This package describes platform calls . 
 *     Each Platform call  takes one or more instances of a model  and returns a model instance as a result.
 *  
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Platform_callPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "platform_call";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/platform_call";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "platform_call";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Platform_callPackage eINSTANCE = org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl <em>Platform Call</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCall()
	 * @generated
	 */
	int PLATFORM_CALL = 5;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL__ERROR_MESSAGE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL__NAME = 1;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL__RETURN_STATUS = 2;

	/**
	 * The number of structural features of the '<em>Platform Call</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Platform Call</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl <em>Compare Attribute Lineage Models</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getCompareAttributeLineageModels()
	 * @generated
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS = 0;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Resulting Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Subset Boolean</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Not Included Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>First Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL = PLATFORM_CALL_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Second Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL = PLATFORM_CALL_FEATURE_COUNT + 4;

	/**
	 * The number of structural features of the '<em>Compare Attribute Lineage Models</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 5;

	/**
	 * The number of operations of the '<em>Compare Attribute Lineage Models</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMPARE_ATTRIBUTE_LINEAGE_MODELS_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.SubsetBooleanImpl <em>Subset Boolean</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.SubsetBooleanImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getSubsetBoolean()
	 * @generated
	 */
	int SUBSET_BOOLEAN = 3;

	/**
	 * The number of structural features of the '<em>Subset Boolean</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBSET_BOOLEAN_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Subset Boolean</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBSET_BOOLEAN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.IsNotSubsetImpl <em>Is Not Subset</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.IsNotSubsetImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getIsNotSubset()
	 * @generated
	 */
	int IS_NOT_SUBSET = 1;

	/**
	 * The number of structural features of the '<em>Is Not Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_NOT_SUBSET_FEATURE_COUNT = SUBSET_BOOLEAN_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Is Not Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_NOT_SUBSET_OPERATION_COUNT = SUBSET_BOOLEAN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.IsSubsetImpl <em>Is Subset</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.IsSubsetImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getIsSubset()
	 * @generated
	 */
	int IS_SUBSET = 2;

	/**
	 * The number of structural features of the '<em>Is Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_SUBSET_FEATURE_COUNT = SUBSET_BOOLEAN_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Is Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_SUBSET_OPERATION_COUNT = SUBSET_BOOLEAN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl <em>Execute Attribute Lineage Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getExecuteAttributeLineageModel()
	 * @generated
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL = 4;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Attribute Lineage Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Inputdir</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Outputdir</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Program Inputs</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS = PLATFORM_CALL_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Execute Attribute Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Execute Attribute Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXECUTE_ATTRIBUTE_LINEAGE_MODEL_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallModuleImpl <em>Platform Call Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallModuleImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCallModule()
	 * @generated
	 */
	int PLATFORM_CALL_MODULE = 6;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Platform Calls</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE__PLATFORM_CALLS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Platform Call Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Platform Call Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PLATFORM_CALL_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageModelImpl <em>Get Attribute Lineage Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageModelImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getGetAttributeLineageModel()
	 * @generated
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL = 7;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Resulting Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Transformation Context</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Unresolved Cube Column Params</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Get Attribute Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Get Attribute Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_MODEL_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageForOneReportCellImpl <em>Get Attribute Lineage For One Report Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageForOneReportCellImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getGetAttributeLineageForOneReportCell()
	 * @generated
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL = 8;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Transformation Context</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__TRANSFORMATION_CONTEXT = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Report Cell</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__REPORT_CELL = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Resulting Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__RESULTING_MODEL = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Get Attribute Lineage For One Report Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Get Attribute Lineage For One Report Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels <em>Compare Attribute Lineage Models</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Compare Attribute Lineage Models</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels
	 * @generated
	 */
	EClass getCompareAttributeLineageModels();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getResultingModel <em>Resulting Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Resulting Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getResultingModel()
	 * @see #getCompareAttributeLineageModels()
	 * @generated
	 */
	EReference getCompareAttributeLineageModels_ResultingModel();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getSubsetBoolean <em>Subset Boolean</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Subset Boolean</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getSubsetBoolean()
	 * @see #getCompareAttributeLineageModels()
	 * @generated
	 */
	EReference getCompareAttributeLineageModels_SubsetBoolean();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getNotIncludedModel <em>Not Included Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Not Included Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getNotIncludedModel()
	 * @see #getCompareAttributeLineageModels()
	 * @generated
	 */
	EReference getCompareAttributeLineageModels_NotIncludedModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getFirstModel <em>First Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>First Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getFirstModel()
	 * @see #getCompareAttributeLineageModels()
	 * @generated
	 */
	EReference getCompareAttributeLineageModels_FirstModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getSecondModel <em>Second Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Second Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels#getSecondModel()
	 * @see #getCompareAttributeLineageModels()
	 * @generated
	 */
	EReference getCompareAttributeLineageModels_SecondModel();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.IsNotSubset <em>Is Not Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Is Not Subset</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.IsNotSubset
	 * @generated
	 */
	EClass getIsNotSubset();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.IsSubset <em>Is Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Is Subset</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.IsSubset
	 * @generated
	 */
	EClass getIsSubset();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.SubsetBoolean <em>Subset Boolean</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Subset Boolean</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.SubsetBoolean
	 * @generated
	 */
	EClass getSubsetBoolean();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel <em>Execute Attribute Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Execute Attribute Lineage Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel
	 * @generated
	 */
	EClass getExecuteAttributeLineageModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getAttributeLineageModel <em>Attribute Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute Lineage Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getAttributeLineageModel()
	 * @see #getExecuteAttributeLineageModel()
	 * @generated
	 */
	EReference getExecuteAttributeLineageModel_AttributeLineageModel();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getInputdir <em>Inputdir</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Inputdir</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getInputdir()
	 * @see #getExecuteAttributeLineageModel()
	 * @generated
	 */
	EAttribute getExecuteAttributeLineageModel_Inputdir();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getOutputdir <em>Outputdir</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Outputdir</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getOutputdir()
	 * @see #getExecuteAttributeLineageModel()
	 * @generated
	 */
	EAttribute getExecuteAttributeLineageModel_Outputdir();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getProgramInputs <em>Program Inputs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Program Inputs</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel#getProgramInputs()
	 * @see #getExecuteAttributeLineageModel()
	 * @generated
	 */
	EReference getExecuteAttributeLineageModel_ProgramInputs();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.PlatformCall <em>Platform Call</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Platform Call</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.PlatformCall
	 * @generated
	 */
	EClass getPlatformCall();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.PlatformCall#getErrorMessage <em>Error Message</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Error Message</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.PlatformCall#getErrorMessage()
	 * @see #getPlatformCall()
	 * @generated
	 */
	EAttribute getPlatformCall_ErrorMessage();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.PlatformCall#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.PlatformCall#getName()
	 * @see #getPlatformCall()
	 * @generated
	 */
	EAttribute getPlatformCall_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.PlatformCall#getReturnStatus <em>Return Status</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Return Status</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.PlatformCall#getReturnStatus()
	 * @see #getPlatformCall()
	 * @generated
	 */
	EAttribute getPlatformCall_ReturnStatus();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.PlatformCallModule <em>Platform Call Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Platform Call Module</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.PlatformCallModule
	 * @generated
	 */
	EClass getPlatformCallModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.controller.core.model.platform_call.PlatformCallModule#getPlatformCalls <em>Platform Calls</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Platform Calls</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.PlatformCallModule#getPlatformCalls()
	 * @see #getPlatformCallModule()
	 * @generated
	 */
	EReference getPlatformCallModule_PlatformCalls();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel <em>Get Attribute Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Get Attribute Lineage Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel
	 * @generated
	 */
	EClass getGetAttributeLineageModel();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel#getResultingModel <em>Resulting Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Resulting Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel#getResultingModel()
	 * @see #getGetAttributeLineageModel()
	 * @generated
	 */
	EReference getGetAttributeLineageModel_ResultingModel();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel#getTransformationContext <em>Transformation Context</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Transformation Context</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel#getTransformationContext()
	 * @see #getGetAttributeLineageModel()
	 * @generated
	 */
	EReference getGetAttributeLineageModel_TransformationContext();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel#getUnresolvedCubeColumnParams <em>Unresolved Cube Column Params</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Unresolved Cube Column Params</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel#getUnresolvedCubeColumnParams()
	 * @see #getGetAttributeLineageModel()
	 * @generated
	 */
	EReference getGetAttributeLineageModel_UnresolvedCubeColumnParams();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell <em>Get Attribute Lineage For One Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Get Attribute Lineage For One Report Cell</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell
	 * @generated
	 */
	EClass getGetAttributeLineageForOneReportCell();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell#getTransformationContext <em>Transformation Context</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Transformation Context</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell#getTransformationContext()
	 * @see #getGetAttributeLineageForOneReportCell()
	 * @generated
	 */
	EReference getGetAttributeLineageForOneReportCell_TransformationContext();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell#getReportCell <em>Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Report Cell</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell#getReportCell()
	 * @see #getGetAttributeLineageForOneReportCell()
	 * @generated
	 */
	EReference getGetAttributeLineageForOneReportCell_ReportCell();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell#getResultingModel <em>Resulting Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Resulting Model</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageForOneReportCell#getResultingModel()
	 * @see #getGetAttributeLineageForOneReportCell()
	 * @generated
	 */
	EReference getGetAttributeLineageForOneReportCell_ResultingModel();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Platform_callFactory getPlatform_callFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl <em>Compare Attribute Lineage Models</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getCompareAttributeLineageModels()
		 * @generated
		 */
		EClass COMPARE_ATTRIBUTE_LINEAGE_MODELS = eINSTANCE.getCompareAttributeLineageModels();

		/**
		 * The meta object literal for the '<em><b>Resulting Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL = eINSTANCE.getCompareAttributeLineageModels_ResultingModel();

		/**
		 * The meta object literal for the '<em><b>Subset Boolean</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN = eINSTANCE.getCompareAttributeLineageModels_SubsetBoolean();

		/**
		 * The meta object literal for the '<em><b>Not Included Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL = eINSTANCE.getCompareAttributeLineageModels_NotIncludedModel();

		/**
		 * The meta object literal for the '<em><b>First Model</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL = eINSTANCE.getCompareAttributeLineageModels_FirstModel();

		/**
		 * The meta object literal for the '<em><b>Second Model</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL = eINSTANCE.getCompareAttributeLineageModels_SecondModel();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.IsNotSubsetImpl <em>Is Not Subset</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.IsNotSubsetImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getIsNotSubset()
		 * @generated
		 */
		EClass IS_NOT_SUBSET = eINSTANCE.getIsNotSubset();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.IsSubsetImpl <em>Is Subset</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.IsSubsetImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getIsSubset()
		 * @generated
		 */
		EClass IS_SUBSET = eINSTANCE.getIsSubset();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.SubsetBooleanImpl <em>Subset Boolean</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.SubsetBooleanImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getSubsetBoolean()
		 * @generated
		 */
		EClass SUBSET_BOOLEAN = eINSTANCE.getSubsetBoolean();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl <em>Execute Attribute Lineage Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getExecuteAttributeLineageModel()
		 * @generated
		 */
		EClass EXECUTE_ATTRIBUTE_LINEAGE_MODEL = eINSTANCE.getExecuteAttributeLineageModel();

		/**
		 * The meta object literal for the '<em><b>Attribute Lineage Model</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL = eINSTANCE.getExecuteAttributeLineageModel_AttributeLineageModel();

		/**
		 * The meta object literal for the '<em><b>Inputdir</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR = eINSTANCE.getExecuteAttributeLineageModel_Inputdir();

		/**
		 * The meta object literal for the '<em><b>Outputdir</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR = eINSTANCE.getExecuteAttributeLineageModel_Outputdir();

		/**
		 * The meta object literal for the '<em><b>Program Inputs</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS = eINSTANCE.getExecuteAttributeLineageModel_ProgramInputs();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl <em>Platform Call</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCall()
		 * @generated
		 */
		EClass PLATFORM_CALL = eINSTANCE.getPlatformCall();

		/**
		 * The meta object literal for the '<em><b>Error Message</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PLATFORM_CALL__ERROR_MESSAGE = eINSTANCE.getPlatformCall_ErrorMessage();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PLATFORM_CALL__NAME = eINSTANCE.getPlatformCall_Name();

		/**
		 * The meta object literal for the '<em><b>Return Status</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PLATFORM_CALL__RETURN_STATUS = eINSTANCE.getPlatformCall_ReturnStatus();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallModuleImpl <em>Platform Call Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallModuleImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCallModule()
		 * @generated
		 */
		EClass PLATFORM_CALL_MODULE = eINSTANCE.getPlatformCallModule();

		/**
		 * The meta object literal for the '<em><b>Platform Calls</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PLATFORM_CALL_MODULE__PLATFORM_CALLS = eINSTANCE.getPlatformCallModule_PlatformCalls();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageModelImpl <em>Get Attribute Lineage Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageModelImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getGetAttributeLineageModel()
		 * @generated
		 */
		EClass GET_ATTRIBUTE_LINEAGE_MODEL = eINSTANCE.getGetAttributeLineageModel();

		/**
		 * The meta object literal for the '<em><b>Resulting Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL = eINSTANCE.getGetAttributeLineageModel_ResultingModel();

		/**
		 * The meta object literal for the '<em><b>Transformation Context</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT = eINSTANCE.getGetAttributeLineageModel_TransformationContext();

		/**
		 * The meta object literal for the '<em><b>Unresolved Cube Column Params</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS = eINSTANCE.getGetAttributeLineageModel_UnresolvedCubeColumnParams();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageForOneReportCellImpl <em>Get Attribute Lineage For One Report Cell</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageForOneReportCellImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getGetAttributeLineageForOneReportCell()
		 * @generated
		 */
		EClass GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL = eINSTANCE.getGetAttributeLineageForOneReportCell();

		/**
		 * The meta object literal for the '<em><b>Transformation Context</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__TRANSFORMATION_CONTEXT = eINSTANCE.getGetAttributeLineageForOneReportCell_TransformationContext();

		/**
		 * The meta object literal for the '<em><b>Report Cell</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__REPORT_CELL = eINSTANCE.getGetAttributeLineageForOneReportCell_ReportCell();

		/**
		 * The meta object literal for the '<em><b>Resulting Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_LINEAGE_FOR_ONE_REPORT_CELL__RESULTING_MODEL = eINSTANCE.getGetAttributeLineageForOneReportCell_ResultingModel();

	}

} //Platform_callPackage
