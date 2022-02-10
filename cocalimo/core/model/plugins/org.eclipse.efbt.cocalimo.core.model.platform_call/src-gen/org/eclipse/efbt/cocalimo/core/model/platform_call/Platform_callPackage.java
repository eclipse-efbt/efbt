/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call;

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
 *  This package describes platform calls . 
 *     Each Platform call  takes one or more instances of a model  and returns a model instance as a result.
 *  
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.Platform_callFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Platform_callPackage extends EPackage {
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
	String eNS_URI = "http://www.eclipse.org/efbt/platform_call";

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
	Platform_callPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallImpl <em>Platform Call</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCall()
	 * @generated
	 */
	int PLATFORM_CALL = 0;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallModuleImpl <em>Platform Call Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCallModule()
	 * @generated
	 */
	int PLATFORM_CALL_MODULE = 1;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl <em>Convert SQL Developer Model To Ecore</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getConvertSQLDeveloperModelToEcore()
	 * @generated
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE = 2;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Input Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__INPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Output Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__OUTPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Convert SQL Developer Model To Ecore</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Convert SQL Developer Model To Ecore</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ImportBIRDFromMSAccessImpl <em>Import BIRD From MS Access</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ImportBIRDFromMSAccessImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getImportBIRDFromMSAccess()
	 * @generated
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS = 3;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Input Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Output Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Import BIRD From MS Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Import BIRD From MS Access</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IMPORT_BIRD_FROM_MS_ACCESS_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl <em>Create Logical Transformation View For Scope</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getCreateLogicalTransformationViewForScope()
	 * @generated
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE = 4;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Logical Transformation Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Attribute Lineage</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Test Scope</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Create Logical Transformation View For Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Create Logical Transformation View For Scope</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertDataMetaModelToEcoreImpl <em>Convert Data Meta Model To Ecore</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertDataMetaModelToEcoreImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getConvertDataMetaModelToEcore()
	 * @generated
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE = 5;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Output Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Entity Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Convert Data Meta Model To Ecore</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Convert Data Meta Model To Ecore</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_DATA_META_MODEL_TO_ECORE_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall <em>Platform Call</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Platform Call</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall
	 * @generated
	 */
	EClass getPlatformCall();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getErrorMessage <em>Error Message</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Error Message</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getErrorMessage()
	 * @see #getPlatformCall()
	 * @generated
	 */
	EAttribute getPlatformCall_ErrorMessage();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getName()
	 * @see #getPlatformCall()
	 * @generated
	 */
	EAttribute getPlatformCall_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getReturnStatus <em>Return Status</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Return Status</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCall#getReturnStatus()
	 * @see #getPlatformCall()
	 * @generated
	 */
	EAttribute getPlatformCall_ReturnStatus();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCallModule <em>Platform Call Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Platform Call Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCallModule
	 * @generated
	 */
	EClass getPlatformCallModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCallModule#getPlatformCalls <em>Platform Calls</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Platform Calls</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.PlatformCallModule#getPlatformCalls()
	 * @see #getPlatformCallModule()
	 * @generated
	 */
	EReference getPlatformCallModule_PlatformCalls();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore <em>Convert SQL Developer Model To Ecore</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Convert SQL Developer Model To Ecore</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore
	 * @generated
	 */
	EClass getConvertSQLDeveloperModelToEcore();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore#getInputDirectory <em>Input Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Input Directory</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore#getInputDirectory()
	 * @see #getConvertSQLDeveloperModelToEcore()
	 * @generated
	 */
	EAttribute getConvertSQLDeveloperModelToEcore_InputDirectory();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore#getOutputDirectory <em>Output Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Output Directory</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertSQLDeveloperModelToEcore#getOutputDirectory()
	 * @see #getConvertSQLDeveloperModelToEcore()
	 * @generated
	 */
	EAttribute getConvertSQLDeveloperModelToEcore_OutputDirectory();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess <em>Import BIRD From MS Access</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Import BIRD From MS Access</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess
	 * @generated
	 */
	EClass getImportBIRDFromMSAccess();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess#getInputDirectory <em>Input Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Input Directory</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess#getInputDirectory()
	 * @see #getImportBIRDFromMSAccess()
	 * @generated
	 */
	EAttribute getImportBIRDFromMSAccess_InputDirectory();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess#getOutputDirectory <em>Output Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Output Directory</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ImportBIRDFromMSAccess#getOutputDirectory()
	 * @see #getImportBIRDFromMSAccess()
	 * @generated
	 */
	EAttribute getImportBIRDFromMSAccess_OutputDirectory();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope <em>Create Logical Transformation View For Scope</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Create Logical Transformation View For Scope</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope
	 * @generated
	 */
	EClass getCreateLogicalTransformationViewForScope();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope#getLogicalTransformationModule <em>Logical Transformation Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Logical Transformation Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope#getLogicalTransformationModule()
	 * @see #getCreateLogicalTransformationViewForScope()
	 * @generated
	 */
	EReference getCreateLogicalTransformationViewForScope_LogicalTransformationModule();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope#getAttributeLineage <em>Attribute Lineage</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Attribute Lineage</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope#getAttributeLineage()
	 * @see #getCreateLogicalTransformationViewForScope()
	 * @generated
	 */
	EReference getCreateLogicalTransformationViewForScope_AttributeLineage();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope#getTestScope <em>Test Scope</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Test Scope</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope#getTestScope()
	 * @see #getCreateLogicalTransformationViewForScope()
	 * @generated
	 */
	EReference getCreateLogicalTransformationViewForScope_TestScope();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertDataMetaModelToEcore <em>Convert Data Meta Model To Ecore</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Convert Data Meta Model To Ecore</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertDataMetaModelToEcore
	 * @generated
	 */
	EClass getConvertDataMetaModelToEcore();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertDataMetaModelToEcore#getOutputDirectory <em>Output Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Output Directory</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertDataMetaModelToEcore#getOutputDirectory()
	 * @see #getConvertDataMetaModelToEcore()
	 * @generated
	 */
	EAttribute getConvertDataMetaModelToEcore_OutputDirectory();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertDataMetaModelToEcore#getEntityModule <em>Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.ConvertDataMetaModelToEcore#getEntityModule()
	 * @see #getConvertDataMetaModelToEcore()
	 * @generated
	 */
	EReference getConvertDataMetaModelToEcore_EntityModule();

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
	interface Literals {
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallImpl <em>Platform Call</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCall()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallModuleImpl <em>Platform Call Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.PlatformCallModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCallModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl <em>Convert SQL Developer Model To Ecore</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getConvertSQLDeveloperModelToEcore()
		 * @generated
		 */
		EClass CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE = eINSTANCE.getConvertSQLDeveloperModelToEcore();

		/**
		 * The meta object literal for the '<em><b>Input Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__INPUT_DIRECTORY = eINSTANCE.getConvertSQLDeveloperModelToEcore_InputDirectory();

		/**
		 * The meta object literal for the '<em><b>Output Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE__OUTPUT_DIRECTORY = eINSTANCE.getConvertSQLDeveloperModelToEcore_OutputDirectory();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ImportBIRDFromMSAccessImpl <em>Import BIRD From MS Access</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ImportBIRDFromMSAccessImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getImportBIRDFromMSAccess()
		 * @generated
		 */
		EClass IMPORT_BIRD_FROM_MS_ACCESS = eINSTANCE.getImportBIRDFromMSAccess();

		/**
		 * The meta object literal for the '<em><b>Input Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY = eINSTANCE.getImportBIRDFromMSAccess_InputDirectory();

		/**
		 * The meta object literal for the '<em><b>Output Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY = eINSTANCE.getImportBIRDFromMSAccess_OutputDirectory();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl <em>Create Logical Transformation View For Scope</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.CreateLogicalTransformationViewForScopeImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getCreateLogicalTransformationViewForScope()
		 * @generated
		 */
		EClass CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE = eINSTANCE.getCreateLogicalTransformationViewForScope();

		/**
		 * The meta object literal for the '<em><b>Logical Transformation Module</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__LOGICAL_TRANSFORMATION_MODULE = eINSTANCE.getCreateLogicalTransformationViewForScope_LogicalTransformationModule();

		/**
		 * The meta object literal for the '<em><b>Attribute Lineage</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__ATTRIBUTE_LINEAGE = eINSTANCE.getCreateLogicalTransformationViewForScope_AttributeLineage();

		/**
		 * The meta object literal for the '<em><b>Test Scope</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE__TEST_SCOPE = eINSTANCE.getCreateLogicalTransformationViewForScope_TestScope();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertDataMetaModelToEcoreImpl <em>Convert Data Meta Model To Ecore</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.ConvertDataMetaModelToEcoreImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.platform_call.impl.Platform_callPackageImpl#getConvertDataMetaModelToEcore()
		 * @generated
		 */
		EClass CONVERT_DATA_META_MODEL_TO_ECORE = eINSTANCE.getConvertDataMetaModelToEcore();

		/**
		 * The meta object literal for the '<em><b>Output Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY = eINSTANCE.getConvertDataMetaModelToEcore_OutputDirectory();

		/**
		 * The meta object literal for the '<em><b>Entity Module</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE = eINSTANCE.getConvertDataMetaModelToEcore_EntityModule();

	}

} //Platform_callPackage
