/**
 */
package org.eclipse.efbt.controller.core.model.platform_call;

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
 * @see org.eclipse.efbt.controller.core.model.platform_call.Platform_callFactory
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
	Platform_callPackage eINSTANCE = org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl <em>Platform Call</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCall()
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
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallModuleImpl <em>Platform Call Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.PlatformCallModuleImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getPlatformCallModule()
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
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithOldTestFormatToCocasonImpl <em>Translate BIRD With Old Test Format To Cocason</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithOldTestFormatToCocasonImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getTranslateBIRDWithOldTestFormatToCocason()
	 * @generated
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON = 2;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Input Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Output Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Test Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Translate BIRD With Old Test Format To Cocason</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Translate BIRD With Old Test Format To Cocason</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl <em>Convert SQL Developer Model To Ecore</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getConvertSQLDeveloperModelToEcore()
	 * @generated
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE = 3;

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
	 * The number of structural features of the '<em>Convert SQL Developer Model To Ecore</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Convert SQL Developer Model To Ecore</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithNewTestFormatToCocasonImpl <em>Translate BIRD With New Test Format To Cocason</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithNewTestFormatToCocasonImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getTranslateBIRDWithNewTestFormatToCocason()
	 * @generated
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON = 4;

	/**
	 * The feature id for the '<em><b>Error Message</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__ERROR_MESSAGE = PLATFORM_CALL__ERROR_MESSAGE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__NAME = PLATFORM_CALL__NAME;

	/**
	 * The feature id for the '<em><b>Return Status</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__RETURN_STATUS = PLATFORM_CALL__RETURN_STATUS;

	/**
	 * The feature id for the '<em><b>Input Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Output Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Test Directory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY = PLATFORM_CALL_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Translate BIRD With New Test Format To Cocason</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON_FEATURE_COUNT = PLATFORM_CALL_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Translate BIRD With New Test Format To Cocason</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON_OPERATION_COUNT = PLATFORM_CALL_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ProgramAndCallsImpl <em>Program And Calls</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.ProgramAndCallsImpl
	 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getProgramAndCalls()
	 * @generated
	 */
	int PROGRAM_AND_CALLS = 5;

	/**
	 * The feature id for the '<em><b>Platform Calls</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROGRAM_AND_CALLS__PLATFORM_CALLS = 0;

	/**
	 * The feature id for the '<em><b>Program</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROGRAM_AND_CALLS__PROGRAM = 1;

	/**
	 * The number of structural features of the '<em>Program And Calls</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROGRAM_AND_CALLS_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Program And Calls</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROGRAM_AND_CALLS_OPERATION_COUNT = 0;


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
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason <em>Translate BIRD With Old Test Format To Cocason</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Translate BIRD With Old Test Format To Cocason</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason
	 * @generated
	 */
	EClass getTranslateBIRDWithOldTestFormatToCocason();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason#getInputDirectory <em>Input Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Input Directory</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason#getInputDirectory()
	 * @see #getTranslateBIRDWithOldTestFormatToCocason()
	 * @generated
	 */
	EAttribute getTranslateBIRDWithOldTestFormatToCocason_InputDirectory();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason#getOutputDirectory <em>Output Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Output Directory</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason#getOutputDirectory()
	 * @see #getTranslateBIRDWithOldTestFormatToCocason()
	 * @generated
	 */
	EAttribute getTranslateBIRDWithOldTestFormatToCocason_OutputDirectory();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason#getTestDirectory <em>Test Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Test Directory</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason#getTestDirectory()
	 * @see #getTranslateBIRDWithOldTestFormatToCocason()
	 * @generated
	 */
	EAttribute getTranslateBIRDWithOldTestFormatToCocason_TestDirectory();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.ConvertSQLDeveloperModelToEcore <em>Convert SQL Developer Model To Ecore</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Convert SQL Developer Model To Ecore</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ConvertSQLDeveloperModelToEcore
	 * @generated
	 */
	EClass getConvertSQLDeveloperModelToEcore();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.ConvertSQLDeveloperModelToEcore#getInputDirectory <em>Input Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Input Directory</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ConvertSQLDeveloperModelToEcore#getInputDirectory()
	 * @see #getConvertSQLDeveloperModelToEcore()
	 * @generated
	 */
	EAttribute getConvertSQLDeveloperModelToEcore_InputDirectory();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason <em>Translate BIRD With New Test Format To Cocason</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Translate BIRD With New Test Format To Cocason</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason
	 * @generated
	 */
	EClass getTranslateBIRDWithNewTestFormatToCocason();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason#getInputDirectory <em>Input Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Input Directory</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason#getInputDirectory()
	 * @see #getTranslateBIRDWithNewTestFormatToCocason()
	 * @generated
	 */
	EAttribute getTranslateBIRDWithNewTestFormatToCocason_InputDirectory();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason#getOutputDirectory <em>Output Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Output Directory</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason#getOutputDirectory()
	 * @see #getTranslateBIRDWithNewTestFormatToCocason()
	 * @generated
	 */
	EAttribute getTranslateBIRDWithNewTestFormatToCocason_OutputDirectory();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason#getTestDirectory <em>Test Directory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Test Directory</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithNewTestFormatToCocason#getTestDirectory()
	 * @see #getTranslateBIRDWithNewTestFormatToCocason()
	 * @generated
	 */
	EAttribute getTranslateBIRDWithNewTestFormatToCocason_TestDirectory();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls <em>Program And Calls</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Program And Calls</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls
	 * @generated
	 */
	EClass getProgramAndCalls();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getPlatformCalls <em>Platform Calls</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Platform Calls</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getPlatformCalls()
	 * @see #getProgramAndCalls()
	 * @generated
	 */
	EReference getProgramAndCalls_PlatformCalls();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getProgram <em>Program</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Program</em>'.
	 * @see org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls#getProgram()
	 * @see #getProgramAndCalls()
	 * @generated
	 */
	EReference getProgramAndCalls_Program();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithOldTestFormatToCocasonImpl <em>Translate BIRD With Old Test Format To Cocason</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithOldTestFormatToCocasonImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getTranslateBIRDWithOldTestFormatToCocason()
		 * @generated
		 */
		EClass TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON = eINSTANCE.getTranslateBIRDWithOldTestFormatToCocason();

		/**
		 * The meta object literal for the '<em><b>Input Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY = eINSTANCE.getTranslateBIRDWithOldTestFormatToCocason_InputDirectory();

		/**
		 * The meta object literal for the '<em><b>Output Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY = eINSTANCE.getTranslateBIRDWithOldTestFormatToCocason_OutputDirectory();

		/**
		 * The meta object literal for the '<em><b>Test Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY = eINSTANCE.getTranslateBIRDWithOldTestFormatToCocason_TestDirectory();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl <em>Convert SQL Developer Model To Ecore</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.ConvertSQLDeveloperModelToEcoreImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getConvertSQLDeveloperModelToEcore()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithNewTestFormatToCocasonImpl <em>Translate BIRD With New Test Format To Cocason</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.TranslateBIRDWithNewTestFormatToCocasonImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getTranslateBIRDWithNewTestFormatToCocason()
		 * @generated
		 */
		EClass TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON = eINSTANCE.getTranslateBIRDWithNewTestFormatToCocason();

		/**
		 * The meta object literal for the '<em><b>Input Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY = eINSTANCE.getTranslateBIRDWithNewTestFormatToCocason_InputDirectory();

		/**
		 * The meta object literal for the '<em><b>Output Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY = eINSTANCE.getTranslateBIRDWithNewTestFormatToCocason_OutputDirectory();

		/**
		 * The meta object literal for the '<em><b>Test Directory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute TRANSLATE_BIRD_WITH_NEW_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY = eINSTANCE.getTranslateBIRDWithNewTestFormatToCocason_TestDirectory();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ProgramAndCallsImpl <em>Program And Calls</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.ProgramAndCallsImpl
		 * @see org.eclipse.efbt.controller.core.model.platform_call.impl.Platform_callPackageImpl#getProgramAndCalls()
		 * @generated
		 */
		EClass PROGRAM_AND_CALLS = eINSTANCE.getProgramAndCalls();

		/**
		 * The meta object literal for the '<em><b>Platform Calls</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROGRAM_AND_CALLS__PLATFORM_CALLS = eINSTANCE.getProgramAndCalls_PlatformCalls();

		/**
		 * The meta object literal for the '<em><b>Program</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROGRAM_AND_CALLS__PROGRAM = eINSTANCE.getProgramAndCalls_Program();

	}

} //Platform_callPackage
