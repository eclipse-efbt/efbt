/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;

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
 *  This package describes advanced Varaible which are in the form of Structs or Arrays.
 *  
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Efbt_advanced_data_definitionPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "efbt_advanced_data_definition";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/efbt_advanced_data_definition";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "efbt_advanced_data_definition";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Efbt_advanced_data_definitionPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.AdvancedTypedVariableImpl <em>Advanced Typed Variable</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.AdvancedTypedVariableImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getAdvancedTypedVariable()
	 * @generated
	 */
	int ADVANCED_TYPED_VARIABLE = 0;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__CODE = CorePackage.VARIABLE__CODE;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__DESCRIPTION = CorePackage.VARIABLE__DESCRIPTION;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__DOMAIN_ID = CorePackage.VARIABLE__DOMAIN_ID;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__MAINTENANCE_AGENCY_ID = CorePackage.VARIABLE__MAINTENANCE_AGENCY_ID;

	/**
	 * The feature id for the '<em><b>Primary concept</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__PRIMARY_CONCEPT = CorePackage.VARIABLE__PRIMARY_CONCEPT;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__VARIABLE_ID = CorePackage.VARIABLE__VARIABLE_ID;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__NAME = CorePackage.VARIABLE__NAME;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE__DISPLAY_NAME = CorePackage.VARIABLE__DISPLAY_NAME;

	/**
	 * The number of structural features of the '<em>Advanced Typed Variable</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE_FEATURE_COUNT = CorePackage.VARIABLE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Advanced Typed Variable</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ADVANCED_TYPED_VARIABLE_OPERATION_COUNT = CorePackage.VARIABLE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableImpl <em>Array Typed Variable</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getArrayTypedVariable()
	 * @generated
	 */
	int ARRAY_TYPED_VARIABLE = 1;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__CODE = ADVANCED_TYPED_VARIABLE__CODE;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__DESCRIPTION = ADVANCED_TYPED_VARIABLE__DESCRIPTION;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__DOMAIN_ID = ADVANCED_TYPED_VARIABLE__DOMAIN_ID;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__MAINTENANCE_AGENCY_ID = ADVANCED_TYPED_VARIABLE__MAINTENANCE_AGENCY_ID;

	/**
	 * The feature id for the '<em><b>Primary concept</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__PRIMARY_CONCEPT = ADVANCED_TYPED_VARIABLE__PRIMARY_CONCEPT;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__VARIABLE_ID = ADVANCED_TYPED_VARIABLE__VARIABLE_ID;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__NAME = ADVANCED_TYPED_VARIABLE__NAME;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__DISPLAY_NAME = ADVANCED_TYPED_VARIABLE__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Cube Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE__CUBE_TYPE = ADVANCED_TYPED_VARIABLE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Array Typed Variable</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_FEATURE_COUNT = ADVANCED_TYPED_VARIABLE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Array Typed Variable</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_OPERATION_COUNT = ADVANCED_TYPED_VARIABLE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableModuleImpl <em>Array Typed Variable Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getArrayTypedVariableModule()
	 * @generated
	 */
	int ARRAY_TYPED_VARIABLE_MODULE = 2;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Array Typed Variables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Array Typed Variable Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Array Typed Variable Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ARRAY_TYPED_VARIABLE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariableImpl <em>Struct Typed Variable</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariableImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getStructTypedVariable()
	 * @generated
	 */
	int STRUCT_TYPED_VARIABLE = 3;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__CODE = ADVANCED_TYPED_VARIABLE__CODE;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__DESCRIPTION = ADVANCED_TYPED_VARIABLE__DESCRIPTION;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__DOMAIN_ID = ADVANCED_TYPED_VARIABLE__DOMAIN_ID;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__MAINTENANCE_AGENCY_ID = ADVANCED_TYPED_VARIABLE__MAINTENANCE_AGENCY_ID;

	/**
	 * The feature id for the '<em><b>Primary concept</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__PRIMARY_CONCEPT = ADVANCED_TYPED_VARIABLE__PRIMARY_CONCEPT;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__VARIABLE_ID = ADVANCED_TYPED_VARIABLE__VARIABLE_ID;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__NAME = ADVANCED_TYPED_VARIABLE__NAME;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__DISPLAY_NAME = ADVANCED_TYPED_VARIABLE__DISPLAY_NAME;

	/**
	 * The feature id for the '<em><b>Cube Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE__CUBE_TYPE = ADVANCED_TYPED_VARIABLE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Struct Typed Variable</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE_FEATURE_COUNT = ADVANCED_TYPED_VARIABLE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Struct Typed Variable</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLE_OPERATION_COUNT = ADVANCED_TYPED_VARIABLE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariablesModuleImpl <em>Struct Typed Variables Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariablesModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getStructTypedVariablesModule()
	 * @generated
	 */
	int STRUCT_TYPED_VARIABLES_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Struct Type Variables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Struct Typed Variables Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Struct Typed Variables Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCT_TYPED_VARIABLES_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.AdvancedTypedVariable <em>Advanced Typed Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Advanced Typed Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.AdvancedTypedVariable
	 * @generated
	 */
	EClass getAdvancedTypedVariable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable <em>Array Typed Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Array Typed Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable
	 * @generated
	 */
	EClass getArrayTypedVariable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable#getCubeType <em>Cube Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable#getCubeType()
	 * @see #getArrayTypedVariable()
	 * @generated
	 */
	EReference getArrayTypedVariable_CubeType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule <em>Array Typed Variable Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Array Typed Variable Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule
	 * @generated
	 */
	EClass getArrayTypedVariableModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule#getArrayTypedVariables <em>Array Typed Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Array Typed Variables</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule#getArrayTypedVariables()
	 * @see #getArrayTypedVariableModule()
	 * @generated
	 */
	EReference getArrayTypedVariableModule_ArrayTypedVariables();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable <em>Struct Typed Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Struct Typed Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable
	 * @generated
	 */
	EClass getStructTypedVariable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable#getCubeType <em>Cube Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable#getCubeType()
	 * @see #getStructTypedVariable()
	 * @generated
	 */
	EReference getStructTypedVariable_CubeType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule <em>Struct Typed Variables Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Struct Typed Variables Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule
	 * @generated
	 */
	EClass getStructTypedVariablesModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule#getStructTypeVariables <em>Struct Type Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Struct Type Variables</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule#getStructTypeVariables()
	 * @see #getStructTypedVariablesModule()
	 * @generated
	 */
	EReference getStructTypedVariablesModule_StructTypeVariables();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Efbt_advanced_data_definitionFactory getEfbt_advanced_data_definitionFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.AdvancedTypedVariableImpl <em>Advanced Typed Variable</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.AdvancedTypedVariableImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getAdvancedTypedVariable()
		 * @generated
		 */
		EClass ADVANCED_TYPED_VARIABLE = eINSTANCE.getAdvancedTypedVariable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableImpl <em>Array Typed Variable</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getArrayTypedVariable()
		 * @generated
		 */
		EClass ARRAY_TYPED_VARIABLE = eINSTANCE.getArrayTypedVariable();

		/**
		 * The meta object literal for the '<em><b>Cube Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_TYPED_VARIABLE__CUBE_TYPE = eINSTANCE.getArrayTypedVariable_CubeType();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableModuleImpl <em>Array Typed Variable Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getArrayTypedVariableModule()
		 * @generated
		 */
		EClass ARRAY_TYPED_VARIABLE_MODULE = eINSTANCE.getArrayTypedVariableModule();

		/**
		 * The meta object literal for the '<em><b>Array Typed Variables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ARRAY_TYPED_VARIABLE_MODULE__ARRAY_TYPED_VARIABLES = eINSTANCE.getArrayTypedVariableModule_ArrayTypedVariables();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariableImpl <em>Struct Typed Variable</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariableImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getStructTypedVariable()
		 * @generated
		 */
		EClass STRUCT_TYPED_VARIABLE = eINSTANCE.getStructTypedVariable();

		/**
		 * The meta object literal for the '<em><b>Cube Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference STRUCT_TYPED_VARIABLE__CUBE_TYPE = eINSTANCE.getStructTypedVariable_CubeType();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariablesModuleImpl <em>Struct Typed Variables Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.StructTypedVariablesModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.Efbt_advanced_data_definitionPackageImpl#getStructTypedVariablesModule()
		 * @generated
		 */
		EClass STRUCT_TYPED_VARIABLES_MODULE = eINSTANCE.getStructTypedVariablesModule();

		/**
		 * The meta object literal for the '<em><b>Struct Type Variables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference STRUCT_TYPED_VARIABLES_MODULE__STRUCT_TYPE_VARIABLES = eINSTANCE.getStructTypedVariablesModule_StructTypeVariables();

	}

} //Efbt_advanced_data_definitionPackage
