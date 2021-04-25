/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

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
 *  This package describes various modules which define groups of structures from the data_definiton package
 *  
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Efbt_data_definitionPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "efbt_data_definition";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/efbt_data_definition";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "efbt_data_definition";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Efbt_data_definitionPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CombinationModuleImpl <em>Combination Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CombinationModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getCombinationModule()
	 * @generated
	 */
	int COMBINATION_MODULE = 0;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Combinations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__COMBINATIONS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Combination items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__COMBINATION_ITEMS = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Cube To Combination Links</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Combination Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Combination Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int COMBINATION_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CubeModuleImpl <em>Cube Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CubeModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getCubeModule()
	 * @generated
	 */
	int CUBE_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Cubes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__CUBES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube Structures</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__CUBE_STRUCTURES = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Cube Structure Items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE__CUBE_STRUCTURE_ITEMS = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Cube Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Cube Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.DomainModuleImpl <em>Domain Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.DomainModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getDomainModule()
	 * @generated
	 */
	int DOMAIN_MODULE = 2;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Domains</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__DOMAINS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberHierarchyModuleImpl <em>Member Hierarchy Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberHierarchyModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getMemberHierarchyModule()
	 * @generated
	 */
	int MEMBER_HIERARCHY_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Member Hierarchies</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Member Hierarchies Nodes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Member Hierarchy Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Member Hierarchy Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberModuleImpl <em>Member Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getMemberModule()
	 * @generated
	 */
	int MEMBER_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Members</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__MEMBERS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Member Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Member Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.VariableModuleImpl <em>Variable Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.VariableModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getVariableModule()
	 * @generated
	 */
	int VARIABLE_MODULE = 5;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Variables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__VARIABLES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Variable Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Variable Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule <em>Combination Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Combination Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule
	 * @generated
	 */
	EClass getCombinationModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCombinations <em>Combinations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Combinations</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCombinations()
	 * @see #getCombinationModule()
	 * @generated
	 */
	EReference getCombinationModule_Combinations();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCombination_items <em>Combination items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Combination items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCombination_items()
	 * @see #getCombinationModule()
	 * @generated
	 */
	EReference getCombinationModule_Combination_items();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCubeToCombinationLinks <em>Cube To Combination Links</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube To Combination Links</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule#getCubeToCombinationLinks()
	 * @see #getCombinationModule()
	 * @generated
	 */
	EReference getCombinationModule_CubeToCombinationLinks();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule <em>Cube Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule
	 * @generated
	 */
	EClass getCubeModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubes <em>Cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cubes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubes()
	 * @see #getCubeModule()
	 * @generated
	 */
	EReference getCubeModule_Cubes();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubeStructures <em>Cube Structures</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube Structures</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubeStructures()
	 * @see #getCubeModule()
	 * @generated
	 */
	EReference getCubeModule_CubeStructures();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubeStructureItems <em>Cube Structure Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube Structure Items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule#getCubeStructureItems()
	 * @see #getCubeModule()
	 * @generated
	 */
	EReference getCubeModule_CubeStructureItems();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule <em>Domain Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Domain Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule
	 * @generated
	 */
	EClass getDomainModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule#getDomains <em>Domains</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Domains</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule#getDomains()
	 * @see #getDomainModule()
	 * @generated
	 */
	EReference getDomainModule_Domains();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberHierarchyModule <em>Member Hierarchy Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Hierarchy Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberHierarchyModule
	 * @generated
	 */
	EClass getMemberHierarchyModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberHierarchyModule#getMemberHierarchies <em>Member Hierarchies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Hierarchies</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberHierarchyModule#getMemberHierarchies()
	 * @see #getMemberHierarchyModule()
	 * @generated
	 */
	EReference getMemberHierarchyModule_MemberHierarchies();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberHierarchyModule#getMemberHierarchiesNodes <em>Member Hierarchies Nodes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Hierarchies Nodes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberHierarchyModule#getMemberHierarchiesNodes()
	 * @see #getMemberHierarchyModule()
	 * @generated
	 */
	EReference getMemberHierarchyModule_MemberHierarchiesNodes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule <em>Member Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule
	 * @generated
	 */
	EClass getMemberModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule#getMembers <em>Members</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Members</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule#getMembers()
	 * @see #getMemberModule()
	 * @generated
	 */
	EReference getMemberModule_Members();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule <em>Variable Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule
	 * @generated
	 */
	EClass getVariableModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule#getVariables <em>Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Variables</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule#getVariables()
	 * @see #getVariableModule()
	 * @generated
	 */
	EReference getVariableModule_Variables();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Efbt_data_definitionFactory getEfbt_data_definitionFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CombinationModuleImpl <em>Combination Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CombinationModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getCombinationModule()
		 * @generated
		 */
		EClass COMBINATION_MODULE = eINSTANCE.getCombinationModule();

		/**
		 * The meta object literal for the '<em><b>Combinations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION_MODULE__COMBINATIONS = eINSTANCE.getCombinationModule_Combinations();

		/**
		 * The meta object literal for the '<em><b>Combination items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION_MODULE__COMBINATION_ITEMS = eINSTANCE.getCombinationModule_Combination_items();

		/**
		 * The meta object literal for the '<em><b>Cube To Combination Links</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference COMBINATION_MODULE__CUBE_TO_COMBINATION_LINKS = eINSTANCE.getCombinationModule_CubeToCombinationLinks();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CubeModuleImpl <em>Cube Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.CubeModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getCubeModule()
		 * @generated
		 */
		EClass CUBE_MODULE = eINSTANCE.getCubeModule();

		/**
		 * The meta object literal for the '<em><b>Cubes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_MODULE__CUBES = eINSTANCE.getCubeModule_Cubes();

		/**
		 * The meta object literal for the '<em><b>Cube Structures</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_MODULE__CUBE_STRUCTURES = eINSTANCE.getCubeModule_CubeStructures();

		/**
		 * The meta object literal for the '<em><b>Cube Structure Items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_MODULE__CUBE_STRUCTURE_ITEMS = eINSTANCE.getCubeModule_CubeStructureItems();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.DomainModuleImpl <em>Domain Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.DomainModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getDomainModule()
		 * @generated
		 */
		EClass DOMAIN_MODULE = eINSTANCE.getDomainModule();

		/**
		 * The meta object literal for the '<em><b>Domains</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DOMAIN_MODULE__DOMAINS = eINSTANCE.getDomainModule_Domains();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberHierarchyModuleImpl <em>Member Hierarchy Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberHierarchyModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getMemberHierarchyModule()
		 * @generated
		 */
		EClass MEMBER_HIERARCHY_MODULE = eINSTANCE.getMemberHierarchyModule();

		/**
		 * The meta object literal for the '<em><b>Member Hierarchies</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES = eINSTANCE.getMemberHierarchyModule_MemberHierarchies();

		/**
		 * The meta object literal for the '<em><b>Member Hierarchies Nodes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES = eINSTANCE.getMemberHierarchyModule_MemberHierarchiesNodes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberModuleImpl <em>Member Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.MemberModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getMemberModule()
		 * @generated
		 */
		EClass MEMBER_MODULE = eINSTANCE.getMemberModule();

		/**
		 * The meta object literal for the '<em><b>Members</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_MODULE__MEMBERS = eINSTANCE.getMemberModule_Members();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.VariableModuleImpl <em>Variable Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.VariableModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl.Efbt_data_definitionPackageImpl#getVariableModule()
		 * @generated
		 */
		EClass VARIABLE_MODULE = eINSTANCE.getVariableModule();

		/**
		 * The meta object literal for the '<em><b>Variables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_MODULE__VARIABLES = eINSTANCE.getVariableModule_Variables();

	}

} //Efbt_data_definitionPackage
