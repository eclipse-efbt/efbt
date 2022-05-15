/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

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
 *  This package describes a class which composes the diferent modules, whereby each of those modules composes  a list of items from the ECBS SMCubes model, which we represent as the core package and the data_definition package. 
 *  
 *  
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface openregspecs_smcubes_core_extensionPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "openregspecs_smcubes_core_extension";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/openregspecs_smcubes_core_extension";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "openregspecs_smcubes_core_extension";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	openregspecs_smcubes_core_extensionPackage eINSTANCE = org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.DomainModuleImpl <em>Domain Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.DomainModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getDomainModule()
	 * @generated
	 */
	int DOMAIN_MODULE = 0;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberHierarchyModuleImpl <em>Member Hierarchy Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberHierarchyModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getMemberHierarchyModule()
	 * @generated
	 */
	int MEMBER_HIERARCHY_MODULE = 1;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberModuleImpl <em>Member Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getMemberModule()
	 * @generated
	 */
	int MEMBER_MODULE = 2;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.VariableModuleImpl <em>Variable Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.VariableModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getVariableModule()
	 * @generated
	 */
	int VARIABLE_MODULE = 3;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SubDomainModuleImpl <em>Sub Domain Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SubDomainModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getSubDomainModule()
	 * @generated
	 */
	int SUB_DOMAIN_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Subdomains</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__SUBDOMAINS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Sub Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Sub Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SMCubesCoreModelImpl <em>SM Cubes Core Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SMCubesCoreModelImpl
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getSMCubesCoreModel()
	 * @generated
	 */
	int SM_CUBES_CORE_MODEL = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL__NAME = 0;

	/**
	 * The number of structural features of the '<em>SM Cubes Core Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>SM Cubes Core Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.DomainModule <em>Domain Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Domain Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.DomainModule
	 * @generated
	 */
	EClass getDomainModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.DomainModule#getDomains <em>Domains</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Domains</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.DomainModule#getDomains()
	 * @see #getDomainModule()
	 * @generated
	 */
	EReference getDomainModule_Domains();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule <em>Member Hierarchy Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Hierarchy Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule
	 * @generated
	 */
	EClass getMemberHierarchyModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule#getMemberHierarchies <em>Member Hierarchies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Hierarchies</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule#getMemberHierarchies()
	 * @see #getMemberHierarchyModule()
	 * @generated
	 */
	EReference getMemberHierarchyModule_MemberHierarchies();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule#getMemberHierarchiesNodes <em>Member Hierarchies Nodes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Hierarchies Nodes</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberHierarchyModule#getMemberHierarchiesNodes()
	 * @see #getMemberHierarchyModule()
	 * @generated
	 */
	EReference getMemberHierarchyModule_MemberHierarchiesNodes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberModule <em>Member Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberModule
	 * @generated
	 */
	EClass getMemberModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberModule#getMembers <em>Members</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Members</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.MemberModule#getMembers()
	 * @see #getMemberModule()
	 * @generated
	 */
	EReference getMemberModule_Members();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.VariableModule <em>Variable Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.VariableModule
	 * @generated
	 */
	EClass getVariableModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.VariableModule#getVariables <em>Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Variables</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.VariableModule#getVariables()
	 * @see #getVariableModule()
	 * @generated
	 */
	EReference getVariableModule_Variables();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SubDomainModule <em>Sub Domain Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Sub Domain Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SubDomainModule
	 * @generated
	 */
	EClass getSubDomainModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SubDomainModule#getSubdomains <em>Subdomains</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Subdomains</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SubDomainModule#getSubdomains()
	 * @see #getSubDomainModule()
	 * @generated
	 */
	EReference getSubDomainModule_Subdomains();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel <em>SM Cubes Core Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Core Model</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel
	 * @generated
	 */
	EClass getSMCubesCoreModel();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel#getName()
	 * @see #getSMCubesCoreModel()
	 * @generated
	 */
	EAttribute getSMCubesCoreModel_Name();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	openregspecs_smcubes_core_extensionFactory getopenregspecs_smcubes_core_extensionFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.DomainModuleImpl <em>Domain Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.DomainModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getDomainModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberHierarchyModuleImpl <em>Member Hierarchy Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberHierarchyModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getMemberHierarchyModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberModuleImpl <em>Member Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.MemberModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getMemberModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.VariableModuleImpl <em>Variable Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.VariableModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getVariableModule()
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

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SubDomainModuleImpl <em>Sub Domain Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SubDomainModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getSubDomainModule()
		 * @generated
		 */
		EClass SUB_DOMAIN_MODULE = eINSTANCE.getSubDomainModule();

		/**
		 * The meta object literal for the '<em><b>Subdomains</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUB_DOMAIN_MODULE__SUBDOMAINS = eINSTANCE.getSubDomainModule_Subdomains();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SMCubesCoreModelImpl <em>SM Cubes Core Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.SMCubesCoreModelImpl
		 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl.openregspecs_smcubes_core_extensionPackageImpl#getSMCubesCoreModel()
		 * @generated
		 */
		EClass SM_CUBES_CORE_MODEL = eINSTANCE.getSMCubesCoreModel();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SM_CUBES_CORE_MODEL__NAME = eINSTANCE.getSMCubesCoreModel_Name();

	}

} //openregspecs_smcubes_core_extensionPackage
