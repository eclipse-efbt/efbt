/**
 */
package bird_model;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
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
 * @see bird_model.Bird_modelFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Bird_modelPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "bird_model";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/bird_model";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "bird_model";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Bird_modelPackage eINSTANCE = bird_model.impl.Bird_modelPackageImpl.init();

	/**
	 * The meta object id for the '{@link bird_model.impl.ModuleImpl <em>Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ModuleImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getModule()
	 * @generated
	 */
	int MODULE = 35;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__DEPENDENCIES = 0;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__THE_DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__LICENSE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__NAME = 3;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__VERSION = 4;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__LONG_NAME = 5;

	/**
	 * The number of structural features of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.DomainModuleImpl <em>Domain Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.DomainModuleImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getDomainModule()
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
	int DOMAIN_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__LONG_NAME = MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Domains</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE__DOMAINS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.MemberHierarchyModuleImpl <em>Member Hierarchy Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.MemberHierarchyModuleImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getMemberHierarchyModule()
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
	int MEMBER_HIERARCHY_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__LONG_NAME = MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Member Hierarchies</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES = MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Member Hierarchies Nodes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE__MEMBER_HIERARCHIES_NODES = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Member Hierarchy Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Member Hierarchy Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.MemberModuleImpl <em>Member Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.MemberModuleImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getMemberModule()
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
	int MEMBER_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__LONG_NAME = MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Members</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE__MEMBERS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Member Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Member Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.VariableModuleImpl <em>Variable Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.VariableModuleImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getVariableModule()
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
	int VARIABLE_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__LONG_NAME = MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Variables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE__VARIABLES = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Variable Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Variable Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.SubDomainModuleImpl <em>Sub Domain Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.SubDomainModuleImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getSubDomainModule()
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
	int SUB_DOMAIN_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__LONG_NAME = MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Subdomains</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE__SUBDOMAINS = MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Sub Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Sub Domain Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUB_DOMAIN_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.SMCubesCoreModelImpl <em>SM Cubes Core Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.SMCubesCoreModelImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getSMCubesCoreModel()
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
	 * The feature id for the '<em><b>Variable Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL__VARIABLE_MODULES = 1;

	/**
	 * The feature id for the '<em><b>Sub Domain Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL__SUB_DOMAIN_MODULES = 2;

	/**
	 * The feature id for the '<em><b>Domain Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL__DOMAIN_MODULES = 3;

	/**
	 * The feature id for the '<em><b>Member Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL__MEMBER_MODULES = 4;

	/**
	 * The number of structural features of the '<em>SM Cubes Core Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>SM Cubes Core Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SM_CUBES_CORE_MODEL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.DOMAINImpl <em>DOMAIN</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.DOMAINImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getDOMAIN()
	 * @generated
	 */
	int DOMAIN = 6;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__CODE = 0;

	/**
	 * The feature id for the '<em><b>Data type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DATA_TYPE = 1;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DESCRIPTION = 2;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DOMAIN_ID = 3;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__FACET_ID = 4;

	/**
	 * The feature id for the '<em><b>Is enumerated</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__IS_ENUMERATED = 5;

	/**
	 * The feature id for the '<em><b>Is reference</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__IS_REFERENCE = 6;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__MAINTENANCE_AGENCY_ID = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__NAME = 8;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN__DISPLAY_NAME = 9;

	/**
	 * The number of structural features of the '<em>DOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_FEATURE_COUNT = 10;

	/**
	 * The number of operations of the '<em>DOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DOMAIN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.FACET_COLLECTIONImpl <em>FACET COLLECTION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.FACET_COLLECTIONImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_COLLECTION()
	 * @generated
	 */
	int FACET_COLLECTION = 7;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__CODE = 0;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__FACET_ID = 1;

	/**
	 * The feature id for the '<em><b>Facet value type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__FACET_VALUE_TYPE = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION__NAME = 4;

	/**
	 * The number of structural features of the '<em>FACET COLLECTION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>FACET COLLECTION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_COLLECTION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.FACET_ENUMERATIONImpl <em>FACET ENUMERATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.FACET_ENUMERATIONImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_ENUMERATION()
	 * @generated
	 */
	int FACET_ENUMERATION = 8;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION__FACET_ID = 0;

	/**
	 * The feature id for the '<em><b>Facet type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION__FACET_TYPE = 1;

	/**
	 * The feature id for the '<em><b>Observation value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION__OBSERVATION_VALUE = 2;

	/**
	 * The number of structural features of the '<em>FACET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>FACET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_ENUMERATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.facet_typeImpl <em>facet type</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.facet_typeImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getfacet_type()
	 * @generated
	 */
	int FACET_TYPE = 9;

	/**
	 * The feature id for the '<em><b>Decimals</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__DECIMALS = 0;

	/**
	 * The feature id for the '<em><b>End Time</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__END_TIME = 1;

	/**
	 * The feature id for the '<em><b>End Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__END_VALUE = 2;

	/**
	 * The feature id for the '<em><b>Interval</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__INTERVAL = 3;

	/**
	 * The feature id for the '<em><b>Is Sequence</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__IS_SEQUENCE = 4;

	/**
	 * The feature id for the '<em><b>Max Length</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MAX_LENGTH = 5;

	/**
	 * The feature id for the '<em><b>Max Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MAX_VALUE = 6;

	/**
	 * The feature id for the '<em><b>Min Length</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MIN_LENGTH = 7;

	/**
	 * The feature id for the '<em><b>Min Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__MIN_VALUE = 8;

	/**
	 * The feature id for the '<em><b>Pattern</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__PATTERN = 9;

	/**
	 * The feature id for the '<em><b>Start Time</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__START_TIME = 10;

	/**
	 * The feature id for the '<em><b>Start Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__START_VALUE = 11;

	/**
	 * The feature id for the '<em><b>Time Interval</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE__TIME_INTERVAL = 12;

	/**
	 * The number of structural features of the '<em>facet type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE_FEATURE_COUNT = 13;

	/**
	 * The number of operations of the '<em>facet type</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FACET_TYPE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.MAINTENANCE_AGENCYImpl <em>MAINTENANCE AGENCY</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.MAINTENANCE_AGENCYImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getMAINTENANCE_AGENCY()
	 * @generated
	 */
	int MAINTENANCE_AGENCY = 10;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY__CODE = 0;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY__NAME = 2;

	/**
	 * The number of structural features of the '<em>MAINTENANCE AGENCY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>MAINTENANCE AGENCY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAINTENANCE_AGENCY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.MEMBERImpl <em>MEMBER</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.MEMBERImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getMEMBER()
	 * @generated
	 */
	int MEMBER = 11;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__DOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__MEMBER_ID = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__NAME = 5;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER__DISPLAY_NAME = 6;

	/**
	 * The number of structural features of the '<em>MEMBER</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>MEMBER</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.MEMBER_HIERARCHYImpl <em>MEMBER HIERARCHY</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.MEMBER_HIERARCHYImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getMEMBER_HIERARCHY()
	 * @generated
	 */
	int MEMBER_HIERARCHY = 12;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__DOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Member hierarchy id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY__NAME = 5;

	/**
	 * The number of structural features of the '<em>MEMBER HIERARCHY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>MEMBER HIERARCHY</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.MEMBER_HIERARCHY_NODEImpl <em>MEMBER HIERARCHY NODE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.MEMBER_HIERARCHY_NODEImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	int MEMBER_HIERARCHY_NODE = 13;

	/**
	 * The feature id for the '<em><b>Comparator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__COMPARATOR = 0;

	/**
	 * The feature id for the '<em><b>Level</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__LEVEL = 1;

	/**
	 * The feature id for the '<em><b>Member hierarchy id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID = 2;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__MEMBER_ID = 3;

	/**
	 * The feature id for the '<em><b>Operator</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__OPERATOR = 4;

	/**
	 * The feature id for the '<em><b>Parent member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID = 5;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__VALID_FROM = 6;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE__VALID_TO = 7;

	/**
	 * The number of structural features of the '<em>MEMBER HIERARCHY NODE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>MEMBER HIERARCHY NODE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_HIERARCHY_NODE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.ElementImpl <em>Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ElementImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getElement()
	 * @generated
	 */
	int ELEMENT = 26;

	/**
	 * The number of structural features of the '<em>Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ELEMENT_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ELEMENT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.NamedElementImpl <em>Named Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.NamedElementImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getNamedElement()
	 * @generated
	 */
	int NAMED_ELEMENT = 27;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__NAME = ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT_FEATURE_COUNT = ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT_OPERATION_COUNT = ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.ClassifierImpl <em>Classifier</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ClassifierImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getClassifier()
	 * @generated
	 */
	int CLASSIFIER = 25;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASSIFIER__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The number of structural features of the '<em>Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASSIFIER_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASSIFIER_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.SUBDOMAINImpl <em>SUBDOMAIN</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.SUBDOMAINImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getSUBDOMAIN()
	 * @generated
	 */
	int SUBDOMAIN = 14;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__NAME = CLASSIFIER__NAME;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__CODE = CLASSIFIER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__DESCRIPTION = CLASSIFIER_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__DOMAIN_ID = CLASSIFIER_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Facet id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__FACET_ID = CLASSIFIER_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Is listed</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__IS_LISTED = CLASSIFIER_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Is natural</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__IS_NATURAL = CLASSIFIER_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__MAINTENANCE_AGENCY_ID = CLASSIFIER_FEATURE_COUNT + 6;

	/**
	 * The feature id for the '<em><b>Subdomain id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__SUBDOMAIN_ID = CLASSIFIER_FEATURE_COUNT + 7;

	/**
	 * The feature id for the '<em><b>Items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN__ITEMS = CLASSIFIER_FEATURE_COUNT + 8;

	/**
	 * The number of structural features of the '<em>SUBDOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_FEATURE_COUNT = CLASSIFIER_FEATURE_COUNT + 9;

	/**
	 * The number of operations of the '<em>SUBDOMAIN</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_OPERATION_COUNT = CLASSIFIER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.SUBDOMAIN_ENUMERATIONImpl <em>SUBDOMAIN ENUMERATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.SUBDOMAIN_ENUMERATIONImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	int SUBDOMAIN_ENUMERATION = 15;

	/**
	 * The feature id for the '<em><b>Member id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__MEMBER_ID = 0;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__ORDER = 1;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__VALID_FROM = 2;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION__VALID_TO = 3;

	/**
	 * The number of structural features of the '<em>SUBDOMAIN ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>SUBDOMAIN ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBDOMAIN_ENUMERATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.VARIABLEImpl <em>VARIABLE</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.VARIABLEImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getVARIABLE()
	 * @generated
	 */
	int VARIABLE = 16;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Domain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__DOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__MAINTENANCE_AGENCY_ID = 3;

	/**
	 * The feature id for the '<em><b>Primary concept</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__PRIMARY_CONCEPT = 4;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__VARIABLE_ID = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__NAME = 6;

	/**
	 * The feature id for the '<em><b>Display Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE__DISPLAY_NAME = 7;

	/**
	 * The number of structural features of the '<em>VARIABLE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_FEATURE_COUNT = 8;

	/**
	 * The number of operations of the '<em>VARIABLE</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.VARIABLE_SETImpl <em>VARIABLE SET</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.VARIABLE_SETImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getVARIABLE_SET()
	 * @generated
	 */
	int VARIABLE_SET = 17;

	/**
	 * The feature id for the '<em><b>Code</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__CODE = 0;

	/**
	 * The feature id for the '<em><b>Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>Maintenance agency id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__MAINTENANCE_AGENCY_ID = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__NAME = 3;

	/**
	 * The feature id for the '<em><b>Variable set id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET__VARIABLE_SET_ID = 4;

	/**
	 * The number of structural features of the '<em>VARIABLE SET</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_FEATURE_COUNT = 5;

	/**
	 * The number of operations of the '<em>VARIABLE SET</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl <em>VARIABLE SET ENUMERATION</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.VARIABLE_SET_ENUMERATIONImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	int VARIABLE_SET_ENUMERATION = 18;

	/**
	 * The feature id for the '<em><b>Is flow</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__IS_FLOW = 0;

	/**
	 * The feature id for the '<em><b>Order</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__ORDER = 1;

	/**
	 * The feature id for the '<em><b>Subdomain id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID = 2;

	/**
	 * The feature id for the '<em><b>Valid from</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VALID_FROM = 3;

	/**
	 * The feature id for the '<em><b>Valid to</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VALID_TO = 4;

	/**
	 * The feature id for the '<em><b>Variable id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VARIABLE_ID = 5;

	/**
	 * The feature id for the '<em><b>Variable set id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID = 6;

	/**
	 * The number of structural features of the '<em>VARIABLE SET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION_FEATURE_COUNT = 7;

	/**
	 * The number of operations of the '<em>VARIABLE SET ENUMERATION</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_SET_ENUMERATION_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.TypedElementImpl <em>Typed Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.TypedElementImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getTypedElement()
	 * @generated
	 */
	int TYPED_ELEMENT = 30;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT__CLASSIFIER = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.StructuralFeatureImpl <em>Structural Feature</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.StructuralFeatureImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getStructuralFeature()
	 * @generated
	 */
	int STRUCTURAL_FEATURE = 29;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE__NAME = TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE__CLASSIFIER = TYPED_ELEMENT__CLASSIFIER;

	/**
	 * The number of structural features of the '<em>Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE_FEATURE_COUNT = TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE_OPERATION_COUNT = TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.AttributeImpl <em>Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.AttributeImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getAttribute()
	 * @generated
	 */
	int ATTRIBUTE = 19;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__NAME = STRUCTURAL_FEATURE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__CLASSIFIER = STRUCTURAL_FEATURE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__IS_PK = STRUCTURAL_FEATURE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__VARIABLE = STRUCTURAL_FEATURE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__ORDERED = STRUCTURAL_FEATURE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FEATURE_COUNT = STRUCTURAL_FEATURE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_OPERATION_COUNT = STRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.RelationshipAttributeImpl <em>Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.RelationshipAttributeImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getRelationshipAttribute()
	 * @generated
	 */
	int RELATIONSHIP_ATTRIBUTE = 20;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__NAME = ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__CLASSIFIER = ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__IS_PK = ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__VARIABLE = ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__ORDERED = ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__ENTITY = ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__CONTAINMENT = ATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__MANDATORY = ATTRIBUTE_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__DOMINANT = ATTRIBUTE_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.OneToOneRelationshipAttributeImpl <em>One To One Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.OneToOneRelationshipAttributeImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getOneToOneRelationshipAttribute()
	 * @generated
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE = 21;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__VARIABLE = RELATIONSHIP_ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>One To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>One To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.ManyToOneRelationshipAttributeImpl <em>Many To One Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ManyToOneRelationshipAttributeImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getManyToOneRelationshipAttribute()
	 * @generated
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE = 22;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__VARIABLE = RELATIONSHIP_ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>Many To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Many To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.OneToManyRelationshipAttributeImpl <em>One To Many Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.OneToManyRelationshipAttributeImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getOneToManyRelationshipAttribute()
	 * @generated
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE = 23;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__VARIABLE = RELATIONSHIP_ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>One To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>One To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.ManyToManyRelationshipAttributeImpl <em>Many To Many Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ManyToManyRelationshipAttributeImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getManyToManyRelationshipAttribute()
	 * @generated
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE = 24;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__VARIABLE = RELATIONSHIP_ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>Many To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Many To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.EntityModuleImpl <em>Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.EntityModuleImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getEntityModule()
	 * @generated
	 */
	int ENTITY_MODULE = 28;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__DEPENDENCIES = MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__THE_DESCRIPTION = MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__LICENSE = MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__NAME = MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__VERSION = MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__LONG_NAME = MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Ns URI</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__NS_URI = MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Ns Prefix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__NS_PREFIX = MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Entities</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__ENTITIES = MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE_FEATURE_COUNT = MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE_OPERATION_COUNT = MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.EntityImpl <em>Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.EntityImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getEntity()
	 * @generated
	 */
	int ENTITY = 32;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The number of structural features of the '<em>Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.GeneratedEntityImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getGeneratedEntity()
	 * @generated
	 */
	int GENERATED_ENTITY = 31;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__ATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Generated Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Generated Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.DerivedEntityImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getDerivedEntity()
	 * @generated
	 */
	int DERIVED_ENTITY = 33;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__ATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.BasicEntityImpl <em>Basic Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.BasicEntityImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getBasicEntity()
	 * @generated
	 */
	int BASIC_ENTITY = 34;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY__ATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Super Class</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY__SUPER_CLASS = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Basic Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Basic Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.ModuleLongNameImpl <em>Module Long Name</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ModuleLongNameImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getModuleLongName()
	 * @generated
	 */
	int MODULE_LONG_NAME = 36;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LONG_NAME__NAME = 0;

	/**
	 * The number of structural features of the '<em>Module Long Name</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LONG_NAME_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Module Long Name</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LONG_NAME_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.ModuleDependenciesImpl <em>Module Dependencies</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ModuleDependenciesImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getModuleDependencies()
	 * @generated
	 */
	int MODULE_DEPENDENCIES = 37;

	/**
	 * The feature id for the '<em><b>The Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCIES__THE_MODULES = 0;

	/**
	 * The number of structural features of the '<em>Module Dependencies</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCIES_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Module Dependencies</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCIES_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.ModuleDependencyImpl <em>Module Dependency</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.ModuleDependencyImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getModuleDependency()
	 * @generated
	 */
	int MODULE_DEPENDENCY = 38;

	/**
	 * The feature id for the '<em><b>Module Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__MODULE_NAME = 0;

	/**
	 * The feature id for the '<em><b>Module Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__MODULE_VERSION = 1;

	/**
	 * The feature id for the '<em><b>The Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__THE_MODULE = 2;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__LONG_NAME = 3;

	/**
	 * The number of structural features of the '<em>Module Dependency</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Module Dependency</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.impl.BIRDModelImpl <em>BIRD Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.impl.BIRDModelImpl
	 * @see bird_model.impl.Bird_modelPackageImpl#getBIRDModel()
	 * @generated
	 */
	int BIRD_MODEL = 39;

	/**
	 * The feature id for the '<em><b>Entity Module</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__ENTITY_MODULE = 0;

	/**
	 * The feature id for the '<em><b>Smcubes Core Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL__SMCUBES_CORE_MODEL = 1;

	/**
	 * The number of structural features of the '<em>BIRD Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>BIRD Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BIRD_MODEL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link bird_model.FACET_VALUE_TYPE <em>FACET VALUE TYPE</em>}' enum.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.FACET_VALUE_TYPE
	 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_VALUE_TYPE()
	 * @generated
	 */
	int FACET_VALUE_TYPE = 40;

	/**
	 * The meta object id for the '<em>FACET VALUE TYPE Object</em>' data type.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see bird_model.FACET_VALUE_TYPE
	 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_VALUE_TYPEObject()
	 * @generated
	 */
	int FACET_VALUE_TYPE_OBJECT = 41;


	/**
	 * Returns the meta object for class '{@link bird_model.DomainModule <em>Domain Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Domain Module</em>'.
	 * @see bird_model.DomainModule
	 * @generated
	 */
	EClass getDomainModule();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.DomainModule#getDomains <em>Domains</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Domains</em>'.
	 * @see bird_model.DomainModule#getDomains()
	 * @see #getDomainModule()
	 * @generated
	 */
	EReference getDomainModule_Domains();

	/**
	 * Returns the meta object for class '{@link bird_model.MemberHierarchyModule <em>Member Hierarchy Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Hierarchy Module</em>'.
	 * @see bird_model.MemberHierarchyModule
	 * @generated
	 */
	EClass getMemberHierarchyModule();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.MemberHierarchyModule#getMemberHierarchies <em>Member Hierarchies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Hierarchies</em>'.
	 * @see bird_model.MemberHierarchyModule#getMemberHierarchies()
	 * @see #getMemberHierarchyModule()
	 * @generated
	 */
	EReference getMemberHierarchyModule_MemberHierarchies();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.MemberHierarchyModule#getMemberHierarchiesNodes <em>Member Hierarchies Nodes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Hierarchies Nodes</em>'.
	 * @see bird_model.MemberHierarchyModule#getMemberHierarchiesNodes()
	 * @see #getMemberHierarchyModule()
	 * @generated
	 */
	EReference getMemberHierarchyModule_MemberHierarchiesNodes();

	/**
	 * Returns the meta object for class '{@link bird_model.MemberModule <em>Member Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Module</em>'.
	 * @see bird_model.MemberModule
	 * @generated
	 */
	EClass getMemberModule();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.MemberModule#getMembers <em>Members</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Members</em>'.
	 * @see bird_model.MemberModule#getMembers()
	 * @see #getMemberModule()
	 * @generated
	 */
	EReference getMemberModule_Members();

	/**
	 * Returns the meta object for class '{@link bird_model.VariableModule <em>Variable Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Module</em>'.
	 * @see bird_model.VariableModule
	 * @generated
	 */
	EClass getVariableModule();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.VariableModule#getVariables <em>Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Variables</em>'.
	 * @see bird_model.VariableModule#getVariables()
	 * @see #getVariableModule()
	 * @generated
	 */
	EReference getVariableModule_Variables();

	/**
	 * Returns the meta object for class '{@link bird_model.SubDomainModule <em>Sub Domain Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Sub Domain Module</em>'.
	 * @see bird_model.SubDomainModule
	 * @generated
	 */
	EClass getSubDomainModule();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.SubDomainModule#getSubdomains <em>Subdomains</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Subdomains</em>'.
	 * @see bird_model.SubDomainModule#getSubdomains()
	 * @see #getSubDomainModule()
	 * @generated
	 */
	EReference getSubDomainModule_Subdomains();

	/**
	 * Returns the meta object for class '{@link bird_model.SMCubesCoreModel <em>SM Cubes Core Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SM Cubes Core Model</em>'.
	 * @see bird_model.SMCubesCoreModel
	 * @generated
	 */
	EClass getSMCubesCoreModel();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SMCubesCoreModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.SMCubesCoreModel#getName()
	 * @see #getSMCubesCoreModel()
	 * @generated
	 */
	EAttribute getSMCubesCoreModel_Name();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.SMCubesCoreModel#getVariableModules <em>Variable Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Variable Modules</em>'.
	 * @see bird_model.SMCubesCoreModel#getVariableModules()
	 * @see #getSMCubesCoreModel()
	 * @generated
	 */
	EReference getSMCubesCoreModel_VariableModules();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.SMCubesCoreModel#getSubDomainModules <em>Sub Domain Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Sub Domain Modules</em>'.
	 * @see bird_model.SMCubesCoreModel#getSubDomainModules()
	 * @see #getSMCubesCoreModel()
	 * @generated
	 */
	EReference getSMCubesCoreModel_SubDomainModules();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.SMCubesCoreModel#getDomainModules <em>Domain Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Domain Modules</em>'.
	 * @see bird_model.SMCubesCoreModel#getDomainModules()
	 * @see #getSMCubesCoreModel()
	 * @generated
	 */
	EReference getSMCubesCoreModel_DomainModules();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.SMCubesCoreModel#getMemberModules <em>Member Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Modules</em>'.
	 * @see bird_model.SMCubesCoreModel#getMemberModules()
	 * @see #getSMCubesCoreModel()
	 * @generated
	 */
	EReference getSMCubesCoreModel_MemberModules();

	/**
	 * Returns the meta object for class '{@link bird_model.DOMAIN <em>DOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>DOMAIN</em>'.
	 * @see bird_model.DOMAIN
	 * @generated
	 */
	EClass getDOMAIN();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.DOMAIN#getCode()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#getData_type <em>Data type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Data type</em>'.
	 * @see bird_model.DOMAIN#getData_type()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Data_type();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see bird_model.DOMAIN#getDescription()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Description();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Domain id</em>'.
	 * @see bird_model.DOMAIN#getDomain_id()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.DOMAIN#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet id</em>'.
	 * @see bird_model.DOMAIN#getFacet_id()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EReference getDOMAIN_Facet_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#isIs_enumerated <em>Is enumerated</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is enumerated</em>'.
	 * @see bird_model.DOMAIN#isIs_enumerated()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Is_enumerated();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#isIs_reference <em>Is reference</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is reference</em>'.
	 * @see bird_model.DOMAIN#isIs_reference()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Is_reference();

	/**
	 * Returns the meta object for the reference '{@link bird_model.DOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see bird_model.DOMAIN#getMaintenance_agency_id()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EReference getDOMAIN_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.DOMAIN#getName()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_Name();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.DOMAIN#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see bird_model.DOMAIN#getDisplayName()
	 * @see #getDOMAIN()
	 * @generated
	 */
	EAttribute getDOMAIN_DisplayName();

	/**
	 * Returns the meta object for class '{@link bird_model.FACET_COLLECTION <em>FACET COLLECTION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>FACET COLLECTION</em>'.
	 * @see bird_model.FACET_COLLECTION
	 * @generated
	 */
	EClass getFACET_COLLECTION();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.FACET_COLLECTION#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.FACET_COLLECTION#getCode()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.FACET_COLLECTION#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Facet id</em>'.
	 * @see bird_model.FACET_COLLECTION#getFacet_id()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Facet_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.FACET_COLLECTION#getFacet_value_type <em>Facet value type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Facet value type</em>'.
	 * @see bird_model.FACET_COLLECTION#getFacet_value_type()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Facet_value_type();

	/**
	 * Returns the meta object for the reference '{@link bird_model.FACET_COLLECTION#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see bird_model.FACET_COLLECTION#getMaintenance_agency_id()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EReference getFACET_COLLECTION_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.FACET_COLLECTION#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.FACET_COLLECTION#getName()
	 * @see #getFACET_COLLECTION()
	 * @generated
	 */
	EAttribute getFACET_COLLECTION_Name();

	/**
	 * Returns the meta object for class '{@link bird_model.FACET_ENUMERATION <em>FACET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>FACET ENUMERATION</em>'.
	 * @see bird_model.FACET_ENUMERATION
	 * @generated
	 */
	EClass getFACET_ENUMERATION();

	/**
	 * Returns the meta object for the reference '{@link bird_model.FACET_ENUMERATION#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet id</em>'.
	 * @see bird_model.FACET_ENUMERATION#getFacet_id()
	 * @see #getFACET_ENUMERATION()
	 * @generated
	 */
	EReference getFACET_ENUMERATION_Facet_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.FACET_ENUMERATION#getFacet_type <em>Facet type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet type</em>'.
	 * @see bird_model.FACET_ENUMERATION#getFacet_type()
	 * @see #getFACET_ENUMERATION()
	 * @generated
	 */
	EReference getFACET_ENUMERATION_Facet_type();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.FACET_ENUMERATION#getObservation_value <em>Observation value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Observation value</em>'.
	 * @see bird_model.FACET_ENUMERATION#getObservation_value()
	 * @see #getFACET_ENUMERATION()
	 * @generated
	 */
	EAttribute getFACET_ENUMERATION_Observation_value();

	/**
	 * Returns the meta object for class '{@link bird_model.facet_type <em>facet type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>facet type</em>'.
	 * @see bird_model.facet_type
	 * @generated
	 */
	EClass getfacet_type();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getDecimals <em>Decimals</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Decimals</em>'.
	 * @see bird_model.facet_type#getDecimals()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_Decimals();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getEndTime <em>End Time</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>End Time</em>'.
	 * @see bird_model.facet_type#getEndTime()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_EndTime();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getEndValue <em>End Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>End Value</em>'.
	 * @see bird_model.facet_type#getEndValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_EndValue();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getInterval <em>Interval</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Interval</em>'.
	 * @see bird_model.facet_type#getInterval()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_Interval();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getIsSequence <em>Is Sequence</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Sequence</em>'.
	 * @see bird_model.facet_type#getIsSequence()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_IsSequence();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getMaxLength <em>Max Length</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Max Length</em>'.
	 * @see bird_model.facet_type#getMaxLength()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MaxLength();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getMaxValue <em>Max Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Max Value</em>'.
	 * @see bird_model.facet_type#getMaxValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MaxValue();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getMinLength <em>Min Length</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Min Length</em>'.
	 * @see bird_model.facet_type#getMinLength()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MinLength();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getMinValue <em>Min Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Min Value</em>'.
	 * @see bird_model.facet_type#getMinValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_MinValue();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getPattern <em>Pattern</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Pattern</em>'.
	 * @see bird_model.facet_type#getPattern()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_Pattern();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getStartTime <em>Start Time</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Start Time</em>'.
	 * @see bird_model.facet_type#getStartTime()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_StartTime();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getStartValue <em>Start Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Start Value</em>'.
	 * @see bird_model.facet_type#getStartValue()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_StartValue();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.facet_type#getTimeInterval <em>Time Interval</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Time Interval</em>'.
	 * @see bird_model.facet_type#getTimeInterval()
	 * @see #getfacet_type()
	 * @generated
	 */
	EAttribute getfacet_type_TimeInterval();

	/**
	 * Returns the meta object for class '{@link bird_model.MAINTENANCE_AGENCY <em>MAINTENANCE AGENCY</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MAINTENANCE AGENCY</em>'.
	 * @see bird_model.MAINTENANCE_AGENCY
	 * @generated
	 */
	EClass getMAINTENANCE_AGENCY();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MAINTENANCE_AGENCY#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.MAINTENANCE_AGENCY#getCode()
	 * @see #getMAINTENANCE_AGENCY()
	 * @generated
	 */
	EAttribute getMAINTENANCE_AGENCY_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MAINTENANCE_AGENCY#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Maintenance agency id</em>'.
	 * @see bird_model.MAINTENANCE_AGENCY#getMaintenance_agency_id()
	 * @see #getMAINTENANCE_AGENCY()
	 * @generated
	 */
	EAttribute getMAINTENANCE_AGENCY_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MAINTENANCE_AGENCY#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.MAINTENANCE_AGENCY#getName()
	 * @see #getMAINTENANCE_AGENCY()
	 * @generated
	 */
	EAttribute getMAINTENANCE_AGENCY_Name();

	/**
	 * Returns the meta object for class '{@link bird_model.MEMBER <em>MEMBER</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER</em>'.
	 * @see bird_model.MEMBER
	 * @generated
	 */
	EClass getMEMBER();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.MEMBER#getCode()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see bird_model.MEMBER#getDescription()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Description();

	/**
	 * Returns the meta object for the reference '{@link bird_model.MEMBER#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see bird_model.MEMBER#getDomain_id()
	 * @see #getMEMBER()
	 * @generated
	 */
	EReference getMEMBER_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.MEMBER#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see bird_model.MEMBER#getMaintenance_agency_id()
	 * @see #getMEMBER()
	 * @generated
	 */
	EReference getMEMBER_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Member id</em>'.
	 * @see bird_model.MEMBER#getMember_id()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Member_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.MEMBER#getName()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_Name();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see bird_model.MEMBER#getDisplayName()
	 * @see #getMEMBER()
	 * @generated
	 */
	EAttribute getMEMBER_DisplayName();

	/**
	 * Returns the meta object for class '{@link bird_model.MEMBER_HIERARCHY <em>MEMBER HIERARCHY</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER HIERARCHY</em>'.
	 * @see bird_model.MEMBER_HIERARCHY
	 * @generated
	 */
	EClass getMEMBER_HIERARCHY();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.MEMBER_HIERARCHY#getCode()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see bird_model.MEMBER_HIERARCHY#getDescription()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Description();

	/**
	 * Returns the meta object for the reference '{@link bird_model.MEMBER_HIERARCHY#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see bird_model.MEMBER_HIERARCHY#getDomain_id()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.MEMBER_HIERARCHY#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see bird_model.MEMBER_HIERARCHY#getMaintenance_agency_id()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY#getMember_hierarchy_id <em>Member hierarchy id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Member hierarchy id</em>'.
	 * @see bird_model.MEMBER_HIERARCHY#getMember_hierarchy_id()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Member_hierarchy_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.MEMBER_HIERARCHY#getName()
	 * @see #getMEMBER_HIERARCHY()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_Name();

	/**
	 * Returns the meta object for class '{@link bird_model.MEMBER_HIERARCHY_NODE <em>MEMBER HIERARCHY NODE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>MEMBER HIERARCHY NODE</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE
	 * @generated
	 */
	EClass getMEMBER_HIERARCHY_NODE();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY_NODE#getComparator <em>Comparator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Comparator</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getComparator()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Comparator();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY_NODE#getLevel <em>Level</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Level</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getLevel()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Level();

	/**
	 * Returns the meta object for the reference '{@link bird_model.MEMBER_HIERARCHY_NODE#getMember_hierarchy_id <em>Member hierarchy id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member hierarchy id</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getMember_hierarchy_id()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_NODE_Member_hierarchy_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.MEMBER_HIERARCHY_NODE#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member id</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getMember_id()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_NODE_Member_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY_NODE#getOperator <em>Operator</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Operator</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getOperator()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Operator();

	/**
	 * Returns the meta object for the reference '{@link bird_model.MEMBER_HIERARCHY_NODE#getParent_member_id <em>Parent member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Parent member id</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getParent_member_id()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EReference getMEMBER_HIERARCHY_NODE_Parent_member_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY_NODE#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getValid_from()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.MEMBER_HIERARCHY_NODE#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see bird_model.MEMBER_HIERARCHY_NODE#getValid_to()
	 * @see #getMEMBER_HIERARCHY_NODE()
	 * @generated
	 */
	EAttribute getMEMBER_HIERARCHY_NODE_Valid_to();

	/**
	 * Returns the meta object for class '{@link bird_model.SUBDOMAIN <em>SUBDOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SUBDOMAIN</em>'.
	 * @see bird_model.SUBDOMAIN
	 * @generated
	 */
	EClass getSUBDOMAIN();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.SUBDOMAIN#getCode()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see bird_model.SUBDOMAIN#getDescription()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Description();

	/**
	 * Returns the meta object for the reference '{@link bird_model.SUBDOMAIN#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see bird_model.SUBDOMAIN#getDomain_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.SUBDOMAIN#getFacet_id <em>Facet id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Facet id</em>'.
	 * @see bird_model.SUBDOMAIN#getFacet_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Facet_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN#isIs_listed <em>Is listed</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is listed</em>'.
	 * @see bird_model.SUBDOMAIN#isIs_listed()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Is_listed();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN#isIs_natural <em>Is natural</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is natural</em>'.
	 * @see bird_model.SUBDOMAIN#isIs_natural()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Is_natural();

	/**
	 * Returns the meta object for the reference '{@link bird_model.SUBDOMAIN#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see bird_model.SUBDOMAIN#getMaintenance_agency_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN#getSubdomain_id <em>Subdomain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Subdomain id</em>'.
	 * @see bird_model.SUBDOMAIN#getSubdomain_id()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_Subdomain_id();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.SUBDOMAIN#getItems <em>Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Items</em>'.
	 * @see bird_model.SUBDOMAIN#getItems()
	 * @see #getSUBDOMAIN()
	 * @generated
	 */
	EReference getSUBDOMAIN_Items();

	/**
	 * Returns the meta object for class '{@link bird_model.SUBDOMAIN_ENUMERATION <em>SUBDOMAIN ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>SUBDOMAIN ENUMERATION</em>'.
	 * @see bird_model.SUBDOMAIN_ENUMERATION
	 * @generated
	 */
	EClass getSUBDOMAIN_ENUMERATION();

	/**
	 * Returns the meta object for the reference '{@link bird_model.SUBDOMAIN_ENUMERATION#getMember_id <em>Member id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Member id</em>'.
	 * @see bird_model.SUBDOMAIN_ENUMERATION#getMember_id()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EReference getSUBDOMAIN_ENUMERATION_Member_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN_ENUMERATION#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see bird_model.SUBDOMAIN_ENUMERATION#getOrder()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_ENUMERATION_Order();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN_ENUMERATION#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see bird_model.SUBDOMAIN_ENUMERATION#getValid_from()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_ENUMERATION_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.SUBDOMAIN_ENUMERATION#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see bird_model.SUBDOMAIN_ENUMERATION#getValid_to()
	 * @see #getSUBDOMAIN_ENUMERATION()
	 * @generated
	 */
	EAttribute getSUBDOMAIN_ENUMERATION_Valid_to();

	/**
	 * Returns the meta object for class '{@link bird_model.VARIABLE <em>VARIABLE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE</em>'.
	 * @see bird_model.VARIABLE
	 * @generated
	 */
	EClass getVARIABLE();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.VARIABLE#getCode()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see bird_model.VARIABLE#getDescription()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Description();

	/**
	 * Returns the meta object for the reference '{@link bird_model.VARIABLE#getDomain_id <em>Domain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Domain id</em>'.
	 * @see bird_model.VARIABLE#getDomain_id()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EReference getVARIABLE_Domain_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.VARIABLE#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see bird_model.VARIABLE#getMaintenance_agency_id()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EReference getVARIABLE_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE#getPrimary_concept <em>Primary concept</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Primary concept</em>'.
	 * @see bird_model.VARIABLE#getPrimary_concept()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Primary_concept();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE#getVariable_id <em>Variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Variable id</em>'.
	 * @see bird_model.VARIABLE#getVariable_id()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Variable_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.VARIABLE#getName()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_Name();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE#getDisplayName <em>Display Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Display Name</em>'.
	 * @see bird_model.VARIABLE#getDisplayName()
	 * @see #getVARIABLE()
	 * @generated
	 */
	EAttribute getVARIABLE_DisplayName();

	/**
	 * Returns the meta object for class '{@link bird_model.VARIABLE_SET <em>VARIABLE SET</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE SET</em>'.
	 * @see bird_model.VARIABLE_SET
	 * @generated
	 */
	EClass getVARIABLE_SET();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET#getCode <em>Code</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Code</em>'.
	 * @see bird_model.VARIABLE_SET#getCode()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Code();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET#getDescription <em>Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Description</em>'.
	 * @see bird_model.VARIABLE_SET#getDescription()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Description();

	/**
	 * Returns the meta object for the reference '{@link bird_model.VARIABLE_SET#getMaintenance_agency_id <em>Maintenance agency id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Maintenance agency id</em>'.
	 * @see bird_model.VARIABLE_SET#getMaintenance_agency_id()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EReference getVARIABLE_SET_Maintenance_agency_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.VARIABLE_SET#getName()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Name();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET#getVariable_set_id <em>Variable set id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Variable set id</em>'.
	 * @see bird_model.VARIABLE_SET#getVariable_set_id()
	 * @see #getVARIABLE_SET()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_Variable_set_id();

	/**
	 * Returns the meta object for class '{@link bird_model.VARIABLE_SET_ENUMERATION <em>VARIABLE SET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>VARIABLE SET ENUMERATION</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION
	 * @generated
	 */
	EClass getVARIABLE_SET_ENUMERATION();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET_ENUMERATION#isIs_flow <em>Is flow</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is flow</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION#isIs_flow()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Is_flow();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET_ENUMERATION#getOrder <em>Order</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Order</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION#getOrder()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Order();

	/**
	 * Returns the meta object for the reference '{@link bird_model.VARIABLE_SET_ENUMERATION#getSubdomain_id <em>Subdomain id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Subdomain id</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION#getSubdomain_id()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EReference getVARIABLE_SET_ENUMERATION_Subdomain_id();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET_ENUMERATION#getValid_from <em>Valid from</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid from</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION#getValid_from()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Valid_from();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.VARIABLE_SET_ENUMERATION#getValid_to <em>Valid to</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Valid to</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION#getValid_to()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EAttribute getVARIABLE_SET_ENUMERATION_Valid_to();

	/**
	 * Returns the meta object for the reference '{@link bird_model.VARIABLE_SET_ENUMERATION#getVariable_id <em>Variable id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable id</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION#getVariable_id()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EReference getVARIABLE_SET_ENUMERATION_Variable_id();

	/**
	 * Returns the meta object for the reference '{@link bird_model.VARIABLE_SET_ENUMERATION#getVariable_set_id <em>Variable set id</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable set id</em>'.
	 * @see bird_model.VARIABLE_SET_ENUMERATION#getVariable_set_id()
	 * @see #getVARIABLE_SET_ENUMERATION()
	 * @generated
	 */
	EReference getVARIABLE_SET_ENUMERATION_Variable_set_id();

	/**
	 * Returns the meta object for class '{@link bird_model.Attribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute</em>'.
	 * @see bird_model.Attribute
	 * @generated
	 */
	EClass getAttribute();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.Attribute#isIsPK <em>Is PK</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is PK</em>'.
	 * @see bird_model.Attribute#isIsPK()
	 * @see #getAttribute()
	 * @generated
	 */
	EAttribute getAttribute_IsPK();

	/**
	 * Returns the meta object for the reference '{@link bird_model.Attribute#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see bird_model.Attribute#getVariable()
	 * @see #getAttribute()
	 * @generated
	 */
	EReference getAttribute_Variable();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.Attribute#isOrdered <em>Ordered</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ordered</em>'.
	 * @see bird_model.Attribute#isOrdered()
	 * @see #getAttribute()
	 * @generated
	 */
	EAttribute getAttribute_Ordered();

	/**
	 * Returns the meta object for class '{@link bird_model.RelationshipAttribute <em>Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Relationship Attribute</em>'.
	 * @see bird_model.RelationshipAttribute
	 * @generated
	 */
	EClass getRelationshipAttribute();

	/**
	 * Returns the meta object for the reference '{@link bird_model.RelationshipAttribute#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see bird_model.RelationshipAttribute#getEntity()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EReference getRelationshipAttribute_Entity();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.RelationshipAttribute#isContainment <em>Containment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Containment</em>'.
	 * @see bird_model.RelationshipAttribute#isContainment()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EAttribute getRelationshipAttribute_Containment();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.RelationshipAttribute#isMandatory <em>Mandatory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Mandatory</em>'.
	 * @see bird_model.RelationshipAttribute#isMandatory()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EAttribute getRelationshipAttribute_Mandatory();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.RelationshipAttribute#isDominant <em>Dominant</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Dominant</em>'.
	 * @see bird_model.RelationshipAttribute#isDominant()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EAttribute getRelationshipAttribute_Dominant();

	/**
	 * Returns the meta object for class '{@link bird_model.OneToOneRelationshipAttribute <em>One To One Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>One To One Relationship Attribute</em>'.
	 * @see bird_model.OneToOneRelationshipAttribute
	 * @generated
	 */
	EClass getOneToOneRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link bird_model.ManyToOneRelationshipAttribute <em>Many To One Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Many To One Relationship Attribute</em>'.
	 * @see bird_model.ManyToOneRelationshipAttribute
	 * @generated
	 */
	EClass getManyToOneRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link bird_model.OneToManyRelationshipAttribute <em>One To Many Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>One To Many Relationship Attribute</em>'.
	 * @see bird_model.OneToManyRelationshipAttribute
	 * @generated
	 */
	EClass getOneToManyRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link bird_model.ManyToManyRelationshipAttribute <em>Many To Many Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Many To Many Relationship Attribute</em>'.
	 * @see bird_model.ManyToManyRelationshipAttribute
	 * @generated
	 */
	EClass getManyToManyRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link bird_model.Classifier <em>Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Classifier</em>'.
	 * @see bird_model.Classifier
	 * @generated
	 */
	EClass getClassifier();

	/**
	 * Returns the meta object for class '{@link bird_model.Element <em>Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Element</em>'.
	 * @see bird_model.Element
	 * @generated
	 */
	EClass getElement();

	/**
	 * Returns the meta object for class '{@link bird_model.NamedElement <em>Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Named Element</em>'.
	 * @see bird_model.NamedElement
	 * @generated
	 */
	EClass getNamedElement();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.NamedElement#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.NamedElement#getName()
	 * @see #getNamedElement()
	 * @generated
	 */
	EAttribute getNamedElement_Name();

	/**
	 * Returns the meta object for class '{@link bird_model.EntityModule <em>Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity Module</em>'.
	 * @see bird_model.EntityModule
	 * @generated
	 */
	EClass getEntityModule();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.EntityModule#getNsURI <em>Ns URI</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns URI</em>'.
	 * @see bird_model.EntityModule#getNsURI()
	 * @see #getEntityModule()
	 * @generated
	 */
	EAttribute getEntityModule_NsURI();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.EntityModule#getNsPrefix <em>Ns Prefix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns Prefix</em>'.
	 * @see bird_model.EntityModule#getNsPrefix()
	 * @see #getEntityModule()
	 * @generated
	 */
	EAttribute getEntityModule_NsPrefix();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.EntityModule#getEntities <em>Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Entities</em>'.
	 * @see bird_model.EntityModule#getEntities()
	 * @see #getEntityModule()
	 * @generated
	 */
	EReference getEntityModule_Entities();

	/**
	 * Returns the meta object for class '{@link bird_model.StructuralFeature <em>Structural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Structural Feature</em>'.
	 * @see bird_model.StructuralFeature
	 * @generated
	 */
	EClass getStructuralFeature();

	/**
	 * Returns the meta object for class '{@link bird_model.TypedElement <em>Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Typed Element</em>'.
	 * @see bird_model.TypedElement
	 * @generated
	 */
	EClass getTypedElement();

	/**
	 * Returns the meta object for the reference '{@link bird_model.TypedElement#getClassifier <em>Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Classifier</em>'.
	 * @see bird_model.TypedElement#getClassifier()
	 * @see #getTypedElement()
	 * @generated
	 */
	EReference getTypedElement_Classifier();

	/**
	 * Returns the meta object for class '{@link bird_model.GeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generated Entity</em>'.
	 * @see bird_model.GeneratedEntity
	 * @generated
	 */
	EClass getGeneratedEntity();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.GeneratedEntity#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see bird_model.GeneratedEntity#getAttributes()
	 * @see #getGeneratedEntity()
	 * @generated
	 */
	EReference getGeneratedEntity_Attributes();

	/**
	 * Returns the meta object for class '{@link bird_model.Entity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity</em>'.
	 * @see bird_model.Entity
	 * @generated
	 */
	EClass getEntity();

	/**
	 * Returns the meta object for class '{@link bird_model.DerivedEntity <em>Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Entity</em>'.
	 * @see bird_model.DerivedEntity
	 * @generated
	 */
	EClass getDerivedEntity();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.DerivedEntity#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see bird_model.DerivedEntity#getAttributes()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_Attributes();

	/**
	 * Returns the meta object for class '{@link bird_model.BasicEntity <em>Basic Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Entity</em>'.
	 * @see bird_model.BasicEntity
	 * @generated
	 */
	EClass getBasicEntity();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.BasicEntity#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see bird_model.BasicEntity#getAttributes()
	 * @see #getBasicEntity()
	 * @generated
	 */
	EReference getBasicEntity_Attributes();

	/**
	 * Returns the meta object for the reference '{@link bird_model.BasicEntity#getSuperClass <em>Super Class</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Super Class</em>'.
	 * @see bird_model.BasicEntity#getSuperClass()
	 * @see #getBasicEntity()
	 * @generated
	 */
	EReference getBasicEntity_SuperClass();

	/**
	 * Returns the meta object for class '{@link bird_model.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module</em>'.
	 * @see bird_model.Module
	 * @generated
	 */
	EClass getModule();

	/**
	 * Returns the meta object for the containment reference '{@link bird_model.Module#getDependencies <em>Dependencies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Dependencies</em>'.
	 * @see bird_model.Module#getDependencies()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_Dependencies();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.Module#getTheDescription <em>The Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>The Description</em>'.
	 * @see bird_model.Module#getTheDescription()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_TheDescription();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.Module#getLicense <em>License</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>License</em>'.
	 * @see bird_model.Module#getLicense()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_License();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.Module#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.Module#getName()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Name();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.Module#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see bird_model.Module#getVersion()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Version();

	/**
	 * Returns the meta object for the containment reference '{@link bird_model.Module#getLongName <em>Long Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Long Name</em>'.
	 * @see bird_model.Module#getLongName()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_LongName();

	/**
	 * Returns the meta object for class '{@link bird_model.ModuleLongName <em>Module Long Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module Long Name</em>'.
	 * @see bird_model.ModuleLongName
	 * @generated
	 */
	EClass getModuleLongName();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.ModuleLongName#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see bird_model.ModuleLongName#getName()
	 * @see #getModuleLongName()
	 * @generated
	 */
	EAttribute getModuleLongName_Name();

	/**
	 * Returns the meta object for class '{@link bird_model.ModuleDependencies <em>Module Dependencies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module Dependencies</em>'.
	 * @see bird_model.ModuleDependencies
	 * @generated
	 */
	EClass getModuleDependencies();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.ModuleDependencies#getTheModules <em>The Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>The Modules</em>'.
	 * @see bird_model.ModuleDependencies#getTheModules()
	 * @see #getModuleDependencies()
	 * @generated
	 */
	EReference getModuleDependencies_TheModules();

	/**
	 * Returns the meta object for class '{@link bird_model.ModuleDependency <em>Module Dependency</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module Dependency</em>'.
	 * @see bird_model.ModuleDependency
	 * @generated
	 */
	EClass getModuleDependency();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.ModuleDependency#getModuleName <em>Module Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Module Name</em>'.
	 * @see bird_model.ModuleDependency#getModuleName()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EAttribute getModuleDependency_ModuleName();

	/**
	 * Returns the meta object for the attribute '{@link bird_model.ModuleDependency#getModuleVersion <em>Module Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Module Version</em>'.
	 * @see bird_model.ModuleDependency#getModuleVersion()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EAttribute getModuleDependency_ModuleVersion();

	/**
	 * Returns the meta object for the reference '{@link bird_model.ModuleDependency#getTheModule <em>The Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>The Module</em>'.
	 * @see bird_model.ModuleDependency#getTheModule()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EReference getModuleDependency_TheModule();

	/**
	 * Returns the meta object for the reference '{@link bird_model.ModuleDependency#getLongName <em>Long Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Long Name</em>'.
	 * @see bird_model.ModuleDependency#getLongName()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EReference getModuleDependency_LongName();

	/**
	 * Returns the meta object for class '{@link bird_model.BIRDModel <em>BIRD Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>BIRD Model</em>'.
	 * @see bird_model.BIRDModel
	 * @generated
	 */
	EClass getBIRDModel();

	/**
	 * Returns the meta object for the containment reference list '{@link bird_model.BIRDModel#getEntityModule <em>Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Entity Module</em>'.
	 * @see bird_model.BIRDModel#getEntityModule()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_EntityModule();

	/**
	 * Returns the meta object for the containment reference '{@link bird_model.BIRDModel#getSmcubesCoreModel <em>Smcubes Core Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Smcubes Core Model</em>'.
	 * @see bird_model.BIRDModel#getSmcubesCoreModel()
	 * @see #getBIRDModel()
	 * @generated
	 */
	EReference getBIRDModel_SmcubesCoreModel();

	/**
	 * Returns the meta object for enum '{@link bird_model.FACET_VALUE_TYPE <em>FACET VALUE TYPE</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for enum '<em>FACET VALUE TYPE</em>'.
	 * @see bird_model.FACET_VALUE_TYPE
	 * @generated
	 */
	EEnum getFACET_VALUE_TYPE();

	/**
	 * Returns the meta object for data type '{@link bird_model.FACET_VALUE_TYPE <em>FACET VALUE TYPE Object</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for data type '<em>FACET VALUE TYPE Object</em>'.
	 * @see bird_model.FACET_VALUE_TYPE
	 * @model instanceClass="bird_model.FACET_VALUE_TYPE"
	 *        extendedMetaData="name='FACET_VALUE_TYPE:Object' baseType='FACET_VALUE_TYPE'"
	 * @generated
	 */
	EDataType getFACET_VALUE_TYPEObject();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Bird_modelFactory getBird_modelFactory();

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
		 * The meta object literal for the '{@link bird_model.impl.DomainModuleImpl <em>Domain Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.DomainModuleImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getDomainModule()
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
		 * The meta object literal for the '{@link bird_model.impl.MemberHierarchyModuleImpl <em>Member Hierarchy Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.MemberHierarchyModuleImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getMemberHierarchyModule()
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
		 * The meta object literal for the '{@link bird_model.impl.MemberModuleImpl <em>Member Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.MemberModuleImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getMemberModule()
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
		 * The meta object literal for the '{@link bird_model.impl.VariableModuleImpl <em>Variable Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.VariableModuleImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getVariableModule()
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
		 * The meta object literal for the '{@link bird_model.impl.SubDomainModuleImpl <em>Sub Domain Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.SubDomainModuleImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getSubDomainModule()
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
		 * The meta object literal for the '{@link bird_model.impl.SMCubesCoreModelImpl <em>SM Cubes Core Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.SMCubesCoreModelImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getSMCubesCoreModel()
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

		/**
		 * The meta object literal for the '<em><b>Variable Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_CORE_MODEL__VARIABLE_MODULES = eINSTANCE.getSMCubesCoreModel_VariableModules();

		/**
		 * The meta object literal for the '<em><b>Sub Domain Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_CORE_MODEL__SUB_DOMAIN_MODULES = eINSTANCE.getSMCubesCoreModel_SubDomainModules();

		/**
		 * The meta object literal for the '<em><b>Domain Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_CORE_MODEL__DOMAIN_MODULES = eINSTANCE.getSMCubesCoreModel_DomainModules();

		/**
		 * The meta object literal for the '<em><b>Member Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SM_CUBES_CORE_MODEL__MEMBER_MODULES = eINSTANCE.getSMCubesCoreModel_MemberModules();

		/**
		 * The meta object literal for the '{@link bird_model.impl.DOMAINImpl <em>DOMAIN</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.DOMAINImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getDOMAIN()
		 * @generated
		 */
		EClass DOMAIN = eINSTANCE.getDOMAIN();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__CODE = eINSTANCE.getDOMAIN_Code();

		/**
		 * The meta object literal for the '<em><b>Data type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DATA_TYPE = eINSTANCE.getDOMAIN_Data_type();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DESCRIPTION = eINSTANCE.getDOMAIN_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DOMAIN_ID = eINSTANCE.getDOMAIN_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DOMAIN__FACET_ID = eINSTANCE.getDOMAIN_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Is enumerated</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__IS_ENUMERATED = eINSTANCE.getDOMAIN_Is_enumerated();

		/**
		 * The meta object literal for the '<em><b>Is reference</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__IS_REFERENCE = eINSTANCE.getDOMAIN_Is_reference();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DOMAIN__MAINTENANCE_AGENCY_ID = eINSTANCE.getDOMAIN_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__NAME = eINSTANCE.getDOMAIN_Name();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute DOMAIN__DISPLAY_NAME = eINSTANCE.getDOMAIN_DisplayName();

		/**
		 * The meta object literal for the '{@link bird_model.impl.FACET_COLLECTIONImpl <em>FACET COLLECTION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.FACET_COLLECTIONImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_COLLECTION()
		 * @generated
		 */
		EClass FACET_COLLECTION = eINSTANCE.getFACET_COLLECTION();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__CODE = eINSTANCE.getFACET_COLLECTION_Code();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__FACET_ID = eINSTANCE.getFACET_COLLECTION_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Facet value type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__FACET_VALUE_TYPE = eINSTANCE.getFACET_COLLECTION_Facet_value_type();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FACET_COLLECTION__MAINTENANCE_AGENCY_ID = eINSTANCE.getFACET_COLLECTION_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_COLLECTION__NAME = eINSTANCE.getFACET_COLLECTION_Name();

		/**
		 * The meta object literal for the '{@link bird_model.impl.FACET_ENUMERATIONImpl <em>FACET ENUMERATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.FACET_ENUMERATIONImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_ENUMERATION()
		 * @generated
		 */
		EClass FACET_ENUMERATION = eINSTANCE.getFACET_ENUMERATION();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FACET_ENUMERATION__FACET_ID = eINSTANCE.getFACET_ENUMERATION_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Facet type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FACET_ENUMERATION__FACET_TYPE = eINSTANCE.getFACET_ENUMERATION_Facet_type();

		/**
		 * The meta object literal for the '<em><b>Observation value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_ENUMERATION__OBSERVATION_VALUE = eINSTANCE.getFACET_ENUMERATION_Observation_value();

		/**
		 * The meta object literal for the '{@link bird_model.impl.facet_typeImpl <em>facet type</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.facet_typeImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getfacet_type()
		 * @generated
		 */
		EClass FACET_TYPE = eINSTANCE.getfacet_type();

		/**
		 * The meta object literal for the '<em><b>Decimals</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__DECIMALS = eINSTANCE.getfacet_type_Decimals();

		/**
		 * The meta object literal for the '<em><b>End Time</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__END_TIME = eINSTANCE.getfacet_type_EndTime();

		/**
		 * The meta object literal for the '<em><b>End Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__END_VALUE = eINSTANCE.getfacet_type_EndValue();

		/**
		 * The meta object literal for the '<em><b>Interval</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__INTERVAL = eINSTANCE.getfacet_type_Interval();

		/**
		 * The meta object literal for the '<em><b>Is Sequence</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__IS_SEQUENCE = eINSTANCE.getfacet_type_IsSequence();

		/**
		 * The meta object literal for the '<em><b>Max Length</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MAX_LENGTH = eINSTANCE.getfacet_type_MaxLength();

		/**
		 * The meta object literal for the '<em><b>Max Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MAX_VALUE = eINSTANCE.getfacet_type_MaxValue();

		/**
		 * The meta object literal for the '<em><b>Min Length</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MIN_LENGTH = eINSTANCE.getfacet_type_MinLength();

		/**
		 * The meta object literal for the '<em><b>Min Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__MIN_VALUE = eINSTANCE.getfacet_type_MinValue();

		/**
		 * The meta object literal for the '<em><b>Pattern</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__PATTERN = eINSTANCE.getfacet_type_Pattern();

		/**
		 * The meta object literal for the '<em><b>Start Time</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__START_TIME = eINSTANCE.getfacet_type_StartTime();

		/**
		 * The meta object literal for the '<em><b>Start Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__START_VALUE = eINSTANCE.getfacet_type_StartValue();

		/**
		 * The meta object literal for the '<em><b>Time Interval</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FACET_TYPE__TIME_INTERVAL = eINSTANCE.getfacet_type_TimeInterval();

		/**
		 * The meta object literal for the '{@link bird_model.impl.MAINTENANCE_AGENCYImpl <em>MAINTENANCE AGENCY</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.MAINTENANCE_AGENCYImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getMAINTENANCE_AGENCY()
		 * @generated
		 */
		EClass MAINTENANCE_AGENCY = eINSTANCE.getMAINTENANCE_AGENCY();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAINTENANCE_AGENCY__CODE = eINSTANCE.getMAINTENANCE_AGENCY_Code();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID = eINSTANCE.getMAINTENANCE_AGENCY_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MAINTENANCE_AGENCY__NAME = eINSTANCE.getMAINTENANCE_AGENCY_Name();

		/**
		 * The meta object literal for the '{@link bird_model.impl.MEMBERImpl <em>MEMBER</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.MEMBERImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getMEMBER()
		 * @generated
		 */
		EClass MEMBER = eINSTANCE.getMEMBER();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__CODE = eINSTANCE.getMEMBER_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__DESCRIPTION = eINSTANCE.getMEMBER_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER__DOMAIN_ID = eINSTANCE.getMEMBER_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER__MAINTENANCE_AGENCY_ID = eINSTANCE.getMEMBER_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__MEMBER_ID = eINSTANCE.getMEMBER_Member_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__NAME = eINSTANCE.getMEMBER_Name();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER__DISPLAY_NAME = eINSTANCE.getMEMBER_DisplayName();

		/**
		 * The meta object literal for the '{@link bird_model.impl.MEMBER_HIERARCHYImpl <em>MEMBER HIERARCHY</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.MEMBER_HIERARCHYImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getMEMBER_HIERARCHY()
		 * @generated
		 */
		EClass MEMBER_HIERARCHY = eINSTANCE.getMEMBER_HIERARCHY();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__CODE = eINSTANCE.getMEMBER_HIERARCHY_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__DESCRIPTION = eINSTANCE.getMEMBER_HIERARCHY_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY__DOMAIN_ID = eINSTANCE.getMEMBER_HIERARCHY_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID = eINSTANCE.getMEMBER_HIERARCHY_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Member hierarchy id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID = eINSTANCE.getMEMBER_HIERARCHY_Member_hierarchy_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY__NAME = eINSTANCE.getMEMBER_HIERARCHY_Name();

		/**
		 * The meta object literal for the '{@link bird_model.impl.MEMBER_HIERARCHY_NODEImpl <em>MEMBER HIERARCHY NODE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.MEMBER_HIERARCHY_NODEImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getMEMBER_HIERARCHY_NODE()
		 * @generated
		 */
		EClass MEMBER_HIERARCHY_NODE = eINSTANCE.getMEMBER_HIERARCHY_NODE();

		/**
		 * The meta object literal for the '<em><b>Comparator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__COMPARATOR = eINSTANCE.getMEMBER_HIERARCHY_NODE_Comparator();

		/**
		 * The meta object literal for the '<em><b>Level</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__LEVEL = eINSTANCE.getMEMBER_HIERARCHY_NODE_Level();

		/**
		 * The meta object literal for the '<em><b>Member hierarchy id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID = eINSTANCE.getMEMBER_HIERARCHY_NODE_Member_hierarchy_id();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_NODE__MEMBER_ID = eINSTANCE.getMEMBER_HIERARCHY_NODE_Member_id();

		/**
		 * The meta object literal for the '<em><b>Operator</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__OPERATOR = eINSTANCE.getMEMBER_HIERARCHY_NODE_Operator();

		/**
		 * The meta object literal for the '<em><b>Parent member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID = eINSTANCE.getMEMBER_HIERARCHY_NODE_Parent_member_id();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__VALID_FROM = eINSTANCE.getMEMBER_HIERARCHY_NODE_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MEMBER_HIERARCHY_NODE__VALID_TO = eINSTANCE.getMEMBER_HIERARCHY_NODE_Valid_to();

		/**
		 * The meta object literal for the '{@link bird_model.impl.SUBDOMAINImpl <em>SUBDOMAIN</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.SUBDOMAINImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getSUBDOMAIN()
		 * @generated
		 */
		EClass SUBDOMAIN = eINSTANCE.getSUBDOMAIN();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__CODE = eINSTANCE.getSUBDOMAIN_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__DESCRIPTION = eINSTANCE.getSUBDOMAIN_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__DOMAIN_ID = eINSTANCE.getSUBDOMAIN_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Facet id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__FACET_ID = eINSTANCE.getSUBDOMAIN_Facet_id();

		/**
		 * The meta object literal for the '<em><b>Is listed</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__IS_LISTED = eINSTANCE.getSUBDOMAIN_Is_listed();

		/**
		 * The meta object literal for the '<em><b>Is natural</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__IS_NATURAL = eINSTANCE.getSUBDOMAIN_Is_natural();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__MAINTENANCE_AGENCY_ID = eINSTANCE.getSUBDOMAIN_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Subdomain id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN__SUBDOMAIN_ID = eINSTANCE.getSUBDOMAIN_Subdomain_id();

		/**
		 * The meta object literal for the '<em><b>Items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN__ITEMS = eINSTANCE.getSUBDOMAIN_Items();

		/**
		 * The meta object literal for the '{@link bird_model.impl.SUBDOMAIN_ENUMERATIONImpl <em>SUBDOMAIN ENUMERATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.SUBDOMAIN_ENUMERATIONImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getSUBDOMAIN_ENUMERATION()
		 * @generated
		 */
		EClass SUBDOMAIN_ENUMERATION = eINSTANCE.getSUBDOMAIN_ENUMERATION();

		/**
		 * The meta object literal for the '<em><b>Member id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SUBDOMAIN_ENUMERATION__MEMBER_ID = eINSTANCE.getSUBDOMAIN_ENUMERATION_Member_id();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN_ENUMERATION__ORDER = eINSTANCE.getSUBDOMAIN_ENUMERATION_Order();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN_ENUMERATION__VALID_FROM = eINSTANCE.getSUBDOMAIN_ENUMERATION_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SUBDOMAIN_ENUMERATION__VALID_TO = eINSTANCE.getSUBDOMAIN_ENUMERATION_Valid_to();

		/**
		 * The meta object literal for the '{@link bird_model.impl.VARIABLEImpl <em>VARIABLE</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.VARIABLEImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getVARIABLE()
		 * @generated
		 */
		EClass VARIABLE = eINSTANCE.getVARIABLE();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__CODE = eINSTANCE.getVARIABLE_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__DESCRIPTION = eINSTANCE.getVARIABLE_Description();

		/**
		 * The meta object literal for the '<em><b>Domain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE__DOMAIN_ID = eINSTANCE.getVARIABLE_Domain_id();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE__MAINTENANCE_AGENCY_ID = eINSTANCE.getVARIABLE_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Primary concept</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__PRIMARY_CONCEPT = eINSTANCE.getVARIABLE_Primary_concept();

		/**
		 * The meta object literal for the '<em><b>Variable id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__VARIABLE_ID = eINSTANCE.getVARIABLE_Variable_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__NAME = eINSTANCE.getVARIABLE_Name();

		/**
		 * The meta object literal for the '<em><b>Display Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE__DISPLAY_NAME = eINSTANCE.getVARIABLE_DisplayName();

		/**
		 * The meta object literal for the '{@link bird_model.impl.VARIABLE_SETImpl <em>VARIABLE SET</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.VARIABLE_SETImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getVARIABLE_SET()
		 * @generated
		 */
		EClass VARIABLE_SET = eINSTANCE.getVARIABLE_SET();

		/**
		 * The meta object literal for the '<em><b>Code</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__CODE = eINSTANCE.getVARIABLE_SET_Code();

		/**
		 * The meta object literal for the '<em><b>Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__DESCRIPTION = eINSTANCE.getVARIABLE_SET_Description();

		/**
		 * The meta object literal for the '<em><b>Maintenance agency id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET__MAINTENANCE_AGENCY_ID = eINSTANCE.getVARIABLE_SET_Maintenance_agency_id();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__NAME = eINSTANCE.getVARIABLE_SET_Name();

		/**
		 * The meta object literal for the '<em><b>Variable set id</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET__VARIABLE_SET_ID = eINSTANCE.getVARIABLE_SET_Variable_set_id();

		/**
		 * The meta object literal for the '{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl <em>VARIABLE SET ENUMERATION</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.VARIABLE_SET_ENUMERATIONImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getVARIABLE_SET_ENUMERATION()
		 * @generated
		 */
		EClass VARIABLE_SET_ENUMERATION = eINSTANCE.getVARIABLE_SET_ENUMERATION();

		/**
		 * The meta object literal for the '<em><b>Is flow</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__IS_FLOW = eINSTANCE.getVARIABLE_SET_ENUMERATION_Is_flow();

		/**
		 * The meta object literal for the '<em><b>Order</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__ORDER = eINSTANCE.getVARIABLE_SET_ENUMERATION_Order();

		/**
		 * The meta object literal for the '<em><b>Subdomain id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID = eINSTANCE.getVARIABLE_SET_ENUMERATION_Subdomain_id();

		/**
		 * The meta object literal for the '<em><b>Valid from</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__VALID_FROM = eINSTANCE.getVARIABLE_SET_ENUMERATION_Valid_from();

		/**
		 * The meta object literal for the '<em><b>Valid to</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VARIABLE_SET_ENUMERATION__VALID_TO = eINSTANCE.getVARIABLE_SET_ENUMERATION_Valid_to();

		/**
		 * The meta object literal for the '<em><b>Variable id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_ENUMERATION__VARIABLE_ID = eINSTANCE.getVARIABLE_SET_ENUMERATION_Variable_id();

		/**
		 * The meta object literal for the '<em><b>Variable set id</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID = eINSTANCE.getVARIABLE_SET_ENUMERATION_Variable_set_id();

		/**
		 * The meta object literal for the '{@link bird_model.impl.AttributeImpl <em>Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.AttributeImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getAttribute()
		 * @generated
		 */
		EClass ATTRIBUTE = eINSTANCE.getAttribute();

		/**
		 * The meta object literal for the '<em><b>Is PK</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE__IS_PK = eINSTANCE.getAttribute_IsPK();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE__VARIABLE = eINSTANCE.getAttribute_Variable();

		/**
		 * The meta object literal for the '<em><b>Ordered</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE__ORDERED = eINSTANCE.getAttribute_Ordered();

		/**
		 * The meta object literal for the '{@link bird_model.impl.RelationshipAttributeImpl <em>Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.RelationshipAttributeImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getRelationshipAttribute()
		 * @generated
		 */
		EClass RELATIONSHIP_ATTRIBUTE = eINSTANCE.getRelationshipAttribute();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RELATIONSHIP_ATTRIBUTE__ENTITY = eINSTANCE.getRelationshipAttribute_Entity();

		/**
		 * The meta object literal for the '<em><b>Containment</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RELATIONSHIP_ATTRIBUTE__CONTAINMENT = eINSTANCE.getRelationshipAttribute_Containment();

		/**
		 * The meta object literal for the '<em><b>Mandatory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RELATIONSHIP_ATTRIBUTE__MANDATORY = eINSTANCE.getRelationshipAttribute_Mandatory();

		/**
		 * The meta object literal for the '<em><b>Dominant</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RELATIONSHIP_ATTRIBUTE__DOMINANT = eINSTANCE.getRelationshipAttribute_Dominant();

		/**
		 * The meta object literal for the '{@link bird_model.impl.OneToOneRelationshipAttributeImpl <em>One To One Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.OneToOneRelationshipAttributeImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getOneToOneRelationshipAttribute()
		 * @generated
		 */
		EClass ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getOneToOneRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ManyToOneRelationshipAttributeImpl <em>Many To One Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ManyToOneRelationshipAttributeImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getManyToOneRelationshipAttribute()
		 * @generated
		 */
		EClass MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getManyToOneRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link bird_model.impl.OneToManyRelationshipAttributeImpl <em>One To Many Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.OneToManyRelationshipAttributeImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getOneToManyRelationshipAttribute()
		 * @generated
		 */
		EClass ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getOneToManyRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ManyToManyRelationshipAttributeImpl <em>Many To Many Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ManyToManyRelationshipAttributeImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getManyToManyRelationshipAttribute()
		 * @generated
		 */
		EClass MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getManyToManyRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ClassifierImpl <em>Classifier</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ClassifierImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getClassifier()
		 * @generated
		 */
		EClass CLASSIFIER = eINSTANCE.getClassifier();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ElementImpl <em>Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ElementImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getElement()
		 * @generated
		 */
		EClass ELEMENT = eINSTANCE.getElement();

		/**
		 * The meta object literal for the '{@link bird_model.impl.NamedElementImpl <em>Named Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.NamedElementImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getNamedElement()
		 * @generated
		 */
		EClass NAMED_ELEMENT = eINSTANCE.getNamedElement();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute NAMED_ELEMENT__NAME = eINSTANCE.getNamedElement_Name();

		/**
		 * The meta object literal for the '{@link bird_model.impl.EntityModuleImpl <em>Entity Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.EntityModuleImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getEntityModule()
		 * @generated
		 */
		EClass ENTITY_MODULE = eINSTANCE.getEntityModule();

		/**
		 * The meta object literal for the '<em><b>Ns URI</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ENTITY_MODULE__NS_URI = eINSTANCE.getEntityModule_NsURI();

		/**
		 * The meta object literal for the '<em><b>Ns Prefix</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ENTITY_MODULE__NS_PREFIX = eINSTANCE.getEntityModule_NsPrefix();

		/**
		 * The meta object literal for the '<em><b>Entities</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENTITY_MODULE__ENTITIES = eINSTANCE.getEntityModule_Entities();

		/**
		 * The meta object literal for the '{@link bird_model.impl.StructuralFeatureImpl <em>Structural Feature</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.StructuralFeatureImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getStructuralFeature()
		 * @generated
		 */
		EClass STRUCTURAL_FEATURE = eINSTANCE.getStructuralFeature();

		/**
		 * The meta object literal for the '{@link bird_model.impl.TypedElementImpl <em>Typed Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.TypedElementImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getTypedElement()
		 * @generated
		 */
		EClass TYPED_ELEMENT = eINSTANCE.getTypedElement();

		/**
		 * The meta object literal for the '<em><b>Classifier</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPED_ELEMENT__CLASSIFIER = eINSTANCE.getTypedElement_Classifier();

		/**
		 * The meta object literal for the '{@link bird_model.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.GeneratedEntityImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getGeneratedEntity()
		 * @generated
		 */
		EClass GENERATED_ENTITY = eINSTANCE.getGeneratedEntity();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERATED_ENTITY__ATTRIBUTES = eINSTANCE.getGeneratedEntity_Attributes();

		/**
		 * The meta object literal for the '{@link bird_model.impl.EntityImpl <em>Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.EntityImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getEntity()
		 * @generated
		 */
		EClass ENTITY = eINSTANCE.getEntity();

		/**
		 * The meta object literal for the '{@link bird_model.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.DerivedEntityImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getDerivedEntity()
		 * @generated
		 */
		EClass DERIVED_ENTITY = eINSTANCE.getDerivedEntity();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__ATTRIBUTES = eINSTANCE.getDerivedEntity_Attributes();

		/**
		 * The meta object literal for the '{@link bird_model.impl.BasicEntityImpl <em>Basic Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.BasicEntityImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getBasicEntity()
		 * @generated
		 */
		EClass BASIC_ENTITY = eINSTANCE.getBasicEntity();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_ENTITY__ATTRIBUTES = eINSTANCE.getBasicEntity_Attributes();

		/**
		 * The meta object literal for the '<em><b>Super Class</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_ENTITY__SUPER_CLASS = eINSTANCE.getBasicEntity_SuperClass();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ModuleImpl <em>Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ModuleImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getModule()
		 * @generated
		 */
		EClass MODULE = eINSTANCE.getModule();

		/**
		 * The meta object literal for the '<em><b>Dependencies</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE__DEPENDENCIES = eINSTANCE.getModule_Dependencies();

		/**
		 * The meta object literal for the '<em><b>The Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__THE_DESCRIPTION = eINSTANCE.getModule_TheDescription();

		/**
		 * The meta object literal for the '<em><b>License</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__LICENSE = eINSTANCE.getModule_License();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__NAME = eINSTANCE.getModule_Name();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__VERSION = eINSTANCE.getModule_Version();

		/**
		 * The meta object literal for the '<em><b>Long Name</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE__LONG_NAME = eINSTANCE.getModule_LongName();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ModuleLongNameImpl <em>Module Long Name</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ModuleLongNameImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getModuleLongName()
		 * @generated
		 */
		EClass MODULE_LONG_NAME = eINSTANCE.getModuleLongName();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE_LONG_NAME__NAME = eINSTANCE.getModuleLongName_Name();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ModuleDependenciesImpl <em>Module Dependencies</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ModuleDependenciesImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getModuleDependencies()
		 * @generated
		 */
		EClass MODULE_DEPENDENCIES = eINSTANCE.getModuleDependencies();

		/**
		 * The meta object literal for the '<em><b>The Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE_DEPENDENCIES__THE_MODULES = eINSTANCE.getModuleDependencies_TheModules();

		/**
		 * The meta object literal for the '{@link bird_model.impl.ModuleDependencyImpl <em>Module Dependency</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.ModuleDependencyImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getModuleDependency()
		 * @generated
		 */
		EClass MODULE_DEPENDENCY = eINSTANCE.getModuleDependency();

		/**
		 * The meta object literal for the '<em><b>Module Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE_DEPENDENCY__MODULE_NAME = eINSTANCE.getModuleDependency_ModuleName();

		/**
		 * The meta object literal for the '<em><b>Module Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE_DEPENDENCY__MODULE_VERSION = eINSTANCE.getModuleDependency_ModuleVersion();

		/**
		 * The meta object literal for the '<em><b>The Module</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE_DEPENDENCY__THE_MODULE = eINSTANCE.getModuleDependency_TheModule();

		/**
		 * The meta object literal for the '<em><b>Long Name</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE_DEPENDENCY__LONG_NAME = eINSTANCE.getModuleDependency_LongName();

		/**
		 * The meta object literal for the '{@link bird_model.impl.BIRDModelImpl <em>BIRD Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.impl.BIRDModelImpl
		 * @see bird_model.impl.Bird_modelPackageImpl#getBIRDModel()
		 * @generated
		 */
		EClass BIRD_MODEL = eINSTANCE.getBIRDModel();

		/**
		 * The meta object literal for the '<em><b>Entity Module</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__ENTITY_MODULE = eINSTANCE.getBIRDModel_EntityModule();

		/**
		 * The meta object literal for the '<em><b>Smcubes Core Model</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BIRD_MODEL__SMCUBES_CORE_MODEL = eINSTANCE.getBIRDModel_SmcubesCoreModel();

		/**
		 * The meta object literal for the '{@link bird_model.FACET_VALUE_TYPE <em>FACET VALUE TYPE</em>}' enum.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.FACET_VALUE_TYPE
		 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_VALUE_TYPE()
		 * @generated
		 */
		EEnum FACET_VALUE_TYPE = eINSTANCE.getFACET_VALUE_TYPE();

		/**
		 * The meta object literal for the '<em>FACET VALUE TYPE Object</em>' data type.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see bird_model.FACET_VALUE_TYPE
		 * @see bird_model.impl.Bird_modelPackageImpl#getFACET_VALUE_TYPEObject()
		 * @generated
		 */
		EDataType FACET_VALUE_TYPE_OBJECT = eINSTANCE.getFACET_VALUE_TYPEObject();

	}

} //Bird_modelPackage
