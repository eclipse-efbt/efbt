/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension;

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
 *  This package describes a class which composes the diferent modules, whereby each of those modules composes  a list of items from the ECBS SMCubes model, which we represent as the core package and the data_definition package. 
 *  
 *  
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Cocalimo_smcubes_extra_extensionPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "cocalimo_smcubes_extra_extension";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/cocalimo_smcubes_extra_extension";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "cocalimo_smcubes_extra_extension";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Cocalimo_smcubes_extra_extensionPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.SmcubesExtraModelImpl <em>Smcubes Extra Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.SmcubesExtraModelImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getSmcubesExtraModel()
	 * @generated
	 */
	int SMCUBES_EXTRA_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_EXTRA_MODEL__NAME = 0;

	/**
	 * The number of structural features of the '<em>Smcubes Extra Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_EXTRA_MODEL_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Smcubes Extra Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SMCUBES_EXTRA_MODEL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CombinationModuleImpl <em>Combination Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CombinationModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCombinationModule()
	 * @generated
	 */
	int COMBINATION_MODULE = 1;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeModuleImpl <em>Cube Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCubeModule()
	 * @generated
	 */
	int CUBE_MODULE = 2;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MappingDefinitionModuleImpl <em>Mapping Definition Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MappingDefinitionModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getMappingDefinitionModule()
	 * @generated
	 */
	int MAPPING_DEFINITION_MODULE = 3;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Mappings</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE__MAPPINGS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Mapping Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Mapping Definition Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MAPPING_DEFINITION_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeMappingModuleImpl <em>Cube Mapping Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeMappingModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCubeMappingModule()
	 * @generated
	 */
	int CUBE_MAPPING_MODULE = 4;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Cube Mappings</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE__CUBE_MAPPINGS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Cube Mapping Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Cube Mapping Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_MAPPING_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MemberMappingModuleImpl <em>Member Mapping Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MemberMappingModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getMemberMappingModule()
	 * @generated
	 */
	int MEMBER_MAPPING_MODULE = 5;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Member Mappings</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Member Mapping Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Member Mapping Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_MAPPING_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.VariableMappingModuleImpl <em>Variable Mapping Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.VariableMappingModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getVariableMappingModule()
	 * @generated
	 */
	int VARIABLE_MAPPING_MODULE = 6;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Variable Mappings</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Variable Mapping Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Variable Mapping Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VARIABLE_MAPPING_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisModuleImpl <em>Axis Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getAxisModule()
	 * @generated
	 */
	int AXIS_MODULE = 7;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Axes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE__AXES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Axis Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Axis Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisOrdinateModuleImpl <em>Axis Ordinate Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisOrdinateModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getAxisOrdinateModule()
	 * @generated
	 */
	int AXIS_ORDINATE_MODULE = 8;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Axis ordinates</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE__AXIS_ORDINATES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Axis Ordinate Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Axis Ordinate Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AXIS_ORDINATE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CellPositionModuleImpl <em>Cell Position Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CellPositionModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCellPositionModule()
	 * @generated
	 */
	int CELL_POSITION_MODULE = 9;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Report Tables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE__REPORT_TABLES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Cell Position Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Cell Position Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELL_POSITION_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.OrdinateItemModuleImpl <em>Ordinate Item Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.OrdinateItemModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getOrdinateItemModule()
	 * @generated
	 */
	int ORDINATE_ITEM_MODULE = 10;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Ordinate Items</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE__ORDINATE_ITEMS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Ordinate Item Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Ordinate Item Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ORDINATE_ITEM_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellImpl <em>Report Cell</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getReportCell()
	 * @generated
	 */
	int REPORT_CELL = 11;

	/**
	 * The feature id for the '<em><b>Combination</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL__COMBINATION = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL__NAME = 1;

	/**
	 * The number of structural features of the '<em>Report Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Report Cell</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellValueImpl <em>Report Cell Value</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellValueImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getReportCellValue()
	 * @generated
	 */
	int REPORT_CELL_VALUE = 12;

	/**
	 * The feature id for the '<em><b>Report Cell</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VALUE__REPORT_CELL = 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VALUE__VALUE = 1;

	/**
	 * The number of structural features of the '<em>Report Cell Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VALUE_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Report Cell Value</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_CELL_VALUE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportTableModuleImpl <em>Report Table Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportTableModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getReportTableModule()
	 * @generated
	 */
	int REPORT_TABLE_MODULE = 13;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Report Tables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE__REPORT_TABLES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Report Table Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Report Table Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int REPORT_TABLE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.TableCellModuleImpl <em>Table Cell Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.TableCellModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getTableCellModule()
	 * @generated
	 */
	int TABLE_CELL_MODULE = 14;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Table Cells</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE__TABLE_CELLS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Table Cell Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Table Cell Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TABLE_CELL_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.SmcubesExtraModel <em>Smcubes Extra Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Smcubes Extra Model</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.SmcubesExtraModel
	 * @generated
	 */
	EClass getSmcubesExtraModel();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.SmcubesExtraModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.SmcubesExtraModel#getName()
	 * @see #getSmcubesExtraModel()
	 * @generated
	 */
	EAttribute getSmcubesExtraModel_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule <em>Combination Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Combination Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule
	 * @generated
	 */
	EClass getCombinationModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule#getCombinations <em>Combinations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Combinations</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule#getCombinations()
	 * @see #getCombinationModule()
	 * @generated
	 */
	EReference getCombinationModule_Combinations();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule#getCombination_items <em>Combination items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Combination items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule#getCombination_items()
	 * @see #getCombinationModule()
	 * @generated
	 */
	EReference getCombinationModule_Combination_items();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule#getCubeToCombinationLinks <em>Cube To Combination Links</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube To Combination Links</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CombinationModule#getCubeToCombinationLinks()
	 * @see #getCombinationModule()
	 * @generated
	 */
	EReference getCombinationModule_CubeToCombinationLinks();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule <em>Cube Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule
	 * @generated
	 */
	EClass getCubeModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule#getCubes <em>Cubes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cubes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule#getCubes()
	 * @see #getCubeModule()
	 * @generated
	 */
	EReference getCubeModule_Cubes();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule#getCubeStructures <em>Cube Structures</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube Structures</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule#getCubeStructures()
	 * @see #getCubeModule()
	 * @generated
	 */
	EReference getCubeModule_CubeStructures();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule#getCubeStructureItems <em>Cube Structure Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube Structure Items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeModule#getCubeStructureItems()
	 * @see #getCubeModule()
	 * @generated
	 */
	EReference getCubeModule_CubeStructureItems();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MappingDefinitionModule <em>Mapping Definition Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Mapping Definition Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MappingDefinitionModule
	 * @generated
	 */
	EClass getMappingDefinitionModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MappingDefinitionModule#getMappings <em>Mappings</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Mappings</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MappingDefinitionModule#getMappings()
	 * @see #getMappingDefinitionModule()
	 * @generated
	 */
	EReference getMappingDefinitionModule_Mappings();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeMappingModule <em>Cube Mapping Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Mapping Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeMappingModule
	 * @generated
	 */
	EClass getCubeMappingModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeMappingModule#getCubeMappings <em>Cube Mappings</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Cube Mappings</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CubeMappingModule#getCubeMappings()
	 * @see #getCubeMappingModule()
	 * @generated
	 */
	EReference getCubeMappingModule_CubeMappings();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MemberMappingModule <em>Member Mapping Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Mapping Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MemberMappingModule
	 * @generated
	 */
	EClass getMemberMappingModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MemberMappingModule#getMemberMappings <em>Member Mappings</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Member Mappings</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.MemberMappingModule#getMemberMappings()
	 * @see #getMemberMappingModule()
	 * @generated
	 */
	EReference getMemberMappingModule_MemberMappings();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.VariableMappingModule <em>Variable Mapping Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Variable Mapping Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.VariableMappingModule
	 * @generated
	 */
	EClass getVariableMappingModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.VariableMappingModule#getVariableMappings <em>Variable Mappings</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Variable Mappings</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.VariableMappingModule#getVariableMappings()
	 * @see #getVariableMappingModule()
	 * @generated
	 */
	EReference getVariableMappingModule_VariableMappings();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisModule <em>Axis Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Axis Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisModule
	 * @generated
	 */
	EClass getAxisModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisModule#getAxes <em>Axes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Axes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisModule#getAxes()
	 * @see #getAxisModule()
	 * @generated
	 */
	EReference getAxisModule_Axes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisOrdinateModule <em>Axis Ordinate Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Axis Ordinate Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisOrdinateModule
	 * @generated
	 */
	EClass getAxisOrdinateModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisOrdinateModule#getAxis_ordinates <em>Axis ordinates</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Axis ordinates</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.AxisOrdinateModule#getAxis_ordinates()
	 * @see #getAxisOrdinateModule()
	 * @generated
	 */
	EReference getAxisOrdinateModule_Axis_ordinates();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CellPositionModule <em>Cell Position Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cell Position Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CellPositionModule
	 * @generated
	 */
	EClass getCellPositionModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CellPositionModule#getReportTables <em>Report Tables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Tables</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.CellPositionModule#getReportTables()
	 * @see #getCellPositionModule()
	 * @generated
	 */
	EReference getCellPositionModule_ReportTables();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.OrdinateItemModule <em>Ordinate Item Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Ordinate Item Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.OrdinateItemModule
	 * @generated
	 */
	EClass getOrdinateItemModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.OrdinateItemModule#getOrdinateItems <em>Ordinate Items</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Ordinate Items</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.OrdinateItemModule#getOrdinateItems()
	 * @see #getOrdinateItemModule()
	 * @generated
	 */
	EReference getOrdinateItemModule_OrdinateItems();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell <em>Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell
	 * @generated
	 */
	EClass getReportCell();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell#getCombination <em>Combination</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Combination</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell#getCombination()
	 * @see #getReportCell()
	 * @generated
	 */
	EReference getReportCell_Combination();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCell#getName()
	 * @see #getReportCell()
	 * @generated
	 */
	EAttribute getReportCell_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue <em>Report Cell Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Cell Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue
	 * @generated
	 */
	EClass getReportCellValue();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue#getReportCell <em>Report Cell</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Report Cell</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue#getReportCell()
	 * @see #getReportCellValue()
	 * @generated
	 */
	EReference getReportCellValue_ReportCell();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportCellValue#getValue()
	 * @see #getReportCellValue()
	 * @generated
	 */
	EAttribute getReportCellValue_Value();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportTableModule <em>Report Table Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Report Table Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportTableModule
	 * @generated
	 */
	EClass getReportTableModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportTableModule#getReportTables <em>Report Tables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Report Tables</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportTableModule#getReportTables()
	 * @see #getReportTableModule()
	 * @generated
	 */
	EReference getReportTableModule_ReportTables();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.TableCellModule <em>Table Cell Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Table Cell Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.TableCellModule
	 * @generated
	 */
	EClass getTableCellModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.TableCellModule#getTableCells <em>Table Cells</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Table Cells</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.TableCellModule#getTableCells()
	 * @see #getTableCellModule()
	 * @generated
	 */
	EReference getTableCellModule_TableCells();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Cocalimo_smcubes_extra_extensionFactory getCocalimo_smcubes_extra_extensionFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.SmcubesExtraModelImpl <em>Smcubes Extra Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.SmcubesExtraModelImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getSmcubesExtraModel()
		 * @generated
		 */
		EClass SMCUBES_EXTRA_MODEL = eINSTANCE.getSmcubesExtraModel();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute SMCUBES_EXTRA_MODEL__NAME = eINSTANCE.getSmcubesExtraModel_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CombinationModuleImpl <em>Combination Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CombinationModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCombinationModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeModuleImpl <em>Cube Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCubeModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MappingDefinitionModuleImpl <em>Mapping Definition Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MappingDefinitionModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getMappingDefinitionModule()
		 * @generated
		 */
		EClass MAPPING_DEFINITION_MODULE = eINSTANCE.getMappingDefinitionModule();

		/**
		 * The meta object literal for the '<em><b>Mappings</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MAPPING_DEFINITION_MODULE__MAPPINGS = eINSTANCE.getMappingDefinitionModule_Mappings();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeMappingModuleImpl <em>Cube Mapping Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CubeMappingModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCubeMappingModule()
		 * @generated
		 */
		EClass CUBE_MAPPING_MODULE = eINSTANCE.getCubeMappingModule();

		/**
		 * The meta object literal for the '<em><b>Cube Mappings</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_MAPPING_MODULE__CUBE_MAPPINGS = eINSTANCE.getCubeMappingModule_CubeMappings();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MemberMappingModuleImpl <em>Member Mapping Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.MemberMappingModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getMemberMappingModule()
		 * @generated
		 */
		EClass MEMBER_MAPPING_MODULE = eINSTANCE.getMemberMappingModule();

		/**
		 * The meta object literal for the '<em><b>Member Mappings</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_MAPPING_MODULE__MEMBER_MAPPINGS = eINSTANCE.getMemberMappingModule_MemberMappings();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.VariableMappingModuleImpl <em>Variable Mapping Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.VariableMappingModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getVariableMappingModule()
		 * @generated
		 */
		EClass VARIABLE_MAPPING_MODULE = eINSTANCE.getVariableMappingModule();

		/**
		 * The meta object literal for the '<em><b>Variable Mappings</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VARIABLE_MAPPING_MODULE__VARIABLE_MAPPINGS = eINSTANCE.getVariableMappingModule_VariableMappings();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisModuleImpl <em>Axis Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getAxisModule()
		 * @generated
		 */
		EClass AXIS_MODULE = eINSTANCE.getAxisModule();

		/**
		 * The meta object literal for the '<em><b>Axes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AXIS_MODULE__AXES = eINSTANCE.getAxisModule_Axes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisOrdinateModuleImpl <em>Axis Ordinate Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.AxisOrdinateModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getAxisOrdinateModule()
		 * @generated
		 */
		EClass AXIS_ORDINATE_MODULE = eINSTANCE.getAxisOrdinateModule();

		/**
		 * The meta object literal for the '<em><b>Axis ordinates</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AXIS_ORDINATE_MODULE__AXIS_ORDINATES = eINSTANCE.getAxisOrdinateModule_Axis_ordinates();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CellPositionModuleImpl <em>Cell Position Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.CellPositionModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getCellPositionModule()
		 * @generated
		 */
		EClass CELL_POSITION_MODULE = eINSTANCE.getCellPositionModule();

		/**
		 * The meta object literal for the '<em><b>Report Tables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CELL_POSITION_MODULE__REPORT_TABLES = eINSTANCE.getCellPositionModule_ReportTables();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.OrdinateItemModuleImpl <em>Ordinate Item Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.OrdinateItemModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getOrdinateItemModule()
		 * @generated
		 */
		EClass ORDINATE_ITEM_MODULE = eINSTANCE.getOrdinateItemModule();

		/**
		 * The meta object literal for the '<em><b>Ordinate Items</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ORDINATE_ITEM_MODULE__ORDINATE_ITEMS = eINSTANCE.getOrdinateItemModule_OrdinateItems();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellImpl <em>Report Cell</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getReportCell()
		 * @generated
		 */
		EClass REPORT_CELL = eINSTANCE.getReportCell();

		/**
		 * The meta object literal for the '<em><b>Combination</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REPORT_CELL__COMBINATION = eINSTANCE.getReportCell_Combination();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REPORT_CELL__NAME = eINSTANCE.getReportCell_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellValueImpl <em>Report Cell Value</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportCellValueImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getReportCellValue()
		 * @generated
		 */
		EClass REPORT_CELL_VALUE = eINSTANCE.getReportCellValue();

		/**
		 * The meta object literal for the '<em><b>Report Cell</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REPORT_CELL_VALUE__REPORT_CELL = eINSTANCE.getReportCellValue_ReportCell();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute REPORT_CELL_VALUE__VALUE = eINSTANCE.getReportCellValue_Value();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportTableModuleImpl <em>Report Table Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.ReportTableModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getReportTableModule()
		 * @generated
		 */
		EClass REPORT_TABLE_MODULE = eINSTANCE.getReportTableModule();

		/**
		 * The meta object literal for the '<em><b>Report Tables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference REPORT_TABLE_MODULE__REPORT_TABLES = eINSTANCE.getReportTableModule_ReportTables();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.TableCellModuleImpl <em>Table Cell Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.TableCellModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.impl.Cocalimo_smcubes_extra_extensionPackageImpl#getTableCellModule()
		 * @generated
		 */
		EClass TABLE_CELL_MODULE = eINSTANCE.getTableCellModule();

		/**
		 * The meta object literal for the '<em><b>Table Cells</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TABLE_CELL_MODULE__TABLE_CELLS = eINSTANCE.getTableCellModule_TableCells();

	}

} //Cocalimo_smcubes_extra_extensionPackage
